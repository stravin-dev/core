package edge

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func buildTestV2Request(cmd string, caps []string, args []string) []byte {
	var buf bytes.Buffer
	buf.WriteString(pktLine("command=" + cmd + "\n"))
	for _, c := range caps {
		buf.WriteString(pktLine(c + "\n"))
	}
	if args != nil {
		buf.WriteString("0001")
		for _, a := range args {
			buf.WriteString(pktLine(a + "\n"))
		}
	}
	buf.WriteString("0000")
	return buf.Bytes()
}

func TestIsCacheableV2FullCloneFetch(t *testing.T) {
	oid1 := strings.Repeat("a", 40)
	oid2 := strings.Repeat("b", 40)

	tests := []struct {
		name string
		raw  []byte
		want bool
	}{
		{
			name: "full clone no-have fetch with done",
			raw: buildTestV2Request("fetch", []string{"agent=git/2.47.0", "object-format=sha1"}, []string{
				"thin-pack",
				"ofs-delta",
				"want " + oid1,
				"done",
			}),
			want: true,
		},
		{
			name: "incremental fetch with have line",
			raw: buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want " + oid1,
				"have " + oid2,
				"done",
			}),
			want: false,
		},
		{
			name: "fetch missing done",
			raw: buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want " + oid1,
			}),
			want: false,
		},
		{
			name: "fetch with want-ref",
			raw: buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want-ref refs/heads/main",
				"done",
			}),
			want: false,
		},
		{
			name: "fetch with no want lines",
			raw:  buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{"done"}),
			want: false,
		},
		{
			name: "ls-refs is not a fetch",
			raw:  buildTestV2Request("ls-refs", nil, []string{"symrefs"}),
			want: false,
		},
		{
			name: "truncated missing flush",
			raw:  append([]byte(pktLine("command=fetch\n")), []byte("0001")...),
			want: false,
		},
		{
			name: "trailing bytes after flush",
			raw:  append(buildTestV2Request("fetch", nil, []string{"want " + oid1, "done"}), []byte("0000")...),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsCacheableV2FullCloneFetch(tc.raw); got != tc.want {
				t.Errorf("IsCacheableV2FullCloneFetch = %v, want %v", got, tc.want)
			}
		})
	}
}

// --- Local TCP RESP2 mock Valkey server for tests ---

type fakeValkeyEntry struct {
	data      []byte
	expiresAt time.Time
	ttlSecs   int64
}

type fakeValkeyServer struct {
	ln      net.Listener
	mu      sync.Mutex
	entries map[string]fakeValkeyEntry
	now     func() time.Time
	wg      sync.WaitGroup
}

func startFakeValkeyServer(t *testing.T, nowFn func() time.Time) *fakeValkeyServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen fake valkey: %v", err)
	}
	if nowFn == nil {
		nowFn = time.Now
	}
	s := &fakeValkeyServer{
		ln:      ln,
		entries: make(map[string]fakeValkeyEntry),
		now:     nowFn,
	}
	s.wg.Add(1)
	go s.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *fakeValkeyServer) Addr() string {
	return s.ln.Addr().String()
}

func (s *fakeValkeyServer) serve() {
	defer s.wg.Done()
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			defer func() { _ = c.Close() }()
			s.handleConn(c)
		}(conn)
	}
}

func (s *fakeValkeyServer) handleConn(conn net.Conn) {
	br := bufio.NewReader(conn)
	for {
		args, err := readRESPArray(br)
		if err != nil {
			return
		}
		if len(args) == 0 {
			return
		}
		cmd := strings.ToUpper(string(args[0]))
		switch cmd {
		case "GET":
			if len(args) != 2 {
				_, _ = io.WriteString(conn, "-ERR wrong number of arguments\r\n")
				continue
			}
			key := string(args[1])
			s.mu.Lock()
			entry, ok := s.entries[key]
			if ok && !entry.expiresAt.IsZero() && s.now().After(entry.expiresAt) {
				delete(s.entries, key)
				ok = false
			}
			s.mu.Unlock()
			if !ok {
				_, _ = io.WriteString(conn, "$-1\r\n")
				continue
			}
			_, _ = fmt.Fprintf(conn, "$%d\r\n", len(entry.data))
			_, _ = conn.Write(entry.data)
			_, _ = io.WriteString(conn, "\r\n")
		case "SET":
			if len(args) < 3 {
				_, _ = io.WriteString(conn, "-ERR wrong number of arguments\r\n")
				continue
			}
			key := string(args[1])
			val := append([]byte(nil), args[2]...)
			var (
				exp     time.Time
				ttlSecs int64
			)
			if len(args) == 5 && strings.ToUpper(string(args[3])) == "EX" {
				secs, parseErr := strconv.ParseInt(string(args[4]), 10, 64)
				if parseErr != nil || secs <= 0 {
					_, _ = io.WriteString(conn, "-ERR invalid expire time\r\n")
					continue
				}
				ttlSecs = secs
				exp = s.now().Add(time.Duration(secs) * time.Second)
			}
			s.mu.Lock()
			s.entries[key] = fakeValkeyEntry{data: val, expiresAt: exp, ttlSecs: ttlSecs}
			s.mu.Unlock()
			_, _ = io.WriteString(conn, "+OK\r\n")
		default:
			_, _ = io.WriteString(conn, "-ERR unknown command\r\n")
		}
	}
}

func readRESPArray(br *bufio.Reader) ([][]byte, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected *, got %q", line)
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil || count < 0 {
		return nil, fmt.Errorf("invalid array count %q", line)
	}
	args := make([][]byte, 0, count)
	for range count {
		hdr, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimSuffix(strings.TrimSuffix(hdr, "\n"), "\r")
		if !strings.HasPrefix(hdr, "$") {
			return nil, fmt.Errorf("expected $, got %q", hdr)
		}
		n, err := strconv.Atoi(hdr[1:])
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid bulk len %q", hdr)
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, err
		}
		args = append(args, buf[:n])
	}
	return args, nil
}

// --- In-memory S3 API fake for S3PackCache tests ---

type fakeS3Obj struct {
	data         []byte
	lastModified time.Time
}

type fakePackCacheS3API struct {
	mu       sync.Mutex
	objects  map[string]fakeS3Obj
	now      func() time.Time
	putCalls atomic.Int32
	getCalls atomic.Int32
	failAll  bool
}

var _ s3.API = (*fakePackCacheS3API)(nil)

func newFakePackCacheS3API(nowFn func() time.Time) *fakePackCacheS3API {
	if nowFn == nil {
		nowFn = time.Now
	}
	return &fakePackCacheS3API{
		objects: make(map[string]fakeS3Obj),
		now:     nowFn,
	}
}

func (f *fakePackCacheS3API) PutObject(_ context.Context, params *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	f.putCalls.Add(1)
	if f.failAll {
		return nil, errors.New("s3 unavailable")
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.objects[aws.ToString(params.Key)] = fakeS3Obj{data: data, lastModified: f.now()}
	f.mu.Unlock()
	return &awss3.PutObjectOutput{}, nil
}

func (f *fakePackCacheS3API) GetObject(_ context.Context, params *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.getCalls.Add(1)
	if f.failAll {
		return nil, errors.New("s3 unavailable")
	}
	f.mu.Lock()
	obj, ok := f.objects[aws.ToString(params.Key)]
	f.mu.Unlock()
	if !ok {
		return nil, &types.NoSuchKey{Message: aws.String("not found")}
	}
	lm := obj.lastModified
	return &awss3.GetObjectOutput{
		Body:         io.NopCloser(bytes.NewReader(obj.data)),
		LastModified: &lm,
	}, nil
}

func (f *fakePackCacheS3API) HeadObject(context.Context, *awss3.HeadObjectInput, ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	return nil, errors.New("not implemented")
}

func (f *fakePackCacheS3API) DeleteObject(context.Context, *awss3.DeleteObjectInput, ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	return nil, errors.New("not implemented")
}

func (f *fakePackCacheS3API) ListObjectsV2(context.Context, *awss3.ListObjectsV2Input, ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	return nil, errors.New("not implemented")
}

func TestValkeyPackCache_GetSetTTLAndSizeCap(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	srv := startFakeValkeyServer(t, clk.Now)

	vc, err := NewValkeyPackCache(ValkeyPackCacheConfig{
		Addr:          srv.Addr(),
		MaxEntryBytes: 32,
	})
	if err != nil {
		t.Fatalf("NewValkeyPackCache: %v", err)
	}
	t.Cleanup(func() { _ = vc.Close() })

	ctx := context.Background()
	if _, hit, err := vc.Get(ctx, "missing"); err != nil || hit {
		t.Fatalf("Get(missing) = (hit=%v, err=%v), want (false, nil)", hit, err)
	}

	// Binary payload containing NUL and CRLF verifies binary-safe RESP2 framing.
	payload := []byte("PACK\x00\x02\r\nhello\r\nend")
	if err := vc.Set(ctx, "k1", payload, 5*time.Minute); err != nil {
		t.Fatalf("Set(k1): %v", err)
	}
	got, hit, err := vc.Get(ctx, "k1")
	if err != nil || !hit || !bytes.Equal(got, payload) {
		t.Fatalf("Get(k1) = (%q, %v, %v), want (%q, true, nil)", got, hit, err, payload)
	}

	// Verify EX TTL was passed to Valkey and expires cleanly.
	srv.mu.Lock()
	entry := srv.entries[defaultValkeyKeyPrefix+"k1"]
	srv.mu.Unlock()
	if entry.ttlSecs != 300 {
		t.Fatalf("valkey EX = %d, want 300", entry.ttlSecs)
	}

	clk.Advance(6 * time.Minute)
	if _, hit, err := vc.Get(ctx, "k1"); err != nil || hit {
		t.Fatalf("Get(k1) after TTL = (hit=%v, err=%v), want (false, nil)", hit, err)
	}

	// Oversized entry rejected without writing.
	oversized := bytes.Repeat([]byte("A"), 33)
	if err := vc.Set(ctx, "big", oversized, 5*time.Minute); !errors.Is(err, ErrPackCacheEntryTooLarge) {
		t.Fatalf("Set(big) err = %v, want ErrPackCacheEntryTooLarge", err)
	}
}

func TestS3PackCache_GetSetTTLAndSizeCap(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	fakeS3 := newFakePackCacheS3API(clk.Now)

	sc, err := NewS3PackCache(S3PackCacheConfig{
		Client:        fakeS3,
		Bucket:        "git-objects-us-east-1",
		MaxEntryBytes: 64,
		TTL:           5 * time.Minute,
		Now:           clk.Now,
	})
	if err != nil {
		t.Fatalf("NewS3PackCache: %v", err)
	}

	ctx := context.Background()
	if _, hit, err := sc.Get(ctx, "abcdef"); err != nil || hit {
		t.Fatalf("Get(missing) = (hit=%v, err=%v), want (false, nil)", hit, err)
	}

	payload := bytes.Repeat([]byte("S"), 48)
	if err := sc.Set(ctx, "abcdef", payload, 5*time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, hit, err := sc.Get(ctx, "abcdef")
	if err != nil || !hit || !bytes.Equal(got, payload) {
		t.Fatalf("Get = (len=%d, hit=%v, err=%v), want (48, true, nil)", len(got), hit, err)
	}

	// Expire past TTL.
	clk.Advance(6 * time.Minute)
	if _, hit, err := sc.Get(ctx, "abcdef"); err != nil || hit {
		t.Fatalf("Get after TTL = (hit=%v, err=%v), want (false, nil)", hit, err)
	}

	// Oversized entry rejected.
	if err := sc.Set(ctx, "oversized", bytes.Repeat([]byte("Z"), 65), 5*time.Minute); !errors.Is(err, ErrPackCacheEntryTooLarge) {
		t.Fatalf("Set(oversized) err = %v, want ErrPackCacheEntryTooLarge", err)
	}
}

func TestTieredPackCache_ValkeyHotAndS3Spillover(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	valkeySrv := startFakeValkeyServer(t, clk.Now)
	vc, err := NewValkeyPackCache(ValkeyPackCacheConfig{
		Addr:          valkeySrv.Addr(),
		MaxEntryBytes: 32,
	})
	if err != nil {
		t.Fatalf("NewValkeyPackCache: %v", err)
	}
	t.Cleanup(func() { _ = vc.Close() })

	fakeS3 := newFakePackCacheS3API(clk.Now)
	sc, err := NewS3PackCache(S3PackCacheConfig{
		Client:        fakeS3,
		Bucket:        "git-objects-us-east-1",
		MaxEntryBytes: 128,
		TTL:           5 * time.Minute,
		Now:           clk.Now,
	})
	if err != nil {
		t.Fatalf("NewS3PackCache: %v", err)
	}

	tiered := NewTieredPackCache(TieredPackCacheConfig{
		Valkey:              vc,
		S3:                  sc,
		ValkeyMaxEntryBytes: 32,
		S3MaxEntryBytes:     128,
	})

	ctx := context.Background()

	// 1. Small entry (<= 32 B) goes to Valkey, not S3.
	small := []byte("small-pack-response")
	if err := tiered.Set(ctx, "smallkey", small, 5*time.Minute); err != nil {
		t.Fatalf("Set(small): %v", err)
	}
	if fakeS3.putCalls.Load() != 0 {
		t.Fatalf("S3 putCalls = %d, want 0 for small entry", fakeS3.putCalls.Load())
	}
	gotSmall, hit, err := tiered.Get(ctx, "smallkey")
	if err != nil || !hit || !bytes.Equal(gotSmall, small) {
		t.Fatalf("Get(small) = (%q, %v, %v), want (%q, true, nil)", gotSmall, hit, err, small)
	}

	// 2. Medium/large entry (> 32 B, <= 128 B) spills over to S3.
	large := bytes.Repeat([]byte("L"), 64)
	if err := tiered.Set(ctx, "largekey", large, 5*time.Minute); err != nil {
		t.Fatalf("Set(large): %v", err)
	}
	if fakeS3.putCalls.Load() != 1 {
		t.Fatalf("S3 putCalls = %d, want 1 for large spillover entry", fakeS3.putCalls.Load())
	}
	gotLarge, hit, err := tiered.Get(ctx, "largekey")
	if err != nil || !hit || !bytes.Equal(gotLarge, large) {
		t.Fatalf("Get(large) = (len=%d, hit=%v, err=%v), want (64, true, nil)", len(gotLarge), hit, err)
	}

	// 3. Entry > S3MaxEntryBytes (128 B) is skipped cleanly.
	huge := bytes.Repeat([]byte("H"), 200)
	if err := tiered.Set(ctx, "hugekey", huge, 5*time.Minute); err != nil {
		t.Fatalf("Set(huge): %v", err)
	}
	if fakeS3.putCalls.Load() != 1 {
		t.Fatalf("S3 putCalls = %d, want 1 (huge entry skipped)", fakeS3.putCalls.Load())
	}
}

type spyPackCache struct {
	inner    PackResponseCache
	getCalls atomic.Int32
	setCalls atomic.Int32
}

func (s *spyPackCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	s.getCalls.Add(1)
	return s.inner.Get(ctx, key)
}

func (s *spyPackCache) Set(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	s.setCalls.Add(1)
	return s.inner.Set(ctx, key, data, ttl)
}

type failingPackCache struct{}

func (failingPackCache) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, errors.New("cache backend unreachable")
}

func (failingPackCache) Set(context.Context, string, []byte, time.Duration) error {
	return errors.New("cache backend unreachable")
}

func TestHTTPHandler_PackCache_AuthBeforeCacheHit(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	valkeySrv := startFakeValkeyServer(t, clk.Now)
	vc, err := NewValkeyPackCache(ValkeyPackCacheConfig{
		Addr:          valkeySrv.Addr(),
		MaxEntryBytes: 1024,
	})
	if err != nil {
		t.Fatalf("NewValkeyPackCache: %v", err)
	}
	t.Cleanup(func() { _ = vc.Close() })

	spy := &spyPackCache{inner: vc}
	coord := NewPackCacheCoordinator(PackCacheCoordinatorConfig{
		Backend:         spy,
		TTL:             5 * time.Minute,
		MaxCaptureBytes: 1024,
	})
	env.handler.SetPackCache(coord)

	var upstreamCalls atomic.Int32
	env.node.onHandle = func(_ context.Context, _ rpc.RequestMeta, stream rpc.GitWireStream) error {
		upstreamCalls.Add(1)
		for {
			f, err := stream.Recv()
			if errors.Is(err, io.EOF) || (err == nil && f.EOF) {
				break
			}
			if err != nil {
				return err
			}
		}
		if err := stream.Send(&rpc.Frame{Data: []byte("SECRET-PRIVATE-PACK")}); err != nil {
			return err
		}
		return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
	}

	httpSrv := httptest.NewServer(env.handler)
	defer httpSrv.Close()

	fullCloneBody := buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"thin-pack",
		"want " + strings.Repeat("a", 40),
		"done",
	})

	doPost := func(authHeader string) (int, string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/acme/core.git/git-upload-pack", bytes.NewReader(fullCloneBody))
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		req.Header.Set("Git-Protocol", "version=2")
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("Do: %v", err)
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	// 1. Authorized acme caller warms the external cache.
	code, body := doPost("Bearer tok-acme")
	if code != http.StatusOK || body != "SECRET-PRIVATE-PACK" {
		t.Fatalf("warm request = (%d, %q), want (200, SECRET-PRIVATE-PACK)", code, body)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("upstreamCalls after warm = %d, want 1", upstreamCalls.Load())
	}

	// 2. Second authorized acme caller hits external cache (0 upstream calls).
	code, body = doPost("Bearer tok-acme")
	if code != http.StatusOK || body != "SECRET-PRIVATE-PACK" {
		t.Fatalf("cached request = (%d, %q), want (200, SECRET-PRIVATE-PACK)", code, body)
	}
	if upstreamCalls.Load() != 1 {
		t.Fatalf("upstreamCalls after cache hit = %d, want 1", upstreamCalls.Load())
	}

	// 3. CRITICAL SECURITY INVARIANT: Unauthorized / wrong-tenant callers MUST be rejected
	// BEFORE any cache lookup occurs (0 cache Get calls, never served cached pack).
	getsBefore := spy.getCalls.Load()

	// 3a. Missing Authorization header -> 401
	code, body = doPost("")
	if code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401", code)
	}
	if strings.Contains(body, "SECRET-PRIVATE-PACK") {
		t.Fatal("SECURITY VIOLATION: leaked cached pack to unauthenticated caller")
	}

	// 3b. Invalid token -> 401
	code, body = doPost("Bearer invalid-token")
	if code != http.StatusUnauthorized {
		t.Fatalf("invalid token status = %d, want 401", code)
	}
	if strings.Contains(body, "SECRET-PRIVATE-PACK") {
		t.Fatal("SECURITY VIOLATION: leaked cached pack to invalid token caller")
	}

	// 3c. Valid token belonging to a DIFFERENT tenant ("beta") -> 403
	code, body = doPost("Bearer tok-beta")
	if code != http.StatusForbidden {
		t.Fatalf("cross-tenant status = %d, want 403", code)
	}
	if strings.Contains(body, "SECRET-PRIVATE-PACK") {
		t.Fatal("SECURITY VIOLATION: leaked cached pack across tenants")
	}

	if spy.getCalls.Load() != getsBefore {
		t.Fatalf("SECURITY VIOLATION: cache Get called %d times on rejected requests (before=%d, after=%d)",
			spy.getCalls.Load()-getsBefore, getsBefore, spy.getCalls.Load())
	}
}

func TestHTTPHandler_PackCache_SingleflightCoalescingSpilloverAndGracefulFallback(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	valkeySrv := startFakeValkeyServer(t, clk.Now)
	vc, err := NewValkeyPackCache(ValkeyPackCacheConfig{
		Addr:          valkeySrv.Addr(),
		MaxEntryBytes: 32,
	})
	if err != nil {
		t.Fatalf("NewValkeyPackCache: %v", err)
	}
	t.Cleanup(func() { _ = vc.Close() })

	fakeS3 := newFakePackCacheS3API(clk.Now)
	sc, err := NewS3PackCache(S3PackCacheConfig{
		Client:        fakeS3,
		Bucket:        "git-objects-us-east-1",
		MaxEntryBytes: 64,
		TTL:           5 * time.Minute,
		Now:           clk.Now,
	})
	if err != nil {
		t.Fatalf("NewS3PackCache: %v", err)
	}

	tiered := NewTieredPackCache(TieredPackCacheConfig{
		Valkey:              vc,
		S3:                  sc,
		ValkeyMaxEntryBytes: 32,
		S3MaxEntryBytes:     64,
	})
	coord := NewPackCacheCoordinator(PackCacheCoordinatorConfig{
		Backend:         tiered,
		TTL:             5 * time.Minute,
		MaxCaptureBytes: tiered.MaxEntryBytes(),
	})
	env.handler.SetPackCache(coord)

	var upstreamCalls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	env.node.onHandle = func(_ context.Context, _ rpc.RequestMeta, stream rpc.GitWireStream) error {
		n := upstreamCalls.Add(1)
		var reqBuf bytes.Buffer
		for {
			f, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return err
			}
			reqBuf.Write(f.Data)
			if f.EOF {
				break
			}
		}
		if n == 1 {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		switch {
		case bytes.Contains(reqBuf.Bytes(), []byte("spill-s3")):
			_ = stream.Send(&rpc.Frame{Data: bytes.Repeat([]byte("S"), 48)})
		case bytes.Contains(reqBuf.Bytes(), []byte("too-large")):
			_ = stream.Send(&rpc.Frame{Data: bytes.Repeat([]byte("X"), 96)})
		default:
			_ = stream.Send(&rpc.Frame{Data: []byte("PACK-COALESCED")})
		}
		return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
	}

	httpSrv := httptest.NewServer(env.handler)
	defer httpSrv.Close()

	doFetch := func(payload []byte) (int, string, error) {
		req, err := http.NewRequest(http.MethodPost, httpSrv.URL+"/acme/core.git/git-upload-pack", bytes.NewReader(payload))
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("Authorization", "Bearer tok-acme")
		req.Header.Set("Git-Protocol", "version=2")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, "", err
		}
		defer func() { _ = resp.Body.Close() }()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), err
	}

	oid1 := strings.Repeat("a", 40)
	fullCloneReq := buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"thin-pack",
		"want " + oid1,
		"done",
	})

	// 1. 4 concurrent identical full-clone requests coalesce into 1 upstream call.
	const callers = 4
	codes := make([]int, callers)
	bodies := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			codes[idx], bodies[idx], errs[idx] = doFetch(fullCloneReq)
		}(i)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for singleflight leader")
	}
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	for i := range callers {
		if errs[i] != nil || codes[i] != http.StatusOK || bodies[i] != "PACK-COALESCED" {
			t.Fatalf("caller %d = (%d, %q, %v), want (200, PACK-COALESCED, nil)", i, codes[i], bodies[i], errs[i])
		}
	}
	if got := upstreamCalls.Load(); got != 1 {
		t.Fatalf("upstreamCalls after %d concurrent fetches = %d, want 1", callers, got)
	}

	// 2. Large response (48 B > Valkey 32 B cap, <= S3 64 B cap) spills to S3 and hits S3 on repeat.
	spillReq := buildTestV2Request("fetch", []string{"agent=spill-s3"}, []string{"want " + oid1, "done"})
	code, body, err := doFetch(spillReq)
	if err != nil || code != http.StatusOK || len(body) != 48 {
		t.Fatalf("spill 1 = (%d, len=%d, %v), want (200, 48, nil)", code, len(body), err)
	}
	if fakeS3.putCalls.Load() != 1 {
		t.Fatalf("S3 putCalls = %d, want 1", fakeS3.putCalls.Load())
	}
	code, body, err = doFetch(spillReq)
	if err != nil || code != http.StatusOK || len(body) != 48 {
		t.Fatalf("spill 2 = (%d, len=%d, %v), want (200, 48, nil)", code, len(body), err)
	}
	if upstreamCalls.Load() != 2 {
		t.Fatalf("upstreamCalls after S3 cache hit = %d, want 2", upstreamCalls.Load())
	}

	// 3. Oversized response (96 B > S3 64 B cap) streams completely without caching.
	overReq := buildTestV2Request("fetch", []string{"agent=too-large"}, []string{"want " + oid1, "done"})
	code, body, err = doFetch(overReq)
	if err != nil || code != http.StatusOK || len(body) != 96 {
		t.Fatalf("over 1 = (%d, len=%d, %v)", code, len(body), err)
	}
	code, body, err = doFetch(overReq)
	if err != nil || code != http.StatusOK || len(body) != 96 {
		t.Fatalf("over 2 = (%d, len=%d, %v)", code, len(body), err)
	}
	if upstreamCalls.Load() != 4 {
		t.Fatalf("upstreamCalls after 2 oversized fetches = %d, want 4", upstreamCalls.Load())
	}

	// 4. Incremental fetch (`have`) bypasses cache every time.
	haveReq := buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"want " + oid1,
		"have " + strings.Repeat("b", 40),
		"done",
	})
	_, _, _ = doFetch(haveReq)
	_, _, _ = doFetch(haveReq)
	if upstreamCalls.Load() != 6 {
		t.Fatalf("upstreamCalls after 2 have fetches = %d, want 6", upstreamCalls.Load())
	}

	// 5. Unreachable cache backend degrades gracefully and still serves 200 OK from upstream.
	env.handler.SetPackCache(NewPackCacheCoordinator(PackCacheCoordinatorConfig{
		Backend:         failingPackCache{},
		TTL:             time.Minute,
		MaxCaptureBytes: 1024,
	}))
	code, body, err = doFetch(fullCloneReq)
	if err != nil || code != http.StatusOK || body != "PACK-COALESCED" {
		t.Fatalf("fallback request = (%d, %q, %v), want (200, PACK-COALESCED, nil)", code, body, err)
	}
	if upstreamCalls.Load() != 7 {
		t.Fatalf("upstreamCalls after cache fallback = %d, want 7", upstreamCalls.Load())
	}
}

func TestUploadPackCacheKey_TenantIsolation(t *testing.T) {
	rk1, _ := tenant.NewRepoKey("acme", "app")
	rk2, _ := tenant.NewRepoKey("other", "app")
	req := buildTestV2Request("fetch", nil, []string{"want " + strings.Repeat("a", 40), "done"})
	k1 := UploadPackCacheKey(rk1, "version=2", req)
	k2 := UploadPackCacheKey(rk2, "version=2", req)
	if k1 == k2 {
		t.Fatalf("expected distinct cache keys across tenants, both = %s", k1)
	}
}
