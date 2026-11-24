package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

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
			raw: buildV2Request("fetch", []string{"agent=git/2.47.0", "object-format=sha1"}, []string{
				"thin-pack",
				"ofs-delta",
				"want " + oid1,
				"done",
			}),
			want: true,
		},
		{
			name: "incremental fetch with have line",
			raw: buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want " + oid1,
				"have " + oid2,
				"done",
			}),
			want: false,
		},
		{
			name: "fetch missing done",
			raw: buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want " + oid1,
			}),
			want: false,
		},
		{
			name: "fetch with want-ref (mutable ref)",
			raw: buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
				"want-ref refs/heads/main",
				"done",
			}),
			want: false,
		},
		{
			name: "fetch with no want lines",
			raw:  buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{"done"}),
			want: false,
		},
		{
			name: "ls-refs is not a fetch",
			raw:  buildV2Request("ls-refs", nil, []string{"symrefs"}),
			want: false,
		},
		{
			name: "truncated missing flush",
			raw:  append(EncodePktLine([]byte("command=fetch\n")), []byte("0001")...),
			want: false,
		},
		{
			name: "trailing bytes after flush",
			raw: append(buildV2Request("fetch", nil, []string{"want " + oid1, "done"}),
				FlushPkt...),
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCacheableV2FullCloneFetch(tc.raw); got != tc.want {
				t.Errorf("isCacheableV2FullCloneFetch = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUploadPackResponseCache_LRUEvictionAndCaps(t *testing.T) {
	c := newUploadPackResponseCache(10, 6)

	// Entry exceeding maxEntryBytes is ignored.
	c.Put("too-big", []byte("1234567"))
	if _, ok := c.Get("too-big"); ok {
		t.Fatal("expected oversized entry not to be cached")
	}

	// Insert a (5 bytes) and b (5 bytes) -> total 10 bytes.
	c.Put("a", []byte("aaaaa"))
	c.Put("b", []byte("bbbbb"))
	if got, ok := c.Get("a"); !ok || string(got) != "aaaaa" {
		t.Fatalf("Get(a) = (%q, %v), want (aaaaa, true)", string(got), ok)
	}
	// Access order: a is MRU, b is LRU. Insert c (4 bytes) -> evicts b (total 9).
	c.Put("c", []byte("cccc"))
	if _, ok := c.Get("b"); ok {
		t.Fatal("expected LRU entry b to be evicted")
	}
	if got, ok := c.Get("a"); !ok || string(got) != "aaaaa" {
		t.Fatalf("Get(a) after eviction = (%q, %v), want (aaaaa, true)", string(got), ok)
	}
	if got, ok := c.Get("c"); !ok || string(got) != "cccc" {
		t.Fatalf("Get(c) = (%q, %v), want (cccc, true)", string(got), ok)
	}
}

func TestDefaultPackCacheCaps(t *testing.T) {
	if defaultPackCacheMaxBytes != 64<<20 {
		t.Fatalf("defaultPackCacheMaxBytes = %d, want %d (64 MiB)", defaultPackCacheMaxBytes, 64<<20)
	}
	if defaultPackCacheMaxEntryBytes != 8<<20 {
		t.Fatalf("defaultPackCacheMaxEntryBytes = %d, want %d (8 MiB)", defaultPackCacheMaxEntryBytes, 8<<20)
	}

	c := newUploadPackResponseCache(defaultPackCacheMaxBytes, defaultPackCacheMaxEntryBytes)
	if c.maxBytes != 64<<20 || c.maxEntryBytes != 8<<20 {
		t.Fatalf("cache caps = (%d, %d), want (%d, %d)", c.maxBytes, c.maxEntryBytes, 64<<20, 8<<20)
	}

	oversized := make([]byte, defaultPackCacheMaxEntryBytes+1)
	c.Put("oversized", oversized)
	if _, ok := c.Get("oversized"); ok {
		t.Fatal("expected entry > 8 MiB not to be cached")
	}

	atCap := make([]byte, defaultPackCacheMaxEntryBytes)
	c.Put("at-cap", atCap)
	if got, ok := c.Get("at-cap"); !ok || len(got) != defaultPackCacheMaxEntryBytes {
		t.Fatalf("Get(at-cap) = (len=%d, ok=%v), want (len=%d, ok=true)", len(got), ok, defaultPackCacheMaxEntryBytes)
	}
}

func TestServer_UploadPackResponseCache_HitCoalesceBypassAndCap(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "pack-cache")
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	nvme, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: newFakePackFetcher(),
		Cache: nvme,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	var execCalls atomic.Int32
	entered := make(chan struct{}, 1)
	release := make(chan struct{})

	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, req gitexec.UploadPackRequest) error {
			n := execCalls.Add(1)
			inBytes, _ := io.ReadAll(req.Stdin)
			if n == 1 {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
			if bytes.Contains(inBytes, []byte("oversized")) {
				_, _ = req.Stdout.Write(bytes.Repeat([]byte("X"), 64))
				return nil
			}
			_, _ = req.Stdout.Write([]byte("PACK-RESPONSE"))
			return nil
		},
	}

	srv, err := NewServer(ServerConfig{
		Materializer:   mat,
		GitExecutor:    execImpl,
		MaxUploadPacks: 1,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	// Small cap to test oversized response streaming without caching.
	srv.packCache = newUploadPackResponseCache(128, 32)

	client := startBufconnNode(t, srv)

	doFetch := func(payload []byte) (string, error) {
		outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
		st, err := client.OpenStream(outCtx, rpc.RequestMeta{
			TenantID:    rk.TenantID,
			RepoID:      rk.RepoID,
			Operation:   rpc.OpUploadPack,
			GitProtocol: "version=2",
		})
		if err != nil {
			return "", err
		}
		if err := st.Send(&rpc.Frame{Data: payload, EOF: true}); err != nil {
			return "", err
		}
		var resp bytes.Buffer
		for {
			f, rErr := st.Recv()
			if errors.Is(rErr, io.EOF) {
				break
			}
			if rErr != nil {
				return "", rErr
			}
			resp.Write(f.Data)
		}
		return resp.String(), nil
	}

	oid1 := strings.Repeat("a", 40)
	fullCloneReq := buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"thin-pack",
		"want " + oid1,
		"done",
	})

	// 1. Concurrent identical full-clone requests coalesce via singleflight -> 1 ExecUploadPack.
	const callers = 4
	results := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx], errs[idx] = doFetch(fullCloneReq)
		}(i)
	}

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for leader ExecUploadPack")
	}
	// Let followers queue on singleflight before releasing leader.
	time.Sleep(30 * time.Millisecond)
	close(release)
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d error: %v", i, errs[i])
		}
		if results[i] != "PACK-RESPONSE" {
			t.Fatalf("caller %d resp = %q, want PACK-RESPONSE", i, results[i])
		}
	}
	if got := execCalls.Load(); got != 1 {
		t.Fatalf("execCalls after %d concurrent identical fetches = %d, want 1", callers, got)
	}

	// 2. Subsequent repeat full-clone hits cache directly (0 GetMeta / 0 SyncAndPin / 0 ExecUploadPack).
	metaCallsBefore := metaStore.GetMetaCalls()
	gotRepeat, err := doFetch(fullCloneReq)
	if err != nil || gotRepeat != "PACK-RESPONSE" {
		t.Fatalf("repeat fetch = (%q, %v), want (PACK-RESPONSE, nil)", gotRepeat, err)
	}
	if execCalls.Load() != 1 {
		t.Fatalf("execCalls after cache hit = %d, want 1", execCalls.Load())
	}
	if metaStore.GetMetaCalls() != metaCallsBefore {
		t.Fatalf("GetMetaCalls increased on cache hit: %d -> %d", metaCallsBefore, metaStore.GetMetaCalls())
	}

	// 3. Request with `have` line bypasses cache and runs ExecUploadPack every time.
	haveReq := buildV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"want " + oid1,
		"have " + strings.Repeat("b", 40),
		"done",
	})
	if _, err := doFetch(haveReq); err != nil {
		t.Fatalf("haveReq 1: %v", err)
	}
	if _, err := doFetch(haveReq); err != nil {
		t.Fatalf("haveReq 2: %v", err)
	}
	if execCalls.Load() != 3 {
		t.Fatalf("execCalls after 2 have fetches = %d, want 3", execCalls.Load())
	}

	// 4. Oversized response (> maxEntryBytes=32) streams completely without caching.
	oversizedReq := buildV2Request("fetch", []string{"agent=oversized"}, []string{
		"want " + oid1,
		"done",
	})
	gotOver1, err := doFetch(oversizedReq)
	if err != nil || len(gotOver1) != 64 {
		t.Fatalf("oversized 1: len=%d err=%v, want len=64", len(gotOver1), err)
	}
	gotOver2, err := doFetch(oversizedReq)
	if err != nil || len(gotOver2) != 64 {
		t.Fatalf("oversized 2: len=%d err=%v, want len=64", len(gotOver2), err)
	}
	if execCalls.Load() != 5 {
		t.Fatalf("execCalls after 2 oversized fetches = %d, want 5", execCalls.Load())
	}
}
