package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

func buildV2Request(cmd string, caps []string, args []string) []byte {
	var buf bytes.Buffer
	buf.Write(EncodePktLine([]byte("command=" + cmd + "\n")))
	for _, c := range caps {
		buf.Write(EncodePktLine([]byte(c + "\n")))
	}
	if args != nil {
		buf.WriteString("0001")
		for _, a := range args {
			buf.Write(EncodePktLine([]byte(a + "\n")))
		}
	}
	buf.Write(FlushPkt)
	return buf.Bytes()
}

func TestParseV2LsRefsRequest(t *testing.T) {
	t.Run("all supported args", func(t *testing.T) {
		raw := buildV2Request(
			"ls-refs",
			[]string{"agent=git/2.47.0", "object-format=sha1"},
			[]string{"peel", "symrefs", "unborn", "ref-prefix HEAD", "ref-prefix refs/heads/"},
		)
		req, ok := parseV2LsRefsRequest(raw)
		if !ok {
			t.Fatalf("parseV2LsRefsRequest returned ok=false")
		}
		if !req.Peel || !req.Symrefs || !req.Unborn {
			t.Fatalf("got %+v, want Peel=true Symrefs=true Unborn=true", req)
		}
		if len(req.RefPrefixes) != 2 || req.RefPrefixes[0] != "HEAD" || req.RefPrefixes[1] != "refs/heads/" {
			t.Fatalf("RefPrefixes = %v, want [HEAD refs/heads/]", req.RefPrefixes)
		}
	})

	t.Run("no delim packet (no args)", func(t *testing.T) {
		raw := buildV2Request("ls-refs", []string{"agent=git/2.47.0"}, nil)
		req, ok := parseV2LsRefsRequest(raw)
		if !ok {
			t.Fatalf("parseV2LsRefsRequest returned ok=false")
		}
		if req.Peel || req.Symrefs || req.Unborn || len(req.RefPrefixes) != 0 {
			t.Fatalf("unexpected non-zero req: %+v", req)
		}
	})

	t.Run("rejects unrecognized arg", func(t *testing.T) {
		raw := buildV2Request("ls-refs", nil, []string{"symrefs", "future-arg"})
		if _, ok := parseV2LsRefsRequest(raw); ok {
			t.Fatal("expected ok=false for unrecognized arg")
		}
	})

	t.Run("rejects missing flush-pkt", func(t *testing.T) {
		raw := append(EncodePktLine([]byte("command=ls-refs\n")), []byte("0001")...)
		if _, ok := parseV2LsRefsRequest(raw); ok {
			t.Fatal("expected ok=false for missing flush-pkt")
		}
	})

	t.Run("rejects trailing bytes after flush-pkt", func(t *testing.T) {
		raw := append(buildV2Request("ls-refs", nil, []string{"symrefs"}), FlushPkt...)
		if _, ok := parseV2LsRefsRequest(raw); ok {
			t.Fatal("expected ok=false for trailing bytes after flush-pkt")
		}
	})

	t.Run("rejects malformed pkt-line", func(t *testing.T) {
		if _, ok := parseV2LsRefsRequest([]byte("zzzz")); ok {
			t.Fatal("expected ok=false for malformed hex length")
		}
	})
}

func TestEncodeV2LsRefsResponse(t *testing.T) {
	mainOID := strings.Repeat("a", 40)
	devOID := strings.Repeat("b", 40)
	tagOID := strings.Repeat("c", 40)
	refs := []ddb.RefState{
		{RefName: "refs/heads/dev", OID: devOID},
		{RefName: "refs/heads/main", OID: mainOID},
		{RefName: "refs/tags/v1.0", OID: tagOID},
	}

	t.Run("full listing with symrefs", func(t *testing.T) {
		out, fallback := encodeV2LsRefsResponse(refs, "refs/heads/main", v2LsRefsRequest{Symrefs: true})
		if fallback {
			t.Fatal("unexpected fallback")
		}
		want := string(EncodePktLine([]byte(mainOID+" HEAD symref-target:refs/heads/main\n"))) +
			string(EncodePktLine([]byte(devOID+" refs/heads/dev\n"))) +
			string(EncodePktLine([]byte(mainOID+" refs/heads/main\n"))) +
			string(EncodePktLine([]byte(tagOID+" refs/tags/v1.0\n"))) +
			"0000"
		if string(out) != want {
			t.Fatalf("got:\n%q\nwant:\n%q", string(out), want)
		}
	})

	t.Run("multiple ref-prefix filters with peel excluding tags", func(t *testing.T) {
		out, fallback := encodeV2LsRefsResponse(refs, "refs/heads/main", v2LsRefsRequest{
			Peel:        true,
			Symrefs:     true,
			RefPrefixes: []string{"HEAD", "refs/heads/"},
		})
		if fallback {
			t.Fatal("unexpected fallback when no matching ref is under refs/tags/")
		}
		want := string(EncodePktLine([]byte(mainOID+" HEAD symref-target:refs/heads/main\n"))) +
			string(EncodePktLine([]byte(devOID+" refs/heads/dev\n"))) +
			string(EncodePktLine([]byte(mainOID+" refs/heads/main\n"))) +
			"0000"
		if string(out) != want {
			t.Fatalf("got:\n%q\nwant:\n%q", string(out), want)
		}
	})

	t.Run("peel with matching tag triggers fallback", func(t *testing.T) {
		_, fallback := encodeV2LsRefsResponse(refs, "refs/heads/main", v2LsRefsRequest{
			Peel:        true,
			RefPrefixes: []string{"refs/tags/"},
		})
		if !fallback {
			t.Fatal("expected fallback=true when peel is requested and refs/tags/ matches")
		}
	})

	t.Run("empty repo with unborn and symrefs", func(t *testing.T) {
		out, fallback := encodeV2LsRefsResponse(nil, "", v2LsRefsRequest{
			Symrefs: true,
			Unborn:  true,
		})
		if fallback {
			t.Fatal("unexpected fallback")
		}
		want := string(EncodePktLine([]byte("unborn HEAD symref-target:refs/heads/main\n"))) + "0000"
		if string(out) != want {
			t.Fatalf("got %q, want %q", string(out), want)
		}
	})

	t.Run("empty repo without unborn emits flush only", func(t *testing.T) {
		out, fallback := encodeV2LsRefsResponse(nil, "refs/heads/main", v2LsRefsRequest{Symrefs: true})
		if fallback {
			t.Fatal("unexpected fallback")
		}
		if string(out) != "0000" {
			t.Fatalf("got %q, want 0000", string(out))
		}
	})
}

func TestServer_PureGoV2LsRefsFastPathAndTagPeelFallback(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "pure-lsrefs")
	mainOID, pBytes, iBytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "hello\n", "commit\n")
	packID := strings.Repeat("1", 64)
	tagOID := mainOID

	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
		RefVersion:     1,
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5LR1", PackID: packID, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: mainOID},
		{Repo: rk, RefName: "refs/tags/v1", OID: tagOID},
	})
	packStore := newFakePackFetcher()
	packStore.Put(packID, pBytes, iBytes)

	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: packStore,
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	var execCalls atomic.Int32
	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, req gitexec.UploadPackRequest) error {
			execCalls.Add(1)
			_, _ = io.ReadAll(req.Stdin)
			_, _ = req.Stdout.Write(EncodePktLine([]byte(tagOID + " refs/tags/v1 peeled:" + mainOID + "\n")))
			_, _ = req.Stdout.Write(FlushPkt)
			return nil
		},
	}

	srv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  execImpl,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	sendV2 := func(payload []byte) string {
		t.Helper()
		outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
		st, err := client.OpenStream(outCtx, rpc.RequestMeta{
			TenantID:    rk.TenantID,
			RepoID:      rk.RepoID,
			Operation:   rpc.OpUploadPack,
			GitProtocol: "version=2",
		})
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if err := st.Send(&rpc.Frame{Data: payload, EOF: true}); err != nil {
			t.Fatalf("Send: %v", err)
		}
		var resp bytes.Buffer
		for {
			f, rErr := st.Recv()
			if errors.Is(rErr, io.EOF) {
				break
			}
			if rErr != nil {
				t.Fatalf("Recv: %v", rErr)
			}
			resp.Write(f.Data)
		}
		return resp.String()
	}

	// 1. Pure-Go fast path: symrefs + ref-prefix HEAD + refs/heads/ (no tags matched).
	fastReq := buildV2Request("ls-refs", []string{"agent=git/2.47.0"}, []string{"peel", "symrefs", "ref-prefix HEAD", "ref-prefix refs/heads/"})
	gotFast := sendV2(fastReq)
	wantFast := string(EncodePktLine([]byte(mainOID+" HEAD symref-target:refs/heads/main\n"))) +
		string(EncodePktLine([]byte(mainOID+" refs/heads/main\n"))) + "0000"
	if gotFast != wantFast {
		t.Fatalf("fast path response = %q, want %q", gotFast, wantFast)
	}
	if execCalls.Load() != 0 {
		t.Fatalf("execCalls = %d, want 0 for pure-Go ls-refs", execCalls.Load())
	}
	// Verify repo was never materialized on disk (0 SyncAndPin).
	if _, pinErr := c.Pin(rk); pinErr == nil {
		t.Fatal("expected repo not to be materialized on disk by pure-Go ls-refs")
	}

	// 2. Fallback path: peel + refs/tags/ matches refs/tags/v1 -> runs git upload-pack.
	fallbackReq := buildV2Request("ls-refs", []string{"agent=git/2.47.0"}, []string{"peel", "ref-prefix refs/tags/"})
	gotFallback := sendV2(fallbackReq)
	if !strings.Contains(gotFallback, "peeled:"+mainOID) {
		t.Fatalf("fallback response = %q, want peeled OID", gotFallback)
	}
	if execCalls.Load() != 1 {
		t.Fatalf("execCalls = %d, want 1 after peel+tag fallback", execCalls.Load())
	}

	// 3. NotFound repo returns gRPC NotFound.
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	st, err := client.OpenStream(outCtx, rpc.RequestMeta{
		TenantID:    "acme",
		RepoID:      "missing-repo",
		Operation:   rpc.OpUploadPack,
		GitProtocol: "version=2",
	})
	if err != nil {
		t.Fatalf("OpenStream missing: %v", err)
	}
	_ = st.Send(&rpc.Frame{Data: fastReq, EOF: true})
	_, rErr := st.Recv()
	if status.Code(rErr) != codes.NotFound {
		t.Fatalf("missing repo status = %v, want NotFound", status.Code(rErr))
	}
}
