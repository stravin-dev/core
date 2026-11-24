package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func startBufconnNode(t *testing.T, handler rpc.GitWireHandler) rpc.GitWireClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = lis.Close() })

	srv := grpc.NewServer()
	rpc.RegisterGitWireServer(srv, handler)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.WireCodec())),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return rpc.NewGRPCGitWireClient(cc)
}

func TestServer_AdvertiseRefsStrictlyFromDynamoDB(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "advert")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	// Plant a disk-only ref inside the cache dir that does NOT exist in DynamoDB.
	h, err := c.Materialize(ctx, rk, nil)
	if err != nil {
		t.Fatalf("cache.Materialize: %v", err)
	}
	diskOnlyOID := strings.Repeat("9", 40)
	if err := os.WriteFile(filepath.Join(h.Path(), "refs", "heads", "disk-only"), []byte(diskOnlyOID+"\n"), 0o600); err != nil {
		t.Fatalf("write disk-only ref: %v", err)
	}
	h.Release()

	ddbOID := strings.Repeat("a", 40)
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: ddbOID},
	})

	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     newFakePackFetcher(),
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	nodeSrv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
		Telemetry:    tp,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	client := startBufconnNode(t, nodeSrv)
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderAdvertiseRefs, "true")
	stream, err := client.OpenStream(outCtx, rpc.RequestMeta{
		TenantID:    rk.TenantID,
		RepoID:      rk.RepoID,
		Operation:   rpc.OpUploadPack,
		TraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	var buf bytes.Buffer
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		buf.Write(frame.Data)
		if frame.EOF {
			if frame.ExitCode != 0 {
				t.Fatalf("exit code = %d, want 0 (err=%q)", frame.ExitCode, frame.ErrMessage)
			}
			break
		}
	}

	advert := buf.String()
	if !strings.Contains(advert, ddbOID+" refs/heads/main\n") {
		t.Fatalf("advertisement missing DDB ref: %q", advert)
	}
	if strings.Contains(advert, "disk-only") {
		t.Fatalf("advertisement leaked local disk ref: %q", advert)
	}

	// Verify OTel spans recorded with propagated trace ID.
	spans := tp.SpanExporter.GetSpans()
	var sawQueryRefs bool
	for _, s := range spans {
		if s.Name == "ddb.query_refs" {
			sawQueryRefs = true
			if got := s.SpanContext.TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Errorf("ddb.query_refs trace_id = %s, want propagated trace_id", got)
			}
		}
	}
	if !sawQueryRefs {
		t.Fatal("expected ddb.query_refs span to be recorded")
	}
}

type stubWriteHandler struct {
	called bool
}

func (w *stubWriteHandler) HandleReceivePack(_ context.Context, _ rpc.RequestMeta, stream rpc.GitWireStream) error {
	w.called = true
	return stream.Send(&rpc.Frame{Data: []byte("write-ok"), EOF: true, ExitCode: 0})
}

func TestServer_ReceivePackWriteHandlerSeam(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "write-seam")
	c, _ := cache.New(cache.Config{Root: t.TempDir()})
	mat, _ := NewMaterializer(MaterializerConfig{
		Meta:  newFakeMetadataStore(),
		Packs: newFakePackFetcher(),
		Cache: c,
	})
	t.Cleanup(mat.Close)

	// Default server returns Unimplemented for git-receive-pack.
	defaultSrv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	defaultClient := startBufconnNode(t, defaultSrv)
	stream, err := defaultClient.OpenStream(ctx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpReceivePack,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("default git-receive-pack code = %v, want Unimplemented", status.Code(err))
	}

	// Plugged WriteHandler handles git-receive-pack cleanly.
	wh := &stubWriteHandler{}
	customSrv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
		WriteHandler: wh,
	})
	if err != nil {
		t.Fatalf("NewServer custom: %v", err)
	}
	customClient := startBufconnNode(t, customSrv)
	stream2, err := customClient.OpenStream(ctx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpReceivePack,
	})
	if err != nil {
		t.Fatalf("OpenStream custom: %v", err)
	}
	frame, err := stream2.Recv()
	if err != nil {
		t.Fatalf("Recv custom: %v", err)
	}
	if !wh.called || string(frame.Data) != "write-ok" || !frame.EOF {
		t.Fatalf("unexpected custom write handler result: called=%v frame=%+v", wh.called, frame)
	}
}

func TestServer_UploadPackRealGitRoundTripOverBufconn(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "real-git")

	// Build a real Git commit and packfile in a scratch repo.
	commitOID, packBytes, idxBytes := buildSampleGitPack(t)
	packSum := sha256.Sum256(packBytes)
	packID := hex.EncodeToString(packSum[:])

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
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: commitOID, PackID: packID},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo:         rk,
		ULID:         "01ARZ3NDEKTSV4RRFFQ69G5FC1",
		PackID:       packID,
		SizeBytes:    int64(len(packBytes)),
		OriginRegion: "us-east-1",
		Status:       ddb.PackStatusActive,
	})

	packStore := newFakePackFetcher()
	packStore.Put(packID, packBytes, idxBytes)

	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     packStore,
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	nodeSrv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
		Telemetry:    tp,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	client := startBufconnNode(t, nodeSrv)
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	traceID := "0af7651916cd43dd8448eb211c80319c"
	stream, err := client.OpenStream(outCtx, rpc.RequestMeta{
		TenantID:    rk.TenantID,
		RepoID:      rk.RepoID,
		Operation:   rpc.OpUploadPack,
		TraceParent: "00-" + traceID + "-b7ad6b7169203331-01",
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	// Send a valid stateless-rpc upload-pack want/done negotiation payload.
	var reqPayload bytes.Buffer
	reqPayload.Write(EncodePktLine([]byte(fmt.Sprintf("want %s\n", commitOID))))
	reqPayload.Write(FlushPkt)
	reqPayload.Write(EncodePktLine([]byte("done\n")))

	if err := stream.Send(&rpc.Frame{Data: reqPayload.Bytes(), EOF: true}); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}

	var resp bytes.Buffer
	for {
		frame, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		resp.Write(frame.Data)
		if frame.EOF {
			if frame.ExitCode != 0 {
				t.Fatalf("git-upload-pack exit code = %d, stderr=%q", frame.ExitCode, frame.ErrMessage)
			}
			// Continue reading until gRPC io.EOF so the server handler returns
			// and runs defer execSpan.End() before we inspect exported spans.
		}
	}

	if !bytes.Contains(resp.Bytes(), []byte("NAK\n")) {
		t.Fatalf("expected NAK in upload-pack response, got %d bytes: %q", resp.Len(), resp.Bytes()[:min(len(resp.Bytes()), 120)])
	}
	if !bytes.Contains(resp.Bytes(), []byte("PACK")) {
		t.Fatalf("expected PACK header in upload-pack response, got %d bytes", resp.Len())
	}

	// Verify all required child spans were emitted under the propagated trace ID.
	wantSpans := map[string]bool{
		"ddb.query_refs":       false,
		"ddb.query_manifest":   false,
		"node.cache.lookup":    false,
		"s3.get_pack":          false,
		"node.git.upload_pack": false,
	}
	for _, s := range tp.SpanExporter.GetSpans() {
		if _, tracked := wantSpans[s.Name]; tracked {
			wantSpans[s.Name] = true
			if got := s.SpanContext.TraceID().String(); got != traceID {
				t.Errorf("span %s trace_id = %s, want %s", s.Name, got, traceID)
			}
		}
	}
	for name, seen := range wantSpans {
		if !seen {
			t.Errorf("missing expected OTel span %q", name)
		}
	}
}

func buildSampleGitPack(t *testing.T) (commitOID string, packBytes, idxBytes []byte) {
	t.Helper()
	dir := t.TempDir()
	runGit := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(),
			"GIT_AUTHOR_NAME=Stravin Test",
			"GIT_AUTHOR_EMAIL=test@stravin.dev",
			"GIT_COMMITTER_NAME=Stravin Test",
			"GIT_COMMITTER_EMAIL=test@stravin.dev",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v failed: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}

	runGit("init", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello stravin\n"), 0o600); err != nil {
		t.Fatalf("write hello.txt: %v", err)
	}
	runGit("add", "hello.txt")
	runGit("commit", "-m", "initial commit")
	commitOID = runGit("rev-parse", "HEAD")
	runGit("repack", "-a", "-d")

	packDir := filepath.Join(dir, ".git", "objects", "pack")
	entries, err := os.ReadDir(packDir)
	if err != nil {
		t.Fatalf("ReadDir pack: %v", err)
	}
	for _, e := range entries {
		switch {
		case strings.HasSuffix(e.Name(), ".pack"):
			packBytes, err = os.ReadFile(filepath.Join(packDir, e.Name()))
			if err != nil {
				t.Fatalf("read .pack: %v", err)
			}
		case strings.HasSuffix(e.Name(), ".idx"):
			idxBytes, err = os.ReadFile(filepath.Join(packDir, e.Name()))
			if err != nil {
				t.Fatalf("read .idx: %v", err)
			}
		}
	}
	if len(packBytes) == 0 || len(idxBytes) == 0 {
		t.Fatalf("failed to generate sample pack/idx files")
	}
	return commitOID, packBytes, idxBytes
}

type fakeGitExecutor struct {
	uploadPackFn func(ctx context.Context, req gitexec.UploadPackRequest) error
}

func (f *fakeGitExecutor) ExecUploadPack(ctx context.Context, req gitexec.UploadPackRequest) error {
	if f.uploadPackFn != nil {
		return f.uploadPackFn(ctx, req)
	}
	return nil
}

func (f *fakeGitExecutor) ExecIndexPack(_ context.Context, _ gitexec.IndexPackRequest) error {
	return nil
}

func (f *fakeGitExecutor) ExecRepack(_ context.Context, _ gitexec.RepackRequest) error {
	return nil
}

type errWriteHandler struct {
	err error
}

func (w *errWriteHandler) HandleReceivePack(_ context.Context, _ rpc.RequestMeta, _ rpc.GitWireStream) error {
	return w.err
}

func setupTestNodeServer(t *testing.T, execImpl gitexec.GitExecutor, wh WriteHandler) (tenant.RepoKey, rpc.GitWireClient, *telemetry.Provider, *Metrics) {
	t.Helper()
	rk, err := tenant.NewRepoKey("acme", "obs-test")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
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
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     newFakePackFetcher(),
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// Pre-warm the bare repo layout so concurrent test requests exercise the
	// upload-pack semaphore rather than cold git-init startup.
	warm, err := mat.SyncAndPin(context.Background(), rk)
	if err != nil {
		t.Fatalf("warm SyncAndPin: %v", err)
	}
	warm.Handle.Release()

	metrics := NewMetrics(tp, "us-east-1")
	srv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  execImpl,
		WriteHandler: wh,
		Telemetry:    tp,
		Metrics:      metrics,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return rk, startBufconnNode(t, srv), tp, metrics
}

func collectNodeRequestCounts(t *testing.T, tp *telemetry.Provider, gitOp string) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	counts := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_node_requests_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("stravin_node_requests_total type = %T, want metricdata.Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				opVal, _ := dp.Attributes.Value(telemetry.AttrGitOperation)
				if opVal.AsString() != gitOp {
					continue
				}
				scVal, ok := dp.Attributes.Value(telemetry.AttrStatusCode)
				if ok {
					counts[scVal.AsString()] += dp.Value
				}
			}
		}
	}
	return counts
}

func assertSpanStringsValidUTF8(t *testing.T, s tracetest.SpanStub) {
	t.Helper()
	if !utf8.ValidString(s.Status.Description) {
		t.Errorf("span %q status description contains invalid UTF-8: %q", s.Name, s.Status.Description)
	}
	for _, ev := range s.Events {
		for _, attr := range ev.Attributes {
			if attr.Value.Type() == attribute.STRING && !utf8.ValidString(attr.Value.AsString()) {
				t.Errorf("span %q event %q attr %q contains invalid UTF-8: %q", s.Name, ev.Name, attr.Key, attr.Value.AsString())
			}
		}
	}
}

func TestServer_UploadPackSanitizesInvalidUTF8Stderr(t *testing.T) {
	rawStderr := "fatal: bad pkt-line \xff\xfe\x80 trailing"
	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, req gitexec.UploadPackRequest) error {
			if req.Stderr != nil {
				_, _ = req.Stderr.Write([]byte(rawStderr + "\n"))
			}
			return &gitexec.ExecError{
				Subcommand: "upload-pack",
				ExitCode:   128,
				Stderr:     rawStderr,
				Err:        errors.New("exit status 128"),
			}
		},
	}

	rk, client, tp, _ := setupTestNodeServer(t, execImpl, nil)
	stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if err := stream.Send(&rpc.Frame{EOF: true}); err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("stream.Send: %v", err)
	}

	var eofFrame *rpc.Frame
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatalf("stream.Recv: %v", recvErr)
		}
		if frame.EOF {
			eofFrame = frame
		}
	}
	if eofFrame == nil {
		t.Fatal("expected EOF frame from server")
	}
	if eofFrame.ExitCode != 128 {
		t.Fatalf("ExitCode = %d, want 128", eofFrame.ExitCode)
	}
	if !utf8.ValidString(eofFrame.ErrMessage) {
		t.Errorf("rpc.Frame.ErrMessage contains invalid UTF-8: %q", eofFrame.ErrMessage)
	}
	if !strings.Contains(eofFrame.ErrMessage, "\uFFFD") || !strings.Contains(eofFrame.ErrMessage, "fatal: bad pkt-line") {
		t.Errorf("rpc.Frame.ErrMessage = %q, want sanitized message containing replacement char", eofFrame.ErrMessage)
	}

	var sawRoot, sawExec bool
	for _, s := range tp.SpanExporter.GetSpans() {
		switch s.Name {
		case "node.git.stream":
			sawRoot = true
			if s.Status.Code != otelcodes.Error {
				t.Errorf("root span status = %v, want Error", s.Status.Code)
			}
			assertSpanStringsValidUTF8(t, s)
		case "node.git.upload_pack":
			sawExec = true
			if s.Status.Code != otelcodes.Error {
				t.Errorf("exec span status = %v, want Error", s.Status.Code)
			}
			assertSpanStringsValidUTF8(t, s)
		}
	}
	if !sawRoot || !sawExec {
		t.Fatalf("missing spans: sawRoot=%v sawExec=%v", sawRoot, sawExec)
	}
}

func TestServer_UploadPackExitStatusMetricAndRootSpan(t *testing.T) {
	tests := []struct {
		name           string
		execErr        error
		wantExitCode   int32
		wantStatusCode string
		wantSpanCode   otelcodes.Code
	}{
		{
			name: "non-zero exit code records error status and root span error",
			execErr: &gitexec.ExecError{
				Subcommand: "upload-pack",
				ExitCode:   128,
				Stderr:     "fatal: not a git repository",
				Err:        errors.New("exit status 128"),
			},
			wantExitCode:   128,
			wantStatusCode: "error",
			wantSpanCode:   otelcodes.Error,
		},
		{
			name:           "zero exit code records ok status and non-error root span",
			execErr:        nil,
			wantExitCode:   0,
			wantStatusCode: "ok",
			wantSpanCode:   otelcodes.Unset,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			execImpl := &fakeGitExecutor{
				uploadPackFn: func(_ context.Context, _ gitexec.UploadPackRequest) error {
					return tc.execErr
				},
			}
			rk, client, tp, metrics := setupTestNodeServer(t, execImpl, nil)
			stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
				TenantID:  rk.TenantID,
				RepoID:    rk.RepoID,
				Operation: rpc.OpUploadPack,
			})
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			if err := stream.Send(&rpc.Frame{EOF: true}); err != nil && !errors.Is(err, io.EOF) {
				t.Fatalf("stream.Send: %v", err)
			}

			var eofFrame *rpc.Frame
			for {
				frame, recvErr := stream.Recv()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				if recvErr != nil {
					t.Fatalf("stream.Recv: %v", recvErr)
				}
				if frame.EOF {
					eofFrame = frame
				}
			}
			if eofFrame == nil || eofFrame.ExitCode != tc.wantExitCode {
				t.Fatalf("EOF frame = %+v, want ExitCode=%d", eofFrame, tc.wantExitCode)
			}

			var sawRoot bool
			for _, s := range tp.SpanExporter.GetSpans() {
				if s.Name == "node.git.stream" {
					sawRoot = true
					if s.Status.Code != tc.wantSpanCode {
						t.Errorf("root span status = %v, want %v", s.Status.Code, tc.wantSpanCode)
					}
				}
			}
			if !sawRoot {
				t.Fatal("expected node.git.stream root span")
			}

			counts := collectNodeRequestCounts(t, tp, "fetch")
			if counts[tc.wantStatusCode] != 1 {
				t.Errorf("stravin_node_requests_total[fetch,%s] = %d, want 1 (all=%v)", tc.wantStatusCode, counts[tc.wantStatusCode], counts)
			}
			other := "ok"
			if tc.wantStatusCode == "ok" {
				other = "error"
			}
			if counts[other] != 0 {
				t.Errorf("stravin_node_requests_total[fetch,%s] = %d, want 0", other, counts[other])
			}

			rec := httptest.NewRecorder()
			metrics.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			wantPromLine := fmt.Sprintf("stravin_node_requests_total{region=\"us-east-1\",status_code=%q} 1", tc.wantStatusCode)
			if !strings.Contains(rec.Body.String(), wantPromLine) {
				t.Errorf("Prometheus /metrics missing %q:\n%s", wantPromLine, rec.Body.String())
			}
		})
	}
}

func TestServer_ReceivePackSanitizesInvalidUTF8Error(t *testing.T) {
	wh := &errWriteHandler{
		err: errors.New("node: receive-pack failed: \xff\xfe\x80 invalid bytes"),
	}
	rk, client, tp, _ := setupTestNodeServer(t, &fakeGitExecutor{}, wh)
	stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpReceivePack,
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, recvErr := stream.Recv(); recvErr == nil {
		t.Fatal("expected error from receive-pack stream")
	}

	var sawRoot bool
	for _, s := range tp.SpanExporter.GetSpans() {
		if s.Name == "node.git.stream" {
			sawRoot = true
			if s.Status.Code != otelcodes.Error {
				t.Errorf("root span status = %v, want Error", s.Status.Code)
			}
			assertSpanStringsValidUTF8(t, s)
		}
	}
	if !sawRoot {
		t.Fatal("expected node.git.stream root span")
	}

	counts := collectNodeRequestCounts(t, tp, "push")
	if counts["error"] != 1 {
		t.Errorf("stravin_node_requests_total[push,error] = %d, want 1 (all=%v)", counts["error"], counts)
	}
}

func setupTestNodeServerWithLimits(
	t *testing.T,
	execImpl gitexec.GitExecutor,
	maxUploadPacks int,
	queueTimeout time.Duration,
) (tenant.RepoKey, rpc.GitWireClient, *telemetry.Provider, *Metrics) {
	t.Helper()
	rk, err := tenant.NewRepoKey("acme", "limit-test")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
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
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     newFakePackFetcher(),
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// Pre-warm the bare repo layout so concurrent test requests exercise the
	// upload-pack semaphore rather than cold git-init startup.
	warm, err := mat.SyncAndPin(context.Background(), rk)
	if err != nil {
		t.Fatalf("warm SyncAndPin: %v", err)
	}
	warm.Handle.Release()

	metrics := NewMetrics(tp, "us-east-1")
	srv, err := NewServer(ServerConfig{
		Materializer:           mat,
		GitExecutor:            execImpl,
		Telemetry:              tp,
		Metrics:                metrics,
		Region:                 "us-east-1",
		MaxUploadPacks:         maxUploadPacks,
		UploadPackQueueTimeout: queueTimeout,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return rk, startBufconnNode(t, srv), tp, metrics
}

func TestServer_NewServerRejectsInvalidUploadPackConfig(t *testing.T) {
	c, _ := cache.New(cache.Config{Root: t.TempDir()})
	mat, _ := NewMaterializer(MaterializerConfig{
		Meta:  newFakeMetadataStore(),
		Packs: newFakePackFetcher(),
		Cache: c,
	})
	t.Cleanup(mat.Close)

	if _, err := NewServer(ServerConfig{
		Materializer:   mat,
		GitExecutor:    &fakeGitExecutor{},
		MaxUploadPacks: -1,
	}); err == nil {
		t.Error("expected error for negative MaxUploadPacks")
	}
	if _, err := NewServer(ServerConfig{
		Materializer:           mat,
		GitExecutor:            &fakeGitExecutor{},
		UploadPackQueueTimeout: -time.Second,
	}); err == nil {
		t.Error("expected error for negative UploadPackQueueTimeout")
	}
}

func TestServer_UploadPackConcurrencyCapAndQueueing(t *testing.T) {
	const maxConcurrent = 2
	const totalReqs = 4

	var active atomic.Int32
	var maxObserved atomic.Int32
	entered := make(chan struct{}, totalReqs)
	release := make(chan struct{})

	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, _ gitexec.UploadPackRequest) error {
			cur := active.Add(1)
			for {
				prev := maxObserved.Load()
				if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			active.Add(-1)
			return nil
		},
	}

	rk, client, tp, _ := setupTestNodeServerWithLimits(t, execImpl, maxConcurrent, 2*time.Second)

	errCh := make(chan error, totalReqs)
	var wg sync.WaitGroup
	for range totalReqs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
				TenantID:  rk.TenantID,
				RepoID:    rk.RepoID,
				Operation: rpc.OpUploadPack,
			})
			if err != nil {
				errCh <- err
				return
			}
			if err := stream.Send(&rpc.Frame{EOF: true}); err != nil && !errors.Is(err, io.EOF) {
				errCh <- err
				return
			}
			for {
				_, recvErr := stream.Recv()
				if errors.Is(recvErr, io.EOF) {
					return
				}
				if recvErr != nil {
					errCh <- recvErr
					return
				}
			}
		}()
	}

	// Wait until exactly maxConcurrent requests are inside ExecUploadPack.
	for range maxConcurrent {
		select {
		case <-entered:
		case err := <-errCh:
			close(release)
			t.Fatalf("upload-pack failed before filling semaphore slots: %v", err)
		case <-time.After(5 * time.Second):
			close(release)
			t.Fatal("timed out waiting for initial upload-pack slots to fill")
		}
	}

	// Briefly confirm no 3rd request bypasses the semaphore while slots are held.
	select {
	case <-entered:
		t.Fatal("more than maxConcurrent upload-packs entered simultaneously")
	case <-time.After(40 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("unexpected upload-pack error: %v", err)
	}

	if got := maxObserved.Load(); got > maxConcurrent {
		t.Fatalf("maxObserved concurrency = %d, want <= %d", got, maxConcurrent)
	}

	var rm metricdata.ResourceMetrics
	if err := tp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	var waitCount uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "stravin_node_upload_pack_queue_wait_seconds" {
				if hist, ok := m.Data.(metricdata.Histogram[float64]); ok && len(hist.DataPoints) > 0 {
					waitCount = hist.DataPoints[0].Count
				}
			}
		}
	}
	if waitCount != totalReqs {
		t.Errorf("queue wait histogram count = %d, want %d", waitCount, totalReqs)
	}
}

func TestServer_UploadPackQueueTimeoutReturnsResourceExhausted(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)

	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, _ gitexec.UploadPackRequest) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	}

	rk, client, _, metrics := setupTestNodeServerWithLimits(t, execImpl, 1, 50*time.Millisecond)

	// Start request 1 to hold the single semaphore slot.
	req1Done := make(chan error, 1)
	go func() {
		stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
			TenantID:  rk.TenantID,
			RepoID:    rk.RepoID,
			Operation: rpc.OpUploadPack,
		})
		if err != nil {
			req1Done <- err
			return
		}
		_ = stream.Send(&rpc.Frame{EOF: true})
		for {
			_, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				req1Done <- nil
				return
			}
			if recvErr != nil {
				req1Done <- recvErr
				return
			}
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request 1 to enter ExecUploadPack")
	}

	// Request 2 should time out waiting in the queue with ResourceExhausted.
	stream2, err := client.OpenStream(context.Background(), rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream req2: %v", err)
	}
	_, recvErr := stream2.Recv()
	if status.Code(recvErr) != codes.ResourceExhausted {
		t.Fatalf("req2 status code = %v (err=%v), want ResourceExhausted", status.Code(recvErr), recvErr)
	}

	rec := httptest.NewRecorder()
	metrics.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	wantProm := `stravin_node_upload_pack_queue_timeouts_total{region="us-east-1"} 1`
	if !strings.Contains(rec.Body.String(), wantProm) {
		t.Errorf("Prometheus /metrics missing %q:\n%s", wantProm, rec.Body.String())
	}
}

func TestServer_UploadPackQueueRespectsContextCancellation(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)

	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, _ gitexec.UploadPackRequest) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	}

	rk, client, _, metrics := setupTestNodeServerWithLimits(t, execImpl, 1, 5*time.Second)

	go func() {
		stream, err := client.OpenStream(context.Background(), rpc.RequestMeta{
			TenantID:  rk.TenantID,
			RepoID:    rk.RepoID,
			Operation: rpc.OpUploadPack,
		})
		if err != nil {
			return
		}
		_ = stream.Send(&rpc.Frame{EOF: true})
		for {
			if _, recvErr := stream.Recv(); recvErr != nil {
				return
			}
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request 1 to enter ExecUploadPack")
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	stream2, err := client.OpenStream(cancelCtx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream req2: %v", err)
	}
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, recvErr := stream2.Recv()
	if status.Code(recvErr) != codes.Canceled {
		t.Fatalf("req2 status code = %v (err=%v), want Canceled", status.Code(recvErr), recvErr)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("canceled request took %v, expected prompt cancellation", elapsed)
	}

	rec := httptest.NewRecorder()
	metrics.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	wantProm := `stravin_node_upload_pack_queue_timeouts_total{region="us-east-1"} 0`
	if !strings.Contains(rec.Body.String(), wantProm) {
		t.Errorf("expected 0 queue timeouts on client cancel, got:\n%s", rec.Body.String())
	}
}

func TestServer_AdvertiseRefsV2SkipsListRefs(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "advert-v2")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

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
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("a", 40)},
	})

	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     newFakePackFetcher(),
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	srv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  &fakeGitExecutor{},
		Telemetry:    tp,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	client := startBufconnNode(t, srv)
	outCtx := metadata.AppendToOutgoingContext(ctx, HeaderAdvertiseRefs, "true")
	stream, err := client.OpenStream(outCtx, rpc.RequestMeta{
		TenantID:    rk.TenantID,
		RepoID:      rk.RepoID,
		Operation:   rpc.OpUploadPack,
		GitProtocol: "version=2",
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	var buf bytes.Buffer
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatalf("Recv: %v", recvErr)
		}
		buf.Write(frame.Data)
	}

	if !strings.Contains(buf.String(), "version 2\n") {
		t.Fatalf("expected v2 capabilities, got %q", buf.String())
	}
	if metaStore.GetMetaCalls() != 1 {
		t.Errorf("GetMetaCalls = %d, want 1", metaStore.GetMetaCalls())
	}
	if metaStore.ListRefsCalls() != 0 {
		t.Errorf("ListRefsCalls = %d, want 0 for protocol v2", metaStore.ListRefsCalls())
	}
}

type fakeBundlePresigner struct {
	mu    sync.Mutex
	url   string
	err   error
	calls int
}

func (f *fakeBundlePresigner) PresignBundleURL(_ context.Context, _ tenant.RepoKey, _ time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.url, f.err
}

func (f *fakeBundlePresigner) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestS3BundleURIResolver_TTLCache(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "bundle-cache")

	var nowMu sync.Mutex
	curTime := time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)
	nowFn := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return curTime
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		curTime = curTime.Add(d)
		nowMu.Unlock()
	}

	presigner := &fakeBundlePresigner{url: "https://s3.example.com/bundle.pack?sig=1"}
	resolver := NewS3BundleURIResolver(presigner, 15*time.Minute)
	resolver.now = nowFn

	// 1. Positive result cached within 5s TTL.
	u1, err := resolver.ResolveBundleURI(ctx, rk)
	if err != nil || u1 != presigner.url {
		t.Fatalf("ResolveBundleURI(1) = (%q, %v), want (%q, nil)", u1, err, presigner.url)
	}
	advance(2 * time.Second)
	u2, err := resolver.ResolveBundleURI(ctx, rk)
	if err != nil || u2 != presigner.url {
		t.Fatalf("ResolveBundleURI(2) = (%q, %v), want (%q, nil)", u2, err, presigner.url)
	}
	if presigner.Calls() != 1 {
		t.Fatalf("presigner calls within TTL = %d, want 1", presigner.Calls())
	}

	// 2. After TTL expires, refreshes and caches negative s3.ErrNotFound.
	advance(4 * time.Second)
	presigner.mu.Lock()
	presigner.url = ""
	presigner.err = s3.ErrNotFound
	presigner.mu.Unlock()

	u3, err := resolver.ResolveBundleURI(ctx, rk)
	if err != nil || u3 != "" {
		t.Fatalf("ResolveBundleURI(3) = (%q, %v), want (\"\", nil)", u3, err)
	}
	advance(2 * time.Second)
	u4, err := resolver.ResolveBundleURI(ctx, rk)
	if err != nil || u4 != "" {
		t.Fatalf("ResolveBundleURI(4) = (%q, %v), want (\"\", nil)", u4, err)
	}
	if presigner.Calls() != 2 {
		t.Fatalf("presigner calls after negative cache hit = %d, want 2", presigner.Calls())
	}

	// 3. Transient errors are NOT cached.
	advance(4 * time.Second)
	presigner.mu.Lock()
	presigner.err = errors.New("s3 503 slow down")
	presigner.mu.Unlock()

	if _, err := resolver.ResolveBundleURI(ctx, rk); err == nil {
		t.Fatal("expected transient S3 error")
	}
	if _, err := resolver.ResolveBundleURI(ctx, rk); err == nil {
		t.Fatal("expected second transient S3 error")
	}
	if presigner.Calls() != 4 {
		t.Fatalf("presigner calls after 2 transient errors = %d, want 4", presigner.Calls())
	}

	// 4. Concurrent access under -race.
	presigner.mu.Lock()
	presigner.url = "https://s3.example.com/bundle.pack?sig=2"
	presigner.err = nil
	presigner.mu.Unlock()

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = resolver.ResolveBundleURI(ctx, rk)
		}()
	}
	wg.Wait()
}

type countingBundleResolver struct {
	uri   string
	calls atomic.Int32
}

func (r *countingBundleResolver) ResolveBundleURI(_ context.Context, _ tenant.RepoKey) (string, error) {
	r.calls.Add(1)
	return r.uri, nil
}

func TestServer_UploadPackV2LsRefsBypassesSemaphoreAndBundleURI(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "v2-lsrefs")
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
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: newFakePackFetcher(),
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)
	warm, err := mat.SyncAndPin(context.Background(), rk)
	if err != nil {
		t.Fatalf("warm SyncAndPin: %v", err)
	}
	warm.Handle.Release()

	fetchEntered := make(chan struct{}, 1)
	fetchRelease := make(chan struct{})
	defer close(fetchRelease)

	var (
		mu             sync.Mutex
		lsRefsStdin    []byte
		lsRefsBundle   string
		fetchBundleURI string
	)
	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, req gitexec.UploadPackRequest) error {
			inBytes, _ := io.ReadAll(req.Stdin)
			if bytes.Contains(inBytes, []byte("command=ls-refs")) {
				mu.Lock()
				lsRefsStdin = inBytes
				lsRefsBundle = req.BundleURI
				mu.Unlock()
				_, _ = req.Stdout.Write([]byte("ls-refs-ok"))
				return nil
			}
			mu.Lock()
			fetchBundleURI = req.BundleURI
			mu.Unlock()
			select {
			case fetchEntered <- struct{}{}:
			default:
			}
			<-fetchRelease
			return nil
		},
	}

	bundleResolver := &countingBundleResolver{uri: "https://s3.example.com/bundle.pack"}
	srv, err := NewServer(ServerConfig{
		Materializer:           mat,
		GitExecutor:            execImpl,
		BundleResolver:         bundleResolver,
		MaxUploadPacks:         1,
		UploadPackQueueTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	// 1. Occupy the single upload-pack slot with a v2 fetch request.
	fetchCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	fetchDone := make(chan error, 1)
	go func() {
		st, openErr := client.OpenStream(fetchCtx, rpc.RequestMeta{
			TenantID:    rk.TenantID,
			RepoID:      rk.RepoID,
			Operation:   rpc.OpUploadPack,
			GitProtocol: "version=2",
		})
		if openErr != nil {
			fetchDone <- openErr
			return
		}
		fetchPayload := append(EncodePktLine([]byte("command=fetch\n")), FlushPkt...)
		_ = st.Send(&rpc.Frame{Data: fetchPayload, EOF: true})
		for {
			_, rErr := st.Recv()
			if errors.Is(rErr, io.EOF) {
				fetchDone <- nil
				return
			}
			if rErr != nil {
				fetchDone <- rErr
				return
			}
		}
	}()

	select {
	case <-fetchEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fetch to hold upload-pack slot")
	}

	// 2. Send a v2 ls-refs request with an unrecognized extension arg (triggers
	// git-upload-pack fallback) split across two frames while the slot is full.
	lsCtx := metadata.AppendToOutgoingContext(context.Background(), HeaderStatelessRPC, "true")
	lsStream, err := client.OpenStream(lsCtx, rpc.RequestMeta{
		TenantID:    rk.TenantID,
		RepoID:      rk.RepoID,
		Operation:   rpc.OpUploadPack,
		GitProtocol: "version=2",
	})
	if err != nil {
		t.Fatalf("OpenStream ls-refs: %v", err)
	}
	fullLsRefs := buildV2Request("ls-refs", nil, []string{"x-fallback-test"})
	if err := lsStream.Send(&rpc.Frame{Data: fullLsRefs[:6]}); err != nil {
		t.Fatalf("Send frame 1: %v", err)
	}
	if err := lsStream.Send(&rpc.Frame{Data: fullLsRefs[6:], EOF: true}); err != nil {
		t.Fatalf("Send frame 2: %v", err)
	}

	var lsResp bytes.Buffer
	for {
		frame, rErr := lsStream.Recv()
		if errors.Is(rErr, io.EOF) {
			break
		}
		if rErr != nil {
			t.Fatalf("ls-refs Recv failed (likely blocked on semaphore): %v", rErr)
		}
		lsResp.Write(frame.Data)
	}

	if lsResp.String() != "ls-refs-ok" {
		t.Fatalf("ls-refs response = %q, want ls-refs-ok", lsResp.String())
	}
	mu.Lock()
	gotStdin := append([]byte(nil), lsRefsStdin...)
	gotLsBundle := lsRefsBundle
	gotFetchBundle := fetchBundleURI
	mu.Unlock()
	if !bytes.Equal(gotStdin, fullLsRefs) {
		t.Fatalf("replayed ls-refs stdin = %q, want %q", gotStdin, fullLsRefs)
	}
	if gotLsBundle != "" {
		t.Errorf("ls-refs BundleURI = %q, want empty", gotLsBundle)
	}
	if gotFetchBundle != bundleResolver.uri {
		t.Errorf("fetch BundleURI = %q, want %q", gotFetchBundle, bundleResolver.uri)
	}
	if bundleResolver.calls.Load() != 1 {
		t.Errorf("bundleResolver calls = %d, want 1 (only for fetch, skipped for ls-refs)", bundleResolver.calls.Load())
	}
}

func TestServer_UploadPackSyncRunsBeforeSlotAcquire(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "sync-before-slot")
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
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: newFakePackFetcher(),
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)
	warm, err := mat.SyncAndPin(context.Background(), rk)
	if err != nil {
		t.Fatalf("warm SyncAndPin: %v", err)
	}
	warm.Handle.Release()

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)

	execImpl := &fakeGitExecutor{
		uploadPackFn: func(_ context.Context, _ gitexec.UploadPackRequest) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	}

	srv, err := NewServer(ServerConfig{
		Materializer:           mat,
		GitExecutor:            execImpl,
		MaxUploadPacks:         1,
		UploadPackQueueTimeout: 80 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	// Hold the single upload-pack slot with request 1.
	go func() {
		st, openErr := client.OpenStream(context.Background(), rpc.RequestMeta{
			TenantID:  rk.TenantID,
			RepoID:    rk.RepoID,
			Operation: rpc.OpUploadPack,
		})
		if openErr != nil {
			return
		}
		_ = st.Send(&rpc.Frame{EOF: true})
		for {
			if _, rErr := st.Recv(); rErr != nil {
				return
			}
		}
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for request 1 to hold slot")
	}

	callsBefore := metaStore.GetMetaCalls()

	// Request 2 should run SyncAndPin (calling GetMeta) BEFORE timing out on the semaphore.
	st2, err := client.OpenStream(context.Background(), rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream req2: %v", err)
	}
	_, recvErr := st2.Recv()
	if status.Code(recvErr) != codes.ResourceExhausted {
		t.Fatalf("req2 status = %v (err=%v), want ResourceExhausted", status.Code(recvErr), recvErr)
	}
	if metaStore.GetMetaCalls() <= callsBefore {
		t.Errorf("GetMetaCalls did not increase (%d -> %d): expected SyncAndPin before acquireUploadPackSlot",
			callsBefore, metaStore.GetMetaCalls())
	}
}

func TestEpochToInt64(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   lease.Epoch
		want int64
	}{
		{name: "zero", in: 0, want: 0},
		{name: "typical", in: 42, want: 42},
		{name: "max_int64", in: lease.Epoch(math.MaxInt64), want: math.MaxInt64},
		{name: "max_int64_plus_one", in: lease.Epoch(math.MaxInt64) + 1, want: math.MaxInt64},
		{name: "max_uint64", in: lease.Epoch(math.MaxUint64), want: math.MaxInt64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := epochToInt64(tc.in); got != tc.want {
				t.Fatalf("epochToInt64(%d) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestLoadAuthoritativeRefsClampsOverflowFailoverEpoch(t *testing.T) {
	ctx := context.Background()
	rk := tenant.RepoKey{TenantID: "t-overflow", RepoID: "r-overflow"}

	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  lease.Epoch(math.MaxUint64),
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("a", 40)},
	})

	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     newFakePackFetcher(),
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	if _, _, err := mat.LoadAuthoritativeRefs(ctx, rk); err != nil {
		t.Fatalf("LoadAuthoritativeRefs: %v", err)
	}

	var found bool
	for _, s := range tp.SpanExporter.GetSpans() {
		if s.Name != "ddb.query_refs" {
			continue
		}
		for _, attr := range s.Attributes {
			if attr.Key == telemetry.AttrFailoverEpoch {
				found = true
				if got := attr.Value.AsInt64(); got != math.MaxInt64 {
					t.Fatalf("ddb.query_refs %s = %d, want %d", telemetry.AttrFailoverEpoch, got, int64(math.MaxInt64))
				}
			}
		}
	}
	if !found {
		t.Fatalf("expected ddb.query_refs span with %s attribute", telemetry.AttrFailoverEpoch)
	}
}
