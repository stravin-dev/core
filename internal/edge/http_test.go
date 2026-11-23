package edge

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type mockGitNode struct {
	mu           sync.Mutex
	lastMeta     rpc.RequestMeta
	lastMD       metadata.MD
	lastPayload  []byte
	responseData []byte
	errMessage   string
	exitCode     int32
	onHandle     func(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error
}

func (m *mockGitNode) HandleGitWire(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error {
	m.mu.Lock()
	custom := m.onHandle
	m.mu.Unlock()
	if custom != nil {
		return custom(ctx, meta, stream)
	}
	var received bytes.Buffer
	for {
		frame, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if len(frame.Data) > 0 {
			received.Write(frame.Data)
		}
		if frame.EOF {
			break
		}
	}

	inMD, _ := metadata.FromIncomingContext(ctx)

	m.mu.Lock()
	m.lastMeta = meta
	m.lastMD = inMD.Copy()
	m.lastPayload = received.Bytes()
	resp := append([]byte(nil), m.responseData...)
	errMsg := m.errMessage
	code := m.exitCode
	m.mu.Unlock()

	if len(resp) > 0 || errMsg != "" {
		if err := stream.Send(&rpc.Frame{Data: resp, ErrMessage: errMsg}); err != nil {
			return err
		}
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: code})
}

func (m *mockGitNode) snapshot() (rpc.RequestMeta, []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMeta, append([]byte(nil), m.lastPayload...)
}

func startMockGRPCNode(t *testing.T, handler rpc.GitWireHandler) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock grpc: %v", err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodec(rpc.WireCodec()))
	rpc.RegisterGitWireServer(srv, handler)
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), func() {
		srv.Stop()
		_ = ln.Close()
	}
}

type testEdgeEnv struct {
	handler    *HTTPHandler
	sshServer  *SSHServer
	node       *mockGitNode
	clk        *fakeClock
	tel        *telemetry.Provider
	clientKey  interface{}
	peerAddr   string
	cleanupFns []func()
}

func (e *testEdgeEnv) close() {
	for i := len(e.cleanupFns) - 1; i >= 0; i-- {
		e.cleanupFns[i]()
	}
}

func newTestEdgeEnv(t *testing.T, limCfg LimiterConfig) *testEdgeEnv {
	t.Helper()
	now := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: now}
	if limCfg.Clock == nil {
		limCfg.Clock = clk
	}

	node := &mockGitNode{
		responseData: []byte("003f0123456789abcdef0123456789abcdef01234567 refs/heads/main\n0000"),
	}
	peerAddr, stopNode := startMockGRPCNode(t, node)

	acmeRepo := tenant.RepoKey{TenantID: "acme", RepoID: "core"}
	remoteRepo := tenant.RepoKey{TenantID: "acme", RepoID: "west"}
	betaRepo := tenant.RepoKey{TenantID: "beta", RepoID: "secret"}

	metaStore := &stubMetaStore{
		metas: map[tenant.RepoKey]ddb.RepoMeta{
			acmeRepo: {
				Repo:           acmeRepo,
				HomeRegion:     "us-east-1",
				FailoverEpoch:  1,
				RefShards:      4,
				ManifestShards: 4,
				WriteState:     lease.ModeActive,
			},
			remoteRepo: {
				Repo:           remoteRepo,
				HomeRegion:     "us-west-2",
				FailoverEpoch:  1,
				RefShards:      4,
				ManifestShards: 4,
				WriteState:     lease.ModeActive,
			},
			betaRepo: {
				Repo:           betaRepo,
				HomeRegion:     "us-east-1",
				FailoverEpoch:  1,
				RefShards:      4,
				ManifestShards: 4,
				WriteState:     lease.ModeActive,
			},
		},
	}
	leaseStore := &stubLeaseStore{
		leases: map[tenant.RepoKey]lease.Lease{
			acmeRepo: {
				Repo:       acmeRepo,
				HomeRegion: "us-east-1",
				Epoch:      1,
				Mode:       lease.ModeActive,
				ExpiresAt:  now.Add(1 * time.Minute),
				HolderID:   "ctrl-1",
			},
			betaRepo: {
				Repo:       betaRepo,
				HomeRegion: "us-east-1",
				Epoch:      1,
				Mode:       lease.ModeActive,
				ExpiresAt:  now.Add(1 * time.Minute),
				HolderID:   "ctrl-1",
			},
		},
	}

	clientSigner, clientPub := generateTestSSHKey(t)
	hostKey, err := LoadOrGenerateHostKey("")
	if err != nil {
		t.Fatalf("LoadOrGenerateHostKey: %v", err)
	}

	auth, err := NewStaticAuthenticator(
		[]TokenEntry{
			{Token: "tok-acme", Principal: Principal{ActorID: "alice", TenantID: "acme"}},
			{Token: "tok-beta", Principal: Principal{ActorID: "bob", TenantID: "beta"}},
		},
		[]SSHKeyEntry{
			{Key: clientPub, Principal: Principal{ActorID: "alice", TenantID: "acme"}},
		},
	)
	if err != nil {
		t.Fatalf("NewStaticAuthenticator: %v", err)
	}

	verifier := lease.NewStoreVerifier(leaseStore, clk, 0)
	resolver, err := NewResolver("us-east-1", metaStore, verifier, 30*time.Second, clk)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	router := NewRendezvousRouter([]string{peerAddr})
	dialer := NewGRPCNodeDialer()
	proxy, err := NewWireProxy(dialer)
	if err != nil {
		t.Fatalf("NewWireProxy: %v", err)
	}

	tel, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	limiter := NewTokenBucketLimiter(limCfg)
	httpHandler, err := NewHTTPHandler(auth, limiter, resolver, router, proxy, metrics, nil)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	sshServer, err := NewSSHServer(hostKey, auth, limiter, resolver, router, proxy, metrics, nil)
	if err != nil {
		t.Fatalf("NewSSHServer: %v", err)
	}

	return &testEdgeEnv{
		handler:   httpHandler,
		sshServer: sshServer,
		node:      node,
		clk:       clk,
		tel:       tel,
		clientKey: clientSigner,
		peerAddr:  peerAddr,
		cleanupFns: []func(){
			func() { _ = tel.Shutdown(context.Background()) },
			func() { _ = dialer.Close() },
			stopNode,
		},
	}
}

func TestHTTPHandler_SmartHTTPEndpointsAndTracing(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	// 1. GET /info/refs?service=git-upload-pack (Bearer auth)
	req := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer tok-acme")
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("info/refs status = %d, body = %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-git-upload-pack-advertisement" {
		t.Fatalf("unexpected Content-Type: %q", ct)
	}
	expectedPrefix := "001e# service=git-upload-pack\n0000"
	if !strings.HasPrefix(rec.Body.String(), expectedPrefix) {
		t.Fatalf("missing smart HTTP pkt-line prefix: got %q", rec.Body.String())
	}

	meta, _ := env.node.snapshot()
	if meta.TenantID != "acme" || meta.RepoID != "core" || meta.Operation != rpc.OpUploadPack {
		t.Fatalf("unexpected rpc meta: %+v", meta)
	}
	if meta.TraceParent == "" {
		t.Fatalf("expected W3C traceparent to be propagated over gRPC")
	}

	// 2. POST /git-upload-pack (Basic auth)
	basic := base64.StdEncoding.EncodeToString([]byte("alice:tok-acme"))
	req = httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want-payload"))
	req.Header.Set("Authorization", "Basic "+basic)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("upload-pack status = %d, body = %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-git-upload-pack-result" {
		t.Fatalf("unexpected upload-pack Content-Type: %q", ct)
	}
	_, payload := env.node.snapshot()
	if string(payload) != "want-payload" {
		t.Fatalf("expected proxied payload %q, got %q", "want-payload", string(payload))
	}

	// 3. POST /git-receive-pack (valid write lease)
	req = httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("PACK-data"))
	req.Header.Set("Authorization", "Bearer tok-acme")
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("receive-pack status = %d, body = %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-git-receive-pack-result" {
		t.Fatalf("unexpected receive-pack Content-Type: %q", ct)
	}

	// 4. Verify OTel spans, OTel metrics (bounded cardinality), /healthz, and /metrics disabled
	spans := env.tel.SpanExporter.GetSpans()
	if len(spans) < 3 {
		t.Fatalf("expected at least 3 edge.ingress spans, got %d", len(spans))
	}
	if spans[0].Name != "edge.ingress" {
		t.Fatalf("expected root span edge.ingress, got %q", spans[0].Name)
	}

	var rm metricdata.ResourceMetrics
	if err := env.tel.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect OTel metrics: %v", err)
	}
	var sawReqTotal, sawReqDur bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "stravin_edge_requests_total":
				sawReqTotal = true
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok || len(sum.DataPoints) == 0 {
					t.Fatalf("expected non-empty int64 sum for stravin_edge_requests_total, got %T", m.Data)
				}
				for _, dp := range sum.DataPoints {
					if dp.Attributes.HasValue("tenant_id") || dp.Attributes.HasValue("repo_id") {
						t.Fatalf("high-cardinality labels leaked into OTel counter: %v", dp.Attributes)
					}
				}
			case "stravin_edge_request_duration_seconds":
				sawReqDur = true
				hist, ok := m.Data.(metricdata.Histogram[float64])
				if !ok || len(hist.DataPoints) == 0 {
					t.Fatalf("expected non-empty float64 histogram for stravin_edge_request_duration_seconds, got %T", m.Data)
				}
				for _, dp := range hist.DataPoints {
					if dp.Attributes.HasValue("tenant_id") || dp.Attributes.HasValue("repo_id") {
						t.Fatalf("high-cardinality labels leaked into OTel histogram: %v", dp.Attributes)
					}
				}
			}
		}
	}
	if !sawReqTotal || !sawReqDur {
		t.Fatalf("expected OTel metrics stravin_edge_requests_total (%v) and stravin_edge_request_duration_seconds (%v)", sawReqTotal, sawReqDur)
	}

	healthRec := httptest.NewRecorder()
	env.handler.ServeHTTP(healthRec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if healthRec.Code != http.StatusOK {
		t.Fatalf("healthz status = %d", healthRec.Code)
	}

	metricsRec := httptest.NewRecorder()
	env.handler.ServeHTTP(metricsRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /metrics on public HTTPHandler, got status=%d body=%q", metricsRec.Code, metricsRec.Body.String())
	}
	if strings.Contains(metricsRec.Body.String(), "stravin_edge_requests_total") {
		t.Fatalf("metrics leaked in 404 body: %q", metricsRec.Body.String())
	}
}

func TestHTTPHandler_AuthCrossTenantTraversalRateLimitAndLease(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{
		RequestsPerSecond: 1,
		RequestBurst:      1,
	})
	defer env.close()

	// Missing auth -> 401
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for missing auth, got %d", rec.Code)
	}

	// Cross-tenant access -> 403
	req := httptest.NewRequest(http.MethodGet, "/beta/secret.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer tok-acme")
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for cross-tenant access, got %d", rec.Code)
	}

	// Path traversal -> 404
	req = httptest.NewRequest(http.MethodGet, "/acme/%2e%2e/beta.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer tok-acme")
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for path traversal, got %d", rec.Code)
	}

	// Rate limit -> 1st succeeds, 2nd returns 429
	req1 := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req1.Header.Set("Authorization", "Bearer tok-acme")
	rec1 := httptest.NewRecorder()
	env.handler.ServeHTTP(rec1, req1)
	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 on first request, got %d", rec1.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req2.Header.Set("Authorization", "Bearer tok-acme")
	rec2 := httptest.NewRecorder()
	env.handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on second request, got %d", rec2.Code)
	}

	// Refill RPS bucket, then test non-home region write -> 503 fail-closed
	env.clk.Advance(2 * time.Second)
	reqNonHome := httptest.NewRequest(http.MethodPost, "/acme/west.git/git-receive-pack", strings.NewReader("PACK"))
	reqNonHome.Header.Set("Authorization", "Bearer tok-acme")
	recNonHome := httptest.NewRecorder()
	env.handler.ServeHTTP(recNonHome, reqNonHome)
	if recNonHome.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for non-home region write, got %d", recNonHome.Code)
	}

	// Advance past lease expiry (1m) -> expired write lease -> 503 fail-closed
	env.clk.Advance(2 * time.Minute)
	reqExpired := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("PACK"))
	reqExpired.Header.Set("Authorization", "Bearer tok-acme")
	recExpired := httptest.NewRecorder()
	env.handler.ServeHTTP(recExpired, reqExpired)
	if recExpired.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for expired write lease, got %d", recExpired.Code)
	}
}

func TestHTTPHandler_BandwidthLimitReturns429WithoutDeadlock(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{
		PackBytesPerMin: 16,
	})
	defer env.close()

	done := make(chan int, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader(strings.Repeat("P", 64)))
		req.Header.Set("Authorization", "Bearer tok-acme")
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)
		done <- rec.Code
	}()

	select {
	case code := <-done:
		if code != http.StatusTooManyRequests {
			t.Fatalf("expected 429 on pack bandwidth limit exceeded, got %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ProxyStream deadlocked when ByteLimitingReader returned ErrBandwidthLimited")
	}
}

func TestHTTPHandler_SetsSmartHTTPGRPCMetadataHeaders(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	req := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer tok-acme")
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("info/refs status = %d, want 200", rec.Code)
	}
	env.node.mu.Lock()
	advVals := env.node.lastMD.Get(headerAdvertiseRefs)
	statelessVals := env.node.lastMD.Get(headerStatelessRPC)
	env.node.mu.Unlock()
	if len(advVals) != 1 || advVals[0] != "true" {
		t.Fatalf("info/refs %s = %v, want [true]", headerAdvertiseRefs, advVals)
	}
	if len(statelessVals) != 0 {
		t.Fatalf("info/refs %s = %v, want empty", headerStatelessRPC, statelessVals)
	}

	reqPost := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("0000"))
	reqPost.Header.Set("Authorization", "Bearer tok-acme")
	recPost := httptest.NewRecorder()
	env.handler.ServeHTTP(recPost, reqPost)
	if recPost.Code != http.StatusOK {
		t.Fatalf("receive-pack status = %d, want 200", recPost.Code)
	}
	env.node.mu.Lock()
	advVals = env.node.lastMD.Get(headerAdvertiseRefs)
	statelessVals = env.node.lastMD.Get(headerStatelessRPC)
	env.node.mu.Unlock()
	if len(statelessVals) != 1 || statelessVals[0] != "true" {
		t.Fatalf("receive-pack %s = %v, want [true]", headerStatelessRPC, statelessVals)
	}
	if len(advVals) != 0 {
		t.Fatalf("receive-pack %s = %v, want empty", headerAdvertiseRefs, advVals)
	}
}

func gzipCompress(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := zw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func TestHTTPHandler_GzipRequestBodyDecompression(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	cases := []struct {
		name     string
		path     string
		encoding string
		payload  string
		wantCT   string
		wantOp   rpc.Operation
		compress bool
	}{
		{
			name:     "gzip git-upload-pack",
			path:     "/acme/core.git/git-upload-pack",
			encoding: "gzip",
			payload:  "0032want 0123456789abcdef0123456789abcdef01234567\n00000009done\n",
			wantCT:   "application/x-git-upload-pack-result",
			wantOp:   rpc.OpUploadPack,
			compress: true,
		},
		{
			name:     "gzip case and whitespace git-upload-pack",
			path:     "/acme/core.git/git-upload-pack",
			encoding: "  GZip  ",
			payload:  "want-mixed-case-payload",
			wantCT:   "application/x-git-upload-pack-result",
			wantOp:   rpc.OpUploadPack,
			compress: true,
		},
		{
			name:     "x-gzip git-receive-pack",
			path:     "/acme/core.git/git-receive-pack",
			encoding: "x-gzip",
			payload:  "PACK-decompressed-receive-pack-payload",
			wantCT:   "application/x-git-receive-pack-result",
			wantOp:   rpc.OpReceivePack,
			compress: true,
		},
		{
			name:     "gzip git-receive-pack",
			path:     "/acme/core.git/git-receive-pack",
			encoding: "gzip",
			payload:  "0000PACK-data-from-gzip",
			wantCT:   "application/x-git-receive-pack-result",
			wantOp:   rpc.OpReceivePack,
			compress: true,
		},
		{
			name:     "identity encoding passthrough",
			path:     "/acme/core.git/git-upload-pack",
			encoding: "identity",
			payload:  "plain-identity-payload",
			wantCT:   "application/x-git-upload-pack-result",
			wantOp:   rpc.OpUploadPack,
			compress: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bodyBytes := []byte(tc.payload)
			if tc.compress {
				bodyBytes = gzipCompress(t, bodyBytes)
			}
			req := httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader(bodyBytes))
			req.Header.Set("Authorization", "Bearer tok-acme")
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body = %q", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != tc.wantCT {
				t.Fatalf("Content-Type = %q, want %q", ct, tc.wantCT)
			}
			meta, gotPayload := env.node.snapshot()
			if meta.Operation != tc.wantOp {
				t.Fatalf("operation = %q, want %q", meta.Operation, tc.wantOp)
			}
			if string(gotPayload) != tc.payload {
				t.Fatalf("upstream payload = %q, want %q", string(gotPayload), tc.payload)
			}
		})
	}
}

func TestHTTPHandler_GzipCorruptAndUnsupportedEncodings(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	// 1. Corrupt gzip header -> 400 Bad Request
	t.Run("corrupt gzip header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("not-a-gzip-stream"))
		req.Header.Set("Authorization", "Bearer tok-acme")
		req.Header.Set("Content-Encoding", "gzip")
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400; body = %q", rec.Code, rec.Body.String())
		}
	})

	// 2. Truncated mid-stream gzip payload -> 400 Bad Request without panic or deadlock
	t.Run("mid-stream truncated gzip", func(t *testing.T) {
		validGzip := gzipCompress(t, bytes.Repeat([]byte("git-pack-object-data-"), 4096))
		// Keep valid gzip header + partial deflate blocks, truncate the rest + CRC trailer.
		truncated := validGzip[:len(validGzip)/2]

		done := make(chan int, 1)
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", bytes.NewReader(truncated))
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header.Set("Content-Encoding", "gzip")
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)
			done <- rec.Code
		}()

		select {
		case code := <-done:
			if code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("ServeHTTP deadlocked on mid-stream truncated gzip body")
		}
	})

	// 3. Unsupported Content-Encoding values -> 415 Unsupported Media Type
	for _, enc := range []string{"br", "deflate", "zstd", "gzip, deflate"} {
		t.Run("unsupported_"+enc, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("payload"))
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header.Set("Content-Encoding", enc)
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("Content-Encoding %q: status = %d, want 415; body = %q", enc, rec.Code, rec.Body.String())
			}
		})
	}

	// 4. Multiple Content-Encoding header lines -> 415 Unsupported Media Type
	t.Run("multiple Content-Encoding headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("payload"))
		req.Header.Set("Authorization", "Bearer tok-acme")
		req.Header.Add("Content-Encoding", "gzip")
		req.Header.Add("Content-Encoding", "br")
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("multiple headers: status = %d, want 415; body = %q", rec.Code, rec.Body.String())
		}
	})
}

func TestHTTPHandler_GzipBombAndDecompressedByteLimit(t *testing.T) {
	// 1. Bounded decompressed max size stops gzip bomb even when PackBytesPerMin is disabled.
	t.Run("max decompressed body size enforced", func(t *testing.T) {
		env := newTestEdgeEnv(t, LimiterConfig{})
		defer env.close()
		env.handler.SetMaxDecompressedBodyBytes(1024)

		// Compress 128 KiB of zeroes into a tiny gzip payload (~200 bytes).
		bomb := gzipCompress(t, bytes.Repeat([]byte{0}, 128*1024))
		if len(bomb) >= 1024 {
			t.Fatalf("expected compressed bomb < 1024 bytes, got %d", len(bomb))
		}

		done := make(chan int, 1)
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", bytes.NewReader(bomb))
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header.Set("Content-Encoding", "gzip")
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)
			done <- rec.Code
		}()

		select {
		case code := <-done:
			if code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want 413 Request Entity Too Large", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("ServeHTTP deadlocked on gzip bomb exceeding MaxDecompressedBodyBytes")
		}
	})

	// 2. ByteLimitingReader enforces tenant PackBytesPerMin on decompressed bytes, not compressed wire bytes.
	t.Run("tenant bandwidth limit enforced on decompressed stream", func(t *testing.T) {
		// Compressed payload is ~60 bytes (< 1024), but decompressed payload is 4096 bytes (> 1024).
		env := newTestEdgeEnv(t, LimiterConfig{
			PackBytesPerMin: 1024,
		})
		defer env.close()

		compressed := gzipCompress(t, bytes.Repeat([]byte("A"), 4096))
		if len(compressed) >= 1024 {
			t.Fatalf("expected compressed payload < 1024 bytes, got %d", len(compressed))
		}

		done := make(chan int, 1)
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", bytes.NewReader(compressed))
			req.Header.Set("Authorization", "Bearer tok-acme")
			req.Header.Set("Content-Encoding", "gzip")
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)
			done <- rec.Code
		}()

		select {
		case code := <-done:
			if code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429 Too Many Requests", code)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("ServeHTTP deadlocked when decompressed stream exceeded PackBytesPerMin")
		}
	})
}

type edgeRequestMetricKey struct {
	statusCode string
	errorClass string
}

func collectEdgeRequestCounts(t *testing.T, tel *telemetry.Provider) map[edgeRequestMetricKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tel.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect OTel metrics: %v", err)
	}
	counts := make(map[edgeRequestMetricKey]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_edge_requests_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("expected int64 sum for stravin_edge_requests_total, got %T", m.Data)
			}
			for _, dp := range sum.DataPoints {
				var k edgeRequestMetricKey
				if v, ok := dp.Attributes.Value(attribute.Key("status_code")); ok {
					k.statusCode = v.AsString()
				}
				if v, ok := dp.Attributes.Value(attribute.Key("error_class")); ok {
					k.errorClass = v.AsString()
				}
				counts[k] += dp.Value
			}
		}
	}
	return counts
}

func TestHTTPHandler_ClientCanceledClassifiedAs499(t *testing.T) {
	t.Run("canceled request context before headers records 499 client_canceled", func(t *testing.T) {
		env := newTestEdgeEnv(t, LimiterConfig{})
		defer env.close()

		started := make(chan struct{})
		env.node.mu.Lock()
		env.node.onHandle = func(ctx context.Context, _ rpc.RequestMeta, _ rpc.GitWireStream) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}
		env.node.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want")).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer tok-acme")
		rec := httptest.NewRecorder()

		done := make(chan struct{})
		go func() {
			defer close(done)
			env.handler.ServeHTTP(rec, req)
		}()

		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for upstream node to start")
		}
		cancel()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("ServeHTTP did not return after client context cancellation")
		}

		if rec.Code != httpStatusClientClosedRequest {
			t.Fatalf("HTTP status = %d, want %d", rec.Code, httpStatusClientClosedRequest)
		}
		counts := collectEdgeRequestCounts(t, env.tel)
		if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 1 {
			t.Fatalf("expected 1 (499, client_canceled) metric, got %d (all: %+v)", got, counts)
		}
		if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 0 {
			t.Fatalf("expected 0 (502, upstream) metrics on client cancel, got %d", got)
		}
	})

	t.Run("canceled request context mid-stream after headers records 499 client_canceled", func(t *testing.T) {
		env := newTestEdgeEnv(t, LimiterConfig{})
		defer env.close()

		sentChunk := make(chan struct{})
		env.node.mu.Lock()
		env.node.onHandle = func(ctx context.Context, _ rpc.RequestMeta, stream rpc.GitWireStream) error {
			if err := stream.Send(&rpc.Frame{Data: []byte("partial-pack-chunk")}); err != nil {
				return err
			}
			close(sentChunk)
			<-ctx.Done()
			return ctx.Err()
		}
		env.node.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want")).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer tok-acme")
		rec := httptest.NewRecorder()

		done := make(chan struct{})
		go func() {
			defer close(done)
			env.handler.ServeHTTP(rec, req)
		}()

		select {
		case <-sentChunk:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for initial chunk")
		}
		// Give the proxy goroutine a moment to flush the first chunk to the ResponseWriter.
		cancel()

		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("ServeHTTP did not return after mid-stream client cancellation")
		}

		counts := collectEdgeRequestCounts(t, env.tel)
		if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 1 {
			t.Fatalf("expected 1 (499, client_canceled) metric, got %d (all: %+v)", got, counts)
		}
		if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 0 {
			t.Fatalf("expected 0 (502, upstream) metrics on mid-stream client cancel, got %d", got)
		}
	})

	t.Run("grpc codes.Canceled from proxy records 499 client_canceled", func(t *testing.T) {
		env := newTestEdgeEnv(t, LimiterConfig{})
		defer env.close()

		env.node.mu.Lock()
		env.node.onHandle = func(_ context.Context, _ rpc.RequestMeta, _ rpc.GitWireStream) error {
			return status.Error(codes.Canceled, "client aborted stream")
		}
		env.node.mu.Unlock()

		req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want"))
		req.Header.Set("Authorization", "Bearer tok-acme")
		rec := httptest.NewRecorder()
		env.handler.ServeHTTP(rec, req)

		if rec.Code != httpStatusClientClosedRequest {
			t.Fatalf("HTTP status = %d, want %d", rec.Code, httpStatusClientClosedRequest)
		}
		counts := collectEdgeRequestCounts(t, env.tel)
		if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 1 {
			t.Fatalf("expected 1 (499, client_canceled) metric, got %d (all: %+v)", got, counts)
		}
		if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 0 {
			t.Fatalf("expected 0 (502, upstream) metrics on codes.Canceled, got %d", got)
		}
	})

	t.Run("handleProxyError classifies direct context.Canceled and wrapped codes.Canceled", func(t *testing.T) {
		cases := []struct {
			name string
			ctx  func() context.Context
			err  error
		}{
			{
				name: "direct context.Canceled",
				ctx:  context.Background,
				err:  fmt.Errorf("proxy read: %w", context.Canceled),
			},
			{
				name: "wrapped grpc codes.Canceled",
				ctx:  context.Background,
				err:  fmt.Errorf("edge: open stream: %w", status.Error(codes.Canceled, "canceled")),
			},
			{
				name: "canceled request ctx with generic transport error",
				ctx: func() context.Context {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					return ctx
				},
				err: io.ErrUnexpectedEOF,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				env := newTestEdgeEnv(t, LimiterConfig{})
				defer env.close()

				rec := httptest.NewRecorder()
				sw := &smartHTTPStreamWriter{w: rec, contentType: "application/x-git-upload-pack-result"}
				env.handler.handleProxyError(rec, tc.ctx(), sw, "fetch", tc.err, time.Now())

				if rec.Code != httpStatusClientClosedRequest {
					t.Fatalf("HTTP status = %d, want %d", rec.Code, httpStatusClientClosedRequest)
				}
				counts := collectEdgeRequestCounts(t, env.tel)
				if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 1 {
					t.Fatalf("expected 1 (499, client_canceled) metric, got %d (all: %+v)", got, counts)
				}
				if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 0 {
					t.Fatalf("expected 0 (502, upstream) metrics, got %d", got)
				}
			})
		}
	})

	t.Run("real upstream errors still record 502 upstream", func(t *testing.T) {
		t.Run("grpc codes.Internal", func(t *testing.T) {
			env := newTestEdgeEnv(t, LimiterConfig{})
			defer env.close()

			env.node.mu.Lock()
			env.node.onHandle = func(_ context.Context, _ rpc.RequestMeta, _ rpc.GitWireStream) error {
				return status.Error(codes.Internal, "disk failure")
			}
			env.node.mu.Unlock()

			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want"))
			req.Header.Set("Authorization", "Bearer tok-acme")
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadGateway {
				t.Fatalf("HTTP status = %d, want 502", rec.Code)
			}
			counts := collectEdgeRequestCounts(t, env.tel)
			if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 1 {
				t.Fatalf("expected 1 (502, upstream) metric, got %d (all: %+v)", got, counts)
			}
			if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 0 {
				t.Fatalf("expected 0 (499, client_canceled) metrics, got %d", got)
			}
		})

		t.Run("non-zero git exit code", func(t *testing.T) {
			env := newTestEdgeEnv(t, LimiterConfig{})
			defer env.close()

			env.node.mu.Lock()
			env.node.exitCode = 128
			env.node.mu.Unlock()

			req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", strings.NewReader("want"))
			req.Header.Set("Authorization", "Bearer tok-acme")
			rec := httptest.NewRecorder()
			env.handler.ServeHTTP(rec, req)

			counts := collectEdgeRequestCounts(t, env.tel)
			if got := counts[edgeRequestMetricKey{statusCode: "502", errorClass: "upstream"}]; got != 1 {
				t.Fatalf("expected 1 (502, upstream) metric on non-zero exit, got %d (all: %+v)", got, counts)
			}
			if got := counts[edgeRequestMetricKey{statusCode: "499", errorClass: "client_canceled"}]; got != 0 {
				t.Fatalf("expected 0 (499, client_canceled) metrics, got %d", got)
			}
		})
	})
}
