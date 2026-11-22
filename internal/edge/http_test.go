package edge

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

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
}

func (m *mockGitNode) HandleGitWire(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error {
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
