package edge

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"stravin.dev/core/internal/health"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 120 * time.Second

	headerAdvertiseRefs = "x-stravin-advertise-refs"
	headerStatelessRPC  = "x-stravin-stateless-rpc"
)

// HTTPHandler serves Git Smart HTTP endpoints alongside Kubernetes health probes.
type HTTPHandler struct {
	auth     Authenticator
	limiter  Limiter
	resolver *Resolver
	router   NodeSelector
	proxy    *WireProxy
	metrics  *Metrics
	logger   *slog.Logger
	probes   *health.Registry
}

// NewHTTPHandler constructs a Smart HTTP handler with all required collaborators.
func NewHTTPHandler(
	auth Authenticator,
	limiter Limiter,
	resolver *Resolver,
	router NodeSelector,
	proxy *WireProxy,
	metrics *Metrics,
	logger *slog.Logger,
) (*HTTPHandler, error) {
	if auth == nil {
		return nil, errors.New("edge: authenticator is required")
	}
	if resolver == nil {
		return nil, errors.New("edge: resolver is required")
	}
	if router == nil {
		return nil, errors.New("edge: node selector is required")
	}
	if proxy == nil {
		return nil, errors.New("edge: wire proxy is required")
	}
	if metrics == nil {
		var err error
		metrics, err = NewMetrics(nil)
		if err != nil {
			return nil, err
		}
	}
	if logger == nil {
		logger = slog.Default()
	}
	probes := health.New("stravin-edge")
	probes.MarkStarted()
	probes.MarkReady()
	return &HTTPHandler{
		auth:     auth,
		limiter:  limiter,
		resolver: resolver,
		router:   router,
		proxy:    proxy,
		metrics:  metrics,
		logger:   logger,
		probes:   probes,
	}, nil
}

// SetProbes attaches a lifecycle-managed health probe registry to the handler.
func (h *HTTPHandler) SetProbes(probes *health.Registry) {
	if probes != nil {
		h.probes = probes
	}
}

// ServeHTTP dispatches health probes and Smart HTTP Git endpoints.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.probes.ServeProbe(w, r) {
		return
	}

	start := time.Now()
	reqPath := r.URL.RawPath
	if reqPath == "" {
		reqPath = r.URL.Path
	}

	route, err := parseSmartHTTPPath(r.Method, reqPath, r.URL.Query().Get("service"))
	if err != nil {
		h.metrics.RecordRequest(r.Context(), "http", "unknown", strconv.Itoa(http.StatusNotFound), "invalid_request", time.Since(start))
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	ctx := telemetry.ExtractHTTP(r.Context(), r.Header)
	ctx, span := h.metrics.StartIngressSpan(ctx, "http", route.metricOp, string(route.repo.TenantID), string(route.repo.RepoID), "")
	defer span.End()

	token, ok := extractHTTPToken(r.Header.Get("Authorization"))
	if !ok {
		h.writeUnauthorized(w, ctx, route.metricOp, start)
		return
	}

	principal, err := h.auth.AuthenticateToken(ctx, token)
	if err != nil {
		h.writeUnauthorized(w, ctx, route.metricOp, start)
		return
	}

	if err := AuthorizeTenant(principal, route.repo.TenantID); err != nil {
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusForbidden), "forbidden", time.Since(start))
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if h.limiter != nil {
		if err := h.limiter.AllowRequest(ctx, route.repo.TenantID); err != nil {
			h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusTooManyRequests), "rate_limited", time.Since(start))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		if route.kind == routeUploadPack {
			release, err := h.limiter.AcquireClone(ctx, route.repo.TenantID)
			if err != nil {
				h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusTooManyRequests), "rate_limited", time.Since(start))
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			defer release()
		}
	}

	var homeRegion string
	if route.op == rpc.OpReceivePack {
		dec, err := h.resolver.ResolveWrite(ctx, route.repo)
		if err != nil {
			h.handleResolveError(w, ctx, route.metricOp, err, start)
			return
		}
		homeRegion = string(dec.HomeRegion)
	} else {
		meta, err := h.resolver.ResolveRead(ctx, route.repo)
		if err != nil {
			h.handleResolveError(w, ctx, route.metricOp, err, start)
			return
		}
		homeRegion = string(meta.HomeRegion)
	}
	span.SetAttributes(telemetry.AttrHomeRegion.String(homeRegion))

	peer, err := h.router.SelectNode(route.repo)
	if err != nil {
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusServiceUnavailable), "upstream", time.Since(start))
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	rpcMeta := rpc.RequestMeta{
		TenantID:    route.repo.TenantID,
		RepoID:      route.repo.RepoID,
		Operation:   route.op,
		TraceParent: telemetry.TraceparentFromContext(ctx),
	}

	sw := &smartHTTPStreamWriter{
		w:           w,
		contentType: route.contentType,
		prefix:      route.responsePrefix,
	}

	var stdin = NewByteLimitingReader(ctx, r.Body, h.limiter, route.repo.TenantID)
	proxyCtx := ctx
	switch route.kind {
	case routeInfoRefs:
		stdin = nil
		proxyCtx = metadata.AppendToOutgoingContext(ctx, headerAdvertiseRefs, "true")
	case routeUploadPack, routeReceivePack:
		proxyCtx = metadata.AppendToOutgoingContext(ctx, headerStatelessRPC, "true")
	}

	exitCode, err := h.proxy.ProxyStream(proxyCtx, peer, rpcMeta, stdin, sw, nil)
	if err != nil {
		h.handleProxyError(w, ctx, sw, route.metricOp, err, start)
		return
	}

	// Ensure headers/prefix are flushed even if upstream returned an empty stream.
	sw.ensureHeader()
	if exitCode != 0 {
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusBadGateway), "upstream", time.Since(start))
		h.logger.WarnContext(ctx, "git smart http upstream non-zero exit",
			"tenant_id", string(route.repo.TenantID),
			"repo_id", string(route.repo.RepoID),
			"operation", string(route.op),
			"exit_code", exitCode,
		)
		return
	}

	h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusOK), "", time.Since(start))
}

func (h *HTTPHandler) writeUnauthorized(w http.ResponseWriter, ctx context.Context, metricOp string, start time.Time) {
	w.Header().Set("WWW-Authenticate", `Basic realm="stravin"`)
	h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusUnauthorized), "auth", time.Since(start))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func (h *HTTPHandler) handleResolveError(w http.ResponseWriter, ctx context.Context, metricOp string, err error, start time.Time) {
	switch {
	case errors.Is(err, ErrRepoNotFound):
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusNotFound), "not_found", time.Since(start))
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, ErrForbidden):
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusForbidden), "forbidden", time.Since(start))
		http.Error(w, "forbidden", http.StatusForbidden)
	case errors.Is(err, ErrWriteUnavailable):
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusServiceUnavailable), "lease_unavailable", time.Since(start))
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	default:
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusServiceUnavailable), "upstream", time.Since(start))
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}
}

func (h *HTTPHandler) handleProxyError(w http.ResponseWriter, ctx context.Context, sw *smartHTTPStreamWriter, metricOp string, err error, start time.Time) {
	if errors.Is(err, ErrRateLimited) {
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusTooManyRequests), "rate_limited", time.Since(start))
		if !sw.wroteHeader {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		}
		return
	}
	h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusBadGateway), "upstream", time.Since(start))
	if !sw.wroteHeader {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
}

type routeKind int

const (
	routeInfoRefs routeKind = iota + 1
	routeUploadPack
	routeReceivePack
)

type smartHTTPRoute struct {
	kind           routeKind
	repo           tenant.RepoKey
	op             rpc.Operation
	metricOp       string
	contentType    string
	responsePrefix []byte
}

func parseSmartHTTPPath(method, rawPath, service string) (smartHTTPRoute, error) {
	if rawPath == "" || strings.ContainsAny(rawPath, "\\\x00") {
		return smartHTTPRoute{}, errors.New("edge: invalid path")
	}
	lower := strings.ToLower(rawPath)
	if strings.Contains(lower, "..") || strings.Contains(lower, "%2e") || strings.Contains(lower, "%2f") || strings.Contains(lower, "%5c") {
		return smartHTTPRoute{}, errors.New("edge: path traversal rejected")
	}

	trimmed := strings.TrimPrefix(rawPath, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 3 {
		return smartHTTPRoute{}, errors.New("edge: incomplete repo path")
	}

	tenantRaw := parts[0]
	repoSeg := parts[1]
	if !strings.HasSuffix(repoSeg, ".git") {
		return smartHTTPRoute{}, errors.New("edge: repo segment must end with .git")
	}
	repoRaw := strings.TrimSuffix(repoSeg, ".git")

	repoKey, err := tenant.NewRepoKey(tenantRaw, repoRaw)
	if err != nil {
		return smartHTTPRoute{}, err
	}

	suffix := strings.Join(parts[2:], "/")
	switch {
	case method == http.MethodGet && suffix == "info/refs":
		op := rpc.Operation(service)
		if err := op.Validate(); err != nil {
			return smartHTTPRoute{}, err
		}
		return smartHTTPRoute{
			kind:           routeInfoRefs,
			repo:           repoKey,
			op:             op,
			metricOp:       "ls-refs",
			contentType:    fmt.Sprintf("application/x-%s-advertisement", op),
			responsePrefix: []byte(pktLine(fmt.Sprintf("# service=%s\n", op)) + "0000"),
		}, nil

	case method == http.MethodPost && suffix == string(rpc.OpUploadPack):
		return smartHTTPRoute{
			kind:        routeUploadPack,
			repo:        repoKey,
			op:          rpc.OpUploadPack,
			metricOp:    "fetch",
			contentType: "application/x-git-upload-pack-result",
		}, nil

	case method == http.MethodPost && suffix == string(rpc.OpReceivePack):
		return smartHTTPRoute{
			kind:        routeReceivePack,
			repo:        repoKey,
			op:          rpc.OpReceivePack,
			metricOp:    "push",
			contentType: "application/x-git-receive-pack-result",
		}, nil

	default:
		return smartHTTPRoute{}, errors.New("edge: unsupported smart http endpoint")
	}
}

func pktLine(payload string) string {
	return fmt.Sprintf("%04x%s", len(payload)+4, payload)
}

func extractHTTPToken(authHeader string) (string, bool) {
	authHeader = strings.TrimSpace(authHeader)
	if authHeader == "" {
		return "", false
	}
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return "", false
	}
	scheme := strings.ToLower(strings.TrimSpace(parts[0]))
	value := strings.TrimSpace(parts[1])
	switch scheme {
	case "bearer":
		if value == "" {
			return "", false
		}
		return value, true
	case "basic":
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return "", false
		}
		user, pass, found := strings.Cut(string(decoded), ":")
		if !found {
			return "", false
		}
		if pass != "" {
			return pass, true
		}
		if user != "" {
			return user, true
		}
		return "", false
	default:
		return "", false
	}
}

type smartHTTPStreamWriter struct {
	w           http.ResponseWriter
	contentType string
	prefix      []byte
	wroteHeader bool
}

func (s *smartHTTPStreamWriter) ensureHeader() {
	if s.wroteHeader {
		return
	}
	s.wroteHeader = true
	s.w.Header().Set("Content-Type", s.contentType)
	s.w.Header().Set("Cache-Control", "no-cache")
	s.w.WriteHeader(http.StatusOK)
	if len(s.prefix) > 0 {
		_, _ = s.w.Write(s.prefix)
	}
}

func (s *smartHTTPStreamWriter) Write(p []byte) (int, error) {
	s.ensureHeader()
	n, err := s.w.Write(p)
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return n, err
}

// NewHTTPServer creates an http.Server with hardened timeouts for Smart HTTP streaming.
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
}
