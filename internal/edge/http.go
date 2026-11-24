package edge

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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

	// httpStatusClientClosedRequest is the non-standard HTTP status code (499)
	// recorded when a client disconnects or cancels the request mid-stream.
	httpStatusClientClosedRequest = 499

	// DefaultMaxDecompressedBodyBytes bounds decompressed Smart HTTP POST bodies (256 MiB)
	// to mitigate gzip-bomb resource exhaustion.
	DefaultMaxDecompressedBodyBytes int64 = 256 << 20

	httpStreamSmallChunkThreshold = 32 * 1024
	httpStreamFlushThreshold      = 256 * 1024

	infoRefsRetryBackoff = 15 * time.Millisecond
)

var (
	// ErrInvalidGzipBody is returned when a Content-Encoding: gzip request body is corrupt or truncated.
	ErrInvalidGzipBody = errors.New("edge: invalid gzip request body")
	// ErrDecompressedBodyTooLarge is returned when a decompressed request body exceeds the configured byte limit.
	ErrDecompressedBodyTooLarge = errors.New("edge: decompressed request body too large")

	errUnsupportedContentEncoding = errors.New("edge: unsupported content-encoding")
)

// HTTPHandler serves Git Smart HTTP endpoints alongside Kubernetes health probes.
type HTTPHandler struct {
	auth                     Authenticator
	limiter                  Limiter
	resolver                 *Resolver
	router                   NodeSelector
	proxy                    *WireProxy
	metrics                  *Metrics
	logger                   *slog.Logger
	probes                   *health.Registry
	packCache                *PackCacheCoordinator
	maxDecompressedBodyBytes int64
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
		auth:                     auth,
		limiter:                  limiter,
		resolver:                 resolver,
		router:                   router,
		proxy:                    proxy,
		metrics:                  metrics,
		logger:                   logger,
		probes:                   probes,
		maxDecompressedBodyBytes: DefaultMaxDecompressedBodyBytes,
	}, nil
}

// SetProbes attaches a lifecycle-managed health probe registry to the handler.
func (h *HTTPHandler) SetProbes(probes *health.Registry) {
	if probes != nil {
		h.probes = probes
	}
}

// SetMaxDecompressedBodyBytes overrides the maximum allowed decompressed size for
// gzip-encoded Smart HTTP POST request bodies. Non-positive values are ignored.
func (h *HTTPHandler) SetMaxDecompressedBodyBytes(limit int64) {
	if limit > 0 {
		h.maxDecompressedBodyBytes = limit
	}
}

// SetPackCache attaches an optional external full-clone pack response cache
// coordinator. Cache lookups always run strictly after authentication,
// tenant authorization, rate limiting, and repository resolution.
func (h *HTTPHandler) SetPackCache(c *PackCacheCoordinator) {
	h.packCache = c
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
		GitProtocol: extractHTTPGitProtocol(r.Header.Values("Git-Protocol")),
	}

	sw := &smartHTTPStreamWriter{
		w:           w,
		contentType: route.contentType,
		prefix:      route.responsePrefix,
	}

	var (
		stdin        io.Reader
		stdout       io.Writer = sw
		captureW     *boundedCaptureWriter
		finishFlight func([]byte)
	)
	proxyCtx := ctx
	switch route.kind {
	case routeInfoRefs:
		proxyCtx = metadata.AppendToOutgoingContext(ctx, headerAdvertiseRefs, "true")
	case routeUploadPack, routeReceivePack:
		body, closeBody, bodyErr := openRequestBody(r.Header.Values("Content-Encoding"), r.Body, h.maxDecompressedBodyBytes)
		if bodyErr != nil {
			h.handleBodyError(w, ctx, route.metricOp, bodyErr, start)
			return
		}
		defer closeBody()
		stdin = NewByteLimitingReader(ctx, body, h.limiter, route.repo.TenantID)
		proxyCtx = metadata.AppendToOutgoingContext(ctx, headerStatelessRPC, "true")

		if route.kind == routeUploadPack && h.packCache != nil && rpc.GitProtocolVersion(rpcMeta.GitProtocol) == "2" {
			reqBytes, completeEOF, readErr := readBoundedRequest(stdin, maxV2FetchCacheReqBytes)
			if readErr != nil {
				h.handleProxyError(w, ctx, sw, route, readErr, start)
				return
			}
			switch {
			case completeEOF && IsCacheableV2FullCloneFetch(reqBytes):
				cacheKey := UploadPackCacheKey(route.repo, rpcMeta.GitProtocol, reqBytes)
				cachedResp, finish, hit, waitErr := h.packCache.GetOrJoin(ctx, cacheKey)
				if waitErr != nil {
					h.handleProxyError(w, ctx, sw, route, waitErr, start)
					return
				}
				if hit {
					if _, wErr := sw.Write(cachedResp); wErr != nil {
						h.handleProxyError(w, ctx, sw, route, wErr, start)
						return
					}
					sw.ensureHeader()
					if sw.unflushed > 0 {
						sw.flush()
					}
					h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusOK), "", time.Since(start))
					return
				}
				if finish != nil {
					finishFlight = finish
					defer finishFlight(nil)
					if capBytes := h.packCache.MaxCaptureBytes(); capBytes > 0 {
						captureW = &boundedCaptureWriter{
							dst:      sw,
							maxBytes: capBytes,
						}
						stdout = captureW
					}
				}
				stdin = bytes.NewReader(reqBytes)
			case completeEOF:
				h.metrics.RecordPackCacheLookup(ctx, "singleflight", "bypass")
				stdin = bytes.NewReader(reqBytes)
			default:
				h.metrics.RecordPackCacheLookup(ctx, "singleflight", "bypass")
				stdin = io.MultiReader(bytes.NewReader(reqBytes), stdin)
			}
		}
	}

	exitCode, err := h.proxy.ProxyStream(proxyCtx, peer, rpcMeta, stdin, stdout, nil)
	if err != nil && route.kind == routeInfoRefs && !sw.wroteHeader && isTransientInfoRefsError(ctx, err) {
		timer := time.NewTimer(infoRefsRetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
			exitCode, err = h.proxy.ProxyStream(proxyCtx, peer, rpcMeta, stdin, stdout, nil)
			h.metrics.RecordInfoRefsRetry(ctx, err == nil && exitCode == 0)
		}
	}
	if err != nil {
		h.handleProxyError(w, ctx, sw, route, err, start)
		return
	}

	// Ensure headers/prefix and any remaining buffered bytes are flushed.
	sw.ensureHeader()
	if sw.unflushed > 0 {
		sw.flush()
	}
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

	if finishFlight != nil {
		if captureW != nil && !captureW.exceeded {
			finishFlight(captureW.buf.Bytes())
		} else {
			finishFlight(nil)
		}
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

func (h *HTTPHandler) handleBodyError(w http.ResponseWriter, ctx context.Context, metricOp string, err error, start time.Time) {
	if errors.Is(err, errUnsupportedContentEncoding) {
		h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusUnsupportedMediaType), "invalid_request", time.Since(start))
		http.Error(w, "unsupported content-encoding", http.StatusUnsupportedMediaType)
		return
	}
	h.metrics.RecordRequest(ctx, "http", metricOp, strconv.Itoa(http.StatusBadRequest), "invalid_request", time.Since(start))
	http.Error(w, "invalid gzip request body", http.StatusBadRequest)
}

func (h *HTTPHandler) handleProxyError(w http.ResponseWriter, ctx context.Context, sw *smartHTTPStreamWriter, route smartHTTPRoute, err error, start time.Time) {
	switch {
	case errors.Is(err, ErrRateLimited):
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusTooManyRequests), "rate_limited", time.Since(start))
		if !sw.wroteHeader {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		}
	case errors.Is(err, ErrDecompressedBodyTooLarge):
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusRequestEntityTooLarge), "payload_too_large", time.Since(start))
		if !sw.wroteHeader {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		}
	case errors.Is(err, ErrInvalidGzipBody):
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusBadRequest), "invalid_request", time.Since(start))
		if !sw.wroteHeader {
			http.Error(w, "invalid gzip request body", http.StatusBadRequest)
		}
	case isClientCanceled(ctx, err):
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(httpStatusClientClosedRequest), "client_canceled", time.Since(start))
		if !sw.wroteHeader {
			w.WriteHeader(httpStatusClientClosedRequest)
		}
	default:
		h.metrics.RecordRequest(ctx, "http", route.metricOp, strconv.Itoa(http.StatusBadGateway), "upstream", time.Since(start))
		h.logger.WarnContext(ctx, "git smart http upstream error",
			"tenant_id", string(route.repo.TenantID),
			"repo_id", string(route.repo.RepoID),
			"operation", string(route.op),
			"error", err,
		)
		if !sw.wroteHeader {
			http.Error(w, "bad gateway", http.StatusBadGateway)
		}
	}
}

func isTransientInfoRefsError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrInvalidGzipBody) ||
		errors.Is(err, ErrDecompressedBodyTooLarge) ||
		isClientCanceled(ctx, err) {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	return true
}

func isClientCanceled(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) || (ctx != nil && errors.Is(ctx.Err(), context.Canceled)) {
		return true
	}
	if status.Code(err) == codes.Canceled {
		return true
	}
	var grpcErr interface{ GRPCStatus() *status.Status }
	return errors.As(err, &grpcErr) && grpcErr.GRPCStatus().Code() == codes.Canceled
}

func parseContentEncoding(headers []string) (string, bool) {
	var encodings []string
	for _, h := range headers {
		for _, part := range strings.Split(h, ",") {
			if v := strings.ToLower(strings.TrimSpace(part)); v != "" {
				encodings = append(encodings, v)
			}
		}
	}
	if len(encodings) == 0 {
		return "", true
	}
	if len(encodings) > 1 {
		return "", false
	}
	return encodings[0], true
}

func openRequestBody(headers []string, raw io.Reader, maxDecompressed int64) (io.Reader, func(), error) {
	enc, ok := parseContentEncoding(headers)
	if !ok {
		return nil, nil, errUnsupportedContentEncoding
	}
	switch enc {
	case "", "identity":
		return raw, func() {}, nil
	case "gzip", "x-gzip":
		gz, err := gzip.NewReader(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", ErrInvalidGzipBody, err)
		}
		if maxDecompressed <= 0 {
			maxDecompressed = DefaultMaxDecompressedBodyBytes
		}
		return &boundedGzipReader{r: gz, remaining: maxDecompressed}, func() { _ = gz.Close() }, nil
	default:
		return nil, nil, errUnsupportedContentEncoding
	}
}

type boundedGzipReader struct {
	r         *gzip.Reader
	remaining int64
}

func (b *boundedGzipReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining <= 0 {
		var probe [1]byte
		n, err := b.r.Read(probe[:])
		if n > 0 {
			return 0, ErrDecompressedBodyTooLarge
		}
		if err == nil {
			return 0, nil
		}
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		return 0, wrapGzipReadError(err)
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.r.Read(p)
	b.remaining -= int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, wrapGzipReadError(err)
	}
	return n, err
}

func wrapGzipReadError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrInvalidGzipBody, err)
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

func extractHTTPGitProtocol(values []string) string {
	if len(values) != 1 {
		return ""
	}
	clean, ok := rpc.SanitizeGitProtocol(values[0])
	if !ok {
		return ""
	}
	return clean
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
	wroteFirst  bool
	unflushed   int
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

func (s *smartHTTPStreamWriter) flush() {
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	s.unflushed = 0
}

func (s *smartHTTPStreamWriter) Write(p []byte) (int, error) {
	s.ensureHeader()
	n, err := s.w.Write(p)
	s.unflushed += n
	if !s.wroteFirst || len(p) < httpStreamSmallChunkThreshold || s.unflushed >= httpStreamFlushThreshold {
		s.wroteFirst = true
		s.flush()
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
