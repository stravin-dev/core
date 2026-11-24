package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	// HeaderAdvertiseRefs requests a pure-DynamoDB pkt-line ref advertisement without git-upload-pack execution.
	HeaderAdvertiseRefs = "x-stravin-advertise-refs"
	// HeaderStatelessRPC invokes git-upload-pack with --stateless-rpc (Smart HTTP POST).
	HeaderStatelessRPC = "x-stravin-stateless-rpc"
	// HeaderSmartHTTPServiceHeader prepends the Smart HTTP `# service=...` pkt-line header.
	HeaderSmartHTTPServiceHeader = "x-stravin-service-header"

	streamChunkSize       = 256 * 1024
	defaultBundleCacheTTL = 5 * time.Second
	maxV2PeekPktLen       = 64
)

// ErrWriteNotImplemented is returned by the default WriteHandler stub until Issue #11 plugs in git-receive-pack.
var ErrWriteNotImplemented = errors.New("node: git-receive-pack not implemented")

// WriteHandler is the GoF Strategy seam for git-receive-pack so Issue #11 can
// plug in its write-path handler without rewriting internal/node/server.go.
type WriteHandler interface {
	HandleReceivePack(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error
}

// UnimplementedWriteHandler is the default WriteHandler stub returning gRPC Unimplemented.
type UnimplementedWriteHandler struct{}

// HandleReceivePack returns codes.Unimplemented.
func (UnimplementedWriteHandler) HandleReceivePack(_ context.Context, _ rpc.RequestMeta, _ rpc.GitWireStream) error {
	return status.Error(codes.Unimplemented, ErrWriteNotImplemented.Error())
}

// BundleURIResolver resolves an optional presigned bundle URL for Protocol v2
// bundle-uri cold-clone offload. Returning ("", nil) means no bundle exists.
type BundleURIResolver interface {
	ResolveBundleURI(ctx context.Context, repo tenant.RepoKey) (string, error)
}

// BundlePresigner is the narrow S3 presign seam satisfied by *s3.Store.
type BundlePresigner interface {
	PresignBundleURL(ctx context.Context, repo tenant.RepoKey, expiry time.Duration) (string, error)
}

type bundleCacheEntry struct {
	uri       string
	expiresAt time.Time
}

// S3BundleURIResolver resolves presigned S3 bundle URLs via *s3.Store with a
// short TTL cache covering both positive presigned URLs and ErrNotFound misses.
type S3BundleURIResolver struct {
	presigner BundlePresigner
	expiry    time.Duration
	cacheTTL  time.Duration
	now       func() time.Time

	mu    sync.Mutex
	cache map[tenant.RepoKey]bundleCacheEntry
}

// NewS3BundleURIResolver creates a BundleURIResolver backed by an S3 presigner.
func NewS3BundleURIResolver(presigner BundlePresigner, expiry time.Duration) *S3BundleURIResolver {
	if presigner == nil {
		return nil
	}
	if expiry <= 0 {
		expiry = 15 * time.Minute
	}
	cacheTTL := defaultBundleCacheTTL
	if expiry < cacheTTL {
		cacheTTL = expiry
	}
	return &S3BundleURIResolver{
		presigner: presigner,
		expiry:    expiry,
		cacheTTL:  cacheTTL,
		now:       time.Now,
		cache:     make(map[tenant.RepoKey]bundleCacheEntry),
	}
}

func (r *S3BundleURIResolver) nowTime() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *S3BundleURIResolver) ttl() time.Duration {
	if r.cacheTTL > 0 {
		return r.cacheTTL
	}
	return defaultBundleCacheTTL
}

// ResolveBundleURI returns a validated presigned bundle URL or ("", nil) when absent.
func (r *S3BundleURIResolver) ResolveBundleURI(ctx context.Context, repo tenant.RepoKey) (string, error) {
	if r == nil || r.presigner == nil {
		return "", nil
	}

	now := r.nowTime()
	r.mu.Lock()
	if entry, ok := r.cache[repo]; ok && now.Before(entry.expiresAt) {
		r.mu.Unlock()
		return entry.uri, nil
	}
	r.mu.Unlock()

	u, err := r.presigner.PresignBundleURL(ctx, repo, r.expiry)
	if err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			r.storeCache(repo, "")
			return "", nil
		}
		return "", err
	}
	if err := gitexec.ValidateBundleURI(u); err != nil {
		return "", err
	}
	r.storeCache(repo, u)
	return u, nil
}

func (r *S3BundleURIResolver) storeCache(repo tenant.RepoKey, uri string) {
	exp := r.nowTime().Add(r.ttl())
	r.mu.Lock()
	if r.cache == nil {
		r.cache = make(map[tenant.RepoKey]bundleCacheEntry)
	}
	r.cache[repo] = bundleCacheEntry{uri: uri, expiresAt: exp}
	r.mu.Unlock()
}

// ServerConfig configures the stravin-node gRPC GitWire server.
type ServerConfig struct {
	Materializer           *Materializer
	GitExecutor            gitexec.GitExecutor
	WriteHandler           WriteHandler
	BundleResolver         BundleURIResolver
	LeaseRefresher         *LeaseRefresher
	Telemetry              *telemetry.Provider
	Metrics                *Metrics
	Logger                 *slog.Logger
	Region                 string
	MaxUploadPacks         int
	UploadPackQueueTimeout time.Duration
}

// Server implements rpc.GitWireHandler for stravin-node.
type Server struct {
	materializer           *Materializer
	gitExec                gitexec.GitExecutor
	writeHandler           WriteHandler
	bundleResolver         BundleURIResolver
	leaseRefresher         *LeaseRefresher
	tracer                 trace.Tracer
	metrics                *Metrics
	logger                 *slog.Logger
	region                 string
	uploadPackSem          chan struct{}
	uploadPackQueueTimeout time.Duration
}

var _ rpc.GitWireHandler = (*Server)(nil)

// NewServer constructs a stravin-node GitWire gRPC handler.
func NewServer(cfg ServerConfig) (*Server, error) {
	if cfg.Materializer == nil {
		return nil, errors.New("node: materializer is required")
	}
	if cfg.GitExecutor == nil {
		return nil, errors.New("node: git executor is required")
	}
	if cfg.MaxUploadPacks < 0 {
		return nil, errors.New("node: max upload-packs must be >= 1")
	}
	if cfg.UploadPackQueueTimeout < 0 {
		return nil, errors.New("node: upload-pack queue timeout must be > 0")
	}
	maxUploadPacks := cfg.MaxUploadPacks
	if maxUploadPacks == 0 {
		maxUploadPacks = defaultMaxUploadPacks()
	}
	queueTimeout := cfg.UploadPackQueueTimeout
	if queueTimeout == 0 {
		queueTimeout = defaultUploadPackQueueTimeout
	}
	wh := cfg.WriteHandler
	if wh == nil {
		wh = UnimplementedWriteHandler{}
	}
	tp := cfg.Telemetry
	if tp == nil {
		tp = telemetry.NewNoopProvider()
	}
	metrics := cfg.Metrics
	if metrics == nil {
		metrics = NewMetrics(tp, cfg.Region)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		materializer:           cfg.Materializer,
		gitExec:                cfg.GitExecutor,
		writeHandler:           wh,
		bundleResolver:         cfg.BundleResolver,
		leaseRefresher:         cfg.LeaseRefresher,
		tracer:                 tp.Tracer("stravin-node"),
		metrics:                metrics,
		logger:                 logger,
		region:                 cfg.Region,
		uploadPackSem:          make(chan struct{}, maxUploadPacks),
		uploadPackQueueTimeout: queueTimeout,
	}, nil
}

// LeaseRefresher returns the server's background regional lease refresher (if configured).
func (s *Server) LeaseRefresher() *LeaseRefresher {
	return s.leaseRefresher
}

// HandleGitWire implements rpc.GitWireHandler.
func (s *Server) HandleGitWire(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) error {
	start := time.Now()
	ctx = extractTraceContext(ctx, meta.TraceParent)

	gitOp := metricOpFor(meta.Operation)
	ctx, rootSpan := s.tracer.Start(ctx, "node.git.stream", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(meta.TenantID)).
			RepoID(string(meta.RepoID)).
			GitOperation(gitOp).
			Build()...,
	))
	defer rootSpan.End()

	var (
		err     error
		execErr error
	)
	switch meta.Operation {
	case rpc.OpUploadPack:
		execErr, err = s.handleUploadPack(ctx, meta, stream)
	case rpc.OpReceivePack:
		err = s.writeHandler.HandleReceivePack(ctx, meta, stream)
	default:
		err = status.Errorf(codes.InvalidArgument, "node: unsupported operation %q", meta.Operation)
	}

	obsErr := err
	if obsErr == nil {
		obsErr = execErr
	}
	statusLabel := "ok"
	if obsErr != nil {
		statusLabel = "error"
		recordSpanError(rootSpan, obsErr)
	}
	s.metrics.RecordRequest(ctx, gitOp, statusLabel, time.Since(start))
	return err
}

func (s *Server) handleUploadPack(ctx context.Context, meta rpc.RequestMeta, stream rpc.GitWireStream) (error, error) {
	repo := meta.RepoKey()

	if boolHeader(ctx, HeaderAdvertiseRefs) {
		return nil, s.serveAdvertiseRefs(ctx, repo, meta.GitProtocol, stream)
	}

	statelessRPC := boolHeader(ctx, HeaderStatelessRPC)

	var (
		peekedBytes []byte
		peekEOF     bool
		isV2LsRefs  bool
	)
	if statelessRPC && rpc.GitProtocolVersion(meta.GitProtocol) == "2" {
		var (
			cmdKind v2CommandKind
			err     error
		)
		peekedBytes, peekEOF, cmdKind, err = readBoundedV2Request(stream)
		if err != nil {
			return nil, err
		}
		isV2LsRefs = cmdKind == v2CmdLsRefs

		if isV2LsRefs && peekEOF {
			if lsReq, ok := parseV2LsRefsRequest(peekedBytes); ok {
				repoMeta, refs, loadErr := s.materializer.LoadAuthoritativeRefs(ctx, repo)
				if loadErr != nil {
					if errors.Is(loadErr, ddb.ErrNotFound) {
						return nil, status.Errorf(codes.NotFound, "repository %s not found", repo)
					}
					return nil, status.Errorf(codes.Internal, "load refs %s: %v", repo, loadErr)
				}
				if payload, needsFallback := encodeV2LsRefsResponse(refs, repoMeta.DefaultBranch, lsReq); !needsFallback {
					w := &streamChunkWriter{stream: stream}
					if _, wErr := w.Write(payload); wErr != nil {
						return nil, wErr
					}
					return nil, stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
				}
			}
		}
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "repository %s not found", repo)
		}
		return nil, status.Errorf(codes.Internal, "sync repository %s: %v", repo, err)
	}
	defer syncRes.Handle.Release()

	if !isV2LsRefs {
		releaseSlot, slotErr := s.acquireUploadPackSlot(ctx)
		if slotErr != nil {
			return nil, slotErr
		}
		defer releaseSlot()
	}

	execCtx, execSpan := s.tracer.Start(ctx, "node.git.upload_pack", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(syncRes.Meta.HomeRegion)).
			FailoverEpoch(epochToInt64(syncRes.Meta.FailoverEpoch)).
			GitOperation("fetch").
			Build()...,
	))
	defer execSpan.End()

	stdinR, stdinW := io.Pipe()
	pumpCtx, cancelPump := context.WithCancel(execCtx)
	var pumpWG sync.WaitGroup
	pumpWG.Add(1)
	go func() {
		defer pumpWG.Done()
		defer func() { _ = stdinW.Close() }()
		if len(peekedBytes) > 0 {
			if _, wErr := stdinW.Write(peekedBytes); wErr != nil {
				return
			}
		}
		if peekEOF {
			return
		}
		for {
			if pumpCtx.Err() != nil {
				return
			}
			frame, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				return
			}
			if recvErr != nil {
				_ = stdinW.CloseWithError(recvErr)
				return
			}
			if len(frame.Data) > 0 {
				if _, wErr := stdinW.Write(frame.Data); wErr != nil {
					return
				}
			}
			if frame.EOF {
				return
			}
		}
	}()

	stdoutW := &streamChunkWriter{stream: stream}

	var stderrBuf bytes.Buffer
	var bundleURI string
	if !isV2LsRefs {
		bundleURI = s.resolveBundleURI(execCtx, repo, meta.GitProtocol)
	}

	execErr := s.gitExec.ExecUploadPack(execCtx, gitexec.UploadPackRequest{
		Repo:         repo,
		RepoDir:      syncRes.Handle.Path(),
		StatelessRPC: statelessRPC,
		GitProtocol:  meta.GitProtocol,
		BundleURI:    bundleURI,
		Stdin:        stdinR,
		Stdout:       stdoutW,
		Stderr:       &stderrBuf,
	})

	cancelPump()
	_ = stdinR.Close()
	pumpWG.Wait()

	if stdoutW.writeErr != nil {
		recordSpanError(execSpan, stdoutW.writeErr)
		return nil, stdoutW.writeErr
	}

	if execErr != nil {
		recordSpanError(execSpan, execErr)

		exitCode := int32(1)
		errMsg := strings.TrimSpace(stderrBuf.String())
		var gitErr *gitexec.ExecError
		if errors.As(execErr, &gitErr) {
			if gitErr.ExitCode > 0 {
				exitCode = int32(gitErr.ExitCode)
			}
			if errMsg == "" {
				errMsg = gitErr.Stderr
			}
		}
		if errMsg == "" {
			errMsg = execErr.Error()
		}
		if sendErr := stream.Send(&rpc.Frame{
			EOF:        true,
			ExitCode:   exitCode,
			ErrMessage: sanitizeUTF8(errMsg),
		}); sendErr != nil {
			return nil, sendErr
		}
		return execErr, nil
	}

	s.logger.DebugContext(execCtx, "node: git-upload-pack completed",
		"tenant_id", string(repo.TenantID),
		"repo_id", string(repo.RepoID),
		"packs_fetched", syncRes.PacksFetched,
		"packs_cached", syncRes.PacksCached,
	)
	return nil, stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func (s *Server) acquireUploadPackSlot(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	waitStart := time.Now()
	select {
	case s.uploadPackSem <- struct{}{}:
		s.metrics.RecordUploadPackQueueWait(ctx, time.Since(waitStart))
		return func() { <-s.uploadPackSem }, nil
	default:
	}

	timer := time.NewTimer(s.uploadPackQueueTimeout)
	defer timer.Stop()

	select {
	case s.uploadPackSem <- struct{}{}:
		s.metrics.RecordUploadPackQueueWait(ctx, time.Since(waitStart))
		return func() { <-s.uploadPackSem }, nil
	case <-timer.C:
		s.metrics.RecordUploadPackQueueWait(ctx, time.Since(waitStart))
		s.metrics.RecordUploadPackQueueTimeout(ctx)
		return nil, status.Errorf(codes.ResourceExhausted, "node: upload-pack queue timed out after %s", s.uploadPackQueueTimeout)
	case <-ctx.Done():
		s.metrics.RecordUploadPackQueueWait(ctx, time.Since(waitStart))
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *Server) serveAdvertiseRefs(ctx context.Context, repoKey tenant.RepoKey, gitProtocol string, stream rpc.GitWireStream) error {
	var payload []byte
	if rpc.GitProtocolVersion(gitProtocol) == "2" {
		repoMeta, err := s.materializer.LoadRepoMeta(ctx, repoKey)
		if err != nil {
			if errors.Is(err, ddb.ErrNotFound) {
				return status.Errorf(codes.NotFound, "repository %s not found", repoKey)
			}
			return status.Errorf(codes.Internal, "load meta %s: %v", repoKey, err)
		}
		opts := AdvertiseOptions{
			IncludeServiceHeader: boolHeader(ctx, HeaderSmartHTTPServiceHeader),
			ServiceName:          string(rpc.OpUploadPack),
			ObjectFormat:         repoMeta.EffectiveObjectFormat(),
			AdvertiseBundleURI:   s.resolveBundleURI(ctx, repoKey, gitProtocol) != "",
		}
		payload = AdvertiseV2Capabilities(opts)
	} else {
		repoMeta, refs, err := s.materializer.LoadAuthoritativeRefs(ctx, repoKey)
		if err != nil {
			if errors.Is(err, ddb.ErrNotFound) {
				return status.Errorf(codes.NotFound, "repository %s not found", repoKey)
			}
			return status.Errorf(codes.Internal, "load refs %s: %v", repoKey, err)
		}
		opts := AdvertiseOptions{
			IncludeServiceHeader: boolHeader(ctx, HeaderSmartHTTPServiceHeader),
			ServiceName:          string(rpc.OpUploadPack),
			ObjectFormat:         repoMeta.EffectiveObjectFormat(),
		}
		payload = AdvertiseRefs(refs, repoMeta.DefaultBranch, opts)
	}

	if len(payload) > 0 {
		if err := stream.Send(&rpc.Frame{Data: payload}); err != nil {
			return err
		}
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
}

func (s *Server) resolveBundleURI(ctx context.Context, repo tenant.RepoKey, gitProtocol string) string {
	if s.bundleResolver == nil || rpc.GitProtocolVersion(gitProtocol) != "2" {
		return ""
	}
	uri, err := s.bundleResolver.ResolveBundleURI(ctx, repo)
	if err != nil {
		s.logger.DebugContext(ctx, "node: skipped bundle-uri resolution",
			"repo", repo.String(),
			"error", err,
		)
		return ""
	}
	if uri == "" {
		return ""
	}
	if err := gitexec.ValidateBundleURI(uri); err != nil {
		s.logger.WarnContext(ctx, "node: rejected invalid bundle-uri",
			"repo", repo.String(),
			"error", err,
		)
		return ""
	}
	return uri
}

type streamChunkWriter struct {
	stream   rpc.GitWireStream
	writeErr error
}

func (w *streamChunkWriter) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > streamChunkSize {
			n = streamChunkSize
		}
		chunk := make([]byte, n)
		copy(chunk, p[:n])
		if err := w.stream.Send(&rpc.Frame{Data: chunk}); err != nil {
			w.writeErr = err
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}

func extractTraceContext(ctx context.Context, traceParent string) context.Context {
	extracted := telemetry.ExtractGRPC(ctx)
	if trace.SpanContextFromContext(extracted).IsValid() {
		return extracted
	}
	if traceParent != "" {
		return telemetry.ContextWithTraceparent(extracted, traceParent)
	}
	return extracted
}

func boolHeader(ctx context.Context, key string) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	vals := md.Get(key)
	if len(vals) == 0 {
		return false
	}
	v, err := strconv.ParseBool(strings.TrimSpace(vals[0]))
	return err == nil && v
}

func metricOpFor(op rpc.Operation) string {
	switch op {
	case rpc.OpUploadPack:
		return "fetch"
	case rpc.OpReceivePack:
		return "push"
	default:
		return "unknown"
	}
}

func sanitizeUTF8(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
}

func sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if utf8.ValidString(msg) {
		return err
	}
	return errors.New(sanitizeUTF8(msg))
}

func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	safeErr := sanitizeError(err)
	span.RecordError(safeErr)
	span.SetStatus(otelcodes.Error, safeErr.Error())
}

// epochToInt64 converts a uint64 lease.Epoch to int64 for telemetry span
// attributes, clamping values above math.MaxInt64 instead of overflowing.
func epochToInt64(epoch lease.Epoch) int64 {
	if epoch > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(epoch)
}
