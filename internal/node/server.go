package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
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
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
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

	streamChunkSize = 32 * 1024
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

// ServerConfig configures the stravin-node gRPC GitWire server.
type ServerConfig struct {
	Materializer   *Materializer
	GitExecutor    gitexec.GitExecutor
	WriteHandler   WriteHandler
	LeaseRefresher *LeaseRefresher
	Telemetry      *telemetry.Provider
	Metrics        *Metrics
	Logger         *slog.Logger
	Region         string
}

// Server implements rpc.GitWireHandler for stravin-node.
type Server struct {
	materializer   *Materializer
	gitExec        gitexec.GitExecutor
	writeHandler   WriteHandler
	leaseRefresher *LeaseRefresher
	tracer         trace.Tracer
	metrics        *Metrics
	logger         *slog.Logger
	region         string
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
		materializer:   cfg.Materializer,
		gitExec:        cfg.GitExecutor,
		writeHandler:   wh,
		leaseRefresher: cfg.LeaseRefresher,
		tracer:         tp.Tracer("stravin-node"),
		metrics:        metrics,
		logger:         logger,
		region:         cfg.Region,
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
		return nil, s.serveAdvertiseRefs(ctx, repo, stream)
	}

	syncRes, err := s.materializer.SyncAndPin(ctx, repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "repository %s not found", repo)
		}
		return nil, status.Errorf(codes.Internal, "sync repository %s: %v", repo, err)
	}
	defer syncRes.Handle.Release()

	statelessRPC := boolHeader(ctx, HeaderStatelessRPC)

	execCtx, execSpan := s.tracer.Start(ctx, "node.git.upload_pack", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(syncRes.Meta.HomeRegion)).
			FailoverEpoch(int64(syncRes.Meta.FailoverEpoch)).
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

	execErr := s.gitExec.ExecUploadPack(execCtx, gitexec.UploadPackRequest{
		Repo:         repo,
		RepoDir:      syncRes.Handle.Path(),
		StatelessRPC: statelessRPC,
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

func (s *Server) serveAdvertiseRefs(ctx context.Context, repoKey tenant.RepoKey, stream rpc.GitWireStream) error {
	repoMeta, refs, err := s.materializer.LoadAuthoritativeRefs(ctx, repoKey)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return status.Errorf(codes.NotFound, "repository %s not found", repoKey)
		}
		return status.Errorf(codes.Internal, "load refs %s: %v", repoKey, err)
	}

	payload := AdvertiseRefs(refs, repoMeta.DefaultBranch, AdvertiseOptions{
		IncludeServiceHeader: boolHeader(ctx, HeaderSmartHTTPServiceHeader),
		ServiceName:          string(rpc.OpUploadPack),
	})

	if len(payload) > 0 {
		if err := stream.Send(&rpc.Frame{Data: payload}); err != nil {
			return err
		}
	}
	return stream.Send(&rpc.Frame{EOF: true, ExitCode: 0})
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
