package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"google.golang.org/grpc"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/health"
	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
)

const (
	defaultRegion                 = "us-east-1"
	defaultGRPCAddr               = ":9090"
	defaultHealthAddr             = ":8081"
	defaultCacheDir               = "/var/lib/stravin/cache"
	defaultDDBTable               = "stravin-core"
	defaultDaemonShutdownWindow   = 5 * time.Second
	defaultGitExecTimeout         = 5 * time.Minute
	defaultUploadPackQueueTimeout = 15 * time.Second
)

func defaultMaxUploadPacks() int {
	return max(runtime.GOMAXPROCS(0), 2)
}

// DaemonConfig configures the full stravin-node process.
type DaemonConfig struct {
	Region                   lease.Region
	GRPCAddr                 string
	HealthAddr               string
	CacheDir                 string
	CacheMaxBytes            int64
	S3Bucket                 string
	S3ForcePathStyle         bool
	KMSKeyID                 string
	DDBTable                 string
	AWSEndpointURL           string
	LeaseRefreshInterval     time.Duration
	MaxUploadPacks           int
	UploadPackQueueTimeout   time.Duration
	ShutdownTimeout          time.Duration
	DrainDelay               time.Duration
	TelemetryShutdownTimeout time.Duration
	Probes                   *health.Registry
	GRPCTransport            rpc.TransportConfig
	GitExec                  gitexec.Config
	WriteHandler             WriteHandler
	OnReady                  func(grpcAddr, healthAddr string)
}

// ParseUploadPackLimitsFromEnv parses and validates STRAVIN_NODE_MAX_UPLOAD_PACKS
// and STRAVIN_NODE_UPLOAD_PACK_QUEUE_TIMEOUT.
func ParseUploadPackLimitsFromEnv() (int, time.Duration, error) {
	maxUploads := defaultMaxUploadPacks()
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_NODE_MAX_UPLOAD_PACKS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return 0, 0, fmt.Errorf("invalid STRAVIN_NODE_MAX_UPLOAD_PACKS %q: must be integer >= 1", raw)
		}
		maxUploads = n
	}

	queueTimeout := defaultUploadPackQueueTimeout
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_NODE_UPLOAD_PACK_QUEUE_TIMEOUT")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return 0, 0, fmt.Errorf("invalid STRAVIN_NODE_UPLOAD_PACK_QUEUE_TIMEOUT %q: must be positive duration", raw)
		}
		queueTimeout = d
	}

	return maxUploads, queueTimeout, nil
}

// LoadDaemonConfigFromEnv reads stravin-node configuration from environment variables.
func LoadDaemonConfigFromEnv() DaemonConfig {
	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", defaultRegion))
	bucket := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-"+region)

	var maxBytes int64
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_CACHE_MAX_BYTES")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			maxBytes = n
		}
	}

	refreshInterval := defaultLeaseRefreshInterval
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_LEASE_REFRESH_INTERVAL")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			refreshInterval = d
		}
	}

	maxUploadPacks := defaultMaxUploadPacks()
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_NODE_MAX_UPLOAD_PACKS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 1 {
			maxUploadPacks = n
		}
	}

	queueTimeout := defaultUploadPackQueueTimeout
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_NODE_UPLOAD_PACK_QUEUE_TIMEOUT")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			queueTimeout = d
		}
	}

	return DaemonConfig{
		Region:                   lease.Region(region),
		GRPCAddr:                 envOrDefault("STRAVIN_GRPC_ADDR", defaultGRPCAddr),
		HealthAddr:               envOrDefault("STRAVIN_HEALTH_ADDR", defaultHealthAddr),
		CacheDir:                 envOrDefault("STRAVIN_CACHE_DIR", defaultCacheDir),
		CacheMaxBytes:            maxBytes,
		S3Bucket:                 bucket,
		S3ForcePathStyle:         s3.ForcePathStyleFromEnv(),
		KMSKeyID:                 s3.KMSKeyIDFromEnv(),
		DDBTable:                 envOrDefault("STRAVIN_DDB_TABLE", defaultDDBTable),
		AWSEndpointURL:           strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")),
		LeaseRefreshInterval:     refreshInterval,
		MaxUploadPacks:           maxUploadPacks,
		UploadPackQueueTimeout:   queueTimeout,
		ShutdownTimeout:          defaultDaemonShutdownWindow,
		TelemetryShutdownTimeout: telemetry.ShutdownTimeoutFromEnv(),
		GRPCTransport:            rpc.LoadTransportConfigFromEnv(),
		GitExec:                  gitexec.LoadConfigFromEnv(defaultGitExecTimeout, nil),
	}
}

// RunWithSignals runs stravin-node until SIGINT or SIGTERM is received.
func RunWithSignals() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return RunDaemon(ctx, LoadDaemonConfigFromEnv())
}

// RunDaemon wires OTel, AWS DynamoDB/S3, warm NVMe cache, sandboxed gitexec,
// background regional lease refresher, gRPC server (:9090), and /healthz + /metrics (:8081).
func RunDaemon(ctx context.Context, cfg DaemonConfig) error {
	if strings.TrimSpace(string(cfg.Region)) == "" {
		cfg.Region = defaultRegion
	}
	if cfg.GRPCAddr == "" {
		cfg.GRPCAddr = defaultGRPCAddr
	}
	if cfg.HealthAddr == "" {
		cfg.HealthAddr = defaultHealthAddr
	}
	if cfg.CacheDir == "" {
		cfg.CacheDir = defaultCacheDir
	}
	if cfg.DDBTable == "" {
		cfg.DDBTable = defaultDDBTable
	}
	if cfg.S3Bucket == "" {
		cfg.S3Bucket = "git-objects-" + string(cfg.Region)
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = defaultDaemonShutdownWindow
	}

	grpcOpts, err := rpc.ServerOptions(cfg.GRPCTransport)
	if err != nil {
		return fmt.Errorf("node: grpc transport config: %w", err)
	}

	tp, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: "stravin-node",
		Region:      string(cfg.Region),
		Mode:        telemetry.ModeOTLP,
		SetGlobal:   true,
	})
	if err != nil {
		return fmt.Errorf("node: init telemetry: %w", err)
	}
	defer func() {
		telTimeout := cfg.TelemetryShutdownTimeout
		if telTimeout <= 0 {
			telTimeout = telemetry.ShutdownTimeoutFromEnv()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telTimeout)
		defer cancel()
		if shutErr := tp.Shutdown(shutdownCtx); shutErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "stravin-node: telemetry shutdown warning: %v\n", shutErr)
		}
	}()

	logger := tp.Logger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(logger)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(string(cfg.Region)))
	if err != nil {
		return fmt.Errorf("node: load aws config: %w", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.AWSEndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpointURL)
		}
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, cfg.DDBTable)
	if err != nil {
		return fmt.Errorf("node: init ddb repository: %w", err)
	}

	var kmsClient crypto.KMSAPI
	if cfg.KMSKeyID != "" {
		kmsClient = crypto.NewKMSClient(awsCfg, cfg.AWSEndpointURL)
	}
	s3Client := s3.NewAWSClient(awsCfg, cfg.AWSEndpointURL, cfg.S3ForcePathStyle)
	s3Store, err := buildS3Store(s3Client, cfg.S3Bucket, kmsClient, cfg.KMSKeyID)
	if err != nil {
		return fmt.Errorf("node: init s3 store: %w", err)
	}

	nvmeCache, err := cache.New(cache.Config{
		Root:      cfg.CacheDir,
		MaxBytes:  cfg.CacheMaxBytes,
		Telemetry: tp,
	})
	if err != nil {
		return fmt.Errorf("node: init nvme cache: %w", err)
	}

	metrics := NewMetrics(tp, string(cfg.Region))
	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:    ddbRepo,
		Region:   cfg.Region,
		Interval: cfg.LeaseRefreshInterval,
		Logger:   logger,
	})
	if err != nil {
		return fmt.Errorf("node: init lease refresher: %w", err)
	}
	refresher.Start(ctx)
	defer refresher.Stop()

	gitExec := gitexec.NewSubprocessExecutor(resolveGitExecConfig(cfg.GitExec, logger))
	materializer, err := NewMaterializer(MaterializerConfig{
		Meta:        ddbRepo,
		Packs:       s3Store,
		Cache:       nvmeCache,
		GitExecutor: gitExec,
		Telemetry:   tp,
		Metrics:     metrics,
		Logger:      logger,
	})
	if err != nil {
		return fmt.Errorf("node: init materializer: %w", err)
	}
	defer materializer.Close()

	writeHandler := cfg.WriteHandler
	if writeHandler == nil {
		hookEngine := wasm.NewEngine(
			NewS3WasmLoader(s3Store),
			NewDDBHookConfigStore(ddbRepo),
			wasm.EngineConfig{Logger: logger},
		)
		defer func() {
			_ = hookEngine.Close(context.Background())
		}()

		writeHandler, err = NewReceivePackHandler(ReceivePackConfig{
			Materializer:   materializer,
			Cache:          nvmeCache,
			Packs:          s3Store,
			Committer:      ddbRepo,
			LeaseRefresher: refresher,
			HookEngine:     hookEngine,
			Region:         cfg.Region,
			Telemetry:      tp,
			Metrics:        metrics,
			Logger:         logger,
		})
		if err != nil {
			return fmt.Errorf("node: init receive-pack handler: %w", err)
		}
	}

	nodeSrv, err := NewServer(ServerConfig{
		Materializer:           materializer,
		GitExecutor:            gitExec,
		WriteHandler:           writeHandler,
		BundleResolver:         NewS3BundleURIResolver(s3Store, 0),
		LeaseRefresher:         refresher,
		Telemetry:              tp,
		Metrics:                metrics,
		Logger:                 logger,
		Region:                 string(cfg.Region),
		MaxUploadPacks:         cfg.MaxUploadPacks,
		UploadPackQueueTimeout: cfg.UploadPackQueueTimeout,
	})
	if err != nil {
		return fmt.Errorf("node: init server: %w", err)
	}

	grpcLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("node: listen grpc %s: %w", cfg.GRPCAddr, err)
	}
	healthLn, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		_ = grpcLn.Close()
		return fmt.Errorf("node: listen health %s: %w", cfg.HealthAddr, err)
	}

	grpcSrv := grpc.NewServer(grpcOpts...)
	rpc.RegisterGitWireServer(grpcSrv, nodeSrv)

	probes := cfg.Probes
	if probes == nil {
		probes = health.New("stravin-node", health.WithMetadata("region", string(cfg.Region)))
	}

	mux := http.NewServeMux()
	probes.Register(mux)
	mux.Handle("/metrics", metrics)
	healthSrv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	probes.MarkStarted()
	probes.MarkReady()

	logger.Info("stravin-node listening",
		"grpc_addr", grpcLn.Addr().String(),
		"health_addr", healthLn.Addr().String(),
		"region", string(cfg.Region),
		"cache_dir", cfg.CacheDir,
	)
	if cfg.OnReady != nil {
		cfg.OnReady(grpcLn.Addr().String(), healthLn.Addr().String())
	}

	errCh := make(chan error, 2)
	go func() {
		if err := grpcSrv.Serve(grpcLn); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- fmt.Errorf("node: grpc serve: %w", err)
			return
		}
		errCh <- nil
	}()
	go func() {
		if err := healthSrv.Serve(healthLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("node: health serve: %w", err)
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		probes.SetDraining()
		grpcSrv.GracefulStop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = healthSrv.Shutdown(shutdownCtx)
		return err
	case <-ctx.Done():
		probes.SetDraining()
		if cfg.DrainDelay > 0 {
			time.Sleep(cfg.DrainDelay)
		}
		stopped := make(chan struct{})
		go func() {
			grpcSrv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(cfg.ShutdownTimeout):
			grpcSrv.Stop()
		}

		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("node: health shutdown: %w", err)
		}
		logger.Info("stravin-node stopped")
		return nil
	}
}

func buildS3Store(s3Client s3.API, bucket string, kmsClient crypto.KMSAPI, kmsKeyID string) (*s3.Store, error) {
	kmsOpt, err := s3.WithKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		return nil, fmt.Errorf("init kms envelope cipher: %w", err)
	}
	return s3.NewStore(s3Client, bucket, kmsOpt)
}

func resolveGitExecConfig(cfg gitexec.Config, logger *slog.Logger) gitexec.Config {
	if cfg.DefaultTimeout <= 0 && cfg.Sandbox == (gitexec.SandboxConfig{}) {
		cfg.Sandbox = gitexec.DefaultSandboxConfig()
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = defaultGitExecTimeout
	}
	if cfg.Pack == (gitexec.PackConfig{}) {
		cfg.Pack = gitexec.DefaultPackConfig()
	}
	if cfg.Logger == nil {
		cfg.Logger = logger
	}
	return cfg
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
