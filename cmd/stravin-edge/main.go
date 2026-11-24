package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/edge"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("stravin-edge exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", "us-east-1"))
	tableName := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	sshAddr := envOrDefault("STRAVIN_SSH_ADDR", ":2222")
	httpAddr := envOrDefault("STRAVIN_HTTP_ADDR", ":8080")
	healthAddr := os.Getenv("STRAVIN_HEALTH_ADDR")
	peers := edge.ParsePeers(os.Getenv("STRAVIN_NODE_PEERS"))

	telProvider, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: "stravin-edge",
		Region:      region,
		Mode:        telemetry.ModeOTLP,
		SetGlobal:   true,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeoutFromEnv())
		defer cancel()
		if shutErr := telProvider.Shutdown(shutdownCtx); shutErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "stravin-edge: telemetry shutdown warning: %v\n", shutErr)
		}
	}()

	logger := telProvider.Logger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(logger)

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if ep := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")); ep != "" {
			o.BaseEndpoint = aws.String(ep)
		}
	})

	ddbRepo, err := ddb.NewRepository(ddbClient, tableName)
	if err != nil {
		return fmt.Errorf("init ddb repository: %w", err)
	}

	edgeMetrics, err := edge.NewMetrics(telProvider)
	if err != nil {
		return fmt.Errorf("init edge metrics: %w", err)
	}

	metaCacheTTL := envDurationOrDefault("STRAVIN_META_CACHE_TTL", 30*time.Second)
	leaseSafetyMargin := envDurationOrDefault("STRAVIN_LEASE_SAFETY_MARGIN", 2*time.Second)
	verifier := lease.NewStoreVerifier(ddbRepo, lease.RealClock{}, leaseSafetyMargin)
	resolver, err := edge.NewResolver(lease.Region(region), ddbRepo, verifier, metaCacheTTL, lease.RealClock{})
	if err != nil {
		return fmt.Errorf("init resolver: %w", err)
	}
	resolver.SetMetrics(edgeMetrics)

	auth, err := loadAuthenticatorFromEnv()
	if err != nil {
		return fmt.Errorf("init authenticator: %w", err)
	}

	hostKey, err := edge.LoadOrGenerateHostKey(os.Getenv("STRAVIN_SSH_HOST_KEY_PATH"))
	if err != nil {
		return fmt.Errorf("init ssh host key: %w", err)
	}

	limiter := edge.NewTokenBucketLimiter(edge.LimiterConfig{
		RequestsPerSecond:   envFloatOrDefault("STRAVIN_RATE_LIMIT_RPS", 100),
		MaxConcurrentClones: envIntOrDefault("STRAVIN_MAX_CONCURRENT_CLONES", 32),
		PackBytesPerMin:     envInt64OrDefault("STRAVIN_PACK_BYTES_PER_MIN", 0),
		Metrics:             edgeMetrics,
	})

	router := edge.NewRendezvousRouter(peers)
	dialer, err := edge.NewGRPCNodeDialerWithConfig(rpc.LoadTransportConfigFromEnv())
	if err != nil {
		return fmt.Errorf("init grpc node dialer: %w", err)
	}
	defer func() { _ = dialer.Close() }()

	proxy, err := edge.NewWireProxy(dialer)
	if err != nil {
		return fmt.Errorf("init wire proxy: %w", err)
	}

	packCache, closePackCache, err := initPackCacheFromEnv(awsCfg, edgeMetrics, logger)
	if err != nil {
		return fmt.Errorf("init pack cache: %w", err)
	}
	defer closePackCache()

	srv, err := edge.NewServer(edge.ServerConfig{
		SSHAddr:                  sshAddr,
		HTTPAddr:                 httpAddr,
		HealthAddr:               healthAddr,
		MaxDecompressedBodyBytes: envInt64OrDefault("STRAVIN_MAX_DECOMPRESSED_BODY_BYTES", edge.DefaultMaxDecompressedBodyBytes),
		HostKey:                  hostKey,
		Authenticator:            auth,
		Limiter:                  limiter,
		Resolver:                 resolver,
		Router:                   router,
		Proxy:                    proxy,
		PackCache:                packCache,
		Telemetry:                telProvider,
		Metrics:                  edgeMetrics,
		Logger:                   logger,
	})
	if err != nil {
		return fmt.Errorf("init edge server: %w", err)
	}

	return srv.Run(ctx)
}

func loadAuthenticatorFromEnv() (edge.Authenticator, error) {
	var tokens []edge.TokenEntry
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_AUTH_TOKENS")); raw != "" {
		parsed, err := edge.ParseTokens(raw)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, parsed...)
	}
	if path := strings.TrimSpace(os.Getenv("STRAVIN_AUTH_TOKENS_FILE")); path != "" {
		parsed, err := edge.LoadTokensFile(path)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, parsed...)
	}

	var keys []edge.SSHKeyEntry
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_SSH_AUTHORIZED_KEYS")); raw != "" {
		parsed, err := edge.ParseAuthorizedKeys([]byte(raw))
		if err != nil {
			return nil, err
		}
		keys = append(keys, parsed...)
	}
	if path := strings.TrimSpace(os.Getenv("STRAVIN_SSH_AUTHORIZED_KEYS_FILE")); path != "" {
		parsed, err := edge.LoadAuthorizedKeysFile(path)
		if err != nil {
			return nil, err
		}
		keys = append(keys, parsed...)
	}
	return edge.NewStaticAuthenticator(tokens, keys)
}

func initPackCacheFromEnv(awsCfg aws.Config, metrics *edge.Metrics, logger *slog.Logger) (*edge.PackCacheCoordinator, func(), error) {
	valkeyAddr := strings.TrimSpace(os.Getenv("STRAVIN_PACK_CACHE_VALKEY_ADDR"))
	s3Bucket := strings.TrimSpace(os.Getenv("STRAVIN_S3_BUCKET"))
	ttl := envDurationOrDefault("STRAVIN_PACK_CACHE_TTL", edge.DefaultPackCacheTTL)
	valkeyMaxBytes := envIntOrDefault("STRAVIN_PACK_CACHE_VALKEY_MAX_ENTRY_BYTES", edge.DefaultPackCacheValkeyMaxEntryBytes)
	s3MaxBytes := envIntOrDefault("STRAVIN_PACK_CACHE_S3_MAX_ENTRY_BYTES", edge.DefaultPackCacheS3MaxEntryBytes)

	var (
		valkeyTier edge.PackResponseCache
		s3Tier     edge.PackResponseCache
		closers    []func()
	)
	if valkeyAddr != "" && valkeyMaxBytes > 0 {
		vc, err := edge.NewValkeyPackCache(edge.ValkeyPackCacheConfig{
			Addr:          valkeyAddr,
			MaxEntryBytes: valkeyMaxBytes,
			Metrics:       metrics,
		})
		if err != nil {
			return nil, func() {}, err
		}
		valkeyTier = vc
		closers = append(closers, func() { _ = vc.Close() })
	}
	if s3Bucket != "" && s3MaxBytes > 0 {
		s3Client := s3.NewAWSClient(awsCfg, strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")), s3.ForcePathStyleFromEnv())
		sc, err := edge.NewS3PackCache(edge.S3PackCacheConfig{
			Client:        s3Client,
			Bucket:        s3Bucket,
			MaxEntryBytes: s3MaxBytes,
			TTL:           ttl,
			Metrics:       metrics,
		})
		if err != nil {
			for _, fn := range closers {
				fn()
			}
			return nil, func() {}, err
		}
		s3Tier = sc
	}

	cleanup := func() {
		for _, fn := range closers {
			fn()
		}
	}
	if valkeyTier == nil && s3Tier == nil {
		return nil, cleanup, nil
	}

	tiered := edge.NewTieredPackCache(edge.TieredPackCacheConfig{
		Valkey:              valkeyTier,
		S3:                  s3Tier,
		ValkeyMaxEntryBytes: valkeyMaxBytes,
		S3MaxEntryBytes:     s3MaxBytes,
		Logger:              logger,
		Metrics:             metrics,
	})
	coord := edge.NewPackCacheCoordinator(edge.PackCacheCoordinatorConfig{
		Backend:         tiered,
		TTL:             ttl,
		MaxCaptureBytes: tiered.MaxEntryBytes(),
		Logger:          logger,
		Metrics:         metrics,
	})
	return coord, cleanup, nil
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return fallback
}

func envFloatOrDefault(key string, fallback float64) float64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 {
			return f
		}
	}
	return fallback
}

func envIntOrDefault(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}

func envInt64OrDefault(key string, fallback int64) int64 {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return fallback
}
