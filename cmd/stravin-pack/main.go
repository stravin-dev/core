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
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/compaction"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/daemon"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	serviceName            = "stravin-pack"
	defaultHealthAddr      = ":8082"
	defaultRegion          = "us-east-1"
	defaultDDBTable        = "stravin-core"
	defaultS3BucketPrefix  = "git-objects-"
	defaultPollInterval    = 2 * time.Second
	defaultGCInterval      = 5 * time.Minute
	defaultGCGraceWindow   = 24 * time.Hour
	defaultLeaseTTL        = 30 * time.Second
	defaultShutdownTimeout = 5 * time.Second
	defaultGitExecTimeout  = 10 * time.Minute
)

type packConfig struct {
	HealthAddr            string
	Region                string
	AWSEndpoint           string
	DDBTable              string
	S3Bucket              string
	KMSKeyID              string
	WorkerID              string
	WorkDir               string
	PollInterval          time.Duration
	GCInterval            time.Duration
	GCGraceWindow         time.Duration
	LeaseTTL              time.Duration
	RequireCRRReplication bool
	CRRBreakerThreshold   int
	CRRBreakerCooldown    time.Duration
	GitExec               gitexec.Config
	StaticRepos           []string
}

func loadConfig() packConfig {
	region := envOrDefault("AWS_REGION", defaultRegion)
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "worker"
	}
	defaultWorkerID := fmt.Sprintf("%s-%s-%d", serviceName, hostname, os.Getpid())

	var staticRepos []string
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_PACK_REPOS")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if p := strings.TrimSpace(part); p != "" {
				staticRepos = append(staticRepos, p)
			}
		}
	}

	return packConfig{
		HealthAddr:            daemon.ListenAddrFromEnv(defaultHealthAddr),
		Region:                region,
		AWSEndpoint:           strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")),
		DDBTable:              envOrDefault("STRAVIN_DDB_TABLE", defaultDDBTable),
		S3Bucket:              envOrDefault("STRAVIN_S3_BUCKET", defaultS3BucketPrefix+region),
		KMSKeyID:              s3.KMSKeyIDFromEnv(),
		WorkerID:              envOrDefault("STRAVIN_PACK_WORKER_ID", defaultWorkerID),
		WorkDir:               strings.TrimSpace(os.Getenv("STRAVIN_PACK_WORK_DIR")),
		PollInterval:          envDuration("STRAVIN_PACK_POLL_INTERVAL", defaultPollInterval),
		GCInterval:            envDuration("STRAVIN_PACK_GC_INTERVAL", defaultGCInterval),
		GCGraceWindow:         envDuration("STRAVIN_PACK_GC_GRACE", defaultGCGraceWindow),
		LeaseTTL:              envDuration("STRAVIN_PACK_LEASE_TTL", defaultLeaseTTL),
		RequireCRRReplication: envBool("STRAVIN_PACK_REQUIRE_CRR_REPLICATION", false),
		CRRBreakerThreshold:   envInt("STRAVIN_PACK_CRR_BREAKER_THRESHOLD", compaction.DefaultCRRBreakerThreshold),
		CRRBreakerCooldown:    envDuration("STRAVIN_PACK_CRR_BREAKER_COOLDOWN", compaction.DefaultCRRBreakerCooldown),
		GitExec:               gitexec.LoadConfigFromEnv(defaultGitExecTimeout, nil),
		StaticRepos:           staticRepos,
	}
}

func buildS3Store(s3Client s3.API, bucket string, kmsClient crypto.KMSAPI, kmsKeyID string) (*s3.Store, error) {
	kmsOpt, err := s3.WithKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		return nil, fmt.Errorf("init kms envelope cipher: %w", err)
	}
	return s3.NewStore(s3Client, bucket, kmsOpt)
}

func resolveGitExecConfig(cfg gitexec.Config) gitexec.Config {
	if cfg.DefaultTimeout <= 0 && cfg.Sandbox == (gitexec.SandboxConfig{}) {
		cfg.Sandbox = gitexec.DefaultSandboxConfig()
	}
	if cfg.DefaultTimeout <= 0 {
		cfg.DefaultTimeout = defaultGitExecTimeout
	}
	return cfg
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return v
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	return fallback
}

type staticRepoDiscoverer struct {
	repo   *ddb.Repository
	region lease.Region
	repos  []tenant.RepoKey
}

func (d *staticRepoDiscoverer) DiscoverTargets(ctx context.Context) ([]compaction.ShardTarget, []compaction.RepoTarget, error) {
	if len(d.repos) == 0 {
		return nil, nil, nil
	}
	var shards []compaction.ShardTarget
	var repoTargets []compaction.RepoTarget
	for _, rk := range d.repos {
		meta, err := d.repo.GetMeta(ctx, rk)
		if err != nil {
			continue
		}
		repoTargets = append(repoTargets, compaction.RepoTarget{
			Repo:           rk,
			ManifestShards: meta.ManifestShards,
			RefShards:      meta.RefShards,
		})
		for shard := 0; shard < meta.ManifestShards; shard++ {
			shards = append(shards, compaction.ShardTarget{
				Repo:           rk,
				Shard:          shard,
				ManifestShards: meta.ManifestShards,
				RefShards:      meta.RefShards,
				OriginRegion:   d.region,
				Tier:           compaction.Tier0,
			})
		}
	}
	return shards, repoTargets, nil
}

func parseStaticRepos(raw []string) ([]tenant.RepoKey, error) {
	out := make([]tenant.RepoKey, 0, len(raw))
	for _, item := range raw {
		parts := strings.SplitN(item, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid STRAVIN_PACK_REPOS entry %q (want tenant/repo)", item)
		}
		rk, err := tenant.NewRepoKey(parts[0], parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid STRAVIN_PACK_REPOS entry %q: %w", item, err)
		}
		out = append(out, rk)
	}
	return out, nil
}

func run(ctx context.Context, cfg packConfig) error {
	telProvider, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: serviceName,
		Region:      cfg.Region,
		Mode:        telemetry.ModeOTLP,
		SetGlobal:   true,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeoutFromEnv())
		defer cancel()
		if shutErr := telProvider.Shutdown(shutCtx); shutErr != nil {
			_, _ = fmt.Fprintf(os.Stderr, "%s: telemetry shutdown warning: %v\n", serviceName, shutErr)
		}
	}()

	logger := telProvider.Logger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(logger)

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if cfg.AWSEndpoint != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpoint)
		}
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, cfg.DDBTable)
	if err != nil {
		return fmt.Errorf("init ddb repository: %w", err)
	}
	gcAdapter, err := compaction.NewDDBManifestGCAdapter(ddbClient, cfg.DDBTable)
	if err != nil {
		return fmt.Errorf("init ddb gc adapter: %w", err)
	}

	var kmsClient crypto.KMSAPI
	if cfg.KMSKeyID != "" {
		kmsClient = crypto.NewKMSClient(awsCfg, cfg.AWSEndpoint)
	}
	s3Client := s3.NewAWSClient(awsCfg, cfg.AWSEndpoint, s3.ForcePathStyleFromEnv() || cfg.AWSEndpoint != "")
	s3Store, err := buildS3Store(s3Client, cfg.S3Bucket, kmsClient, cfg.KMSKeyID)
	if err != nil {
		return fmt.Errorf("init s3 store: %w", err)
	}

	metrics, err := compaction.NewMetrics(telProvider)
	if err != nil {
		return fmt.Errorf("init compaction metrics: %w", err)
	}

	leaseMgr, err := compaction.NewLeaseManager(ddbRepo, compaction.LeaseManagerConfig{
		OwnerID: cfg.WorkerID,
		TTL:     cfg.LeaseTTL,
		Metrics: metrics,
	})
	if err != nil {
		return fmt.Errorf("init lease manager: %w", err)
	}

	tier0, err := compaction.NewTier0Strategy(s3Store, pack.DefaultLimits())
	if err != nil {
		return fmt.Errorf("init tier0 strategy: %w", err)
	}

	gitExecCfg := resolveGitExecConfig(cfg.GitExec)
	gitExec := gitexec.NewSubprocessExecutor(gitExecCfg)
	tier1, err := compaction.NewTier1Strategy(s3Store, gitExec, compaction.Tier1Config{
		WorkDir: cfg.WorkDir,
		Timeout: gitExecCfg.DefaultTimeout,
		Limits:  pack.DefaultLimits(),
	})
	if err != nil {
		return fmt.Errorf("init tier1 strategy: %w", err)
	}

	engine, err := compaction.NewEngine(compaction.EngineConfig{
		Leases:        leaseMgr,
		Manifests:     ddbRepo,
		ManifestGC:    gcAdapter,
		Refs:          ddbRepo,
		GCGraceWindow: cfg.GCGraceWindow,
		Metrics:       metrics,
		Strategies: map[compaction.Tier]compaction.CompactionStrategy{
			compaction.Tier0: tier0,
			compaction.Tier1: tier1,
		},
	})
	if err != nil {
		return fmt.Errorf("init compaction engine: %w", err)
	}

	var replicationGate compaction.ReplicationGate
	if cfg.RequireCRRReplication {
		breaker := compaction.NewCRRCircuitBreaker(compaction.CRRCircuitBreakerConfig{
			Threshold: cfg.CRRBreakerThreshold,
			Cooldown:  cfg.CRRBreakerCooldown,
		})
		replicationGate, err = compaction.NewS3ReplicationGate(compaction.S3ReplicationGateConfig{
			Objects: s3Store,
			Breaker: breaker,
			Metrics: metrics,
		})
		if err != nil {
			return fmt.Errorf("init crr replication gate: %w", err)
		}
	}

	sweeper, err := compaction.NewGCSweeper(compaction.GCSweeperConfig{
		Manifests:         ddbRepo,
		ManifestGC:        gcAdapter,
		Objects:           s3Store,
		ReplicationGate:   replicationGate,
		GraceWindow:       cfg.GCGraceWindow,
		OrphanGraceWindow: cfg.GCGraceWindow,
		Metrics:           metrics,
	})
	if err != nil {
		return fmt.Errorf("init gc sweeper: %w", err)
	}

	staticRepos, err := parseStaticRepos(cfg.StaticRepos)
	if err != nil {
		return err
	}

	worker, err := compaction.NewWorker(compaction.WorkerConfig{
		Engine:       engine,
		Sweeper:      sweeper,
		Queue:        compaction.NewFairQueue(),
		Discoverer:   &staticRepoDiscoverer{repo: ddbRepo, region: lease.Region(cfg.Region), repos: staticRepos},
		PollInterval: cfg.PollInterval,
		GCInterval:   cfg.GCInterval,
	})
	if err != nil {
		return fmt.Errorf("init compaction worker: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Run(runCtx)
	}()

	healthErr := daemon.Run(runCtx, daemon.Config{
		ServiceName:     serviceName,
		ListenAddr:      cfg.HealthAddr,
		ShutdownTimeout: defaultShutdownTimeout,
	})
	cancel()
	workerErr := <-workerDone

	if healthErr != nil {
		return healthErr
	}
	return workerErr
}

func main() {
	if err := runWithSignals(); err != nil {
		slog.Error("stravin-pack exited with error", "error", err)
		os.Exit(1)
	}
}

func runWithSignals() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return run(ctx, loadConfig())
}
