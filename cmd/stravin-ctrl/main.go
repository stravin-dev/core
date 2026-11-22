package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/ctrl"
	"stravin.dev/core/internal/daemon"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	serviceName            = "stravin-ctrl"
	defaultListenAddr      = "127.0.0.1:8083"
	defaultRegion          = "us-east-1"
	defaultDDBTable        = "stravin-core"
	defaultS3BucketPrefix  = "git-objects-"
	defaultShutdownTimeout = 5 * time.Second
)

type ctrlConfig struct {
	ListenAddr             string
	AdminToken             string
	Region                 string
	AWSEndpoint            string
	DDBTable               string
	S3Bucket               string
	KMSKeyID               string
	HolderID               string
	LeaseTTL               time.Duration
	HeartbeatInterval      time.Duration
	FenceWait              time.Duration
	ScrubLookback          time.Duration
	ScrubConcurrency       int
	RequirePromotionGuard  bool
	PromotionGuardPolicy   string
	PromotionApprovalToken string
	WitnessEndpoints       []string
	WitnessQuorum          int
	WitnessTimeout         time.Duration
	RecoveryOnBoot         bool
	RecoveryRepos          []string
	RecoveryConcurrency    int
	RecoveryLookback       time.Duration
}

func loadConfig() ctrlConfig {
	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", defaultRegion))
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "ctrl"
	}
	defaultHolderID := fmt.Sprintf("%s-%s-%d", serviceName, hostname, os.Getpid())

	listenAddr := envOrDefault("STRAVIN_CTRL_ADDR", envOrDefault("STRAVIN_CTRL_LISTEN_ADDR", daemon.ListenAddrFromEnv(defaultListenAddr)))

	return ctrlConfig{
		ListenAddr:             listenAddr,
		AdminToken:             strings.TrimSpace(os.Getenv("STRAVIN_CTRL_ADMIN_TOKEN")),
		Region:                 region,
		AWSEndpoint:            strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL")),
		DDBTable:               envOrDefault("STRAVIN_DDB_TABLE", defaultDDBTable),
		S3Bucket:               envOrDefault("STRAVIN_S3_BUCKET", defaultS3BucketPrefix+region),
		KMSKeyID:               s3.KMSKeyIDFromEnv(),
		HolderID:               envOrDefault("STRAVIN_CTRL_HOLDER_ID", defaultHolderID),
		LeaseTTL:               envDuration("STRAVIN_CTRL_LEASE_TTL", ctrl.DefaultLeaseTTL),
		HeartbeatInterval:      envDuration("STRAVIN_CTRL_HEARTBEAT_INTERVAL", ctrl.DefaultHeartbeatInterval),
		FenceWait:              envDurationAllowZero("STRAVIN_CTRL_FENCE_WAIT", 0),
		ScrubLookback:          envDuration("STRAVIN_CTRL_SCRUB_LOOKBACK", ctrl.DefaultScrubLookback),
		ScrubConcurrency:       envInt("STRAVIN_CTRL_SCRUB_CONCURRENCY", ctrl.DefaultScrubConcurrency),
		RequirePromotionGuard:  envBool("STRAVIN_CTRL_REQUIRE_PROMOTION_GUARD", false),
		PromotionGuardPolicy:   envOrDefault("STRAVIN_CTRL_PROMOTION_GUARD_POLICY", string(ctrl.CompositePolicyRequireAll)),
		PromotionApprovalToken: strings.TrimSpace(os.Getenv("STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN")),
		WitnessEndpoints:       envCSV("STRAVIN_CTRL_WITNESS_ENDPOINTS"),
		WitnessQuorum:          envIntAllowZero("STRAVIN_CTRL_WITNESS_QUORUM", 0),
		WitnessTimeout:         envDuration("STRAVIN_CTRL_WITNESS_TIMEOUT", ctrl.DefaultWitnessTimeout),
		RecoveryOnBoot:         envBool("STRAVIN_CTRL_RECOVERY_ON_BOOT", false),
		RecoveryRepos:          envCSV("STRAVIN_CTRL_RECOVERY_REPOS"),
		RecoveryConcurrency:    envInt("STRAVIN_CTRL_RECOVERY_CONCURRENCY", ctrl.DefaultRecoveryConcurrency),
		RecoveryLookback:       envDurationAllowZero("STRAVIN_CTRL_RECOVERY_LOOKBACK", 0),
	}
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

func envDurationAllowZero(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
		return d
	}
	if secs, err := strconv.Atoi(raw); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return fallback
}

func envInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if v, err := strconv.Atoi(raw); err == nil && v > 0 {
		return v
	}
	return fallback
}

func envIntAllowZero(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if v, err := strconv.Atoi(raw); err == nil && v >= 0 {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	if b, err := strconv.ParseBool(raw); err == nil {
		return b
	}
	return fallback
}

func envCSV(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func buildS3Store(s3Client s3.API, bucket string, kmsClient crypto.KMSAPI, kmsKeyID string) (*s3.Store, error) {
	kmsOpt, err := s3.WithKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		return nil, fmt.Errorf("init kms envelope cipher: %w", err)
	}
	return s3.NewStore(s3Client, bucket, kmsOpt)
}

func buildPromotionGuard(cfg ctrlConfig) (ctrl.PromotionGuard, error) {
	if !cfg.RequirePromotionGuard {
		return nil, nil
	}
	var guards []ctrl.PromotionGuard
	if len(cfg.WitnessEndpoints) > 0 {
		wg, err := ctrl.NewWitnessQuorumGuard(ctrl.WitnessQuorumConfig{
			Endpoints: cfg.WitnessEndpoints,
			Quorum:    cfg.WitnessQuorum,
			Timeout:   cfg.WitnessTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("init witness quorum guard: %w", err)
		}
		guards = append(guards, wg)
	}
	if cfg.PromotionApprovalToken != "" {
		guards = append(guards, ctrl.NewOperatorPromotionGuard(cfg.PromotionApprovalToken))
	}
	if len(guards) == 0 {
		return nil, errors.New("STRAVIN_CTRL_REQUIRE_PROMOTION_GUARD=true requires STRAVIN_CTRL_WITNESS_ENDPOINTS and/or STRAVIN_CTRL_PROMOTION_APPROVAL_TOKEN")
	}
	policy := ctrl.CompositePolicy(strings.TrimSpace(cfg.PromotionGuardPolicy))
	return ctrl.NewCompositePromotionGuard(policy, guards...)
}

func run(ctx context.Context, cfg ctrlConfig) error {
	if strings.TrimSpace(cfg.AdminToken) == "" {
		return errors.New("STRAVIN_CTRL_ADMIN_TOKEN is required")
	}

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
		shutCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		if shutErr := telProvider.Shutdown(shutCtx); shutErr != nil {
			slog.Warn("telemetry shutdown error", "service", serviceName, "error", shutErr)
		}
	}()

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
	auditStore, err := ctrl.NewDDBAuditStore(ddbClient, cfg.DDBTable)
	if err != nil {
		return fmt.Errorf("init ddb audit store: %w", err)
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

	metrics, err := ctrl.NewMetrics(telProvider)
	if err != nil {
		return fmt.Errorf("init ctrl metrics: %w", err)
	}

	promotionGuard, err := buildPromotionGuard(cfg)
	if err != nil {
		return fmt.Errorf("init promotion guard: %w", err)
	}

	leaseMgr, err := ctrl.NewLeaseManager(ctrl.LeaseManagerConfig{
		Leases:            ddbRepo,
		Meta:              ddbRepo,
		Region:            lease.Region(cfg.Region),
		HolderID:          cfg.HolderID,
		LeaseTTL:          cfg.LeaseTTL,
		HeartbeatInterval: cfg.HeartbeatInterval,
		FenceWait:         cfg.FenceWait,
		PromotionGuard:    promotionGuard,
		PromotionAudit:    auditStore,
		Metrics:           metrics,
	})
	if err != nil {
		return fmt.Errorf("init lease manager: %w", err)
	}

	scrubber, err := ctrl.NewScrubber(ctrl.ScrubConfig{
		DDB:           ddbRepo,
		S3:            s3Store,
		Audit:         auditStore,
		Region:        lease.Region(cfg.Region),
		DefaultWindow: cfg.ScrubLookback,
		Concurrency:   cfg.ScrubConcurrency,
		Metrics:       metrics,
	})
	if err != nil {
		return fmt.Errorf("init scrubber: %w", err)
	}

	reconciler, err := ctrl.NewRecoveryReconciler(ctrl.RecoveryConfig{
		DDB:           ddbRepo,
		S3:            s3Store,
		Audit:         auditStore,
		Region:        lease.Region(cfg.Region),
		DefaultWindow: cfg.RecoveryLookback,
		Concurrency:   cfg.RecoveryConcurrency,
		Metrics:       metrics,
	})
	if err != nil {
		return fmt.Errorf("init recovery reconciler: %w", err)
	}

	handler, err := ctrl.NewHandler(ctrl.HandlerConfig{
		ServiceName: serviceName,
		Leases:      leaseMgr,
		Scrubber:    scrubber,
		Recovery:    reconciler,
		AdminToken:  cfg.AdminToken,
	})
	if err != nil {
		return fmt.Errorf("init http handler: %w", err)
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}

	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if cfg.RecoveryOnBoot && len(cfg.RecoveryRepos) > 0 {
		if _, renewErr := leaseMgr.RenewOnce(runCtx); renewErr != nil {
			slog.Warn("ctrl: initial lease renew before recovery boot failed", "region", cfg.Region, "error", renewErr)
		} else if shouldRunRecoveryBoot(leaseMgr.Status().Mode) {
			runRecoveryBoot(runCtx, reconciler, cfg)
		}
	}

	leaseDone := make(chan error, 1)
	go func() {
		leaseDone <- leaseMgr.Run(runCtx)
	}()

	httpErrCh := make(chan error, 1)
	go func() {
		slog.Info("stravin-ctrl listening", "service", serviceName, "addr", ln.Addr().String(), "region", cfg.Region)
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			httpErrCh <- serveErr
			return
		}
		httpErrCh <- nil
	}()

	var serveErr error
	select {
	case serveErr = <-httpErrCh:
		cancel()
	case <-ctx.Done():
		cancel()
		shutCtx, shutCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer shutCancel()
		if shutErr := srv.Shutdown(shutCtx); shutErr != nil {
			serveErr = fmt.Errorf("http shutdown: %w", shutErr)
		} else {
			serveErr = <-httpErrCh
		}
	}

	leaseErr := <-leaseDone
	if serveErr != nil {
		return serveErr
	}
	return leaseErr
}

func shouldRunRecoveryBoot(mode lease.Mode) bool {
	return mode == lease.ModeDraining || mode == lease.ModeReadOnly
}

func parseRecoveryRepoKey(raw string) (tenant.RepoKey, error) {
	tenantID, repoID, ok := strings.Cut(strings.TrimSpace(raw), "/")
	if !ok {
		return tenant.RepoKey{}, fmt.Errorf("invalid recovery repo %q: want tenant/repo", raw)
	}
	return tenant.NewRepoKey(tenantID, repoID)
}

func runRecoveryBoot(ctx context.Context, reconciler *ctrl.RecoveryReconciler, cfg ctrlConfig) {
	for _, raw := range cfg.RecoveryRepos {
		rk, err := parseRecoveryRepoKey(raw)
		if err != nil {
			slog.Warn("ctrl: skipping invalid recovery boot repo", "repo", raw, "error", err)
			continue
		}
		report, err := reconciler.ReconcileRepo(ctx, ctrl.RecoveryRequest{
			Repo:           rk,
			LookbackWindow: cfg.RecoveryLookback,
			Concurrency:    cfg.RecoveryConcurrency,
		})
		if err != nil {
			slog.Warn("ctrl: recovery boot reconciliation failed",
				"tenant_id", string(rk.TenantID),
				"repo_id", string(rk.RepoID),
				"error", err,
			)
			continue
		}
		slog.Info("ctrl: recovery boot reconciliation completed",
			"tenant_id", string(rk.TenantID),
			"repo_id", string(rk.RepoID),
			"status", report.Status,
			"published", len(report.Published),
		)
	}
}

func main() {
	if err := runWithSignals(); err != nil {
		slog.Error("stravin-ctrl exited with error", "error", err)
		os.Exit(1)
	}
}

func runWithSignals() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return run(ctx, loadConfig())
}
