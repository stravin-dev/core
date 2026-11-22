package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/flocitest/ddbsync"
)

type envConfig struct {
	SourceEndpoint       string
	SourceRegion         string
	SourceTable          string
	TargetEndpoint       string
	TargetRegion         string
	TargetTable          string
	LagDelay             time.Duration
	PollInterval         time.Duration
	IteratorType         streamtypes.ShardIteratorType
	DeleteSuppressionTTL time.Duration
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("floci-test-ddb-sync exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := loadEnvConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sourceAWS, err := loadFlociAWSConfig(ctx, cfg.SourceRegion, cfg.SourceEndpoint)
	if err != nil {
		return fmt.Errorf("load source aws config: %w", err)
	}
	targetAWS, err := loadFlociAWSConfig(ctx, cfg.TargetRegion, cfg.TargetEndpoint)
	if err != nil {
		return fmt.Errorf("load target aws config: %w", err)
	}

	sourceDDB := dynamodb.NewFromConfig(sourceAWS)
	sourceStreams := dynamodbstreams.NewFromConfig(sourceAWS)
	targetDDB := dynamodb.NewFromConfig(targetAWS)

	replicator, err := ddbsync.NewReplicator(ddbsync.Config{
		SourceRegion:         cfg.SourceRegion,
		SourceTable:          cfg.SourceTable,
		TargetRegion:         cfg.TargetRegion,
		TargetTable:          cfg.TargetTable,
		PollInterval:         cfg.PollInterval,
		LagDelay:             cfg.LagDelay,
		IteratorType:         cfg.IteratorType,
		DeleteSuppressionTTL: cfg.DeleteSuppressionTTL,
		Tables:               sourceDDB,
		Streams:              sourceStreams,
		Target:               targetDDB,
		Logger:               logger,
	})
	if err != nil {
		return fmt.Errorf("init replicator: %w", err)
	}

	logger.Info("starting floci-test-ddb-sync",
		"source_endpoint", cfg.SourceEndpoint,
		"source_region", cfg.SourceRegion,
		"source_table", cfg.SourceTable,
		"target_endpoint", cfg.TargetEndpoint,
		"target_region", cfg.TargetRegion,
		"target_table", cfg.TargetTable,
		"poll_interval", cfg.PollInterval.String(),
		"lag_delay", cfg.LagDelay.String(),
		"iterator_type", string(cfg.IteratorType),
	)

	if err := replicator.Run(ctx); err != nil {
		return err
	}
	logger.Info("floci-test-ddb-sync stopped cleanly")
	return nil
}

func loadEnvConfig() (envConfig, error) {
	cfg := envConfig{
		SourceEndpoint: strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_ENDPOINT")),
		SourceRegion:   strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_REGION")),
		SourceTable:    strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_TABLE")),
		TargetEndpoint: strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_ENDPOINT")),
		TargetRegion:   strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_REGION")),
		TargetTable:    strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_TABLE")),
		PollInterval:   ddbsync.DefaultPollInterval,
		IteratorType:   streamtypes.ShardIteratorTypeTrimHorizon,
	}

	if cfg.SourceEndpoint == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_SOURCE_ENDPOINT is required")
	}
	if cfg.SourceRegion == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_SOURCE_REGION is required")
	}
	if cfg.SourceTable == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_SOURCE_TABLE is required")
	}
	if cfg.TargetEndpoint == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_TARGET_ENDPOINT is required")
	}
	if cfg.TargetRegion == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_TARGET_REGION is required")
	}
	if cfg.TargetTable == "" {
		return envConfig{}, errors.New("FLOCI_SYNC_TARGET_TABLE is required")
	}

	if raw := strings.TrimSpace(os.Getenv("FLOCI_SYNC_LAG_DELAY")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 {
			return envConfig{}, fmt.Errorf("invalid FLOCI_SYNC_LAG_DELAY %q", raw)
		}
		cfg.LagDelay = d
	}

	if raw := strings.TrimSpace(os.Getenv("FLOCI_SYNC_POLL_INTERVAL")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return envConfig{}, fmt.Errorf("invalid FLOCI_SYNC_POLL_INTERVAL %q", raw)
		}
		cfg.PollInterval = d
	}

	if raw := strings.TrimSpace(os.Getenv("FLOCI_SYNC_ITERATOR_TYPE")); raw != "" {
		iterType := streamtypes.ShardIteratorType(raw)
		if iterType != streamtypes.ShardIteratorTypeTrimHorizon && iterType != streamtypes.ShardIteratorTypeLatest {
			return envConfig{}, fmt.Errorf("invalid FLOCI_SYNC_ITERATOR_TYPE %q (want TRIM_HORIZON or LATEST)", raw)
		}
		cfg.IteratorType = iterType
	}

	if raw := strings.TrimSpace(os.Getenv("FLOCI_SYNC_DELETE_SUPPRESSION_TTL")); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return envConfig{}, fmt.Errorf("invalid FLOCI_SYNC_DELETE_SUPPRESSION_TTL %q", raw)
		}
		cfg.DeleteSuppressionTTL = d
	}

	return cfg, nil
}

func loadFlociAWSConfig(ctx context.Context, region, endpoint string) (aws.Config, error) {
	accessKey := envOrDefault("AWS_ACCESS_KEY_ID", "test")
	secretKey := envOrDefault("AWS_SECRET_ACCESS_KEY", "test")
	creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID:     accessKey,
			SecretAccessKey: secretKey,
			Source:          "floci-test-ddb-sync",
		}, nil
	})
	return config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(creds),
		config.WithBaseEndpoint(endpoint),
	)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
