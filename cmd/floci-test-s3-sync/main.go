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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"stravin.dev/core/internal/flocitest/s3sync"
)

const (
	defaultSourceRegion = "us-east-1"
	defaultTargetRegion = "us-west-2"
)

type syncConfig struct {
	SourceEndpoint   string
	SourceRegion     string
	SourceBucket     string
	SourceQueueURL   string
	TargetEndpoint   string
	TargetRegion     string
	TargetBucket     string
	LagDelay         time.Duration
	ReplicateDeletes bool
}

func loadConfig() (syncConfig, error) {
	cfg := syncConfig{
		SourceEndpoint: strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_ENDPOINT")),
		SourceRegion:   envOrDefault("FLOCI_SYNC_SOURCE_REGION", defaultSourceRegion),
		SourceBucket:   strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_BUCKET")),
		SourceQueueURL: strings.TrimSpace(os.Getenv("FLOCI_SYNC_SOURCE_QUEUE_URL")),
		TargetEndpoint: strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_ENDPOINT")),
		TargetRegion:   envOrDefault("FLOCI_SYNC_TARGET_REGION", defaultTargetRegion),
		TargetBucket:   strings.TrimSpace(os.Getenv("FLOCI_SYNC_TARGET_BUCKET")),
	}

	var missing []string
	if cfg.SourceEndpoint == "" {
		missing = append(missing, "FLOCI_SYNC_SOURCE_ENDPOINT")
	}
	if cfg.SourceBucket == "" {
		missing = append(missing, "FLOCI_SYNC_SOURCE_BUCKET")
	}
	if cfg.SourceQueueURL == "" {
		missing = append(missing, "FLOCI_SYNC_SOURCE_QUEUE_URL")
	}
	if cfg.TargetEndpoint == "" {
		missing = append(missing, "FLOCI_SYNC_TARGET_ENDPOINT")
	}
	if cfg.TargetBucket == "" {
		missing = append(missing, "FLOCI_SYNC_TARGET_BUCKET")
	}
	if len(missing) > 0 {
		return syncConfig{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	if rawLag := strings.TrimSpace(os.Getenv("FLOCI_SYNC_LAG_DELAY")); rawLag != "" {
		d, err := time.ParseDuration(rawLag)
		if err != nil {
			return syncConfig{}, fmt.Errorf("parse FLOCI_SYNC_LAG_DELAY %q: %w", rawLag, err)
		}
		if d < 0 {
			return syncConfig{}, fmt.Errorf("FLOCI_SYNC_LAG_DELAY must be non-negative, got %s", d)
		}
		cfg.LagDelay = d
	}

	if rawDel := strings.TrimSpace(os.Getenv("FLOCI_SYNC_REPLICATE_DELETES")); rawDel != "" {
		b, err := strconv.ParseBool(rawDel)
		if err != nil {
			return syncConfig{}, fmt.Errorf("parse FLOCI_SYNC_REPLICATE_DELETES %q: %w", rawDel, err)
		}
		cfg.ReplicateDeletes = b
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func run(ctx context.Context, cfg syncConfig) error {
	srcAWS, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.SourceRegion))
	if err != nil {
		return fmt.Errorf("load source aws config: %w", err)
	}
	dstAWS, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.TargetRegion))
	if err != nil {
		return fmt.Errorf("load target aws config: %w", err)
	}

	sqsClient := sqs.NewFromConfig(srcAWS, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(cfg.SourceEndpoint)
	})
	srcS3Client := s3.NewFromConfig(srcAWS, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.SourceEndpoint)
		o.UsePathStyle = true
	})
	dstS3Client := s3.NewFromConfig(dstAWS, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.TargetEndpoint)
		o.UsePathStyle = true
	})

	consumer, err := s3sync.NewSQSConsumer(sqsClient, cfg.SourceQueueURL)
	if err != nil {
		return err
	}
	reader, err := s3sync.NewS3ObjectClient(srcS3Client)
	if err != nil {
		return err
	}
	writer, err := s3sync.NewS3ObjectClient(dstS3Client)
	if err != nil {
		return err
	}

	replicator, err := s3sync.NewReplicator(s3sync.Config{
		SourceRegion:     cfg.SourceRegion,
		SourceBucket:     cfg.SourceBucket,
		TargetRegion:     cfg.TargetRegion,
		TargetBucket:     cfg.TargetBucket,
		LagDelay:         cfg.LagDelay,
		ReplicateDeletes: cfg.ReplicateDeletes,
		Logger:           slog.Default(),
	}, consumer, reader, writer)
	if err != nil {
		return err
	}

	return replicator.Run(ctx)
}

func runWithSignals() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg)
}

func main() {
	if err := runWithSignals(); err != nil {
		slog.Error("floci-test-s3-sync exited with error", "error", err)
		os.Exit(1)
	}
}
