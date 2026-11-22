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
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/kms"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/daemon"
	"stravin.dev/core/internal/hooks/async"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("stravin-hook exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", "us-east-1"))
	tableName := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	kmsKeyID := envOrDefault("STRAVIN_KMS_KEY_ID", envOrDefault("STRAVIN_KMS_ALIAS", "alias/stravin-local-dev"))
	healthAddr := daemon.ListenAddrFromEnv(":8084")
	consumerID := envOrDefault("STRAVIN_HOOK_CONSUMER_ID", "stravin-hook")
	pollInterval := envDurationOrDefault("STRAVIN_HOOK_POLL_INTERVAL", 500*time.Millisecond)
	requestTimeout := envDurationOrDefault("STRAVIN_WEBHOOK_TIMEOUT", 10*time.Second)
	maxAttempts := envIntOrDefault("STRAVIN_WEBHOOK_MAX_ATTEMPTS", 3)
	allowPrivateNetworks := allowPrivateNetworksFromEnv()

	telProvider, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: "stravin-hook",
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
			_, _ = fmt.Fprintf(os.Stderr, "stravin-hook: telemetry shutdown warning: %v\n", shutErr)
		}
	}()

	logger := telProvider.Logger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(logger)
	if allowPrivateNetworks {
		logger.Warn("webhook private network delivery enabled via STRAVIN_WEBHOOK_ALLOW_PRIVATE_NETWORKS; do not enable in production")
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("load aws config: %w", err)
	}
	endpoint := strings.TrimSpace(os.Getenv("AWS_ENDPOINT_URL"))

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	streamsClient := dynamodbstreams.NewFromConfig(awsCfg, func(o *dynamodbstreams.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	kmsClient := kms.NewFromConfig(awsCfg, func(o *kms.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})

	ddbRepo, err := ddb.NewRepository(ddbClient, tableName)
	if err != nil {
		return fmt.Errorf("init ddb repository: %w", err)
	}

	cipher, err := crypto.NewKMSEnvelopeCipher(kmsClient, kmsKeyID)
	if err != nil {
		return fmt.Errorf("init kms cipher: %w", err)
	}

	ddbDLQ, err := async.NewDynamoDBDLQSink(ddbClient, tableName)
	if err != nil {
		return fmt.Errorf("init dynamodb dlq sink: %w", err)
	}
	dlqSink := async.NewMultiDLQSink(async.NewLoggingDLQSink(logger), ddbDLQ)

	dispatcher, err := async.NewWebhookDispatcher(async.DispatcherConfig{
		Webhooks:             ddbRepo,
		Cipher:               cipher,
		DLQ:                  dlqSink,
		Telemetry:            telProvider,
		Logger:               logger,
		RequestTimeout:       requestTimeout,
		MaxAttempts:          maxAttempts,
		AllowPrivateNetworks: allowPrivateNetworks,
	})
	if err != nil {
		return fmt.Errorf("init webhook dispatcher: %w", err)
	}

	consumer, err := async.NewConsumer(async.ConsumerConfig{
		TableName:    tableName,
		ConsumerID:   consumerID,
		PollInterval: pollInterval,
		Tables:       ddbClient,
		Streams:      streamsClient,
		Checkpoints:  ddbRepo,
		Observers:    []async.EventObserver{dispatcher},
		Logger:       logger,
	})
	if err != nil {
		return fmt.Errorf("init streams consumer: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 2)
	go func() {
		errCh <- daemon.Run(runCtx, daemon.Config{
			ServiceName: "stravin-hook",
			ListenAddr:  healthAddr,
		})
	}()
	go func() {
		errCh <- consumer.Run(runCtx)
	}()

	firstErr := <-errCh
	cancel()
	secondErr := <-errCh
	if firstErr != nil {
		return firstErr
	}
	return secondErr
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

func envIntOrDefault(key string, fallback int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func allowPrivateNetworksFromEnv() bool {
	if v := strings.TrimSpace(os.Getenv("STRAVIN_WEBHOOK_ALLOW_PRIVATE_NETWORKS")); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return false
}
