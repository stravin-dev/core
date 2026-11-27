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

	"stravin.dev/core/internal/daemon"
	"stravin.dev/core/internal/relay"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		slog.Error("stravin-relay exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	region := envOrDefault("STRAVIN_REGION", envOrDefault("AWS_REGION", "us-east-1"))
	tableName := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	healthAddr := daemon.ListenAddrFromEnv(":8084")
	consumerID := consumerIDFromEnv()
	pollInterval := pollIntervalFromEnv()
	natsURL := natsURLFromEnv()
	publishTimeout := envDurationOrDefault("STRAVIN_NATS_PUBLISH_TIMEOUT", 5*time.Second)
	maxAttempts := envIntOrDefault("STRAVIN_RELAY_MAX_ATTEMPTS", 3)

	telProvider, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: "stravin-relay",
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
			_, _ = fmt.Fprintf(os.Stderr, "stravin-relay: telemetry shutdown warning: %v\n", shutErr)
		}
	}()

	logger := telProvider.Logger(os.Stdout, slog.LevelInfo)
	slog.SetDefault(logger)

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

	ddbRepo, err := ddb.NewRepository(ddbClient, tableName)
	if err != nil {
		return fmt.Errorf("init ddb repository: %w", err)
	}

	var pub relay.Publisher
	if natsURL == "" {
		logger.Warn("STRAVIN_NATS_URL not set; running stravin-relay in degraded no-op publish mode")
		pub = relay.NewNoopPublisher(logger)
	} else {
		jsPub, jsErr := relay.NewJetStreamPublisher(relay.JetStreamConfig{
			URL:            natsURL,
			ClientName:     consumerID,
			PublishTimeout: publishTimeout,
		})
		if jsErr != nil {
			return fmt.Errorf("init nats jetstream publisher: %w", jsErr)
		}
		pub = jsPub
	}
	defer func() {
		if closeErr := pub.Close(); closeErr != nil {
			logger.Warn("stravin-relay publisher close warning", "error", closeErr)
		}
	}()

	relayMetrics, err := relay.NewMetrics(telProvider)
	if err != nil {
		return fmt.Errorf("init relay metrics: %w", err)
	}

	dispatcher, err := relay.NewRelayDispatcher(relay.DispatcherConfig{
		Publisher:   pub,
		Telemetry:   telProvider,
		Metrics:     relayMetrics,
		Logger:      logger,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		return fmt.Errorf("init relay dispatcher: %w", err)
	}

	consumer, err := relay.NewConsumer(relay.ConsumerConfig{
		TableName:    tableName,
		ConsumerID:   consumerID,
		PollInterval: pollInterval,
		Tables:       ddbClient,
		Streams:      streamsClient,
		Checkpoints:  ddbRepo,
		Observers:    []relay.EventObserver{dispatcher},
		Telemetry:    telProvider,
		Metrics:      relayMetrics,
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
			ServiceName: "stravin-relay",
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

func consumerIDFromEnv() string {
	return envOrDefault("STRAVIN_RELAY_CONSUMER_ID", envOrDefault("STRAVIN_HOOK_CONSUMER_ID", "stravin-relay"))
}

func pollIntervalFromEnv() time.Duration {
	if v := strings.TrimSpace(os.Getenv("STRAVIN_RELAY_POLL_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
	}
	return envDurationOrDefault("STRAVIN_HOOK_POLL_INTERVAL", 500*time.Millisecond)
}

func natsURLFromEnv() string {
	return envOrDefault("STRAVIN_NATS_URL", envOrDefault("NATS_URL", ""))
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
