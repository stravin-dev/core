package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"stravin.dev/core/internal/chaos"
	"stravin.dev/core/internal/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintf(os.Stderr, "stravin-chaos: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	return runWithTelemetry(ctx, args, stdout, stderr, telemetry.Init)
}

func runWithTelemetry(
	ctx context.Context,
	args []string,
	stdout, stderr io.Writer,
	initTelemetry func(context.Context, telemetry.Config) (*telemetry.Provider, error),
) error {
	fs := flag.NewFlagSet("stravin-chaos", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		toxiproxyURL   string
		seed           int64
		interval       time.Duration
		faultDuration  time.Duration
		duration       time.Duration
		enableRestarts bool
		composeFile    string
		restartTargets string
		otelEndpoint   string
	)

	fs.StringVar(&toxiproxyURL, "toxiproxy-url", "http://127.0.0.1:8474", "Toxiproxy HTTP admin API base URL")
	fs.Int64Var(&seed, "seed", time.Now().UnixNano(), "Deterministic PRNG seed for fault scheduling")
	fs.DurationVar(&interval, "interval", 5*time.Second, "Wait interval between injected faults")
	fs.DurationVar(&faultDuration, "fault-duration", 3*time.Second, "Duration each toxic remains active before recovery")
	fs.DurationVar(&duration, "duration", 0, "Total run duration (0 = continuous until SIGINT/SIGTERM)")
	fs.BoolVar(&enableRestarts, "enable-restarts", false, "Enable optional service restart faults (never restarts floci)")
	fs.StringVar(&composeFile, "compose-file", "docker-compose.chaos.yml", "Docker Compose file used for optional service restarts")
	fs.StringVar(&restartTargets, "restart-targets", strings.Join(chaos.DefaultRestartTargets, ","), "Comma-separated list of services eligible for restarts")
	fs.StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP gRPC endpoint (defaults to OTEL_EXPORTER_OTLP_ENDPOINT)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	provider, err := initTelemetry(ctx, telemetry.Config{
		ServiceName: "stravin-chaos",
		Environment: "local-chaos",
		Endpoint:    otelEndpoint,
		Insecure:    true,
		Mode:        telemetry.ModeOTLP,
		SetGlobal:   true,
	})
	if err != nil {
		return fmt.Errorf("init telemetry: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), telemetry.ShutdownTimeoutFromEnv())
		defer cancel()
		if shutdownErr := provider.Shutdown(flushCtx); shutdownErr != nil {
			_, _ = fmt.Fprintf(stderr, "stravin-chaos: telemetry shutdown warning: %v\n", shutdownErr)
		}
	}()

	logger := provider.Logger(stdout, slog.LevelInfo)
	slog.SetDefault(logger)

	targets := parseCSV(restartTargets)
	proxyClient := chaos.NewToxiproxyClient(toxiproxyURL, nil)
	svcCtrl := chaos.NewComposeServiceController(composeFile)

	nemesis, err := chaos.NewNemesis(chaos.Config{
		Seed:           seed,
		Interval:       interval,
		FaultDuration:  faultDuration,
		Duration:       duration,
		EnableRestarts: enableRestarts,
		RestartTargets: targets,
	}, chaos.Dependencies{
		Proxy:   proxyClient,
		Service: svcCtrl,
		Tracer:  provider.Tracer("stravin-chaos"),
		Logger:  logger,
	})
	if err != nil {
		return err
	}

	logger.InfoContext(ctx, "stravin-chaos starting",
		slog.String("toxiproxy_url", toxiproxyURL),
		slog.Int64("seed", seed),
		slog.Duration("interval", interval),
		slog.Duration("fault_duration", faultDuration),
		slog.Duration("duration", duration),
		slog.Bool("enable_restarts", enableRestarts),
	)

	return nemesis.Run(ctx)
}

func parseCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
