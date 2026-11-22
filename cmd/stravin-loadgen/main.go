// Command stravin-loadgen is the standalone Stravin load generator driving
// configurable parallel developer and AI agent Git workloads with client-side
// integrity verification and OpenTelemetry traces, metrics, and logs.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"stravin.dev/core/internal/loadgen"
)

type runtimeDeps struct {
	newTelemetry func(ctx context.Context, endpoint string) (*loadgen.Telemetry, error)
	newRegistry  func() *loadgen.Registry
}

func defaultDeps(logOut io.Writer) runtimeDeps {
	return runtimeDeps{
		newTelemetry: func(ctx context.Context, endpoint string) (*loadgen.Telemetry, error) {
			return loadgen.NewTelemetry(ctx, endpoint, logOut)
		},
		newRegistry: func() *loadgen.Registry {
			reg := loadgen.NewRegistry()
			if err := loadgen.RegisterBaselineScenarios(reg); err != nil {
				panic(err)
			}
			return reg
		},
	}
}

func main() {
	os.Exit(mainWithSignals())
}

func mainWithSignals() int {
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return run(sigCtx, os.Args[1:], defaultDeps(os.Stderr), os.Stdout, os.Stderr)
}

func run(ctx context.Context, args []string, deps runtimeDeps, stdout, stderr io.Writer) int {
	cfg, err := loadgen.ParseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		_, _ = fmt.Fprintf(stderr, "stravin-loadgen: %v\n", err)
		return 1
	}

	tel, err := deps.newTelemetry(ctx, cfg.OtelEndpoint)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "stravin-loadgen: init telemetry: %v\n", err)
		return 1
	}

	reg := deps.newRegistry()
	oracle := loadgen.NewOracle()

	runner, err := loadgen.NewRunner(cfg, reg, tel, oracle)
	if err != nil {
		flushTelemetry(tel, cfg, stderr)
		_, _ = fmt.Fprintf(stderr, "stravin-loadgen: %v\n", err)
		return 1
	}

	result := runner.Run(ctx)

	// Flush all OTel providers (traces, metrics, logs) using a fresh timeout context
	// so Ctrl+C / SIGTERM cancellation never drops buffered telemetry.
	flushTelemetry(tel, cfg, stderr)

	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		_, _ = fmt.Fprintf(stderr, "stravin-loadgen: encode summary: %v\n", err)
		return 1
	}

	if result.Oracle.HasFailures() {
		return 1
	}
	return 0
}

func flushTelemetry(tel *loadgen.Telemetry, cfg loadgen.Config, stderr io.Writer) {
	if tel == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	defer cancel()
	if err := tel.Shutdown(flushCtx); err != nil {
		_, _ = fmt.Fprintf(stderr, "stravin-loadgen: telemetry flush warning: %v\n", err)
	}
}
