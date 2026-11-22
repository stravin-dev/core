package telemetry_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/telemetry"
)

func TestNewNoopProvider_ZeroNetworkAndGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	prov := telemetry.NewNoopProvider()

	_, span := prov.Tracer("stravin.test").Start(ctx, "noop.op")
	span.End()

	c, err := prov.Meter("stravin.test").Int64Counter("test_counter")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(ctx, 1, metric.WithAttributes(attribute.String("git.operation", "fetch")))

	if err := prov.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestNewInMemoryProvider_CapturesSpansAndMetrics(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-pack")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	_, span := prov.Tracer("stravin.pack").Start(ctx, "pack.compact.shard")
	span.SetAttributes(telemetry.NewSpanAttrs().ManifestShard(4).Build()...)
	span.End()

	spans := prov.SpanExporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span captured, got %d", len(spans))
	}
	if spans[0].Name != "pack.compact.shard" {
		t.Errorf("span name = %q, want pack.compact.shard", spans[0].Name)
	}

	c, err := prov.Meter("stravin.pack").Int64Counter("stravin_pack_gc_reaped_total")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(ctx, 3)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected at least one ScopeMetrics entry")
	}
}

func TestInit_EmptyEndpointFallsBackToNoop(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	ctx := context.Background()

	prov, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName: "stravin-ctrl",
		Mode:        telemetry.ModeOTLP,
	})
	if err != nil {
		t.Fatalf("Init with empty endpoint: %v", err)
	}
	if err := prov.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestInit_OTLPModeInitializesAndShutsDown(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.Init(ctx, telemetry.Config{
		ServiceName:    "stravin-edge",
		ServiceVersion: "1.0.0",
		Environment:    "local",
		Region:         "us-east-1",
		Endpoint:       "127.0.0.1:4317",
		Insecure:       true,
		Mode:           telemetry.ModeOTLP,
	})
	if err != nil {
		t.Fatalf("Init ModeOTLP: %v", err)
	}

	// Shutdown with an already-canceled context or short timeout should return
	// cleanly without panicking even when no collector is listening.
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_ = prov.Shutdown(cancelCtx)
}

func TestNewInMemoryProvider_CapturesLogsAndFlushesOnShutdown(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}

	var buf bytes.Buffer
	logger := prov.Logger(&buf, slog.LevelInfo)

	spanCtx, span := prov.Tracer("stravin.edge").Start(ctx, "edge.handle")
	wantTraceID := span.SpanContext().TraceID()
	logger.DebugContext(spanCtx, "filtered debug log")
	logger.InfoContext(spanCtx, "handled request", "repo", "demo")
	span.End()

	if err := prov.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if !prov.LogExporter.WasFlushed() {
		t.Error("expected LogExporter.ForceFlush to be called")
	}

	if err := prov.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !prov.LogExporter.WasShutdown() {
		t.Error("expected LogExporter.Shutdown to be called")
	}

	records := prov.LogExporter.Records()
	if len(records) != 1 {
		t.Fatalf("exported OTLP logs = %d, want 1", len(records))
	}
	if records[0].TraceID() != wantTraceID {
		t.Errorf("OTLP log TraceID = %s, want %s", records[0].TraceID(), wantTraceID)
	}
	if !strings.Contains(buf.String(), wantTraceID.String()) {
		t.Errorf("JSON stdout log missing trace_id %s: %s", wantTraceID, buf.String())
	}
	if strings.Contains(buf.String(), "filtered debug log") {
		t.Errorf("expected debug log filtered out, got: %s", buf.String())
	}
}

func TestShutdownTimeoutFromEnv(t *testing.T) {
	tests := []struct {
		name string
		val  string
		want time.Duration
	}{
		{name: "unset", val: "", want: telemetry.DefaultShutdownTimeout},
		{name: "valid duration", val: "12s", want: 12 * time.Second},
		{name: "trimmed valid duration", val: "  250ms ", want: 250 * time.Millisecond},
		{name: "invalid string", val: "not-a-duration", want: telemetry.DefaultShutdownTimeout},
		{name: "zero duration", val: "0s", want: telemetry.DefaultShutdownTimeout},
		{name: "negative duration", val: "-3s", want: telemetry.DefaultShutdownTimeout},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("STRAVIN_OTEL_SHUTDOWN_TIMEOUT", tc.val)
			if got := telemetry.ShutdownTimeoutFromEnv(); got != tc.want {
				t.Errorf("ShutdownTimeoutFromEnv(%q) = %v, want %v", tc.val, got, tc.want)
			}
		})
	}
}
