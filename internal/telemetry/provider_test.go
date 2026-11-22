package telemetry_test

import (
	"context"
	"testing"

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
