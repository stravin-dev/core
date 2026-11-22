package loadgen

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestInMemoryTelemetryExportsTracesMetricsLogsOnShutdown(t *testing.T) {
	var logBuf bytes.Buffer
	tel, exp, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry error: %v", err)
	}

	ctx, span := tel.Tracer.Start(context.Background(), "loadgen.test.op")
	tel.Logger.InfoContext(ctx, "hello trace-correlated log", "scenario", "smoke")

	tel.RecordOp(ctx, PersonaDev, "smoke", "push", "ok", 42*time.Millisecond)
	tel.RecordRetry(ctx, PersonaDev, "smoke", "retryable_conflict")
	tel.RecordUnexpectedError(ctx, PersonaDev, "smoke", "push")
	tel.RecordFsckFailure(ctx, "smoke")
	tel.AddActiveThreads(ctx, PersonaDev, 1)
	tel.AddActiveThreads(ctx, PersonaDev, -1)
	span.End()

	// Collect metrics before shutdown (ManualReader collects on demand).
	var rm metricdata.ResourceMetrics
	if err := exp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect error: %v", err)
	}

	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}

	// Verify spans exported and TracerProvider Shutdown called.
	spans := exp.SpanExporter.Spans()
	if len(spans) != 1 || spans[0].Name() != "loadgen.test.op" {
		t.Fatalf("exported spans = %+v, want 1 span named loadgen.test.op", spans)
	}
	if !exp.SpanExporter.WasShutdown() {
		t.Error("expected SpanExporter.Shutdown to be called")
	}

	// Verify OTLP log exporter received trace-correlated log record AND Shutdown was called.
	logs := exp.LogExporter.Records()
	if len(logs) != 1 {
		t.Fatalf("exported OTLP logs = %d, want 1", len(logs))
	}
	if logs[0].TraceID() != spans[0].SpanContext().TraceID() {
		t.Errorf("log TraceID = %s, want %s", logs[0].TraceID(), spans[0].SpanContext().TraceID())
	}
	if !exp.LogExporter.WasShutdown() {
		t.Error("expected LogExporter.Shutdown to be called")
	}

	// Verify JSON stdout log also carries trace_id.
	if !strings.Contains(logBuf.String(), spans[0].SpanContext().TraceID().String()) {
		t.Errorf("logBuf missing trace_id: %s", logBuf.String())
	}

	// Verify all 6 required metrics are present.
	found := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			found[m.Name] = true
		}
	}
	requiredMetrics := []string{
		"stravin_loadgen_ops_total",
		"stravin_loadgen_op_duration_seconds",
		"stravin_loadgen_retries_total",
		"stravin_loadgen_unexpected_errors_total",
		"stravin_loadgen_fsck_failures_total",
		"stravin_loadgen_active_threads",
	}
	for _, name := range requiredMetrics {
		if !found[name] {
			t.Errorf("missing required metric %q; found: %v", name, found)
		}
	}
	_ = sdkmetric.NewManualReader
}

func TestNewTelemetryEmptyEndpointNoop(t *testing.T) {
	var logBuf bytes.Buffer
	tel, err := NewTelemetry(context.Background(), "", &logBuf)
	if err != nil {
		t.Fatalf("NewTelemetry empty endpoint error: %v", err)
	}
	ctx, span := tel.Tracer.Start(context.Background(), "noop-span")
	tel.Logger.InfoContext(ctx, "noop-log")
	span.End()
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown error: %v", err)
	}
}

func TestTelemetryParallelShutdownDoesNotStarveFastExporters(t *testing.T) {
	var fastDone atomic.Bool
	errSlow := errors.New("slow exporter blocked")
	errFast := errors.New("fast exporter error")

	tel := &Telemetry{
		shutdownFns: []func(context.Context) error{
			func(ctx context.Context) error {
				<-ctx.Done()
				return errSlow
			},
			func(ctx context.Context) error {
				if ctx.Err() == nil {
					fastDone.Store(true)
				}
				return errFast
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	err := tel.Shutdown(ctx)
	if !fastDone.Load() {
		t.Fatal("expected fast exporter to finish in parallel despite slow exporter")
	}
	if !errors.Is(err, errSlow) || !errors.Is(err, errFast) {
		t.Fatalf("Shutdown error = %v, want both %v and %v", err, errSlow, errFast)
	}
}
