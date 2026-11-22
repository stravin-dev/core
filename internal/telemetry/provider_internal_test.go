package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type retainingSpanExporter struct {
	mu       sync.Mutex
	spans    []sdktrace.ReadOnlySpan
	shutdown bool
}

func (e *retainingSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *retainingSpanExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

func (e *retainingSpanExporter) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.spans)
}

func (e *retainingSpanExporter) wasShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

func TestProvider_ParallelShutdownDoesNotStarveFastExporters(t *testing.T) {
	var fast1Done, fast2Done atomic.Bool
	errSlow := errors.New("slow exporter timed out")
	errFast := errors.New("fast exporter warning")

	p := &Provider{
		shutdownFns: []func(context.Context) error{
			func(ctx context.Context) error {
				<-ctx.Done()
				return errSlow
			},
			func(ctx context.Context) error {
				if ctx.Err() == nil {
					fast1Done.Store(true)
				}
				return nil
			},
			func(ctx context.Context) error {
				if ctx.Err() == nil {
					fast2Done.Store(true)
				}
				return errFast
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.Shutdown(ctx)
	elapsed := time.Since(start)

	if !fast1Done.Load() || !fast2Done.Load() {
		t.Fatalf("expected fast exporters to run in parallel despite slow exporter (fast1=%v fast2=%v)", fast1Done.Load(), fast2Done.Load())
	}
	if !errors.Is(err, errSlow) || !errors.Is(err, errFast) {
		t.Fatalf("Shutdown error = %v, want errors.Join containing both %v and %v", err, errSlow, errFast)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Shutdown took %v, expected bounded parallel execution", elapsed)
	}
}

func TestProvider_BufferedBatchProcessorsFlushOnForceFlushAndShutdown(t *testing.T) {
	ctx := context.Background()

	spanExp := &retainingSpanExporter{}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExp, sdktrace.WithBatchTimeout(time.Hour)),
	)

	metricReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))

	logExp := NewInMemoryLogExporter()
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp, sdklog.WithExportInterval(time.Hour))),
	)

	p := &Provider{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
		Propagator:     NewPropagator(),
		MetricReader:   metricReader,
		LogExporter:    logExp,
		serviceName:    "stravin-buffered-test",
		forceFlushFns:  []func(context.Context) error{tp.ForceFlush, mp.ForceFlush, lp.ForceFlush},
		shutdownFns:    []func(context.Context) error{tp.Shutdown, mp.Shutdown, lp.Shutdown},
	}

	var buf bytes.Buffer
	logger := p.Logger(&buf, slog.LevelInfo)

	spanCtx, span := p.Tracer("buffered").Start(ctx, "buffered.op")
	logger.InfoContext(spanCtx, "buffered log message")
	c, err := p.Meter("buffered").Int64Counter("buffered_counter")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	c.Add(spanCtx, 5)
	span.End()

	if err := p.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush: %v", err)
	}
	if spanExp.count() != 1 {
		t.Fatalf("spans after ForceFlush = %d, want 1", spanExp.count())
	}
	if len(logExp.Records()) != 1 {
		t.Fatalf("logs after ForceFlush = %d, want 1", len(logExp.Records()))
	}

	var rm metricdata.ResourceMetrics
	if err := metricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("expected metrics collected")
	}

	// Emit one more span + log and verify Shutdown flushes them too.
	spanCtx2, span2 := p.Tracer("buffered").Start(ctx, "buffered.op.shutdown")
	logger.InfoContext(spanCtx2, "shutdown log message")
	span2.End()

	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if spanExp.count() != 2 {
		t.Fatalf("spans after Shutdown = %d, want 2", spanExp.count())
	}
	if !spanExp.wasShutdown() {
		t.Error("expected span exporter Shutdown to be called")
	}
	if len(logExp.Records()) != 2 {
		t.Fatalf("logs after Shutdown = %d, want 2", len(logExp.Records()))
	}
	if !logExp.WasShutdown() {
		t.Error("expected log exporter Shutdown to be called")
	}
}
