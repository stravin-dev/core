package loadgen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	loadgenServiceName    = "stravin-loadgen"
	loadgenMetricInterval = 5 * time.Second
)

// Telemetry bundles OpenTelemetry traces, metrics, and trace-correlated OTLP logs
// for stravin-loadgen, with unified Shutdown flushing on Ctrl+C.
type Telemetry struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	LoggerProvider *sdklog.LoggerProvider

	Tracer trace.Tracer
	Logger *slog.Logger

	opsTotal         metric.Int64Counter
	opDuration       metric.Float64Histogram
	retriesTotal     metric.Int64Counter
	unexpectedErrors metric.Int64Counter
	fsckFailures     metric.Int64Counter
	activeThreads    metric.Int64UpDownCounter

	shutdownFns []func(context.Context) error
}

// InMemoryExporters exposes deterministic test exporters for traces, metrics, and logs.
type InMemoryExporters struct {
	SpanExporter *InMemorySpanExporter
	MetricReader *sdkmetric.ManualReader
	LogExporter  *InMemoryLogExporter
}

// InMemorySpanExporter is a thread-safe sdktrace.SpanExporter that retains spans across Shutdown.
type InMemorySpanExporter struct {
	mu       sync.Mutex
	spans    []sdktrace.ReadOnlySpan
	shutdown bool
}

// ExportSpans captures spans in memory.
func (e *InMemorySpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

// Shutdown marks the exporter as shut down while preserving exported spans for inspection.
func (e *InMemorySpanExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

// Spans returns a snapshot of exported spans.
func (e *InMemorySpanExporter) Spans() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

// WasShutdown reports whether Shutdown was invoked.
func (e *InMemorySpanExporter) WasShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

// InMemoryLogExporter is a thread-safe sdklog.Exporter for unit tests.
type InMemoryLogExporter struct {
	mu       sync.Mutex
	records  []sdklog.Record
	shutdown bool
}

// Export captures log records in memory.
func (e *InMemoryLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range records {
		e.records = append(e.records, records[i].Clone())
	}
	return nil
}

// Shutdown marks the exporter as shut down.
func (e *InMemoryLogExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

// ForceFlush is a no-op for the in-memory exporter.
func (e *InMemoryLogExporter) ForceFlush(context.Context) error { return nil }

// Records returns a snapshot of exported log records.
func (e *InMemoryLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdklog.Record, len(e.records))
	copy(out, e.records)
	return out
}

// WasShutdown reports whether Shutdown was invoked.
func (e *InMemoryLogExporter) WasShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

// NewTelemetry initializes OTel traces, metrics, and OTLP logs.
// When endpoint is empty, it uses zero-network noop providers and writes JSON logs to logWriter.
func NewTelemetry(ctx context.Context, endpoint string, logWriter io.Writer) (*Telemetry, error) {
	if logWriter == nil {
		logWriter = io.Discard
	}
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return newNoopTelemetry(logWriter)
	}

	res := newLoadgenResource()

	traceExp, err := otlptracegrpc.New(ctx, buildTraceOpts(endpoint)...)
	if err != nil {
		return nil, fmt.Errorf("loadgen telemetry: init OTLP trace exporter: %w", err)
	}

	metricExp, err := otlpmetricgrpc.New(ctx, buildMetricOpts(endpoint)...)
	if err != nil {
		_ = traceExp.Shutdown(ctx)
		return nil, fmt.Errorf("loadgen telemetry: init OTLP metric exporter: %w", err)
	}

	logExp, err := otlploggrpc.New(ctx, buildLogOpts(endpoint)...)
	if err != nil {
		_ = metricExp.Shutdown(ctx)
		_ = traceExp.Shutdown(ctx)
		return nil, fmt.Errorf("loadgen telemetry: init OTLP log exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(loadgenMetricInterval))),
		sdkmetric.WithResource(res),
	)
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)

	return buildTelemetry(tp, mp, lp, logWriter, []func(context.Context) error{
		tp.Shutdown,
		mp.Shutdown,
		lp.Shutdown,
	})
}

// NewInMemoryTelemetry initializes a deterministic in-memory Telemetry instance for tests.
func NewInMemoryTelemetry(logWriter io.Writer) (*Telemetry, *InMemoryExporters, error) {
	if logWriter == nil {
		logWriter = io.Discard
	}
	res := newLoadgenResource()

	spanExp := &InMemorySpanExporter{}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExp),
		sdktrace.WithResource(res),
	)

	metricReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(metricReader),
		sdkmetric.WithResource(res),
	)

	logExp := &InMemoryLogExporter{}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(logExp)),
		sdklog.WithResource(res),
	)

	tel, err := buildTelemetry(tp, mp, lp, logWriter, []func(context.Context) error{
		tp.Shutdown,
		mp.Shutdown,
		lp.Shutdown,
	})
	if err != nil {
		return nil, nil, err
	}
	return tel, &InMemoryExporters{
		SpanExporter: spanExp,
		MetricReader: metricReader,
		LogExporter:  logExp,
	}, nil
}

func newNoopTelemetry(logWriter io.Writer) (*Telemetry, error) {
	return buildTelemetry(
		tracenoop.NewTracerProvider(),
		metricnoop.NewMeterProvider(),
		nil,
		logWriter,
		nil,
	)
}

func buildTelemetry(
	tp trace.TracerProvider,
	mp metric.MeterProvider,
	lp *sdklog.LoggerProvider,
	logWriter io.Writer,
	shutdownFns []func(context.Context) error,
) (*Telemetry, error) {
	meter := mp.Meter(loadgenServiceName)

	opsTotal, err := meter.Int64Counter(
		"stravin_loadgen_ops_total",
		metric.WithDescription("Total loadgen operations executed"),
	)
	if err != nil {
		return nil, err
	}
	opDuration, err := meter.Float64Histogram(
		"stravin_loadgen_op_duration_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of loadgen operations in seconds"),
	)
	if err != nil {
		return nil, err
	}
	retriesTotal, err := meter.Int64Counter(
		"stravin_loadgen_retries_total",
		metric.WithDescription("Total loadgen retries by reason"),
	)
	if err != nil {
		return nil, err
	}
	unexpectedErrors, err := meter.Int64Counter(
		"stravin_loadgen_unexpected_errors_total",
		metric.WithDescription("Total unexpected errors observed by loadgen oracle"),
	)
	if err != nil {
		return nil, err
	}
	fsckFailures, err := meter.Int64Counter(
		"stravin_loadgen_fsck_failures_total",
		metric.WithDescription("Total git fsck --full --strict failures"),
	)
	if err != nil {
		return nil, err
	}
	activeThreads, err := meter.Int64UpDownCounter(
		"stravin_loadgen_active_threads",
		metric.WithDescription("Active loadgen worker goroutines per persona"),
	)
	if err != nil {
		return nil, err
	}

	jsonHandler := &traceCorrelatedHandler{
		inner: slog.NewJSONHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelInfo}),
	}
	var rootHandler slog.Handler = jsonHandler
	if lp != nil {
		otelHandler := otelslog.NewHandler(loadgenServiceName, otelslog.WithLoggerProvider(lp))
		rootHandler = &fanoutHandler{handlers: []slog.Handler{jsonHandler, otelHandler}}
	}

	return &Telemetry{
		TracerProvider:   tp,
		MeterProvider:    mp,
		LoggerProvider:   lp,
		Tracer:           tp.Tracer(loadgenServiceName),
		Logger:           slog.New(rootHandler),
		opsTotal:         opsTotal,
		opDuration:       opDuration,
		retriesTotal:     retriesTotal,
		unexpectedErrors: unexpectedErrors,
		fsckFailures:     fsckFailures,
		activeThreads:    activeThreads,
		shutdownFns:      shutdownFns,
	}, nil
}

// RecordOp records operation count and latency histogram with low-cardinality labels.
func (t *Telemetry) RecordOp(ctx context.Context, persona Persona, scenario, op, outcome string, dur time.Duration) {
	t.opsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("persona", string(persona)),
		attribute.String("scenario", scenario),
		attribute.String("op", op),
		attribute.String("outcome", outcome),
	))
	t.opDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(
		attribute.String("persona", string(persona)),
		attribute.String("scenario", scenario),
		attribute.String("op", op),
	))
}

// RecordRetry increments stravin_loadgen_retries_total.
func (t *Telemetry) RecordRetry(ctx context.Context, persona Persona, scenario, reason string) {
	t.retriesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("persona", string(persona)),
		attribute.String("scenario", scenario),
		attribute.String("reason", reason),
	))
}

// RecordUnexpectedError increments stravin_loadgen_unexpected_errors_total.
func (t *Telemetry) RecordUnexpectedError(ctx context.Context, persona Persona, scenario, op string) {
	t.unexpectedErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("persona", string(persona)),
		attribute.String("scenario", scenario),
		attribute.String("op", op),
	))
}

// RecordFsckFailure increments stravin_loadgen_fsck_failures_total.
func (t *Telemetry) RecordFsckFailure(ctx context.Context, scenario string) {
	t.fsckFailures.Add(ctx, 1, metric.WithAttributes(
		attribute.String("scenario", scenario),
	))
}

// AddActiveThreads adjusts stravin_loadgen_active_threads for persona.
func (t *Telemetry) AddActiveThreads(ctx context.Context, persona Persona, delta int64) {
	t.activeThreads.Add(ctx, delta, metric.WithAttributes(
		attribute.String("persona", string(persona)),
	))
}

// Shutdown flushes and shuts down TracerProvider, MeterProvider, and LoggerProvider.
func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range t.shutdownFns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func newLoadgenResource() *resource.Resource {
	return resource.NewSchemaless(
		attribute.String("service.name", loadgenServiceName),
	)
}

func buildTraceOpts(endpoint string) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
	}
	if strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://") {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

func buildMetricOpts(endpoint string) []otlpmetricgrpc.Option {
	var opts []otlpmetricgrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlpmetricgrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlpmetricgrpc.WithEndpoint(endpoint))
	}
	if strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://") {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	return opts
}

func buildLogOpts(endpoint string) []otlploggrpc.Option {
	var opts []otlploggrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlploggrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlploggrpc.WithEndpoint(endpoint))
	}
	if strings.HasPrefix(endpoint, "http://") || !strings.Contains(endpoint, "://") {
		opts = append(opts, otlploggrpc.WithInsecure())
	}
	return opts
}

// traceCorrelatedHandler injects trace_id and span_id into local JSON slog output.
type traceCorrelatedHandler struct {
	inner slog.Handler
}

func (h *traceCorrelatedHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceCorrelatedHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		cloned := r.Clone()
		cloned.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
		return h.inner.Handle(ctx, cloned)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceCorrelatedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceCorrelatedHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceCorrelatedHandler) WithGroup(name string) slog.Handler {
	return &traceCorrelatedHandler{inner: h.inner.WithGroup(name)}
}

// fanoutHandler dispatches slog records to multiple handlers (JSON + OTLP bridge).
type fanoutHandler struct {
	handlers []slog.Handler
}

func (f *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (f *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: next}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithGroup(name)
	}
	return &fanoutHandler{handlers: next}
}
