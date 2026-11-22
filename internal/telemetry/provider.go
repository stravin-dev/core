package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	defaultMetricInterval = 15 * time.Second

	// DefaultShutdownTimeout is the default bounded timeout for flushing and
	// shutting down OpenTelemetry providers across Stravin binaries.
	DefaultShutdownTimeout = 5 * time.Second

	shutdownTimeoutEnvVar = "STRAVIN_OTEL_SHUTDOWN_TIMEOUT"
)

// ShutdownTimeoutFromEnv returns the telemetry shutdown timeout parsed from
// STRAVIN_OTEL_SHUTDOWN_TIMEOUT, falling back to DefaultShutdownTimeout when
// unset, invalid, or non-positive.
func ShutdownTimeoutFromEnv() time.Duration {
	raw := strings.TrimSpace(os.Getenv(shutdownTimeoutEnvVar))
	if raw == "" {
		return DefaultShutdownTimeout
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return DefaultShutdownTimeout
	}
	return d
}

// InMemoryLogExporter is a thread-safe sdklog.Exporter for deterministic unit tests.
type InMemoryLogExporter struct {
	mu       sync.Mutex
	records  []sdklog.Record
	flushed  bool
	shutdown bool
}

// NewInMemoryLogExporter returns an empty InMemoryLogExporter.
func NewInMemoryLogExporter() *InMemoryLogExporter {
	return &InMemoryLogExporter{}
}

// Export captures cloned log records in memory.
func (e *InMemoryLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range records {
		e.records = append(e.records, records[i].Clone())
	}
	return nil
}

// ForceFlush marks the exporter as flushed.
func (e *InMemoryLogExporter) ForceFlush(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.flushed = true
	return nil
}

// Shutdown marks the exporter as shut down while preserving captured records.
func (e *InMemoryLogExporter) Shutdown(context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.shutdown = true
	return nil
}

// Records returns a snapshot of exported log records.
func (e *InMemoryLogExporter) Records() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdklog.Record, len(e.records))
	copy(out, e.records)
	return out
}

// WasFlushed reports whether ForceFlush was invoked.
func (e *InMemoryLogExporter) WasFlushed() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.flushed
}

// WasShutdown reports whether Shutdown was invoked.
func (e *InMemoryLogExporter) WasShutdown() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.shutdown
}

// Mode selects the telemetry exporter backend.
type Mode int

const (
	// ModeNoop disables live telemetry export (zero network I/O).
	ModeNoop Mode = iota
	// ModeInMemory captures spans, metrics, and logs in memory for deterministic unit tests.
	ModeInMemory
	// ModeOTLP exports traces, metrics, and logs over OTLP/gRPC to an OpenTelemetry Collector.
	ModeOTLP
)

// Config configures Stravin's OpenTelemetry Provider.
type Config struct {
	ServiceName    string
	ServiceVersion string
	Environment    string
	Region         string
	Endpoint       string
	Insecure       bool
	Mode           Mode
	SampleRatio    float64
	MetricInterval time.Duration
	SetGlobal      bool
}

// Provider bundles configured OTel trace, metric, log, and propagation primitives.
type Provider struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	LoggerProvider *sdklog.LoggerProvider
	Propagator     propagation.TextMapPropagator

	// SpanExporter, MetricReader, and LogExporter are populated only in ModeInMemory.
	SpanExporter *tracetest.InMemoryExporter
	MetricReader *sdkmetric.ManualReader
	LogExporter  *InMemoryLogExporter

	serviceName   string
	forceFlushFns []func(context.Context) error
	shutdownFns   []func(context.Context) error
}

// NewNoopProvider returns a muted no-op telemetry Provider with zero network export.
func NewNoopProvider() *Provider {
	return &Provider{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		Propagator:     NewPropagator(),
	}
}

// NewInMemoryProvider returns a deterministic in-memory telemetry Provider for unit tests.
func NewInMemoryProvider(serviceName string) (*Provider, error) {
	if serviceName == "" {
		serviceName = "stravin-test"
	}
	return Init(context.Background(), Config{
		ServiceName: serviceName,
		Environment: "test",
		Mode:        ModeInMemory,
	})
}

// Init initializes a Stravin telemetry Provider according to cfg.
// In ModeOTLP, if neither cfg.Endpoint nor OTEL_EXPORTER_OTLP_ENDPOINT is set,
// Init falls back to a muted no-op provider so local runs and tests without a
// collector never overflow buffers or fail startup.
func Init(ctx context.Context, cfg Config) (*Provider, error) {
	switch cfg.Mode {
	case ModeNoop:
		p := NewNoopProvider()
		p.serviceName = cfg.ServiceName
		if cfg.SetGlobal {
			p.installGlobals()
		}
		return p, nil

	case ModeInMemory:
		return initInMemory(cfg)

	case ModeOTLP:
		endpoint := cfg.Endpoint
		if endpoint == "" {
			endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
		}
		if endpoint == "" {
			p := NewNoopProvider()
			p.serviceName = cfg.ServiceName
			if cfg.SetGlobal {
				p.installGlobals()
			}
			return p, nil
		}
		cfg.Endpoint = endpoint
		return initOTLP(ctx, cfg)

	default:
		return nil, fmt.Errorf("telemetry: unknown mode %d", cfg.Mode)
	}
}

func initInMemory(cfg Config) (*Provider, error) {
	res, err := NewResource(ResourceConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		Environment:    cfg.Environment,
		Region:         cfg.Region,
	})
	if err != nil {
		return nil, err
	}

	spanExp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExp),
		sdktrace.WithResource(res),
	)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	logExp := NewInMemoryLogExporter()
	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(logExp)),
		sdklog.WithResource(res),
	)

	p := &Provider{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
		Propagator:     NewPropagator(),
		SpanExporter:   spanExp,
		MetricReader:   reader,
		LogExporter:    logExp,
		serviceName:    cfg.ServiceName,
		forceFlushFns:  []func(context.Context) error{tp.ForceFlush, mp.ForceFlush, lp.ForceFlush},
		shutdownFns:    []func(context.Context) error{tp.Shutdown, mp.Shutdown, lp.Shutdown},
	}
	if cfg.SetGlobal {
		p.installGlobals()
	}
	return p, nil
}

func initOTLP(ctx context.Context, cfg Config) (*Provider, error) {
	res, err := NewResource(ResourceConfig{
		ServiceName:    cfg.ServiceName,
		ServiceVersion: cfg.ServiceVersion,
		Environment:    cfg.Environment,
		Region:         cfg.Region,
	})
	if err != nil {
		return nil, err
	}

	traceOpts := buildOTLPTraceOpts(cfg.Endpoint, cfg.Insecure)
	traceExp, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: init OTLP trace exporter: %w", err)
	}

	metricOpts := buildOTLPMetricOpts(cfg.Endpoint, cfg.Insecure)
	metricExp, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		_ = traceExp.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry: init OTLP metric exporter: %w", err)
	}

	logOpts := buildOTLPLogOpts(cfg.Endpoint, cfg.Insecure)
	logExp, err := otlploggrpc.New(ctx, logOpts...)
	if err != nil {
		_ = metricExp.Shutdown(ctx)
		_ = traceExp.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry: init OTLP log exporter: %w", err)
	}

	sampler := sdktrace.AlwaysSample()
	if cfg.SampleRatio > 0 && cfg.SampleRatio < 1 {
		sampler = sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	interval := cfg.MetricInterval
	if interval <= 0 {
		interval = defaultMetricInterval
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp, sdkmetric.WithInterval(interval))),
		sdkmetric.WithResource(res),
	)

	lp := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp)),
		sdklog.WithResource(res),
	)

	p := &Provider{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
		Propagator:     NewPropagator(),
		serviceName:    cfg.ServiceName,
		forceFlushFns:  []func(context.Context) error{tp.ForceFlush, mp.ForceFlush, lp.ForceFlush},
		shutdownFns:    []func(context.Context) error{tp.Shutdown, mp.Shutdown, lp.Shutdown},
	}
	if cfg.SetGlobal {
		p.installGlobals()
	}
	return p, nil
}

func buildOTLPTraceOpts(endpoint string, insecure bool) []otlptracegrpc.Option {
	var opts []otlptracegrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
	}
	if insecure || strings.HasPrefix(endpoint, "http://") {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	return opts
}

func buildOTLPMetricOpts(endpoint string, insecure bool) []otlpmetricgrpc.Option {
	var opts []otlpmetricgrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlpmetricgrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlpmetricgrpc.WithEndpoint(endpoint))
	}
	if insecure || strings.HasPrefix(endpoint, "http://") {
		opts = append(opts, otlpmetricgrpc.WithInsecure())
	}
	return opts
}

func buildOTLPLogOpts(endpoint string, insecure bool) []otlploggrpc.Option {
	var opts []otlploggrpc.Option
	if strings.Contains(endpoint, "://") {
		opts = append(opts, otlploggrpc.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlploggrpc.WithEndpoint(endpoint))
	}
	if insecure || strings.HasPrefix(endpoint, "http://") {
		opts = append(opts, otlploggrpc.WithInsecure())
	}
	return opts
}

func (p *Provider) installGlobals() {
	otel.SetTracerProvider(p.TracerProvider)
	otel.SetMeterProvider(p.MeterProvider)
	otel.SetTextMapPropagator(p.Propagator)
}

// Tracer returns a named trace.Tracer backed by this Provider.
func (p *Provider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return p.TracerProvider.Tracer(name, opts...)
}

// Meter returns a cardinality-guarded Meter backed by this Provider.
func (p *Provider) Meter(name string, opts ...GuardOption) *GuardedMeter {
	return NewGuardedMeter(p.MeterProvider.Meter(name), opts...)
}

// Logger returns a structured JSON slog.Logger writing to w with OTel trace
// correlation. When this Provider has an active OTLP LoggerProvider, logs are
// fanned out to both w and the OTLP log bridge.
func (p *Provider) Logger(w io.Writer, level slog.Leveler) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	jsonHandler := NewTraceHandler(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level}))
	if p == nil || p.LoggerProvider == nil {
		return slog.New(jsonHandler)
	}
	scope := p.serviceName
	if scope == "" {
		scope = "stravin"
	}
	otelHandler := newLevelFilterHandler(
		level,
		otelslog.NewHandler(scope, otelslog.WithLoggerProvider(p.LoggerProvider)),
	)
	return slog.New(NewFanoutHandler(jsonHandler, otelHandler))
}

// ForceFlush flushes buffered traces, metrics, and logs concurrently within ctx.
func (p *Provider) ForceFlush(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return runParallel(ctx, p.forceFlushFns)
}

// Shutdown flushes and shuts down trace, metric, and log providers concurrently
// within ctx so one slow exporter cannot starve the others.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	return runParallel(ctx, p.shutdownFns)
}

func runParallel(ctx context.Context, fns []func(context.Context) error) error {
	if len(fns) == 0 {
		return nil
	}
	errs := make([]error, len(fns))
	var wg sync.WaitGroup
	for i, fn := range fns {
		if fn == nil {
			continue
		}
		wg.Add(1)
		go func(idx int, call func(context.Context) error) {
			defer wg.Done()
			errs[idx] = call(ctx)
		}(i, fn)
	}
	wg.Wait()
	return errors.Join(errs...)
}
