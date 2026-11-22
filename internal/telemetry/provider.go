package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const defaultMetricInterval = 15 * time.Second

// Mode selects the telemetry exporter backend.
type Mode int

const (
	// ModeNoop disables live telemetry export (zero network I/O).
	ModeNoop Mode = iota
	// ModeInMemory captures spans and metrics in memory for deterministic unit tests.
	ModeInMemory
	// ModeOTLP exports traces and metrics over OTLP/gRPC to an OpenTelemetry Collector.
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

// Provider bundles configured OTel trace, metric, and propagation primitives.
type Provider struct {
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Propagator     propagation.TextMapPropagator

	// SpanExporter and MetricReader are populated only in ModeInMemory.
	SpanExporter *tracetest.InMemoryExporter
	MetricReader *sdkmetric.ManualReader

	shutdownFns []func(context.Context) error
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

	p := &Provider{
		TracerProvider: tp,
		MeterProvider:  mp,
		Propagator:     NewPropagator(),
		SpanExporter:   spanExp,
		MetricReader:   reader,
		shutdownFns:    []func(context.Context) error{tp.Shutdown, mp.Shutdown},
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

	p := &Provider{
		TracerProvider: tp,
		MeterProvider:  mp,
		Propagator:     NewPropagator(),
		shutdownFns:    []func(context.Context) error{tp.Shutdown, mp.Shutdown},
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

// Shutdown flushes and shuts down the trace and metric providers cleanly.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range p.shutdownFns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
