package edge

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/telemetry"
)

// Metrics instruments stravin-edge with bounded-cardinality OTel counters/histograms
// exported over OTLP/gRPC via internal/telemetry.
type Metrics struct {
	tracer   trace.Tracer
	reqTotal metric.Int64Counter
	reqDur   metric.Float64Histogram
}

// NewMetrics creates edge tracing and bounded-cardinality metrics backed by provider.
// If provider is nil, a muted no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter("stravin.dev/core/internal/edge")
	reqTotal, err := meter.Int64Counter(
		"stravin_edge_requests_total",
		metric.WithDescription("Total Git ingress requests handled by stravin-edge."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_requests_total: %w", err)
	}
	reqDur, err := meter.Float64Histogram(
		"stravin_edge_request_duration_seconds",
		metric.WithDescription("Duration of Git ingress requests in seconds."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_request_duration_seconds: %w", err)
	}
	return &Metrics{
		tracer:   provider.Tracer("stravin.dev/core/internal/edge"),
		reqTotal: reqTotal,
		reqDur:   reqDur,
	}, nil
}

// StartIngressSpan opens the root edge.ingress span with standardized Stravin attributes.
func (m *Metrics) StartIngressSpan(ctx context.Context, protocol, operation, tenantID, repoID, homeRegion string) (context.Context, trace.Span) {
	attrs := telemetry.NewSpanAttrs().
		GitProtocol(protocol).
		GitOperation(operation).
		TenantID(tenantID).
		RepoID(repoID).
		HomeRegion(homeRegion).
		Build()
	return m.tracer.Start(ctx, "edge.ingress", trace.WithAttributes(attrs...))
}

// RecordRequest records bounded-cardinality request count and duration metrics.
func (m *Metrics) RecordRequest(ctx context.Context, protocol, operation, statusCode, errorClass string, d time.Duration) {
	countAttrs := []attribute.KeyValue{
		telemetry.AttrGitProtocol.String(protocol),
		telemetry.AttrGitOperation.String(operation),
		telemetry.AttrStatusCode.String(statusCode),
		telemetry.AttrErrorClass.String(errorClass),
	}
	durAttrs := []attribute.KeyValue{
		telemetry.AttrGitProtocol.String(protocol),
		telemetry.AttrGitOperation.String(operation),
		telemetry.AttrStatusCode.String(statusCode),
	}
	m.reqTotal.Add(ctx, 1, metric.WithAttributes(countAttrs...))
	m.reqDur.Record(ctx, d.Seconds(), metric.WithAttributes(durAttrs...))
}
