package relay

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"stravin.dev/core/internal/telemetry"
)

// Metrics records bounded-cardinality OpenTelemetry metrics for stravin-relay
// exported over OTLP/gRPC via internal/telemetry.
type Metrics struct {
	publishesTotal  metric.Int64Counter
	publishDuration metric.Float64Histogram
	streamRecords   metric.Int64Counter
}

// NewMetrics initializes bounded-cardinality OTel instruments for stravin-relay
// using GuardedMeter. If provider is nil, a no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter("stravin.dev/core/internal/relay")

	publishesTotal, err := meter.Int64Counter(
		"stravin_relay_publishes_total",
		metric.WithDescription("Total NATS JetStream outbox publishes by event_type and result (ok|duplicate|retry|error)."),
	)
	if err != nil {
		return nil, fmt.Errorf("relay: init stravin_relay_publishes_total: %w", err)
	}

	publishDuration, err := meter.Float64Histogram(
		"stravin_relay_publish_duration_seconds",
		metric.WithDescription("Duration of NATS JetStream outbox publishes in seconds."),
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		return nil, fmt.Errorf("relay: init stravin_relay_publish_duration_seconds: %w", err)
	}

	streamRecords, err := meter.Int64Counter(
		"stravin_relay_stream_records_total",
		metric.WithDescription("Total DynamoDB Streams outbox records processed by result (ok|error)."),
	)
	if err != nil {
		return nil, fmt.Errorf("relay: init stravin_relay_stream_records_total: %w", err)
	}

	return &Metrics{
		publishesTotal:  publishesTotal,
		publishDuration: publishDuration,
		streamRecords:   streamRecords,
	}, nil
}

// RecordPublish records a JetStream publish outcome (ok|duplicate|retry|error).
func (m *Metrics) RecordPublish(ctx context.Context, eventType, result string) {
	if m == nil || m.publishesTotal == nil {
		return
	}
	m.publishesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event_type", eventType),
		attribute.String("result", result),
	))
}

// RecordPublishDuration records end-to-end JetStream publish duration in seconds.
func (m *Metrics) RecordPublishDuration(ctx context.Context, eventType, result string, d time.Duration) {
	if m == nil || m.publishDuration == nil {
		return
	}
	m.publishDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("event_type", eventType),
		attribute.String("result", result),
	))
}

// RecordStreamRecord records a DynamoDB Streams outbox record processing outcome (ok|error).
func (m *Metrics) RecordStreamRecord(ctx context.Context, result string) {
	if m == nil || m.streamRecords == nil {
		return
	}
	m.streamRecords.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}
