package async

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"stravin.dev/core/internal/telemetry"
)

// Metrics records bounded-cardinality OpenTelemetry metrics for stravin-hook
// exported over OTLP/gRPC via internal/telemetry.
type Metrics struct {
	deliveriesTotal  metric.Int64Counter
	deliveryDuration metric.Float64Histogram
	streamRecords    metric.Int64Counter
}

// NewMetrics initializes bounded-cardinality OTel instruments for stravin-hook
// using GuardedMeter. If provider is nil, a no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter("stravin.dev/core/internal/hooks/async")

	deliveriesTotal, err := meter.Int64Counter(
		"stravin_hook_deliveries_total",
		metric.WithDescription("Total async webhook deliveries by result (ok|retry|dlq|error)."),
	)
	if err != nil {
		return nil, fmt.Errorf("async: init stravin_hook_deliveries_total: %w", err)
	}

	deliveryDuration, err := meter.Float64Histogram(
		"stravin_hook_delivery_duration_seconds",
		metric.WithDescription("Duration of async webhook deliveries in seconds."),
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		return nil, fmt.Errorf("async: init stravin_hook_delivery_duration_seconds: %w", err)
	}

	streamRecords, err := meter.Int64Counter(
		"stravin_hook_stream_records_total",
		metric.WithDescription("Total DynamoDB Streams Ref History records processed by result (ok|error)."),
	)
	if err != nil {
		return nil, fmt.Errorf("async: init stravin_hook_stream_records_total: %w", err)
	}

	return &Metrics{
		deliveriesTotal:  deliveriesTotal,
		deliveryDuration: deliveryDuration,
		streamRecords:    streamRecords,
	}, nil
}

// RecordDelivery records a webhook delivery outcome (ok|retry|dlq|error).
func (m *Metrics) RecordDelivery(ctx context.Context, result string) {
	if m == nil || m.deliveriesTotal == nil {
		return
	}
	m.deliveriesTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}

// RecordDeliveryDuration records end-to-end webhook delivery duration in seconds.
func (m *Metrics) RecordDeliveryDuration(ctx context.Context, result string, d time.Duration) {
	if m == nil || m.deliveryDuration == nil {
		return
	}
	m.deliveryDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("result", result),
	))
}

// RecordStreamRecord records a DynamoDB Streams Ref History record processing outcome (ok|error).
func (m *Metrics) RecordStreamRecord(ctx context.Context, result string) {
	if m == nil || m.streamRecords == nil {
		return
	}
	m.streamRecords.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}
