// Package ctrl implements the stravin-ctrl single-region control plane:
// regional write-lease heartbeats, monotonic failover_epoch fencing,
// deterministic S3 HeadObject integrity scrubbing with Ref History rewind,
// immutable audit trails, and the HTTP admin API.
package ctrl

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/telemetry"
)

const instrumentationScope = "stravin.dev/core/internal/ctrl"

// Metrics holds cardinality-guarded OTel instruments and tracer for stravin-ctrl.
type Metrics struct {
	tracer trace.Tracer

	leaseRenewals    metric.Int64Counter
	epochTransitions metric.Int64Counter
	scrubRuns        metric.Int64Counter
	scrubHeadChecks  metric.Int64Counter
	scrubRewoundRefs metric.Int64Counter
	scrubDuration    metric.Float64Histogram
}

// NewMetrics constructs stravin-ctrl telemetry instruments backed by provider.
// If provider is nil, a muted no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter(instrumentationScope)

	leaseRenewals, err := meter.Int64Counter(
		"stravin_ctrl_lease_renewals_total",
		metric.WithDescription("Total regional write-lease renewal attempts by region and result"),
	)
	if err != nil {
		return nil, err
	}

	epochTransitions, err := meter.Int64Counter(
		"stravin_ctrl_epoch_transitions_total",
		metric.WithDescription("Total monotonic failover_epoch or write_state fencing transitions"),
	)
	if err != nil {
		return nil, err
	}

	scrubRuns, err := meter.Int64Counter(
		"stravin_ctrl_scrub_runs_total",
		metric.WithDescription("Total S3 integrity scrub executions by region and result"),
	)
	if err != nil {
		return nil, err
	}

	scrubHeadChecks, err := meter.Int64Counter(
		"stravin_ctrl_scrub_head_checks_total",
		metric.WithDescription("Total S3 HeadObject checks performed during integrity scrubs"),
	)
	if err != nil {
		return nil, err
	}

	scrubRewoundRefs, err := meter.Int64Counter(
		"stravin_ctrl_scrub_rewound_refs_total",
		metric.WithDescription("Total refs rewound or deleted during integrity scrubs"),
	)
	if err != nil {
		return nil, err
	}

	scrubDuration, err := meter.Float64Histogram(
		"stravin_ctrl_scrub_duration_seconds",
		metric.WithDescription("End-to-end duration of an S3 integrity scrub and ref rewind run"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		tracer:           provider.Tracer(instrumentationScope),
		leaseRenewals:    leaseRenewals,
		epochTransitions: epochTransitions,
		scrubRuns:        scrubRuns,
		scrubHeadChecks:  scrubHeadChecks,
		scrubRewoundRefs: scrubRewoundRefs,
		scrubDuration:    scrubDuration,
	}, nil
}

// Tracer returns the package OpenTelemetry tracer.
func (m *Metrics) Tracer() trace.Tracer {
	if m == nil {
		return telemetry.NewNoopProvider().Tracer(instrumentationScope)
	}
	return m.tracer
}

func (m *Metrics) recordLeaseRenewal(ctx context.Context, region lease.Region, result string) {
	if m == nil {
		return
	}
	m.leaseRenewals.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrRegion.String(string(region)),
		attribute.String("result", result),
	))
}

func (m *Metrics) recordEpochTransition(ctx context.Context, region lease.Region, mode lease.Mode, result string) {
	if m == nil {
		return
	}
	m.epochTransitions.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrRegion.String(string(region)),
		attribute.String("mode", string(mode)),
		attribute.String("result", result),
	))
}

func (m *Metrics) recordScrubRun(ctx context.Context, region lease.Region, result string, dur time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		telemetry.AttrRegion.String(string(region)),
		attribute.String("result", result),
	)
	m.scrubRuns.Add(ctx, 1, attrs)
	m.scrubDuration.Record(ctx, dur.Seconds(), attrs)
}

func (m *Metrics) recordHeadCheck(ctx context.Context, region lease.Region, result string) {
	if m == nil {
		return
	}
	m.scrubHeadChecks.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrRegion.String(string(region)),
		attribute.String("result", result),
	))
}

func (m *Metrics) recordRewoundRef(ctx context.Context, region lease.Region, action string) {
	if m == nil {
		return
	}
	m.scrubRewoundRefs.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrRegion.String(string(region)),
		attribute.String("action", action),
	))
}
