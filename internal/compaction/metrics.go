package compaction

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/telemetry"
)

const instrumentationScope = "stravin.dev/core/internal/compaction"

// Metrics holds cardinality-guarded OTel instruments and tracer for stravin-pack.
type Metrics struct {
	tracer trace.Tracer

	compactionCycles   metric.Int64Counter
	bytesReclaimed     metric.Int64Counter
	leaseContention    metric.Int64Counter
	gcReaped           metric.Int64Counter
	gcCRRBlocked       metric.Int64Counter
	gcCRRBreakerOpen   metric.Int64Gauge
	uncompactedPacks   metric.Int64Gauge
	compactionDuration metric.Float64Histogram
}

// NewMetrics creates compaction telemetry instruments backed by provider.
// If provider is nil, a muted no-op provider is used.
func NewMetrics(provider *telemetry.Provider) (*Metrics, error) {
	if provider == nil {
		provider = telemetry.NewNoopProvider()
	}
	meter := provider.Meter(instrumentationScope)

	cycles, err := meter.Int64Counter(
		"stravin_pack_compaction_cycles_total",
		metric.WithDescription("Total compaction cycles executed by tier and result"),
	)
	if err != nil {
		return nil, err
	}

	reclaimed, err := meter.Int64Counter(
		"stravin_pack_bytes_reclaimed_total",
		metric.WithDescription("Total pack bytes reclaimed by compaction and delayed GC"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	contention, err := meter.Int64Counter(
		"stravin_pack_lease_contention_total",
		metric.WithDescription("Total shard lease acquisition contentions"),
	)
	if err != nil {
		return nil, err
	}

	gcReaped, err := meter.Int64Counter(
		"stravin_pack_gc_reaped_total",
		metric.WithDescription("Total packfiles reaped by delayed GC sweeper"),
	)
	if err != nil {
		return nil, err
	}

	gcCRRBlocked, err := meter.Int64Counter(
		"stravin_pack_gc_crr_blocked_total",
		metric.WithDescription("Total delayed GC deletions blocked by S3 CRR replication gate or circuit breaker"),
	)
	if err != nil {
		return nil, err
	}

	gcCRRBreakerOpen, err := meter.Int64Gauge(
		"stravin_pack_gc_crr_breaker_open",
		metric.WithDescription("Whether the stravin-pack GC CRR-lag circuit breaker is currently open (1=open, 0=closed)"),
	)
	if err != nil {
		return nil, err
	}

	uncompacted, err := meter.Int64Gauge(
		"stravin_pack_uncompacted_packs",
		metric.WithDescription("Active pack count observed in a manifest shard before compaction"),
	)
	if err != nil {
		return nil, err
	}

	duration, err := meter.Float64Histogram(
		"stravin_pack_compaction_duration_seconds",
		metric.WithDescription("End-to-end duration of a shard compaction cycle"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		tracer:             provider.Tracer(instrumentationScope),
		compactionCycles:   cycles,
		bytesReclaimed:     reclaimed,
		leaseContention:    contention,
		gcReaped:           gcReaped,
		gcCRRBlocked:       gcCRRBlocked,
		gcCRRBreakerOpen:   gcCRRBreakerOpen,
		uncompactedPacks:   uncompacted,
		compactionDuration: duration,
	}, nil
}

func (m *Metrics) Tracer() trace.Tracer {
	if m == nil {
		return telemetry.NewNoopProvider().Tracer(instrumentationScope)
	}
	return m.tracer
}

func (m *Metrics) recordCompactionCycle(ctx context.Context, tier Tier, result string, dur time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("tier", string(tier)),
		attribute.String("result", result),
	)
	m.compactionCycles.Add(ctx, 1, attrs)
	m.compactionDuration.Record(ctx, dur.Seconds(), attrs)
}

func (m *Metrics) recordBytesReclaimed(ctx context.Context, kind string, bytes int64) {
	if m == nil || bytes <= 0 {
		return
	}
	m.bytesReclaimed.Add(ctx, bytes, metric.WithAttributes(attribute.String("kind", kind)))
}

func (m *Metrics) recordLeaseContention(ctx context.Context, shard int) {
	if m == nil {
		return
	}
	m.leaseContention.Add(ctx, 1, metric.WithAttributes(
		telemetry.AttrShardBucket.String(strconv.Itoa(shard)),
	))
}

func (m *Metrics) recordGCReaped(ctx context.Context, kind string, count int) {
	if m == nil || count <= 0 {
		return
	}
	m.gcReaped.Add(ctx, int64(count), metric.WithAttributes(attribute.String("kind", kind)))
}

func (m *Metrics) recordUncompactedPacks(ctx context.Context, shard int, count int) {
	if m == nil {
		return
	}
	m.uncompactedPacks.Record(ctx, int64(count), metric.WithAttributes(
		telemetry.AttrShardBucket.String(strconv.Itoa(shard)),
	))
}

func (m *Metrics) recordGCCRRBlocked(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.gcCRRBlocked.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *Metrics) recordGCCRRBreakerOpen(ctx context.Context, open bool) {
	if m == nil {
		return
	}
	var val int64
	if open {
		val = 1
	}
	m.gcCRRBreakerOpen.Record(ctx, val)
}
