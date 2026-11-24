package edge

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/telemetry"
)

// Metrics instruments stravin-edge with bounded-cardinality OTel counters/histograms
// exported over OTLP/gRPC via internal/telemetry.
type Metrics struct {
	tracer           trace.Tracer
	reqTotal         metric.Int64Counter
	reqDur           metric.Float64Histogram
	packCacheLookups metric.Int64Counter
	packCacheWrites  metric.Int64Counter
	packCacheOpDur   metric.Float64Histogram
	packCacheBytes   metric.Int64Counter
	valkeyPoolDials  metric.Int64Counter
	metaCacheLookups metric.Int64Counter
	activeClones     metric.Int64UpDownCounter
	infoRefsRetries  metric.Int64Counter
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
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_request_duration_seconds: %w", err)
	}
	packCacheLookups, err := meter.Int64Counter(
		"stravin_edge_pack_cache_lookups_total",
		metric.WithDescription("Total pack cache lookups by tier and result."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_pack_cache_lookups_total: %w", err)
	}
	packCacheWrites, err := meter.Int64Counter(
		"stravin_edge_pack_cache_writes_total",
		metric.WithDescription("Total pack cache writes by tier and result."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_pack_cache_writes_total: %w", err)
	}
	packCacheOpDur, err := meter.Float64Histogram(
		"stravin_edge_pack_cache_op_duration_seconds",
		metric.WithDescription("Duration of external pack cache operations in seconds."),
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_pack_cache_op_duration_seconds: %w", err)
	}
	packCacheBytes, err := meter.Int64Counter(
		"stravin_edge_pack_cache_bytes_total",
		metric.WithDescription("Total pack cache bytes served on hit or written by tier."),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_pack_cache_bytes_total: %w", err)
	}
	valkeyPoolDials, err := meter.Int64Counter(
		"stravin_edge_valkey_pool_dials_total",
		metric.WithDescription("Total Valkey pool connection acquisitions by result and reuse."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_valkey_pool_dials_total: %w", err)
	}
	metaCacheLookups, err := meter.Int64Counter(
		"stravin_edge_meta_cache_lookups_total",
		metric.WithDescription("Total repository META cache lookups by result."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_meta_cache_lookups_total: %w", err)
	}
	activeClones, err := meter.Int64UpDownCounter(
		"stravin_edge_active_clones",
		metric.WithDescription("Current number of active concurrent clone slots in stravin-edge."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_active_clones: %w", err)
	}
	infoRefsRetries, err := meter.Int64Counter(
		"stravin_edge_info_refs_retries_total",
		metric.WithDescription("Total Smart HTTP info/refs transient error retries by result."),
	)
	if err != nil {
		return nil, fmt.Errorf("edge: init stravin_edge_info_refs_retries_total: %w", err)
	}
	return &Metrics{
		tracer:           provider.Tracer("stravin.dev/core/internal/edge"),
		reqTotal:         reqTotal,
		reqDur:           reqDur,
		packCacheLookups: packCacheLookups,
		packCacheWrites:  packCacheWrites,
		packCacheOpDur:   packCacheOpDur,
		packCacheBytes:   packCacheBytes,
		valkeyPoolDials:  valkeyPoolDials,
		metaCacheLookups: metaCacheLookups,
		activeClones:     activeClones,
		infoRefsRetries:  infoRefsRetries,
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
	if m == nil {
		return
	}
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

// RecordPackCacheLookup records a pack cache lookup outcome for tier (valkey|s3|singleflight)
// and result (hit|miss|error|bypass).
func (m *Metrics) RecordPackCacheLookup(ctx context.Context, tier, result string) {
	if m == nil || m.packCacheLookups == nil {
		return
	}
	m.packCacheLookups.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tier", tier),
		attribute.String("result", result),
	))
}

// RecordPackCacheWrite records a pack cache write outcome for tier (valkey|s3)
// and result (ok|error|too_large).
func (m *Metrics) RecordPackCacheWrite(ctx context.Context, tier, result string) {
	if m == nil || m.packCacheWrites == nil {
		return
	}
	m.packCacheWrites.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tier", tier),
		attribute.String("result", result),
	))
}

// RecordPackCacheOpDuration records the latency of an external pack cache operation
// for tier (valkey|s3) and op (get|set).
func (m *Metrics) RecordPackCacheOpDuration(ctx context.Context, tier, op string, d time.Duration) {
	if m == nil || m.packCacheOpDur == nil {
		return
	}
	m.packCacheOpDur.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("tier", tier),
		attribute.String("op", op),
	))
}

// RecordPackCacheBytes records pack cache bytes served on hit or written for
// tier (valkey|s3|singleflight) and op (hit_served|written).
func (m *Metrics) RecordPackCacheBytes(ctx context.Context, tier, op string, n int64) {
	if m == nil || m.packCacheBytes == nil || n <= 0 {
		return
	}
	m.packCacheBytes.Add(ctx, n, metric.WithAttributes(
		attribute.String("tier", tier),
		attribute.String("op", op),
	))
}

// RecordValkeyPoolDial records a Valkey connection acquisition outcome (ok|error)
// and whether an existing pooled connection was reused.
func (m *Metrics) RecordValkeyPoolDial(ctx context.Context, result string, reused bool) {
	if m == nil || m.valkeyPoolDials == nil {
		return
	}
	m.valkeyPoolDials.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
		attribute.String("reused", strconv.FormatBool(reused)),
	))
}

// RecordMetaCacheLookup records an in-memory repository META cache hit or miss.
func (m *Metrics) RecordMetaCacheLookup(ctx context.Context, hit bool) {
	if m == nil || m.metaCacheLookups == nil {
		return
	}
	result := "miss"
	if hit {
		result = "hit"
	}
	m.metaCacheLookups.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}

// AddActiveClones adjusts the current active concurrent clone count.
func (m *Metrics) AddActiveClones(ctx context.Context, delta int64) {
	if m == nil || m.activeClones == nil || delta == 0 {
		return
	}
	m.activeClones.Add(ctx, delta)
}

// RecordInfoRefsRetry records the outcome of a Smart HTTP info/refs retry attempt.
func (m *Metrics) RecordInfoRefsRetry(ctx context.Context, ok bool) {
	if m == nil || m.infoRefsRetries == nil {
		return
	}
	result := "error"
	if ok {
		result = "ok"
	}
	m.infoRefsRetries.Add(ctx, 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}
