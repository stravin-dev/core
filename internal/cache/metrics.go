package cache

import (
	"context"

	"go.opentelemetry.io/otel/metric"

	"stravin.dev/core/internal/telemetry"
)

const instrumentationScope = "stravin.dev/core/internal/cache"

// cacheMetrics holds bounded-cardinality OpenTelemetry instruments for the warm NVMe cache.
type cacheMetrics struct {
	activePacks         metric.Int64Gauge
	pendingPrunePacks   metric.Int64Gauge
	maxRepoActivePacks  metric.Int64Gauge
	maxRepoPendingPrune metric.Int64Gauge
	packsPrunedTotal    metric.Int64Counter
}

func newCacheMetrics(provider *telemetry.Provider) *cacheMetrics {
	if provider == nil {
		return nil
	}
	meter := provider.Meter(instrumentationScope)

	activePacks, _ := meter.Int64Gauge(
		"stravin_cache_active_packs",
		metric.WithDescription("Total NVMe packfiles currently cached across repositories"),
	)
	pendingPrunePacks, _ := meter.Int64Gauge(
		"stravin_cache_pending_prune_packs",
		metric.WithDescription("Total superseded NVMe packfiles awaiting prior-generation pin release"),
	)
	maxRepoActivePacks, _ := meter.Int64Gauge(
		"stravin_cache_repo_active_packs_max",
		metric.WithDescription("Maximum NVMe packfiles currently cached for any single repository"),
	)
	maxRepoPendingPrune, _ := meter.Int64Gauge(
		"stravin_cache_repo_pending_prune_packs_max",
		metric.WithDescription("Maximum superseded packfiles pending prune for any single repository"),
	)
	packsPrunedTotal, _ := meter.Int64Counter(
		"stravin_cache_packs_pruned_total",
		metric.WithDescription("Total superseded NVMe packfiles unlinked by cache pruning"),
	)

	return &cacheMetrics{
		activePacks:         activePacks,
		pendingPrunePacks:   pendingPrunePacks,
		maxRepoActivePacks:  maxRepoActivePacks,
		maxRepoPendingPrune: maxRepoPendingPrune,
		packsPrunedTotal:    packsPrunedTotal,
	}
}

func (m *cacheMetrics) recordPackCounts(totalActive, totalPending, maxRepoActive, maxRepoPending int64) {
	if m == nil {
		return
	}
	ctx := context.Background()
	if m.activePacks != nil {
		m.activePacks.Record(ctx, totalActive)
	}
	if m.pendingPrunePacks != nil {
		m.pendingPrunePacks.Record(ctx, totalPending)
	}
	if m.maxRepoActivePacks != nil {
		m.maxRepoActivePacks.Record(ctx, maxRepoActive)
	}
	if m.maxRepoPendingPrune != nil {
		m.maxRepoPendingPrune.Record(ctx, maxRepoPending)
	}
}

func (m *cacheMetrics) recordPacksPruned(count int64) {
	if m == nil || count <= 0 || m.packsPrunedTotal == nil {
		return
	}
	m.packsPrunedTotal.Add(context.Background(), count)
}
