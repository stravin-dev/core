package node

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"stravin.dev/core/internal/telemetry"
)

// Metrics records bounded-cardinality OpenTelemetry metrics and serves a
// Prometheus text-format /metrics endpoint for stravin-node.
type Metrics struct {
	region string

	requestsTotal           metric.Int64Counter
	requestDuration         metric.Float64Histogram
	cacheHits               metric.Int64Counter
	cacheMisses             metric.Int64Counter
	cacheBytesUsed          metric.Int64Gauge
	s3PackFetches           metric.Int64Counter
	auxIndexRefreshFailures metric.Int64Counter
	uploadPackQueueWait     metric.Float64Histogram
	uploadPackQueueTimeouts metric.Int64Counter

	promRequestsOK              atomic.Int64
	promRequestsErr             atomic.Int64
	promCacheHits               atomic.Int64
	promCacheMisses             atomic.Int64
	promCacheBytes              atomic.Int64
	promS3FetchesOK             atomic.Int64
	promS3FetchesErr            atomic.Int64
	promAuxIndexRefreshFailures atomic.Int64
	promUploadPackQueueTimeouts atomic.Int64
}

// NewMetrics initializes bounded-cardinality OTel instruments via GuardedMeter.
func NewMetrics(tp *telemetry.Provider, region string) *Metrics {
	if tp == nil {
		tp = telemetry.NewNoopProvider()
	}
	if region == "" {
		region = "unknown"
	}
	meter := tp.Meter("stravin-node")
	reqTotal, _ := meter.Int64Counter("stravin_node_requests_total",
		metric.WithDescription("Total stravin-node GitWire requests by operation and status"),
	)
	reqDur, _ := meter.Float64Histogram("stravin_node_request_duration_seconds",
		metric.WithDescription("Duration of stravin-node GitWire requests in seconds"),
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	hits, _ := meter.Int64Counter("stravin_node_cache_hits_total",
		metric.WithDescription("Warm NVMe pack cache hits"),
	)
	misses, _ := meter.Int64Counter("stravin_node_cache_misses_total",
		metric.WithDescription("Warm NVMe pack cache misses"),
	)
	bytesUsed, _ := meter.Int64Gauge("stravin_node_cache_bytes_used",
		metric.WithDescription("Total warm NVMe cache bytes currently in use"),
		metric.WithUnit("By"),
	)
	s3Fetches, _ := meter.Int64Counter("stravin_node_s3_pack_fetches_total",
		metric.WithDescription("S3 pack fetch operations by result"),
	)
	auxIndexFailures, _ := meter.Int64Counter("stravin_node_aux_index_refresh_failures_total",
		metric.WithDescription("Failed auxiliary index (multi-pack-index and commit-graph) refresh attempts"),
	)
	queueWait, _ := meter.Float64Histogram("stravin_node_upload_pack_queue_wait_seconds",
		metric.WithDescription("Time spent waiting for an upload-pack concurrency slot in seconds"),
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	queueTimeouts, _ := meter.Int64Counter("stravin_node_upload_pack_queue_timeouts_total",
		metric.WithDescription("Upload-pack requests rejected after waiting past queue timeout"),
	)

	return &Metrics{
		region:                  region,
		requestsTotal:           reqTotal,
		requestDuration:         reqDur,
		cacheHits:               hits,
		cacheMisses:             misses,
		cacheBytesUsed:          bytesUsed,
		s3PackFetches:           s3Fetches,
		auxIndexRefreshFailures: auxIndexFailures,
		uploadPackQueueWait:     queueWait,
		uploadPackQueueTimeouts: queueTimeouts,
	}
}

// RecordRequest records a completed GitWire stream request with bounded labels only.
func (m *Metrics) RecordRequest(ctx context.Context, gitOp, statusCode string, dur time.Duration) {
	if m == nil {
		return
	}
	attrs := []attribute.KeyValue{
		telemetry.AttrRegion.String(m.region),
		telemetry.AttrGitOperation.String(gitOp),
		telemetry.AttrStatusCode.String(statusCode),
	}
	if m.requestsTotal != nil {
		m.requestsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if m.requestDuration != nil {
		m.requestDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))
	}
	if statusCode == "ok" {
		m.promRequestsOK.Add(1)
	} else {
		m.promRequestsErr.Add(1)
	}
}

// RecordCacheLookup records a warm NVMe cache hit or miss.
func (m *Metrics) RecordCacheLookup(ctx context.Context, hit bool) {
	if m == nil {
		return
	}
	attr := telemetry.AttrRegion.String(m.region)
	if hit {
		m.promCacheHits.Add(1)
		if m.cacheHits != nil {
			m.cacheHits.Add(ctx, 1, metric.WithAttributes(attr))
		}
		return
	}
	m.promCacheMisses.Add(1)
	if m.cacheMisses != nil {
		m.cacheMisses.Add(ctx, 1, metric.WithAttributes(attr))
	}
}

// RecordCacheBytes updates the warm NVMe cache byte usage gauge.
func (m *Metrics) RecordCacheBytes(ctx context.Context, totalBytes int64) {
	if m == nil {
		return
	}
	m.promCacheBytes.Store(totalBytes)
	if m.cacheBytesUsed != nil {
		m.cacheBytesUsed.Record(ctx, totalBytes, metric.WithAttributes(telemetry.AttrRegion.String(m.region)))
	}
}

// RecordS3PackFetch records an S3 pack+idx fetch attempt with bounded status_code.
func (m *Metrics) RecordS3PackFetch(ctx context.Context, statusCode string) {
	if m == nil {
		return
	}
	if statusCode == "ok" {
		m.promS3FetchesOK.Add(1)
	} else {
		m.promS3FetchesErr.Add(1)
	}
	if m.s3PackFetches != nil {
		m.s3PackFetches.Add(ctx, 1, metric.WithAttributes(
			telemetry.AttrRegion.String(m.region),
			telemetry.AttrStatusCode.String(statusCode),
		))
	}
}

// RecordAuxIndexRefreshFailure records a failed auxiliary index refresh attempt
// with bounded labels (region and index_type).
func (m *Metrics) RecordAuxIndexRefreshFailure(ctx context.Context, indexType string) {
	if m == nil {
		return
	}
	m.promAuxIndexRefreshFailures.Add(1)
	if m.auxIndexRefreshFailures != nil {
		m.auxIndexRefreshFailures.Add(ctx, 1, metric.WithAttributes(
			telemetry.AttrRegion.String(m.region),
			attribute.String("index_type", indexType),
		))
	}
}

// RecordUploadPackQueueWait records time spent waiting for an upload-pack concurrency slot.
func (m *Metrics) RecordUploadPackQueueWait(ctx context.Context, dur time.Duration) {
	if m == nil {
		return
	}
	if m.uploadPackQueueWait != nil {
		m.uploadPackQueueWait.Record(ctx, dur.Seconds(), metric.WithAttributes(
			telemetry.AttrRegion.String(m.region),
		))
	}
}

// RecordUploadPackQueueTimeout records an upload-pack request rejected due to queue timeout.
func (m *Metrics) RecordUploadPackQueueTimeout(ctx context.Context) {
	if m == nil {
		return
	}
	m.promUploadPackQueueTimeouts.Add(1)
	if m.uploadPackQueueTimeouts != nil {
		m.uploadPackQueueTimeouts.Add(ctx, 1, metric.WithAttributes(
			telemetry.AttrRegion.String(m.region),
		))
	}
}

// ServeHTTP exposes bounded-cardinality Prometheus metrics at /metrics.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintf(w,
		"# HELP stravin_node_requests_total Total stravin-node GitWire requests.\n"+
			"# TYPE stravin_node_requests_total counter\n"+
			"stravin_node_requests_total{region=%q,status_code=\"ok\"} %d\n"+
			"stravin_node_requests_total{region=%q,status_code=\"error\"} %d\n"+
			"# HELP stravin_node_cache_hits_total Warm NVMe pack cache hits.\n"+
			"# TYPE stravin_node_cache_hits_total counter\n"+
			"stravin_node_cache_hits_total{region=%q} %d\n"+
			"# HELP stravin_node_cache_misses_total Warm NVMe pack cache misses.\n"+
			"# TYPE stravin_node_cache_misses_total counter\n"+
			"stravin_node_cache_misses_total{region=%q} %d\n"+
			"# HELP stravin_node_cache_bytes_used Warm NVMe cache bytes in use.\n"+
			"# TYPE stravin_node_cache_bytes_used gauge\n"+
			"stravin_node_cache_bytes_used{region=%q} %d\n"+
			"# HELP stravin_node_s3_pack_fetches_total S3 pack fetch operations.\n"+
			"# TYPE stravin_node_s3_pack_fetches_total counter\n"+
			"stravin_node_s3_pack_fetches_total{region=%q,status_code=\"ok\"} %d\n"+
			"stravin_node_s3_pack_fetches_total{region=%q,status_code=\"error\"} %d\n"+
			"# HELP stravin_node_aux_index_refresh_failures_total Failed auxiliary index refresh attempts.\n"+
			"# TYPE stravin_node_aux_index_refresh_failures_total counter\n"+
			"stravin_node_aux_index_refresh_failures_total{region=%q} %d\n"+
			"# HELP stravin_node_upload_pack_queue_timeouts_total Upload-pack requests rejected due to queue timeout.\n"+
			"# TYPE stravin_node_upload_pack_queue_timeouts_total counter\n"+
			"stravin_node_upload_pack_queue_timeouts_total{region=%q} %d\n",
		m.region, m.promRequestsOK.Load(),
		m.region, m.promRequestsErr.Load(),
		m.region, m.promCacheHits.Load(),
		m.region, m.promCacheMisses.Load(),
		m.region, m.promCacheBytes.Load(),
		m.region, m.promS3FetchesOK.Load(),
		m.region, m.promS3FetchesErr.Load(),
		m.region, m.promAuxIndexRefreshFailures.Load(),
		m.region, m.promUploadPackQueueTimeouts.Load(),
	)
}
