package node

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/telemetry"
)

func TestRequestDurationHistogram_UsesSecondScaleBuckets(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m := NewMetrics(prov, "us-east-1")
	m.RecordRequest(ctx, "fetch", "ok", 4*time.Millisecond)
	m.RecordRequest(ctx, "fetch", "ok", 20*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	var dp *metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, metricItem := range sm.Metrics {
			if metricItem.Name != "stravin_node_request_duration_seconds" {
				continue
			}
			hist, ok := metricItem.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("data type = %T, want metricdata.Histogram[float64]", metricItem.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("data points = %d, want 1", len(hist.DataPoints))
			}
			dp = &hist.DataPoints[0]
		}
	}
	if dp == nil {
		t.Fatal("stravin_node_request_duration_seconds not found")
	}
	if !slices.Equal(dp.Bounds, telemetry.DurationSecondsBuckets) {
		t.Fatalf("Bounds = %v, want %v", dp.Bounds, telemetry.DurationSecondsBuckets)
	}
	if dp.BucketCounts[1] != 1 {
		t.Errorf("bucket (0.001, 0.005] count = %d, want 1 (counts: %v)", dp.BucketCounts[1], dp.BucketCounts)
	}
	if dp.BucketCounts[3] != 1 {
		t.Errorf("bucket (0.01, 0.025] count = %d, want 1 (counts: %v)", dp.BucketCounts[3], dp.BucketCounts)
	}
}

func TestRecordAuxIndexRefreshFailure_ExposesOTelAndPrometheusMetrics(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m := NewMetrics(prov, "us-east-1")
	m.RecordAuxIndexRefreshFailure(ctx, "midx")
	m.RecordAuxIndexRefreshFailure(ctx, "commit_graph")

	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	wantProm := `stravin_node_aux_index_refresh_failures_total{region="us-east-1"} 2`
	if !strings.Contains(body, wantProm) {
		t.Fatalf("expected Prometheus body to contain %q, got:\n%s", wantProm, body)
	}
}

func TestUploadPackQueueMetrics_ExposesOTelAndPrometheus(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m := NewMetrics(prov, "us-east-1")
	m.RecordUploadPackQueueWait(ctx, 12*time.Millisecond)
	m.RecordUploadPackQueueTimeout(ctx)
	m.RecordUploadPackQueueTimeout(ctx)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	var sawWait, sawTimeout bool
	for _, sm := range rm.ScopeMetrics {
		for _, item := range sm.Metrics {
			switch item.Name {
			case "stravin_node_upload_pack_queue_wait_seconds":
				hist, ok := item.Data.(metricdata.Histogram[float64])
				if !ok || len(hist.DataPoints) != 1 {
					t.Fatalf("unexpected queue wait metric data: %T (%+v)", item.Data, item.Data)
				}
				if !slices.Equal(hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets) {
					t.Errorf("Bounds = %v, want %v", hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets)
				}
				if hist.DataPoints[0].Count != 1 {
					t.Errorf("Count = %d, want 1", hist.DataPoints[0].Count)
				}
				sawWait = true
			case "stravin_node_upload_pack_queue_timeouts_total":
				sum, ok := item.Data.(metricdata.Sum[int64])
				if !ok || len(sum.DataPoints) != 1 {
					t.Fatalf("unexpected queue timeout metric data: %T (%+v)", item.Data, item.Data)
				}
				if sum.DataPoints[0].Value != 2 {
					t.Errorf("timeouts = %d, want 2", sum.DataPoints[0].Value)
				}
				sawTimeout = true
			}
		}
	}
	if !sawWait || !sawTimeout {
		t.Fatalf("missing OTel metrics: sawWait=%v sawTimeout=%v", sawWait, sawTimeout)
	}

	rec := httptest.NewRecorder()
	m.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	wantProm := `stravin_node_upload_pack_queue_timeouts_total{region="us-east-1"} 2`
	if !strings.Contains(rec.Body.String(), wantProm) {
		t.Fatalf("expected Prometheus body to contain %q, got:\n%s", wantProm, rec.Body.String())
	}
}
