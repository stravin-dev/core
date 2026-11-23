package ctrl

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/telemetry"
)

func TestScrubDurationHistogram_UsesSecondScaleBuckets(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-ctrl")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m, err := NewMetrics(prov)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	m.recordScrubRun(ctx, lease.Region("us-east-1"), "ok", 4*time.Millisecond)
	m.recordScrubRun(ctx, lease.Region("us-east-1"), "ok", 20*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	var dp *metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, metricItem := range sm.Metrics {
			if metricItem.Name != "stravin_ctrl_scrub_duration_seconds" {
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
		t.Fatal("stravin_ctrl_scrub_duration_seconds not found")
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
