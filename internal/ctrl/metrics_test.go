package ctrl

import (
	"context"
	"math"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

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

func TestEpochToInt64(t *testing.T) {
	tests := []struct {
		name  string
		epoch lease.Epoch
		want  int64
	}{
		{name: "zero", epoch: 0, want: 0},
		{name: "small", epoch: 42, want: 42},
		{name: "max int64", epoch: lease.Epoch(math.MaxInt64), want: math.MaxInt64},
		{name: "max int64 plus one", epoch: lease.Epoch(math.MaxInt64) + 1, want: math.MaxInt64},
		{name: "max uint64", epoch: lease.Epoch(math.MaxUint64), want: math.MaxInt64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := epochToInt64(tc.epoch); got != tc.want {
				t.Errorf("epochToInt64(%d) = %d, want %d", tc.epoch, got, tc.want)
			}
		})
	}
}

func assertSpanFailoverEpoch(t *testing.T, spans tracetest.SpanStubs, spanName string, want int64) {
	t.Helper()
	for _, sp := range spans {
		if sp.Name != spanName {
			continue
		}
		for _, attr := range sp.Attributes {
			if attr.Key == telemetry.AttrFailoverEpoch {
				if got := attr.Value.AsInt64(); got != want {
					t.Errorf("span %q %s = %d, want %d", spanName, telemetry.AttrFailoverEpoch, got, want)
				}
				return
			}
		}
		t.Errorf("span %q missing attribute %s", spanName, telemetry.AttrFailoverEpoch)
		return
	}
	t.Fatalf("span %q not found in %d recorded spans", spanName, len(spans))
}
