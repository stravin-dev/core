package telemetry_test

import (
	"context"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/telemetry"
)

func TestWithDurationSecondsBuckets_PopulatesSubSecondBuckets(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(ctx) }()

	hist, err := mp.Meter("test").Float64Histogram(
		"stravin_test_duration_seconds",
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		t.Fatalf("Float64Histogram: %v", err)
	}

	// Record 4ms (0.004s -> le=0.005, idx 1) and 20ms (0.020s -> le=0.025, idx 3).
	hist.Record(ctx, 0.004)
	hist.Record(ctx, 0.020)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	dp := findFloat64HistogramDataPoint(t, rm, "stravin_test_duration_seconds")
	if !slices.Equal(dp.Bounds, telemetry.DurationSecondsBuckets) {
		t.Fatalf("Bounds = %v, want %v", dp.Bounds, telemetry.DurationSecondsBuckets)
	}
	if dp.Count != 2 {
		t.Fatalf("Count = %d, want 2", dp.Count)
	}
	if dp.BucketCounts[1] != 1 {
		t.Errorf("bucket (0.001, 0.005] count = %d, want 1 (all counts: %v)", dp.BucketCounts[1], dp.BucketCounts)
	}
	if dp.BucketCounts[3] != 1 {
		t.Errorf("bucket (0.01, 0.025] count = %d, want 1 (all counts: %v)", dp.BucketCounts[3], dp.BucketCounts)
	}
	if dp.BucketCounts[10] != 0 {
		t.Errorf("bucket (2.5, 5] count = %d, want 0 (sub-second values must not collapse into le=5)", dp.BucketCounts[10])
	}
}

func TestWithDurationSecondsBuckets_ViaInMemoryProviderAndGuardedMeter(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	hist, err := prov.Meter("stravin.test").Float64Histogram(
		"stravin_guarded_duration_seconds",
		metric.WithUnit("s"),
		telemetry.WithDurationSecondsBuckets(),
	)
	if err != nil {
		t.Fatalf("GuardedMeter.Float64Histogram: %v", err)
	}

	hist.Record(ctx, 0.004)
	hist.Record(ctx, 0.020)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	dp := findFloat64HistogramDataPoint(t, rm, "stravin_guarded_duration_seconds")
	if !slices.Equal(dp.Bounds, telemetry.DurationSecondsBuckets) {
		t.Fatalf("Bounds = %v, want %v", dp.Bounds, telemetry.DurationSecondsBuckets)
	}
	if dp.BucketCounts[1] != 1 || dp.BucketCounts[3] != 1 {
		t.Errorf("unexpected sub-second bucket counts: %v", dp.BucketCounts)
	}
}

func findFloat64HistogramDataPoint(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %q data type = %T, want metricdata.Histogram[float64]", name, m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("metric %q data points = %d, want 1", name, len(hist.DataPoints))
			}
			return hist.DataPoints[0]
		}
	}
	t.Fatalf("metric %q not found in collected ResourceMetrics", name)
	return metricdata.HistogramDataPoint[float64]{}
}
