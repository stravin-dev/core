package telemetry

import "go.opentelemetry.io/otel/metric"

// DurationSecondsBuckets defines explicit second-scale histogram bucket
// boundaries (1ms to 30s) for *_seconds Float64Histogram instruments.
// Without explicit boundaries, Go's OpenTelemetry SDK defaults to
// millisecond-scale boundaries ([0, 5, 10, ...]), collapsing all sub-second
// durations into the le="5" bucket.
var DurationSecondsBuckets = []float64{
	0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

// WithDurationSecondsBuckets returns a Float64HistogramOption that configures
// explicit second-scale bucket boundaries (DurationSecondsBuckets).
func WithDurationSecondsBuckets() metric.Float64HistogramOption {
	return metric.WithExplicitBucketBoundaries(DurationSecondsBuckets...)
}
