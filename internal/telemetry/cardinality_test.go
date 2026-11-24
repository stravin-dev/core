package telemetry_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/telemetry"
)

func TestValidateMetricAttributes_RejectsUnboundedLabels(t *testing.T) {
	forbidden := []string{
		"repo_id",
		"ref_name",
		"pack_id",
		"oid",
		"actor_id",
		"tenant_id",
		"hook_id",
		"failover_epoch",
	}

	for _, key := range forbidden {
		err := telemetry.ValidateMetricAttributes(
			attribute.String("git.operation", "push"),
			attribute.String(key, "unbounded-value"),
		)
		if !errors.Is(err, telemetry.ErrHighCardinalityLabel) {
			t.Errorf("key %q: got err %v, want ErrHighCardinalityLabel", key, err)
		}
	}
}

func TestValidateMetricAttributes_AllowsBoundedLabels(t *testing.T) {
	err := telemetry.ValidateMetricAttributes(
		attribute.String("git.protocol", "ssh"),
		attribute.String("git.operation", "push"),
		attribute.String("status_code", "200"),
		attribute.String("error_class", "none"),
		attribute.String("fail_mode", "closed"),
		attribute.Int("shard_bucket", 5),
		attribute.Int("ref_shard", 12),
		attribute.Int("manifest_shard", 3),
	)
	if err != nil {
		t.Fatalf("unexpected error for bounded labels: %v", err)
	}
}

func TestSanitizeMetricAttributes_StripsForbiddenLabels(t *testing.T) {
	clean, dropped := telemetry.SanitizeMetricAttributes(
		attribute.String("git.operation", "fetch"),
		attribute.String("repo_id", "repo-123"),
		attribute.String("ref_name", "refs/heads/main"),
		attribute.String("pack_id", "pack-abc"),
		attribute.String("oid", "deadbeef"),
		attribute.String("actor_id", "user-1"),
		attribute.String("git.protocol", "http"),
	)

	if len(clean) != 2 {
		t.Fatalf("expected 2 clean attributes, got %d: %v", len(clean), clean)
	}
	if len(dropped) != 5 {
		t.Fatalf("expected 5 dropped attributes, got %d: %v", len(dropped), dropped)
	}
}

func TestGuardedMeter_StripsForbiddenLabelsFromMetrics(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	var violations []string
	meter := prov.Meter("stravin.edge", telemetry.WithOnViolation(func(keys []string) {
		violations = append(violations, keys...)
	}))

	counter, err := meter.Int64Counter("stravin_edge_requests_total")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}

	counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("git.operation", "push"),
		attribute.String("repo_id", "repo-secret"),
		attribute.String("ref_name", "refs/heads/main"),
		attribute.String("pack_id", "pack-99"),
		attribute.String("oid", "0123456789abcdef"),
	))

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	if len(violations) != 4 {
		t.Fatalf("expected 4 violations recorded, got %d: %v", len(violations), violations)
	}

	foundMetric := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_edge_requests_total" {
				continue
			}
			foundMetric = true
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("unexpected metric data type %T", m.Data)
			}
			if len(sum.DataPoints) != 1 {
				t.Fatalf("expected 1 datapoint, got %d", len(sum.DataPoints))
			}
			dp := sum.DataPoints[0]
			for _, kv := range dp.Attributes.ToSlice() {
				if telemetry.IsForbiddenMetricLabel(string(kv.Key)) {
					t.Errorf("forbidden label %q leaked onto metric", kv.Key)
				}
			}
			val, ok := dp.Attributes.Value("git.operation")
			if !ok || val.AsString() != "push" {
				t.Errorf("expected bounded label git.operation=push, got %v (ok=%v)", val, ok)
			}
		}
	}
	if !foundMetric {
		t.Fatal("expected stravin_edge_requests_total metric to be collected")
	}
}

func TestGuardedMeter_RejectPolicyDropsRecording(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	var violated bool
	meter := prov.Meter("stravin.edge",
		telemetry.WithCardinalityPolicy(telemetry.PolicyReject),
		telemetry.WithOnViolation(func(_ []string) { violated = true }),
	)

	hist, err := meter.Float64Histogram("stravin_edge_request_duration_seconds")
	if err != nil {
		t.Fatalf("Float64Histogram: %v", err)
	}

	hist.Record(ctx, 0.25, metric.WithAttributes(
		attribute.String("git.operation", "push"),
		attribute.String("repo_id", "repo-1"),
	))

	if !violated {
		t.Fatal("expected OnViolation callback to fire under PolicyReject")
	}

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "stravin_edge_request_duration_seconds" {
				t.Fatal("expected rejected metric recording not to be emitted")
			}
		}
	}
}

func TestGuardedMeter_InstrumentsDelegateEnabled(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	meter := prov.Meter("stravin.edge")
	ic, err := meter.Int64Counter("test_ic")
	if err != nil || !ic.Enabled(ctx) {
		t.Fatalf("Int64Counter.Enabled = %v, err = %v; want true, nil", ic != nil && ic.Enabled(ctx), err)
	}
	fg, err := meter.Float64Gauge("test_fg")
	if err != nil || !fg.Enabled(ctx) {
		t.Fatalf("Float64Gauge.Enabled = %v, err = %v; want true, nil", fg != nil && fg.Enabled(ctx), err)
	}
}
