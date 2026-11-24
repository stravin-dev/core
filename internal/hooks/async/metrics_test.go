package async

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func collectHookMetrics(t *testing.T, prov *telemetry.Provider) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func hookSumValue(t *testing.T, m metricdata.Metrics, result string) int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %s data = %T, want Sum[int64]", m.Name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		got, ok := dp.Attributes.Value(attribute.Key("result"))
		if ok && got.AsString() == result {
			total += dp.Value
		}
	}
	return total
}

func TestHookDispatcherAndConsumerMetrics(t *testing.T) {
	ctx := context.Background()
	tel, err := telemetry.NewInMemoryProvider("stravin-hook-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(ctx) }()

	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	var flakeyAttempts atomic.Int32
	flakeySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if flakeyAttempts.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer flakeySrv.Close()

	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer failSrv.Close()

	rk := tenant.RepoKey{TenantID: "acme", RepoID: "core"}
	repo := &fakeWebhookRepo{
		hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
			rk: {
				{
					WebhookID: "wh-ok-after-retry",
					URL:       flakeySrv.URL,
					RefGlob:   "refs/heads/*",
					Enabled:   true,
				},
				{
					WebhookID: "wh-dlq",
					URL:       failSrv.URL,
					RefGlob:   "refs/heads/*",
					Enabled:   true,
				},
				{
					WebhookID:       "wh-bad-secret",
					URL:             flakeySrv.URL,
					EncryptedSecret: []byte("wrong-tenant:secret"),
					RefGlob:         "refs/heads/*",
					Enabled:         true,
				},
			},
		},
	}

	disp, err := NewWebhookDispatcher(DispatcherConfig{
		Webhooks:             repo,
		Cipher:               fakeCipher{},
		DLQ:                  NewMemoryDLQSink(),
		Telemetry:            tel,
		Metrics:              metrics,
		MaxAttempts:          2,
		InitialBackoff:       time.Millisecond,
		MaxBackoff:           2 * time.Millisecond,
		Sleep:                func(context.Context, time.Duration) error { return nil },
		AllowPrivateNetworks: true,
	})
	if err != nil {
		t.Fatalf("NewWebhookDispatcher: %v", err)
	}

	// Wire consumer with one valid Ref History record and one malformed Ref History record.
	shardID := "shardId-0001"
	validRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("seq-1"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":        &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#core#RH#1"},
				"SK":        &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J1"},
				"tenant_id": &streamtypes.AttributeValueMemberS{Value: "acme"},
				"repo_id":   &streamtypes.AttributeValueMemberS{Value: "core"},
				"ref_name":  &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
				"oid":       &streamtypes.AttributeValueMemberS{Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				"pack_id":   &streamtypes.AttributeValueMemberS{Value: "pack-1"},
			},
		},
	}
	malformedRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("seq-2"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":         &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#core#RH#1"},
				"SK":         &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J2"},
				"updated_at": &streamtypes.AttributeValueMemberS{Value: "not-a-timestamp"},
			},
		},
	}

	streams := &fakeStreamsClient{
		shards: []streamtypes.Shard{{ShardId: aws.String(shardID)}},
		recordsByIter: map[string][]streamtypes.Record{
			"iter-shardId-0001-TRIM_HORIZON": {validRec, malformedRec},
		},
	}
	consumer, err := NewConsumer(ConsumerConfig{
		TableName:   "stravin-core",
		ConsumerID:  "stravin-hook",
		Tables:      &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000000000000:table/stravin-core/stream/1"},
		Streams:     streams,
		Checkpoints: &fakeCheckpointRepo{},
		Observers:   []EventObserver{disp},
		Telemetry:   tel,
		Metrics:     metrics,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.PollOnce(ctx); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	got := collectHookMetrics(t, tel)

	// Assert no forbidden high-cardinality labels on any hook metric.
	for name, m := range got {
		switch d := m.Data.(type) {
		case metricdata.Sum[int64]:
			for _, dp := range d.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if telemetry.IsForbiddenMetricLabel(string(kv.Key)) {
						t.Errorf("metric %s has forbidden label %q", name, kv.Key)
					}
				}
			}
		case metricdata.Histogram[float64]:
			for _, dp := range d.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if telemetry.IsForbiddenMetricLabel(string(kv.Key)) {
						t.Errorf("metric %s has forbidden label %q", name, kv.Key)
					}
				}
			}
		}
	}

	deliveries, ok := got["stravin_hook_deliveries_total"]
	if !ok {
		t.Fatal("stravin_hook_deliveries_total not found")
	}
	if v := hookSumValue(t, deliveries, "ok"); v != 1 {
		t.Errorf("deliveries ok = %d, want 1", v)
	}
	if v := hookSumValue(t, deliveries, "retry"); v != 1 {
		t.Errorf("deliveries retry = %d, want 1", v)
	}
	if v := hookSumValue(t, deliveries, "dlq"); v != 2 {
		t.Errorf("deliveries dlq = %d, want 2 (bad HTTP 400 + decrypt failure)", v)
	}
	if v := hookSumValue(t, deliveries, "error"); v != 1 {
		t.Errorf("deliveries error = %d, want 1 (decrypt failure)", v)
	}

	durMetric, ok := got["stravin_hook_delivery_duration_seconds"]
	if !ok {
		t.Fatal("stravin_hook_delivery_duration_seconds not found")
	}
	hist, ok := durMetric.Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) == 0 {
		t.Fatalf("unexpected delivery duration histogram: %T", durMetric.Data)
	}
	if !slices.Equal(hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets) {
		t.Errorf("delivery duration bounds = %v, want %v", hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets)
	}

	streamRecs, ok := got["stravin_hook_stream_records_total"]
	if !ok {
		t.Fatal("stravin_hook_stream_records_total not found")
	}
	if v := hookSumValue(t, streamRecs, "ok"); v != 1 {
		t.Errorf("stream_records ok = %d, want 1", v)
	}
	if v := hookSumValue(t, streamRecs, "error"); v != 1 {
		t.Errorf("stream_records error = %d, want 1", v)
	}
}
