package edge

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func TestRequestDurationHistogram_UsesSecondScaleBuckets(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m, err := NewMetrics(prov)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	m.RecordRequest(ctx, "http", "fetch", "ok", "none", 4*time.Millisecond)
	m.RecordRequest(ctx, "http", "fetch", "ok", "none", 20*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}

	var dp *metricdata.HistogramDataPoint[float64]
	for _, sm := range rm.ScopeMetrics {
		for _, metricItem := range sm.Metrics {
			if metricItem.Name != "stravin_edge_request_duration_seconds" {
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
		t.Fatal("stravin_edge_request_duration_seconds not found")
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

func collectMetricMap(t *testing.T, prov *telemetry.Provider) map[string]metricdata.Metrics {
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

func sumInt64Value(t *testing.T, m metricdata.Metrics, wantAttrs map[string]string) int64 {
	t.Helper()
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %s data = %T, want Sum[int64]", m.Name, m.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		if attrsMatch(dp.Attributes, wantAttrs) {
			total += dp.Value
		}
	}
	return total
}

func attrsMatch(set attribute.Set, want map[string]string) bool {
	for k, v := range want {
		got, ok := set.Value(attribute.Key(k))
		if !ok || got.AsString() != v {
			return false
		}
	}
	return true
}

func assertNoForbiddenLabels(t *testing.T, metrics map[string]metricdata.Metrics) {
	t.Helper()
	for name, m := range metrics {
		switch d := m.Data.(type) {
		case metricdata.Sum[int64]:
			for _, dp := range d.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if telemetry.IsForbiddenMetricLabel(string(kv.Key)) {
						t.Errorf("metric %s has forbidden high-cardinality label %q", name, kv.Key)
					}
				}
			}
		case metricdata.Histogram[float64]:
			for _, dp := range d.DataPoints {
				for _, kv := range dp.Attributes.ToSlice() {
					if telemetry.IsForbiddenMetricLabel(string(kv.Key)) {
						t.Errorf("metric %s has forbidden high-cardinality label %q", name, kv.Key)
					}
				}
			}
		}
	}
}

func TestEdgePackCacheAndIngressMetrics(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	m, err := NewMetrics(prov)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	// 1. Valkey pack cache: miss, set ok, hit, too_large, pool reuse, and closed error.
	clk := &fakeClock{now: time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)}
	vkSrv := startFakeValkeyServer(t, clk.Now)
	vc, err := NewValkeyPackCache(ValkeyPackCacheConfig{
		Addr:          vkSrv.Addr(),
		MaxEntryBytes: 32,
		Metrics:       m,
	})
	if err != nil {
		t.Fatalf("NewValkeyPackCache: %v", err)
	}

	if _, hit, err := vc.Get(ctx, "k1"); err != nil || hit {
		t.Fatalf("valkey miss: hit=%v err=%v", hit, err)
	}
	payload := []byte("pack-data-123456")
	if err := vc.Set(ctx, "k1", payload, time.Minute); err != nil {
		t.Fatalf("valkey set: %v", err)
	}
	if got, hit, err := vc.Get(ctx, "k1"); err != nil || !hit || !bytes.Equal(got, payload) {
		t.Fatalf("valkey hit: hit=%v err=%v got=%q", hit, err, got)
	}
	if err := vc.Set(ctx, "kbig", bytes.Repeat([]byte("x"), 64), time.Minute); !errors.Is(err, ErrPackCacheEntryTooLarge) {
		t.Fatalf("valkey too_large: err=%v, want ErrPackCacheEntryTooLarge", err)
	}
	_ = vc.Close()
	if _, _, err := vc.Get(ctx, "k1"); err == nil {
		t.Fatal("expected error after valkey close")
	}

	// 2. S3 pack cache: miss, set ok, hit, too_large, and backend error.
	s3Fake := newFakePackCacheS3API(clk.Now)
	sc, err := NewS3PackCache(S3PackCacheConfig{
		Client:        s3Fake,
		Bucket:        "stravin-test",
		MaxEntryBytes: 32,
		TTL:           time.Minute,
		Now:           clk.Now,
		Metrics:       m,
	})
	if err != nil {
		t.Fatalf("NewS3PackCache: %v", err)
	}
	if _, hit, err := sc.Get(ctx, "s1"); err != nil || hit {
		t.Fatalf("s3 miss: hit=%v err=%v", hit, err)
	}
	if err := sc.Set(ctx, "s1", payload, time.Minute); err != nil {
		t.Fatalf("s3 set: %v", err)
	}
	if got, hit, err := sc.Get(ctx, "s1"); err != nil || !hit || !bytes.Equal(got, payload) {
		t.Fatalf("s3 hit: hit=%v err=%v", hit, err)
	}
	if err := sc.Set(ctx, "sbig", bytes.Repeat([]byte("y"), 64), time.Minute); !errors.Is(err, ErrPackCacheEntryTooLarge) {
		t.Fatalf("s3 too_large: err=%v", err)
	}
	s3Fake.failAll = true
	if _, _, err := sc.Get(ctx, "s1"); err == nil {
		t.Fatal("expected s3 get error")
	}
	if err := sc.Set(ctx, "s2", payload, time.Minute); err == nil {
		t.Fatal("expected s3 set error")
	}
	s3Fake.failAll = false

	// 3. Singleflight hit in PackCacheCoordinator.
	coord := NewPackCacheCoordinator(PackCacheCoordinatorConfig{
		Backend:         NoopPackResponseCache{},
		MaxCaptureBytes: 64,
		Metrics:         m,
	})
	_, finish, hit, err := coord.GetOrJoin(ctx, "sf-key")
	if err != nil || hit || finish == nil {
		t.Fatalf("leader GetOrJoin: hit=%v err=%v finish=%v", hit, err, finish != nil)
	}
	followerDone := make(chan []byte, 1)
	go func() {
		data, _, fHit, fErr := coord.GetOrJoin(ctx, "sf-key")
		if fErr == nil && fHit {
			followerDone <- data
			return
		}
		followerDone <- nil
	}()
	// Wait briefly until follower joins inflight map, then finish leader.
	time.Sleep(15 * time.Millisecond)
	finish(payload)
	if got := <-followerDone; !bytes.Equal(got, payload) {
		t.Fatalf("singleflight follower got %q, want %q", got, payload)
	}

	// 4. Resolver META cache miss then hit.
	rk := tenant.RepoKey{TenantID: "acme", RepoID: "core"}
	metaStore := &fakeMetaStore{
		metas: map[tenant.RepoKey]ddb.RepoMeta{
			rk: {Repo: rk, HomeRegion: "us-east-1", WriteState: lease.ModeActive},
		},
	}
	leaseStore := &fakeLeaseStore{}
	resolver, err := NewResolver("us-east-1", metaStore, lease.NewStoreVerifier(leaseStore, clk, 0), time.Minute, clk)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	resolver.SetMetrics(m)
	if _, err := resolver.ResolveRead(ctx, rk); err != nil {
		t.Fatalf("ResolveRead miss: %v", err)
	}
	if _, err := resolver.ResolveRead(ctx, rk); err != nil {
		t.Fatalf("ResolveRead hit: %v", err)
	}

	// 5. TokenBucketLimiter active clones UpDownCounter.
	limiter := NewTokenBucketLimiter(LimiterConfig{
		MaxConcurrentClones: 2,
		Clock:               clk,
		Metrics:             m,
	})
	rel1, err := limiter.AcquireClone(ctx, "acme")
	if err != nil {
		t.Fatalf("AcquireClone 1: %v", err)
	}
	rel2, err := limiter.AcquireClone(ctx, "acme")
	if err != nil {
		t.Fatalf("AcquireClone 2: %v", err)
	}
	// Check active_clones == 2 while held.
	midMetrics := collectMetricMap(t, prov)
	if got := sumInt64Value(t, midMetrics["stravin_edge_active_clones"], nil); got != 2 {
		t.Errorf("active_clones while held = %d, want 2", got)
	}
	rel1()
	rel2()
	rel2() // idempotent

	// 6. Info-refs retry and pack-cache bypass recordings.
	m.RecordInfoRefsRetry(ctx, true)
	m.RecordInfoRefsRetry(ctx, false)
	m.RecordPackCacheLookup(ctx, "singleflight", "bypass")

	gotMetrics := collectMetricMap(t, prov)
	assertNoForbiddenLabels(t, gotMetrics)

	// Verify pack cache lookup counters.
	lookups := gotMetrics["stravin_edge_pack_cache_lookups_total"]
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "valkey", "result": "hit"}); v != 1 {
		t.Errorf("valkey hit = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "valkey", "result": "miss"}); v != 1 {
		t.Errorf("valkey miss = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "valkey", "result": "error"}); v != 1 {
		t.Errorf("valkey error = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "s3", "result": "hit"}); v != 1 {
		t.Errorf("s3 hit = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "s3", "result": "miss"}); v != 1 {
		t.Errorf("s3 miss = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "s3", "result": "error"}); v != 1 {
		t.Errorf("s3 error = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "singleflight", "result": "hit"}); v != 1 {
		t.Errorf("singleflight hit = %d, want 1", v)
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "singleflight", "result": "bypass"}); v != 1 {
		t.Errorf("singleflight bypass = %d, want 1", v)
	}

	// Verify pack cache write counters.
	writes := gotMetrics["stravin_edge_pack_cache_writes_total"]
	if v := sumInt64Value(t, writes, map[string]string{"tier": "valkey", "result": "ok"}); v != 1 {
		t.Errorf("valkey write ok = %d, want 1", v)
	}
	if v := sumInt64Value(t, writes, map[string]string{"tier": "valkey", "result": "too_large"}); v != 1 {
		t.Errorf("valkey write too_large = %d, want 1", v)
	}
	if v := sumInt64Value(t, writes, map[string]string{"tier": "s3", "result": "ok"}); v != 1 {
		t.Errorf("s3 write ok = %d, want 1", v)
	}
	if v := sumInt64Value(t, writes, map[string]string{"tier": "s3", "result": "too_large"}); v != 1 {
		t.Errorf("s3 write too_large = %d, want 1", v)
	}
	if v := sumInt64Value(t, writes, map[string]string{"tier": "s3", "result": "error"}); v != 1 {
		t.Errorf("s3 write error = %d, want 1", v)
	}

	// Verify op duration histogram buckets.
	durMetric, ok := gotMetrics["stravin_edge_pack_cache_op_duration_seconds"]
	if !ok {
		t.Fatal("stravin_edge_pack_cache_op_duration_seconds not found")
	}
	hist, ok := durMetric.Data.(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) == 0 {
		t.Fatalf("unexpected duration histogram data: %T", durMetric.Data)
	}
	if !slices.Equal(hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets) {
		t.Errorf("pack cache op duration bounds = %v, want %v", hist.DataPoints[0].Bounds, telemetry.DurationSecondsBuckets)
	}

	// Verify bytes counters.
	bytesM := gotMetrics["stravin_edge_pack_cache_bytes_total"]
	if v := sumInt64Value(t, bytesM, map[string]string{"tier": "valkey", "op": "written"}); v != int64(len(payload)) {
		t.Errorf("valkey bytes written = %d, want %d", v, len(payload))
	}
	if v := sumInt64Value(t, bytesM, map[string]string{"tier": "valkey", "op": "hit_served"}); v != int64(len(payload)) {
		t.Errorf("valkey bytes hit_served = %d, want %d", v, len(payload))
	}
	if v := sumInt64Value(t, bytesM, map[string]string{"tier": "s3", "op": "written"}); v != int64(len(payload)) {
		t.Errorf("s3 bytes written = %d, want %d", v, len(payload))
	}
	if v := sumInt64Value(t, bytesM, map[string]string{"tier": "s3", "op": "hit_served"}); v != int64(len(payload)) {
		t.Errorf("s3 bytes hit_served = %d, want %d", v, len(payload))
	}
	if v := sumInt64Value(t, bytesM, map[string]string{"tier": "singleflight", "op": "hit_served"}); v != int64(len(payload)) {
		t.Errorf("singleflight bytes hit_served = %d, want %d", v, len(payload))
	}

	// Verify Valkey pool dial metrics.
	dials := gotMetrics["stravin_edge_valkey_pool_dials_total"]
	if v := sumInt64Value(t, dials, map[string]string{"result": "ok", "reused": "false"}); v != 1 {
		t.Errorf("valkey dial new = %d, want 1", v)
	}
	if v := sumInt64Value(t, dials, map[string]string{"result": "ok", "reused": "true"}); v < 1 {
		t.Errorf("valkey dial reused = %d, want >= 1", v)
	}
	if v := sumInt64Value(t, dials, map[string]string{"result": "error", "reused": "false"}); v != 1 {
		t.Errorf("valkey dial error = %d, want 1", v)
	}

	// Verify META cache lookup metrics.
	metaM := gotMetrics["stravin_edge_meta_cache_lookups_total"]
	if v := sumInt64Value(t, metaM, map[string]string{"result": "miss"}); v != 1 {
		t.Errorf("meta cache miss = %d, want 1", v)
	}
	if v := sumInt64Value(t, metaM, map[string]string{"result": "hit"}); v != 1 {
		t.Errorf("meta cache hit = %d, want 1", v)
	}

	// Verify active_clones returned to 0 after releases.
	if v := sumInt64Value(t, gotMetrics["stravin_edge_active_clones"], nil); v != 0 {
		t.Errorf("active_clones after release = %d, want 0", v)
	}

	// Verify info/refs retry metrics.
	retries := gotMetrics["stravin_edge_info_refs_retries_total"]
	if v := sumInt64Value(t, retries, map[string]string{"result": "ok"}); v != 1 {
		t.Errorf("info_refs retry ok = %d, want 1", v)
	}
	if v := sumInt64Value(t, retries, map[string]string{"result": "error"}); v != 1 {
		t.Errorf("info_refs retry error = %d, want 1", v)
	}
}

func TestHTTPHandler_PackCacheBypassMetric(t *testing.T) {
	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()

	env.handler.SetPackCache(NewPackCacheCoordinator(PackCacheCoordinatorConfig{
		Backend:         NoopPackResponseCache{},
		MaxCaptureBytes: 1024,
		Metrics:         env.handler.metrics,
	}))

	// Incremental fetch (contains `have`) should bypass pack cache and increment bypass metric.
	body := buildTestV2Request("fetch", []string{"agent=git/2.47.0"}, []string{
		"want " + strings.Repeat("a", 40),
		"have " + strings.Repeat("b", 40),
		"done",
	})
	req := httptest.NewRequest(http.MethodPost, "/acme/core.git/git-upload-pack", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer tok-acme")
	req.Header.Set("Git-Protocol", "version=2")
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body = %q", rec.Code, rec.Body.String())
	}

	gotMetrics := collectMetricMap(t, env.tel)
	lookups, ok := gotMetrics["stravin_edge_pack_cache_lookups_total"]
	if !ok {
		t.Fatal("stravin_edge_pack_cache_lookups_total not recorded")
	}
	if v := sumInt64Value(t, lookups, map[string]string{"tier": "singleflight", "result": "bypass"}); v != 1 {
		t.Errorf("singleflight bypass = %d, want 1", v)
	}
}
