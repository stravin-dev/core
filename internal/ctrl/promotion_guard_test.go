package ctrl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type memPromotionAuditStore struct {
	mu      sync.Mutex
	records []PromotionAuditRecord
}

func (s *memPromotionAuditStore) RecordPromotionAudit(_ context.Context, rec PromotionAuditRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, rec)
	return nil
}

func (s *memPromotionAuditStore) Records() []PromotionAuditRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]PromotionAuditRecord, len(s.records))
	copy(out, s.records)
	return out
}

func TestOperatorPromotionGuard(t *testing.T) {
	t.Run("rejects empty configured token fail-closed", func(t *testing.T) {
		g := NewOperatorPromotionGuard("")
		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			OperatorID:    "alice@stravin.dev",
			ApprovalToken: "anything",
		})
		if !errors.Is(err, ErrPromotionDenied) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPromotionDenied)", dec.Allowed, err)
		}
	})

	g := NewOperatorPromotionGuard("op-secret-approval-token")

	t.Run("rejects missing operator_id", func(t *testing.T) {
		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			ApprovalToken: "op-secret-approval-token",
		})
		if !errors.Is(err, ErrPromotionDenied) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPromotionDenied)", dec.Allowed, err)
		}
	})

	t.Run("rejects wrong approval token", func(t *testing.T) {
		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			OperatorID:    "alice@stravin.dev",
			ApprovalToken: "wrong-token",
		})
		if !errors.Is(err, ErrPromotionDenied) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPromotionDenied)", dec.Allowed, err)
		}
		if dec.OperatorID != "alice@stravin.dev" {
			t.Fatalf("OperatorID = %q, want %q", dec.OperatorID, "alice@stravin.dev")
		}
	})

	t.Run("allows valid operator and matching token", func(t *testing.T) {
		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			OperatorID:    "alice@stravin.dev",
			ApprovalToken: "op-secret-approval-token",
		})
		if err != nil || !dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (true, nil)", dec.Allowed, err)
		}
		if dec.GuardType != "operator" || dec.OperatorID != "alice@stravin.dev" {
			t.Fatalf("unexpected decision: %+v", dec)
		}
	})
}

func TestWitnessQuorumGuard(t *testing.T) {
	t.Run("default HTTP client disables proxy", func(t *testing.T) {
		g, err := NewWitnessQuorumGuard(WitnessQuorumConfig{
			Endpoints: []string{"http://127.0.0.1:19999/witness"},
			Quorum:    1,
			Timeout:   250 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWitnessQuorumGuard: %v", err)
		}
		tr, ok := g.httpClient.Transport.(*http.Transport)
		if !ok {
			t.Fatalf("expected *http.Transport, got %T", g.httpClient.Transport)
		}
		if tr.Proxy != nil {
			t.Fatalf("expected Transport.Proxy == nil for witness HTTP client")
		}
		if g.httpClient.Timeout != 250*time.Millisecond {
			t.Fatalf("Timeout = %v, want 250ms", g.httpClient.Timeout)
		}
	})

	t.Run("healthy-primary asymmetric split blocks promotion", func(t *testing.T) {
		w1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": false, "primary_healthy": true, "state": "healthy"})
		}))
		defer w1.Close()
		w2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": false, "primary_healthy": true, "state": "healthy"})
		}))
		defer w2.Close()

		g, err := NewWitnessQuorumGuard(WitnessQuorumConfig{
			Endpoints: []string{w1.URL, w2.URL},
			Quorum:    2,
			Timeout:   time.Second,
		})
		if err != nil {
			t.Fatalf("NewWitnessQuorumGuard: %v", err)
		}

		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:       "us-west-2",
			HomeRegion:   "us-east-1",
			CurrentEpoch: 1,
			TargetEpoch:  2,
		})
		if !errors.Is(err, ErrPrimaryHealthy) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPrimaryHealthy)", dec.Allowed, err)
		}
		if len(dec.WitnessVotes) != 2 || dec.QuorumReached != 0 || dec.QuorumNeeded != 2 {
			t.Fatalf("unexpected decision: %+v", dec)
		}
	})

	t.Run("valid 2-of-3 witness quorum allows promotion", func(t *testing.T) {
		fencedSrv := func() *httptest.Server {
			return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("region") != "us-east-1" {
					t.Errorf("expected region=us-east-1, got %q", r.URL.Query().Get("region"))
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": true, "primary_healthy": false, "state": "fenced"})
			}))
		}
		w1 := fencedSrv()
		defer w1.Close()
		w2 := fencedSrv()
		defer w2.Close()
		w3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer w3.Close()

		g, err := NewWitnessQuorumGuard(WitnessQuorumConfig{
			Endpoints: []string{w1.URL, w2.URL, w3.URL},
			Quorum:    2,
			Timeout:   time.Second,
		})
		if err != nil {
			t.Fatalf("NewWitnessQuorumGuard: %v", err)
		}

		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:       "us-west-2",
			HomeRegion:   "us-east-1",
			CurrentEpoch: 1,
			TargetEpoch:  2,
		})
		if err != nil || !dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (true, nil)", dec.Allowed, err)
		}
		if dec.QuorumReached != 2 || dec.QuorumNeeded != 2 || len(dec.WitnessVotes) != 3 {
			t.Fatalf("unexpected decision: %+v", dec)
		}
	})

	t.Run("unreachable witnesses below quorum fail closed", func(t *testing.T) {
		w1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": true, "state": "fenced"})
		}))
		defer w1.Close()
		w2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(250 * time.Millisecond)
			_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": true, "state": "fenced"})
		}))
		defer w2.Close()
		w3 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer w3.Close()

		g, err := NewWitnessQuorumGuard(WitnessQuorumConfig{
			Endpoints: []string{w1.URL, w2.URL, w3.URL},
			Quorum:    2,
			Timeout:   60 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("NewWitnessQuorumGuard: %v", err)
		}

		dec, err := g.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:     "us-west-2",
			HomeRegion: "us-east-1",
		})
		if !errors.Is(err, ErrPromotionDenied) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPromotionDenied)", dec.Allowed, err)
		}
		if dec.QuorumReached != 1 || dec.QuorumNeeded != 2 {
			t.Fatalf("unexpected decision: %+v", dec)
		}
	})
}

func TestCompositePromotionGuard(t *testing.T) {
	fencedWitness := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": true, "state": "fenced"})
	}))
	defer fencedWitness.Close()

	healthyWitness := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": false, "primary_healthy": true, "state": "healthy"})
	}))
	defer healthyWitness.Close()

	opGuard := NewOperatorPromotionGuard("secret-approval")
	fencedGuard, _ := NewWitnessQuorumGuard(WitnessQuorumConfig{Endpoints: []string{fencedWitness.URL}, Quorum: 1})
	healthyGuard, _ := NewWitnessQuorumGuard(WitnessQuorumConfig{Endpoints: []string{healthyWitness.URL}, Quorum: 1})

	t.Run("require_all denies when operator token missing even if witness fenced", func(t *testing.T) {
		comp, err := NewCompositePromotionGuard(CompositePolicyRequireAll, fencedGuard, opGuard)
		if err != nil {
			t.Fatalf("NewCompositePromotionGuard: %v", err)
		}
		dec, err := comp.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:     "us-west-2",
			HomeRegion: "us-east-1",
		})
		if !errors.Is(err, ErrPromotionDenied) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPromotionDenied)", dec.Allowed, err)
		}
	})

	t.Run("require_all allows when both witness and operator succeed", func(t *testing.T) {
		comp, err := NewCompositePromotionGuard(CompositePolicyRequireAll, fencedGuard, opGuard)
		if err != nil {
			t.Fatalf("NewCompositePromotionGuard: %v", err)
		}
		dec, err := comp.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			HomeRegion:    "us-east-1",
			OperatorID:    "oncall@stravin.dev",
			ApprovalToken: "secret-approval",
		})
		if err != nil || !dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (true, nil)", dec.Allowed, err)
		}
		if dec.OperatorID != "oncall@stravin.dev" || len(dec.WitnessVotes) != 1 {
			t.Fatalf("unexpected composite decision: %+v", dec)
		}
	})

	t.Run("witness_or_operator allows valid operator override when witness says healthy", func(t *testing.T) {
		comp, err := NewCompositePromotionGuard(CompositePolicyWitnessOrOperator, healthyGuard, opGuard)
		if err != nil {
			t.Fatalf("NewCompositePromotionGuard: %v", err)
		}
		dec, err := comp.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:        "us-west-2",
			HomeRegion:    "us-east-1",
			OperatorID:    "incident-cmd@stravin.dev",
			ApprovalToken: "secret-approval",
		})
		if err != nil || !dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (true, nil)", dec.Allowed, err)
		}
		if dec.OperatorID != "incident-cmd@stravin.dev" {
			t.Fatalf("expected OperatorID preserved, got %+v", dec)
		}
	})

	t.Run("witness_or_operator returns ErrPrimaryHealthy when witness sees healthy primary and no operator approval", func(t *testing.T) {
		comp, err := NewCompositePromotionGuard(CompositePolicyWitnessOrOperator, healthyGuard, opGuard)
		if err != nil {
			t.Fatalf("NewCompositePromotionGuard: %v", err)
		}
		dec, err := comp.AuthorizePromotion(context.Background(), PromotionRequest{
			Region:     "us-west-2",
			HomeRegion: "us-east-1",
		})
		if !errors.Is(err, ErrPrimaryHealthy) || dec.Allowed {
			t.Fatalf("got (allowed=%v, err=%v), want (false, ErrPrimaryHealthy)", dec.Allowed, err)
		}
	})
}

func TestHandler_PromotionGuardIntegration(t *testing.T) {
	newGuardedSetup := func(t *testing.T, guard PromotionGuard) (*Handler, *LeaseManager, *memPromotionAuditStore, *sdkmetric.ManualReader) {
		t.Helper()
		clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
		rk, _ := tenant.NewRepoKey("acme", "guarded-repo")

		leaseStore := newMemLeaseStore(clk)
		leaseStore.metas[rk.String()] = ddb.RepoMeta{
			Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
		}
		auditStore := &memPromotionAuditStore{}

		tel, err := telemetry.NewInMemoryProvider("stravin-ctrl-guard-test")
		if err != nil {
			t.Fatalf("NewInMemoryProvider: %v", err)
		}
		metrics, err := NewMetrics(tel)
		if err != nil {
			t.Fatalf("NewMetrics: %v", err)
		}

		mgr, err := NewLeaseManager(LeaseManagerConfig{
			Leases:         leaseStore,
			Meta:           leaseStore,
			Region:         "us-west-2",
			HolderID:       "ctrl-west",
			Clock:          clk,
			Metrics:        metrics,
			PromotionGuard: guard,
			PromotionAudit: auditStore,
		})
		if err != nil {
			t.Fatalf("NewLeaseManager: %v", err)
		}
		if _, err := mgr.RenewOnce(context.Background()); err != nil {
			t.Fatalf("RenewOnce: %v", err)
		}

		scrubber, err := NewScrubber(ScrubConfig{
			DDB: newMemScrubDDB(), S3: newMemScrubS3(), Region: "us-west-2", Clock: clk,
		})
		if err != nil {
			t.Fatalf("NewScrubber: %v", err)
		}

		handler, err := NewHandler(HandlerConfig{
			Leases:     mgr,
			Scrubber:   scrubber,
			AdminToken: testAdminToken,
		})
		if err != nil {
			t.Fatalf("NewHandler: %v", err)
		}
		return handler, mgr, auditStore, tel.MetricReader
	}

	readPromotionGuardCounter := func(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
		t.Helper()
		var rm metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &rm); err != nil {
			t.Fatalf("Collect metrics: %v", err)
		}
		counts := make(map[string]int64)
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				if m.Name != "stravin_ctrl_promotion_guard_total" {
					continue
				}
				sum, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					continue
				}
				for _, dp := range sum.DataPoints {
					if val, ok := dp.Attributes.Value("outcome"); ok {
						counts[val.AsString()] += dp.Value
					}
				}
			}
		}
		return counts
	}

	t.Run("healthy-primary split returns 409, does not advance epoch, records audit and metric", func(t *testing.T) {
		w := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"primary_fenced": false, "primary_healthy": true, "state": "healthy"})
		}))
		defer w.Close()

		guard, err := NewWitnessQuorumGuard(WitnessQuorumConfig{Endpoints: []string{w.URL}, Quorum: 1})
		if err != nil {
			t.Fatalf("NewWitnessQuorumGuard: %v", err)
		}
		handler, mgr, audits, reader := newGuardedSetup(t, guard)

		rec := httptest.NewRecorder()
		body := bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"guarded-repo","mode":"ACTIVE"}`)
		handler.ServeHTTP(rec, newAuthedRequest(http.MethodPost, "/v1/epoch/advance", body))

		if rec.Code != http.StatusConflict {
			t.Fatalf("POST /v1/epoch/advance = %d (%s), want 409", rec.Code, rec.Body.String())
		}
		if mgr.Status().Epoch != 1 {
			t.Fatalf("epoch advanced despite denied guard: got %d, want 1", mgr.Status().Epoch)
		}
		recs := audits.Records()
		if len(recs) != 1 || recs[0].Decision.Allowed || recs[0].HomeRegion != "us-east-1" {
			t.Fatalf("unexpected audit records: %+v", recs)
		}
		counts := readPromotionGuardCounter(t, reader)
		if counts["denied"] != 1 {
			t.Fatalf("stravin_ctrl_promotion_guard_total{outcome=denied} = %d, want 1", counts["denied"])
		}
	})

	t.Run("operator gate denies wrong token with 403 and allows valid approval with audit", func(t *testing.T) {
		guard := NewOperatorPromotionGuard("valid-op-token")
		handler, mgr, audits, reader := newGuardedSetup(t, guard)

		// Wrong token -> 403
		rec := httptest.NewRecorder()
		body := bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"guarded-repo","operator_id":"alice","approval_token":"wrong"}`)
		handler.ServeHTTP(rec, newAuthedRequest(http.MethodPost, "/v1/epoch/advance", body))
		if rec.Code != http.StatusForbidden {
			t.Fatalf("wrong operator token = %d (%s), want 403", rec.Code, rec.Body.String())
		}
		if mgr.Status().Epoch != 1 {
			t.Fatalf("epoch should remain 1, got %d", mgr.Status().Epoch)
		}

		// Valid token -> 200
		rec = httptest.NewRecorder()
		body = bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"guarded-repo","operator_id":"alice","approval_token":"valid-op-token","reason":"region us-east-1 outage"}`)
		handler.ServeHTTP(rec, newAuthedRequest(http.MethodPost, "/v1/epoch/advance", body))
		if rec.Code != http.StatusOK {
			t.Fatalf("valid operator token = %d (%s), want 200", rec.Code, rec.Body.String())
		}
		if mgr.Status().Epoch != 2 {
			t.Fatalf("epoch should advance to 2, got %d", mgr.Status().Epoch)
		}

		recs := audits.Records()
		if len(recs) != 2 {
			t.Fatalf("expected 2 audit records, got %d", len(recs))
		}
		if !recs[1].Decision.Allowed || recs[1].Decision.OperatorID != "alice" {
			t.Fatalf("unexpected allowed audit record: %+v", recs[1])
		}
		rawAudit, _ := json.Marshal(recs[1])
		if strings.Contains(string(rawAudit), "valid-op-token") {
			t.Fatalf("approval token leaked into audit JSON: %s", string(rawAudit))
		}

		counts := readPromotionGuardCounter(t, reader)
		if counts["denied"] != 1 || counts["allowed"] != 1 {
			t.Fatalf("unexpected metric counts: %+v", counts)
		}
	})

	t.Run("disabled promotion guard keeps single-region epoch advance unchanged", func(t *testing.T) {
		handler, mgr, audits, _ := newGuardedSetup(t, nil)

		rec := httptest.NewRecorder()
		body := bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"guarded-repo","mode":"ACTIVE"}`)
		handler.ServeHTTP(rec, newAuthedRequest(http.MethodPost, "/v1/epoch/advance", body))
		if rec.Code != http.StatusOK {
			t.Fatalf("disabled guard POST /v1/epoch/advance = %d (%s), want 200", rec.Code, rec.Body.String())
		}
		if mgr.Status().Epoch != 2 {
			t.Fatalf("epoch = %d, want 2", mgr.Status().Epoch)
		}
		if len(audits.Records()) != 0 {
			t.Fatalf("expected 0 promotion guard audit records when guard disabled, got %d", len(audits.Records()))
		}
	})
}

type fakeDDBPutClient struct {
	ddb.API
	lastPut *dynamodb.PutItemInput
}

func (f *fakeDDBPutClient) PutItem(_ context.Context, params *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.lastPut = params
	return &dynamodb.PutItemOutput{}, nil
}

func TestDDBAuditStore_RecordPromotionAudit(t *testing.T) {
	fake := &fakeDDBPutClient{}
	store, err := NewDDBAuditStore(fake, "stravin-core")
	if err != nil {
		t.Fatalf("NewDDBAuditStore: %v", err)
	}
	rk, _ := tenant.NewRepoKey("acme", "repo1")
	rec := PromotionAuditRecord{
		AuditID:      "01JTESTAUDIT00000000000001",
		Region:       "us-west-2",
		HomeRegion:   "us-east-1",
		Repo:         &rk,
		CurrentEpoch: 1,
		TargetEpoch:  2,
		TargetMode:   lease.ModeActive,
		Decision: PromotionDecision{
			Allowed:    true,
			GuardType:  "operator",
			OperatorID: "alice",
			Reason:     "operator approval verified",
		},
		EvaluatedAt: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC),
	}
	if err := store.RecordPromotionAudit(context.Background(), rec); err != nil {
		t.Fatalf("RecordPromotionAudit: %v", err)
	}
	if fake.lastPut == nil {
		t.Fatal("expected PutItem call")
	}
	pkAttr := fake.lastPut.Item[ddb.AttrPK].(*types.AttributeValueMemberS).Value
	skAttr := fake.lastPut.Item[ddb.AttrSK].(*types.AttributeValueMemberS).Value
	if pkAttr != "T#acme#REPO#repo1#AUDIT" || skAttr != "PROMOTION#01JTESTAUDIT00000000000001" {
		t.Fatalf("unexpected keys PK=%q SK=%q", pkAttr, skAttr)
	}
}
