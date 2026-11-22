package ctrl

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

func TestHandler_AdminEndpoints(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "api-repo")
	packA := testPackSHA("a")
	oid1 := testOID("1")

	leaseStore := newMemLeaseStore(clk)
	leaseStore.metas[rk.String()] = ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	}

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(leaseStore.metas[rk.String()])
	ddbStore.manifests[rk.String()] = []ddb.PackManifest{
		{Repo: rk, ULID: "01H0001", PackID: packA, SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-1 * time.Minute)},
	}
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oid1, PackID: packA, Epoch: 1, OriginRegion: "us-east-1",
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packA)

	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases: leaseStore, Meta: leaseStore, Region: "us-east-1", HolderID: "ctrl-api", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	if _, err := mgr.RenewOnce(context.Background()); err != nil {
		t.Fatalf("RenewOnce: %v", err)
	}

	scrubber, err := NewScrubber(ScrubConfig{
		DDB: ddbStore, S3: s3Store, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewScrubber: %v", err)
	}

	handler, err := NewHandler(HandlerConfig{
		Leases:   mgr,
		Scrubber: scrubber,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	// 1. GET /healthz
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200", rec.Code)
	}

	// 2. GET /v1/lease and /v1/lease/status
	for _, path := range []string{"/v1/lease", "/v1/lease/status"} {
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = %d, want 200", path, rec.Code)
		}
		var st LeaseStatus
		if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
			t.Fatalf("unmarshal %s: %v", path, err)
		}
		if st.Epoch != 1 || !st.AllowsWrites || st.Region != "us-east-1" {
			t.Fatalf("unexpected lease status from %s: %+v", path, st)
		}
	}

	// 3. POST /v1/lease/fence
	rec = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"mode":"DRAINING","advance_epoch":true}`)
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/lease/fence", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/lease/fence = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var fenceRes FenceResult
	if err := json.Unmarshal(rec.Body.Bytes(), &fenceRes); err != nil {
		t.Fatalf("unmarshal fenceRes: %v", err)
	}
	if fenceRes.CurrentEpoch != 2 || fenceRes.CurrentMode != lease.ModeDraining {
		t.Fatalf("unexpected fenceRes: %+v", fenceRes)
	}

	// 4. POST /v1/epoch/advance with repo META
	rec = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"api-repo","mode":"ACTIVE"}`)
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/epoch/advance", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/epoch/advance = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var advRes FenceResult
	if err := json.Unmarshal(rec.Body.Bytes(), &advRes); err != nil {
		t.Fatalf("unmarshal advRes: %v", err)
	}
	if advRes.CurrentEpoch != 3 || advRes.RepoMeta == nil || advRes.RepoMeta.FailoverEpoch != 3 {
		t.Fatalf("unexpected advRes: %+v", advRes)
	}

	// 5. POST /v1/scrub
	rec = httptest.NewRecorder()
	body = bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"api-repo","lookback_seconds":1200}`)
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/scrub", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/scrub = %d (%s), want 200", rec.Code, rec.Body.String())
	}
	var scrubRep ScrubReport
	if err := json.Unmarshal(rec.Body.Bytes(), &scrubRep); err != nil {
		t.Fatalf("unmarshal scrubRep: %v", err)
	}
	if scrubRep.Status != "clean" || scrubRep.CheckedPacks != 1 {
		t.Fatalf("unexpected scrubRep: %+v", scrubRep)
	}

	// 6. Error mapping checks (405, 400, 404).
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/scrub", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/scrub = %d, want 405", rec.Code)
	}

	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/scrub", bytes.NewBufferString(`{"tenant_id":"acme","repo_id":"missing"}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("POST /v1/scrub (missing repo) = %d, want 404", rec.Code)
	}
}
