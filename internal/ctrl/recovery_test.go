package ctrl

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type errRecoveryS3 struct {
	err error
}

func (e errRecoveryS3) HeadObject(_ context.Context, _ string) (s3.ObjectMeta, error) {
	return s3.ObjectMeta{}, e.err
}

type memRecoveryAuditStore struct {
	mu      sync.Mutex
	records []RecoveryAuditRecord
	err     error
}

func (a *memRecoveryAuditStore) RecordRecoveryAudit(_ context.Context, record RecoveryAuditRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return a.err
	}
	a.records = append(a.records, record)
	return nil
}

func TestFormatRecoveryRef(t *testing.T) {
	oid := "abcdef0123456789abcdef0123456789abcdef01"
	tests := []struct {
		name      string
		sourceRef string
		oid       string
		want      string
		wantErr   bool
	}{
		{
			name:      "standard branch",
			sourceRef: "refs/heads/main",
			oid:       oid,
			want:      "refs/stravin-recovery/heads-main-abcdef012345",
		},
		{
			name:      "nested feature branch",
			sourceRef: "refs/heads/feat/issue-58/reconciler",
			oid:       oid,
			want:      "refs/stravin-recovery/heads-feat-issue-58-reconciler-abcdef012345",
		},
		{
			name:      "sanitizes unsafe chars and collapses dashes",
			sourceRef: "refs/heads/foo..bar@{1} baz",
			oid:       oid,
			want:      "refs/stravin-recovery/heads-foo-bar-1-baz-abcdef012345",
		},
		{
			name:      "upper case oid normalized to lowercase",
			sourceRef: "refs/tags/v1.0.0",
			oid:       "ABCDEF0123456789ABCDEF0123456789ABCDEF01",
			want:      "refs/stravin-recovery/tags-v1.0.0-abcdef012345",
		},
		{
			name:      "rejects invalid oid",
			sourceRef: "refs/heads/main",
			oid:       "not-a-sha",
			wantErr:   true,
		},
		{
			name:      "rejects empty ref",
			sourceRef: "",
			oid:       oid,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := FormatRecoveryRef(tc.sourceRef, tc.oid)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("FormatRecoveryRef(%q, %q) = %q, want error", tc.sourceRef, tc.oid, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("FormatRecoveryRef(%q, %q) unexpected error: %v", tc.sourceRef, tc.oid, err)
			}
			if got != tc.want {
				t.Fatalf("FormatRecoveryRef(%q, %q) = %q, want %q", tc.sourceRef, tc.oid, got, tc.want)
			}
		})
	}
}

func TestRecoveryReconciler_DivergedRPOCommitPublishedAndActiveBranchUntouched(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "core")
	oidBase := testOID("1")
	oidRPO := testOID("a")
	packBase := testPackSHA("1")
	packRPO := testPackSHA("a")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-west-2", FailoverEpoch: 2, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeDraining,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oidBase, PackID: packBase, Epoch: 2, OriginRegion: "us-west-2",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oidBase, PrevOID: ddb.ZeroOID, PackID: packBase, Epoch: 1, OriginRegion: "us-east-1", UpdatedAt: clk.Now().Add(-10 * time.Minute)},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oidRPO, PrevOID: oidBase, PackID: packRPO, Epoch: 1, OriginRegion: "us-east-1", UpdatedAt: clk.Now().Add(-5 * time.Minute)},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0003", OID: oidBase, PrevOID: oidRPO, PackID: "", Epoch: 2, OriginRegion: "us-west-2", UpdatedAt: clk.Now().Add(-2 * time.Minute)},
	}
	ddbStore.manifests[rk.String()] = []ddb.PackManifest{
		{Repo: rk, ULID: "01H0001", PackID: packBase, SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-10 * time.Minute)},
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packBase)
	s3Store.putPack(rk, packRPO)

	auditStore := &memRecoveryAuditStore{}
	metrics, err := NewMetrics(telemetry.NewNoopProvider())
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB:     ddbStore,
		S3:      s3Store,
		Audit:   auditStore,
		Region:  "us-east-1",
		Clock:   clk,
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	report, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk})
	if err != nil {
		t.Fatalf("ReconcileRepo: %v", err)
	}

	wantRef := "refs/stravin-recovery/heads-main-" + oidRPO[:12]
	if report.Status != "published" {
		t.Fatalf("report.Status = %q, want %q", report.Status, "published")
	}
	if len(report.Published) != 1 {
		t.Fatalf("len(report.Published) = %d, want 1 (%+v)", len(report.Published), report.Published)
	}
	pub := report.Published[0]
	if pub.RecoveryRef != wantRef || pub.CommitOID != oidRPO || pub.PackID != packRPO || pub.SourceRef != "refs/heads/main" {
		t.Fatalf("unexpected published entry: %+v", pub)
	}

	// Active branch must remain completely untouched.
	mainRef := ddbStore.refs[rk.String()]["refs/heads/main"]
	if mainRef.OID != oidBase {
		t.Fatalf("active branch refs/heads/main mutated: got %s, want %s", mainRef.OID, oidBase)
	}

	// Recovery ref must exist with the RPO commit OID and pack ID.
	recRef, ok := ddbStore.refs[rk.String()][wantRef]
	if !ok {
		t.Fatalf("recovery ref %s not written to DDB", wantRef)
	}
	if recRef.OID != oidRPO || recRef.PackID != packRPO {
		t.Fatalf("unexpected recovery RefState: %+v", recRef)
	}

	// Pack manifest must now contain an ACTIVE entry for packRPO to protect it from delayed GC.
	foundActiveManifest := false
	for _, m := range ddbStore.manifests[rk.String()] {
		if m.PackID == packRPO && m.Status == ddb.PackStatusActive {
			foundActiveManifest = true
			break
		}
	}
	if !foundActiveManifest {
		t.Fatalf("expected ACTIVE pack manifest entry for %s", packRPO)
	}

	// Audit record must be persisted.
	if len(auditStore.records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(auditStore.records))
	}
	if auditStore.records[0].RecoveryRef != wantRef || auditStore.records[0].CommitOID != oidRPO || auditStore.records[0].PackID != packRPO {
		t.Fatalf("unexpected audit record: %+v", auditStore.records[0])
	}
}

func TestRecoveryReconciler_IdempotentOnRepeatedRuns(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 5, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "idempotent")
	oidBase := testOID("1")
	oidRPO := testOID("b")
	packBase := testPackSHA("1")
	packRPO := testPackSHA("b")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-west-2", FailoverEpoch: 2, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeReadOnly,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oidBase, PackID: packBase, Epoch: 2, OriginRegion: "us-west-2",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oidBase, PrevOID: ddb.ZeroOID, PackID: packBase, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oidRPO, PrevOID: oidBase, PackID: packRPO, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0003", OID: oidBase, PrevOID: oidRPO, PackID: "", Epoch: 2, OriginRegion: "us-west-2"},
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packBase)
	s3Store.putPack(rk, packRPO)
	auditStore := &memRecoveryAuditStore{}

	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB: ddbStore, S3: s3Store, Audit: auditStore, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	first, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk})
	if err != nil {
		t.Fatalf("first ReconcileRepo: %v", err)
	}
	if len(first.Published) != 1 {
		t.Fatalf("first Published = %d, want 1", len(first.Published))
	}

	second, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk})
	if err != nil {
		t.Fatalf("second ReconcileRepo: %v", err)
	}
	if second.Status != "clean" {
		t.Fatalf("second Status = %q, want %q", second.Status, "clean")
	}
	if len(second.Published) != 0 {
		t.Fatalf("second Published = %d, want 0", len(second.Published))
	}
	if len(second.AlreadyExists) != 1 {
		t.Fatalf("second AlreadyExists = %d, want 1", len(second.AlreadyExists))
	}
	if len(auditStore.records) != 1 {
		t.Fatalf("audit records after second run = %d, want 1", len(auditStore.records))
	}
}

func TestRecoveryReconciler_SkipsMissingS3PackCleanly(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 10, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "missing-pack")
	oidBase := testOID("1")
	oidMissing := testOID("c")
	packBase := testPackSHA("1")
	packMissing := testPackSHA("c")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-west-2", FailoverEpoch: 2, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeDraining,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oidBase, PackID: packBase, Epoch: 2, OriginRegion: "us-west-2",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oidBase, PrevOID: ddb.ZeroOID, PackID: packBase, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oidMissing, PrevOID: oidBase, PackID: packMissing, Epoch: 1, OriginRegion: "us-east-1"},
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packBase)
	// packMissing is deliberately absent from S3.

	auditStore := &memRecoveryAuditStore{}
	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB: ddbStore, S3: s3Store, Audit: auditStore, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	report, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk})
	if err != nil {
		t.Fatalf("ReconcileRepo: %v", err)
	}
	if report.Status != "clean" {
		t.Fatalf("Status = %q, want %q", report.Status, "clean")
	}
	if len(report.Published) != 0 {
		t.Fatalf("Published = %d, want 0", len(report.Published))
	}
	if len(report.SkippedMissingPacks) != 1 || report.SkippedMissingPacks[0] != packMissing {
		t.Fatalf("SkippedMissingPacks = %v, want [%s]", report.SkippedMissingPacks, packMissing)
	}
	if len(ddbStore.refs[rk.String()]) != 1 {
		t.Fatalf("expected no recovery ref created, got %d refs", len(ddbStore.refs[rk.String()]))
	}
	if len(auditStore.records) != 0 {
		t.Fatalf("expected 0 audit records, got %d", len(auditStore.records))
	}
}

func TestRecoveryReconciler_IgnoresNormalLinearHistoryAndRecoveryRefs(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 15, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "linear")
	oid1 := testOID("1")
	oid2 := testOID("2")
	oid3 := testOID("3")
	pack1 := testPackSHA("1")
	pack2 := testPackSHA("2")
	pack3 := testPackSHA("3")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oid3, PackID: pack3, Epoch: 1, OriginRegion: "us-east-1",
	}
	// Existing recovery ref should never be treated as a source ref to reconcile again.
	ddbStore.refs[rk.String()]["refs/stravin-recovery/heads-old-111111111111"] = ddb.RefState{
		Repo: rk, RefName: "refs/stravin-recovery/heads-old-111111111111", OID: oid1, PackID: pack1, Epoch: 1, OriginRegion: "us-east-1",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oid1, PrevOID: ddb.ZeroOID, PackID: pack1, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oid2, PrevOID: oid1, PackID: pack2, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0003", OID: oid3, PrevOID: oid2, PackID: pack3, Epoch: 1, OriginRegion: "us-east-1"},
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, pack1)
	s3Store.putPack(rk, pack2)
	s3Store.putPack(rk, pack3)

	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB: ddbStore, S3: s3Store, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	report, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk})
	if err != nil {
		t.Fatalf("ReconcileRepo: %v", err)
	}
	if report.Status != "clean" || len(report.Published) != 0 {
		t.Fatalf("expected clean report with 0 published, got %+v", report)
	}
}

func TestRecoveryReconciler_DryRunDoesNotMutateOrAudit(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 20, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "dryrun")
	oidBase := testOID("1")
	oidRPO := testOID("d")
	packBase := testPackSHA("1")
	packRPO := testPackSHA("d")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-west-2", FailoverEpoch: 2, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeDraining,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oidBase, PackID: packBase, Epoch: 2, OriginRegion: "us-west-2",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oidBase, PrevOID: ddb.ZeroOID, PackID: packBase, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oidRPO, PrevOID: oidBase, PackID: packRPO, Epoch: 1, OriginRegion: "us-east-1"},
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packBase)
	s3Store.putPack(rk, packRPO)
	auditStore := &memRecoveryAuditStore{}

	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB: ddbStore, S3: s3Store, Audit: auditStore, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	report, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk, DryRun: true})
	if err != nil {
		t.Fatalf("ReconcileRepo DryRun: %v", err)
	}
	if report.Status != "dry_run" || len(report.Published) != 1 {
		t.Fatalf("unexpected dry_run report: %+v", report)
	}
	if len(ddbStore.refs[rk.String()]) != 1 {
		t.Fatalf("DryRun mutated refs: got %d, want 1", len(ddbStore.refs[rk.String()]))
	}
	if len(auditStore.records) != 0 {
		t.Fatalf("DryRun wrote audit records: got %d, want 0", len(auditStore.records))
	}
}

func TestRecoveryReconciler_S3TransientErrorPropagated(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 15, 25, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "s3-err")
	oidBase := testOID("1")
	oidRPO := testOID("e")
	packBase := testPackSHA("1")
	packRPO := testPackSHA("e")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-west-2", FailoverEpoch: 2, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeDraining,
	})
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oidBase, PackID: packBase, Epoch: 2, OriginRegion: "us-west-2",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oidBase, PrevOID: ddb.ZeroOID, PackID: packBase, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oidRPO, PrevOID: oidBase, PackID: packRPO, Epoch: 1, OriginRegion: "us-east-1"},
	}

	reconciler, err := NewRecoveryReconciler(RecoveryConfig{
		DDB: ddbStore, S3: errRecoveryS3{err: errors.New("s3 throttled")}, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewRecoveryReconciler: %v", err)
	}

	if _, err := reconciler.ReconcileRepo(context.Background(), RecoveryRequest{Repo: rk}); err == nil {
		t.Fatal("expected error on transient S3 failure, got nil")
	}
}

func TestRecoveryAuditKey(t *testing.T) {
	rk, _ := tenant.NewRepoKey("acme", "core")
	key, err := RecoveryAuditKey(RecoveryAuditRecord{
		AuditID: "01J00000000000000000000001",
		Repo:    rk,
	})
	if err != nil {
		t.Fatalf("RecoveryAuditKey: %v", err)
	}
	if key.PK != "T#acme#REPO#core#AUDIT" || key.SK != "RECOVERY#01J00000000000000000000001" {
		t.Fatalf("unexpected RecoveryAuditKey: %+v", key)
	}
	if _, err := RecoveryAuditKey(RecoveryAuditRecord{Repo: rk}); err == nil {
		t.Fatal("expected error for empty audit_id")
	}
}
