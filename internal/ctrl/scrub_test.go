package ctrl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type memScrubDDB struct {
	mu        sync.Mutex
	metas     map[string]ddb.RepoMeta
	manifests map[string][]ddb.PackManifest
	refs      map[string]map[string]ddb.RefState
	history   map[string]map[string][]ddb.RefHistory
	ulidSeq   int
}

func newMemScrubDDB() *memScrubDDB {
	return &memScrubDDB{
		metas:     make(map[string]ddb.RepoMeta),
		manifests: make(map[string][]ddb.PackManifest),
		refs:      make(map[string]map[string]ddb.RefState),
		history:   make(map[string]map[string][]ddb.RefHistory),
	}
}

func (d *memScrubDDB) seedRepo(meta ddb.RepoMeta) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := meta.Repo.String()
	d.metas[key] = meta
	if d.refs[key] == nil {
		d.refs[key] = make(map[string]ddb.RefState)
	}
	if d.history[key] == nil {
		d.history[key] = make(map[string][]ddb.RefHistory)
	}
}

func (d *memScrubDDB) GetMeta(_ context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.metas[repo.String()]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return m, nil
}

func (d *memScrubDDB) ListManifestsSince(_ context.Context, repo tenant.RepoKey, _ int, _ string, activeOnly bool) ([]ddb.PackManifest, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []ddb.PackManifest
	for _, m := range d.manifests[repo.String()] {
		if activeOnly && m.Status != ddb.PackStatusActive {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (d *memScrubDDB) ListRefs(_ context.Context, repo tenant.RepoKey, _ int) ([]ddb.RefState, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	refMap := d.refs[repo.String()]
	out := make([]ddb.RefState, 0, len(refMap))
	for _, r := range refMap {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].RefName < out[j].RefName
	})
	return out, nil
}

func (d *memScrubDDB) ListRefHistory(_ context.Context, repo tenant.RepoKey, refName string, _ int, _ string, _ int32) ([]ddb.RefHistory, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h := d.history[repo.String()][refName]
	out := make([]ddb.RefHistory, len(h))
	copy(out, h)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ULID < out[j].ULID
	})
	return out, nil
}

func (d *memScrubDDB) CommitPush(_ context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	key := in.Repo.String()
	if d.refs[key] == nil {
		d.refs[key] = make(map[string]ddb.RefState)
	}
	if d.history[key] == nil {
		d.history[key] = make(map[string][]ddb.RefHistory)
	}

	// Validate CAS before applying any updates.
	for _, u := range in.Updates {
		cur, exists := d.refs[key][u.RefName]
		switch {
		case u.IsCreate():
			if exists {
				return ddb.CommitPushResult{}, &ddb.RefConflictError{ConflictingRefs: []string{u.RefName}}
			}
		default:
			if !exists || cur.OID != u.OldOID {
				return ddb.CommitPushResult{}, &ddb.RefConflictError{ConflictingRefs: []string{u.RefName}}
			}
		}
	}

	d.ulidSeq++
	ulid := fmt.Sprintf("01H%023d", d.ulidSeq)
	packID := ""
	if in.Pack != nil {
		packID = in.Pack.PackID
		d.manifests[key] = append(d.manifests[key], ddb.PackManifest{
			Repo:         in.Repo,
			ULID:         ulid,
			PackID:       in.Pack.PackID,
			SizeBytes:    in.Pack.SizeBytes,
			OriginRegion: in.OriginRegion,
			Status:       ddb.PackStatusActive,
			CreatedAt:    time.Now().UTC(),
		})
	}

	for _, u := range in.Updates {
		if u.IsDelete() {
			delete(d.refs[key], u.RefName)
		} else {
			d.refs[key][u.RefName] = ddb.RefState{
				Repo:         in.Repo,
				RefName:      u.RefName,
				OID:          u.NewOID,
				PrevOID:      u.OldOID,
				PackID:       packID,
				Epoch:        in.Epoch,
				OriginRegion: in.OriginRegion,
			}
		}
		d.history[key][u.RefName] = append(d.history[key][u.RefName], ddb.RefHistory{
			Repo:         in.Repo,
			RefName:      u.RefName,
			ULID:         ulid,
			OID:          u.NewOID,
			PrevOID:      u.OldOID,
			PackID:       packID,
			Epoch:        in.Epoch,
			OriginRegion: in.OriginRegion,
		})
	}

	return ddb.CommitPushResult{
		ULID:        ulid,
		CommittedAt: time.Now().UTC(),
		RefCount:    len(in.Updates),
	}, nil
}

type memScrubS3 struct {
	mu      sync.Mutex
	objects map[string]s3.ObjectMeta
	failErr error
}

func newMemScrubS3() *memScrubS3 {
	return &memScrubS3{
		objects: make(map[string]s3.ObjectMeta),
	}
}

func (m *memScrubS3) putPack(repo tenant.RepoKey, packSHA string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pk, _ := s3.PackKey(repo, packSHA)
	ik, _ := s3.IndexKey(repo, packSHA)
	m.objects[pk] = s3.ObjectMeta{Key: pk, SizeBytes: 128}
	m.objects[ik] = s3.ObjectMeta{Key: ik, SizeBytes: 64}
}

func (m *memScrubS3) deleteKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
}

func (m *memScrubS3) HeadObject(_ context.Context, key string) (s3.ObjectMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failErr != nil {
		return s3.ObjectMeta{}, m.failErr
	}
	meta, ok := m.objects[key]
	if !ok {
		return s3.ObjectMeta{}, fmt.Errorf("%w: %s", s3.ErrNotFound, key)
	}
	return meta, nil
}

type memAuditStore struct {
	mu      sync.Mutex
	reports []ScrubReport
}

func (a *memAuditStore) RecordScrubAudit(_ context.Context, report ScrubReport) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reports = append(a.reports, report)
	return nil
}

func testPackSHA(ch string) string {
	return strings.Repeat(ch, 64)
}

func testOID(ch string) string {
	return strings.Repeat(ch, 40)
}

func TestScrubber_CleanScrubAllPacksIntact(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "clean-repo")
	packA := testPackSHA("a")
	oid1 := testOID("1")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  2,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
	})
	ddbStore.manifests[rk.String()] = []ddb.PackManifest{
		{Repo: rk, ULID: "01H0001", PackID: packA, SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-5 * time.Minute)},
	}
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oid1, PackID: packA, Epoch: 2, OriginRegion: "us-east-1",
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packA)

	audit := &memAuditStore{}
	tel, _ := telemetry.NewInMemoryProvider("stravin-ctrl-test")
	metrics, _ := NewMetrics(tel)

	scrubber, err := NewScrubber(ScrubConfig{
		DDB:     ddbStore,
		S3:      s3Store,
		Audit:   audit,
		Region:  "us-east-1",
		Clock:   clk,
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewScrubber: %v", err)
	}

	report, err := scrubber.ScrubRepo(context.Background(), ScrubRequest{Repo: rk})
	if err != nil {
		t.Fatalf("ScrubRepo: %v", err)
	}
	if report.Status != "clean" {
		t.Fatalf("status = %q, want clean", report.Status)
	}
	if report.CheckedPacks != 1 || report.HeadChecks != 2 {
		t.Fatalf("expected 1 checked pack / 2 head checks, got %d / %d", report.CheckedPacks, report.HeadChecks)
	}
	if len(report.MissingPackIDs) != 0 || len(report.RewoundRefs) != 0 || len(report.TruncatedOIDs) != 0 {
		t.Fatalf("expected zero missing/rewound/truncated, got %+v", report)
	}
	if len(audit.reports) != 1 || audit.reports[0].AuditID == "" {
		t.Fatalf("expected 1 persisted audit report with audit_id, got %+v", audit.reports)
	}

	// Verify ctrl.scrub.region span was emitted.
	foundSpan := false
	for _, sp := range tel.SpanExporter.GetSpans() {
		if sp.Name == "ctrl.scrub.region" {
			foundSpan = true
			break
		}
	}
	if !foundSpan {
		t.Fatalf("expected ctrl.scrub.region span in exporter")
	}
}

func TestScrubber_FaultInjectedMissingPackRewindsAffectedRefsOnly(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "fault-repo")

	packA := testPackSHA("a") // intact (older than 20m window — must still be verified via Ref History!)
	packB := testPackSHA("b") // missing .pack
	packC := testPackSHA("c") // missing .idx

	oid1 := testOID("1")
	oid2 := testOID("2")
	oid3 := testOID("3")
	oidFeat := testOID("4")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  3,
		RefShards:      4,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
	})
	ddbStore.manifests[rk.String()] = []ddb.PackManifest{
		{Repo: rk, ULID: "01H0001", PackID: packA, SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-30 * time.Minute)},
		{Repo: rk, ULID: "01H0002", PackID: packB, SizeBytes: 120, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-10 * time.Minute)},
		{Repo: rk, ULID: "01H0003", PackID: packC, SizeBytes: 140, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-2 * time.Minute)},
	}

	// refs/heads/main: oid1 (packA intact) -> oid2 (packB missing) -> oid3 (packC missing)
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oid3, PrevOID: oid2, PackID: packC, Epoch: 3, OriginRegion: "us-east-1",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oid1, PrevOID: ddb.ZeroOID, PackID: packA, Epoch: 1, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0002", OID: oid2, PrevOID: oid1, PackID: packB, Epoch: 2, OriginRegion: "us-east-1"},
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0003", OID: oid3, PrevOID: oid2, PackID: packC, Epoch: 3, OriginRegion: "us-east-1"},
	}

	// refs/heads/feature: created in missing packB with no prior intact pack -> must be deleted
	ddbStore.refs[rk.String()]["refs/heads/feature"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/feature", OID: oidFeat, PrevOID: ddb.ZeroOID, PackID: packB, Epoch: 2, OriginRegion: "us-east-1",
	}
	ddbStore.history[rk.String()]["refs/heads/feature"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/feature", ULID: "01H0002", OID: oidFeat, PrevOID: ddb.ZeroOID, PackID: packB, Epoch: 2, OriginRegion: "us-east-1"},
	}

	// refs/heads/release: points to intact packA -> must remain untouched
	ddbStore.refs[rk.String()]["refs/heads/release"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/release", OID: oid1, PrevOID: ddb.ZeroOID, PackID: packA, Epoch: 1, OriginRegion: "us-east-1",
	}

	s3Store := newMemScrubS3()
	s3Store.putPack(rk, packA)
	s3Store.putPack(rk, packB)
	s3Store.putPack(rk, packC)

	// Fault injection: delete packB's .pack and packC's .idx
	packBKey, _ := s3.PackKey(rk, packB)
	packCIdx, _ := s3.IndexKey(rk, packC)
	s3Store.deleteKey(packBKey)
	s3Store.deleteKey(packCIdx)

	audit := &memAuditStore{}
	scrubber, err := NewScrubber(ScrubConfig{
		DDB:         ddbStore,
		S3:          s3Store,
		Audit:       audit,
		Region:      "us-east-1",
		Concurrency: 4,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("NewScrubber: %v", err)
	}

	report, err := scrubber.ScrubRepo(context.Background(), ScrubRequest{
		Repo:           rk,
		LookbackWindow: 20 * time.Minute,
	})
	if err != nil {
		t.Fatalf("ScrubRepo: %v", err)
	}

	if report.Status != "rewound" {
		t.Fatalf("status = %q, want rewound", report.Status)
	}
	// Only packB and packC were inside the 20m lookback window.
	if report.CheckedPacks != 2 {
		t.Fatalf("checked_packs = %d, want 2", report.CheckedPacks)
	}
	if len(report.MissingPackIDs) != 2 || report.MissingPackIDs[0] != packB || report.MissingPackIDs[1] != packC {
		t.Fatalf("missing_pack_ids = %v, want [%s, %s]", report.MissingPackIDs, packB, packC)
	}
	if len(report.RewoundRefs) != 2 {
		t.Fatalf("rewound_refs count = %d, want 2: %+v", len(report.RewoundRefs), report.RewoundRefs)
	}

	// Refs are processed in sorted order: refs/heads/feature, then refs/heads/main.
	featAct := report.RewoundRefs[0]
	if featAct.RefName != "refs/heads/feature" || featAct.Action != "delete" || featAct.FromOID != oidFeat || featAct.ToOID != ddb.ZeroOID {
		t.Fatalf("unexpected feature rewind action: %+v", featAct)
	}
	mainAct := report.RewoundRefs[1]
	if mainAct.RefName != "refs/heads/main" || mainAct.Action != "rewind" || mainAct.FromOID != oid3 || mainAct.ToOID != oid1 || mainAct.TargetPackID != packA {
		t.Fatalf("unexpected main rewind action: %+v", mainAct)
	}

	// Verify actual DDB state after rewind.
	refsAfter, _ := ddbStore.ListRefs(context.Background(), rk, 4)
	refMap := make(map[string]ddb.RefState, len(refsAfter))
	for _, r := range refsAfter {
		refMap[r.RefName] = r
	}
	if _, stillExists := refMap["refs/heads/feature"]; stillExists {
		t.Fatalf("expected refs/heads/feature to be deleted")
	}
	if refMap["refs/heads/main"].OID != oid1 {
		t.Fatalf("expected refs/heads/main rewound to %s, got %s", oid1, refMap["refs/heads/main"].OID)
	}
	if refMap["refs/heads/release"].OID != oid1 || refMap["refs/heads/release"].PackID != packA {
		t.Fatalf("expected refs/heads/release untouched, got %+v", refMap["refs/heads/release"])
	}

	// Verify truncated OIDs recorded in audit trail.
	wantTrunc := []string{oid2, oid3, oidFeat}
	sort.Strings(wantTrunc)
	if len(report.TruncatedOIDs) != len(wantTrunc) {
		t.Fatalf("truncated_oids = %v, want %v", report.TruncatedOIDs, wantTrunc)
	}
	for i := range wantTrunc {
		if report.TruncatedOIDs[i] != wantTrunc[i] {
			t.Fatalf("truncated_oids[%d] = %s, want %s", i, report.TruncatedOIDs[i], wantTrunc[i])
		}
	}
	if len(audit.reports) != 1 {
		t.Fatalf("expected 1 audit record, got %d", len(audit.reports))
	}
}

func TestScrubber_DryRunAndTransientS3Error(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	rk, _ := tenant.NewRepoKey("acme", "dryrun-repo")
	packA := testPackSHA("a")
	oid1 := testOID("1")

	ddbStore := newMemScrubDDB()
	ddbStore.seedRepo(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	ddbStore.manifests[rk.String()] = []ddb.PackManifest{
		{Repo: rk, ULID: "01H0001", PackID: packA, SizeBytes: 100, OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now().Add(-1 * time.Minute)},
	}
	ddbStore.refs[rk.String()]["refs/heads/main"] = ddb.RefState{
		Repo: rk, RefName: "refs/heads/main", OID: oid1, PackID: packA, Epoch: 1, OriginRegion: "us-east-1",
	}
	ddbStore.history[rk.String()]["refs/heads/main"] = []ddb.RefHistory{
		{Repo: rk, RefName: "refs/heads/main", ULID: "01H0001", OID: oid1, PackID: packA, Epoch: 1, OriginRegion: "us-east-1"},
	}

	s3Store := newMemScrubS3() // empty -> packA missing
	audit := &memAuditStore{}
	scrubber, err := NewScrubber(ScrubConfig{
		DDB: ddbStore, S3: s3Store, Audit: audit, Region: "us-east-1", Clock: clk,
	})
	if err != nil {
		t.Fatalf("NewScrubber: %v", err)
	}

	// DryRun must not delete the ref or persist an audit entry.
	rep, err := scrubber.ScrubRepo(context.Background(), ScrubRequest{Repo: rk, DryRun: true})
	if err != nil {
		t.Fatalf("ScrubRepo(DryRun): %v", err)
	}
	if rep.Status != "dry_run" || len(rep.RewoundRefs) != 1 || len(audit.reports) != 0 {
		t.Fatalf("unexpected dry_run report: %+v (audits=%d)", rep, len(audit.reports))
	}
	if _, ok := ddbStore.refs[rk.String()]["refs/heads/main"]; !ok {
		t.Fatalf("DryRun mutated ref state")
	}

	// Transient S3 error must fail the scrub without touching refs.
	s3Store.failErr = errors.New("s3 503 slow down")
	if _, err := scrubber.ScrubRepo(context.Background(), ScrubRequest{Repo: rk}); err == nil {
		t.Fatalf("expected transient S3 error to fail scrub")
	}
	if _, ok := ddbStore.refs[rk.String()]["refs/heads/main"]; !ok {
		t.Fatalf("transient S3 error must not mutate ref state")
	}
}
