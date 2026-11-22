package compaction

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type staticClock struct {
	mu  sync.Mutex
	now time.Time
}

func newStaticClock(t time.Time) *staticClock {
	return &staticClock{now: t.UTC()}
}

func (c *staticClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *staticClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeS3Object struct {
	data         []byte
	lastModified time.Time
}

type fakePackObjectStore struct {
	mu          sync.Mutex
	objects     map[string]fakeS3Object
	clock       *staticClock
	onPutPack   func(packSHA string)
	deletedKeys []string
}

func newFakePackObjectStore(clk *staticClock) *fakePackObjectStore {
	return &fakePackObjectStore{
		objects: make(map[string]fakeS3Object),
		clock:   clk,
	}
}

func (f *fakePackObjectStore) PutPack(_ context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	packKey, err := s3.PackKey(repo, packSHA256)
	if err != nil {
		return err
	}
	idxKey, err := s3.IndexKey(repo, packSHA256)
	if err != nil {
		return err
	}
	now := f.clock.Now()
	f.mu.Lock()
	f.objects[packKey] = fakeS3Object{data: append([]byte(nil), packData...), lastModified: now}
	f.objects[idxKey] = fakeS3Object{data: append([]byte(nil), idxData...), lastModified: now}
	cb := f.onPutPack
	f.mu.Unlock()
	if cb != nil {
		cb(packSHA256)
	}
	return nil
}

func (f *fakePackObjectStore) GetPack(_ context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	key, err := s3.PackKey(repo, packSHA256)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, s3.ErrNotFound
	}
	return append([]byte(nil), obj.data...), nil
}

func (f *fakePackObjectStore) GetIndex(_ context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error) {
	key, err := s3.IndexKey(repo, packSHA256)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[key]
	if !ok {
		return nil, s3.ErrNotFound
	}
	return append([]byte(nil), obj.data...), nil
}

func (f *fakePackObjectStore) PutAuxMIDX(_ context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error {
	key, err := s3.AuxMIDXKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeS3Object{data: append([]byte(nil), data...), lastModified: f.clock.Now()}
	return nil
}

func (f *fakePackObjectStore) PutAuxGraph(_ context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error {
	key, err := s3.AuxGraphKey(repo, hashPrefixOrSHA, manifestEpoch)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeS3Object{data: append([]byte(nil), data...), lastModified: f.clock.Now()}
	return nil
}

func (f *fakePackObjectStore) DeleteObject(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.objects[key]; !ok {
		return s3.ErrNotFound
	}
	delete(f.objects, key)
	f.deletedKeys = append(f.deletedKeys, key)
	return nil
}

func (f *fakePackObjectStore) ListRepoPacks(_ context.Context, repo tenant.RepoKey) ([]s3.PackObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []s3.PackObjectInfo
	for key, obj := range f.objects {
		parts := strings.Split(key, "/")
		if len(parts) != 5 {
			continue
		}
		if parts[1] != string(repo.TenantID) || parts[2] != string(repo.RepoID) || parts[3] != "packs" || !strings.HasSuffix(parts[4], ".pack") {
			continue
		}
		sha := strings.TrimSuffix(parts[4], ".pack")
		out = append(out, s3.PackObjectInfo{
			Key:          key,
			PackSHA256:   sha,
			SizeBytes:    int64(len(obj.data)),
			LastModified: obj.lastModified,
		})
	}
	return out, nil
}

func (f *fakePackObjectStore) exists(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[key]
	return ok
}

type fakeManifestStore struct {
	mu             sync.Mutex
	items          map[string]ddb.PackManifest
	gcAfter        map[string]time.Time
	events         []string
	failPublish    error
	failSupersede  error
	manifestShards int
}

func newFakeManifestStore(manifestShards int) *fakeManifestStore {
	return &fakeManifestStore{
		items:          make(map[string]ddb.PackManifest),
		gcAfter:        make(map[string]time.Time),
		manifestShards: manifestShards,
	}
}

func manifestItemKey(m ddb.PackManifest) string {
	return m.Repo.DynamoPrefix() + "#" + m.ULID + "#" + m.PackID
}

func (f *fakeManifestStore) put(m ddb.PackManifest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[manifestItemKey(m)] = m
}

func (f *fakeManifestStore) QueryManifestShard(_ context.Context, repo tenant.RepoKey, shard int, _ string, activeOnly bool) ([]ddb.PackManifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ddb.PackManifest
	for _, m := range f.items {
		if m.Repo != repo {
			continue
		}
		s, err := ddb.ManifestShard(m.PackID, f.manifestShards)
		if err != nil || s != shard {
			continue
		}
		if activeOnly && m.Status != ddb.PackStatusActive {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeManifestStore) ListManifestsSince(_ context.Context, repo tenant.RepoKey, _ int, _ string, activeOnly bool) ([]ddb.PackManifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []ddb.PackManifest
	for _, m := range f.items {
		if m.Repo != repo {
			continue
		}
		if activeOnly && m.Status != ddb.PackStatusActive {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}

func (f *fakeManifestStore) PublishCompactedPacks(_ context.Context, repo tenant.RepoKey, _ int, packs []ddb.PackManifest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "publish")
	if f.failPublish != nil {
		return f.failPublish
	}
	for _, p := range packs {
		p.Repo = repo
		p.Status = ddb.PackStatusActive
		f.items[manifestItemKey(p)] = p
	}
	return nil
}

func (f *fakeManifestStore) SupersedePacks(_ context.Context, repo tenant.RepoKey, _ int, packs []ddb.PackManifest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "supersede")
	if f.failSupersede != nil {
		return f.failSupersede
	}
	for _, p := range packs {
		p.Repo = repo
		k := manifestItemKey(p)
		cur, ok := f.items[k]
		if !ok || cur.Status != ddb.PackStatusActive {
			return ddb.ErrRefConflict
		}
		cur.Status = ddb.PackStatusCompacted
		f.items[k] = cur
	}
	return nil
}

func (f *fakeManifestStore) MarkGCEligible(_ context.Context, repo tenant.RepoKey, _ int, packs []ddb.PackManifest, _, gcAfter time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, "mark_gc")
	for _, p := range packs {
		p.Repo = repo
		f.gcAfter[manifestItemKey(p)] = gcAfter
	}
	return nil
}

func (f *fakeManifestStore) GetGCEligibility(_ context.Context, repo tenant.RepoKey, _ int, pack ddb.PackManifest) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pack.Repo = repo
	ts, ok := f.gcAfter[manifestItemKey(pack)]
	return ts, ok, nil
}

func (f *fakeManifestStore) DeleteManifest(_ context.Context, repo tenant.RepoKey, _ int, pack ddb.PackManifest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	pack.Repo = repo
	k := manifestItemKey(pack)
	delete(f.items, k)
	delete(f.gcAfter, k)
	return nil
}

func TestGCSweeper_ReapsExpiredCompactedAndOrphanedPacks(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-gc", "repo-gc")
	manifests := newFakeManifestStore(2)
	objects := newFakePackObjectStore(clk)

	tel, err := telemetry.NewInMemoryProvider("stravin-pack")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	activeSHA := strings.Repeat("a", 64)
	compactedSHA := strings.Repeat("b", 64)
	orphanSHA := strings.Repeat("c", 64)

	ctx := context.Background()
	for _, sha := range []string{activeSHA, compactedSHA, orphanSHA} {
		if err := objects.PutPack(ctx, rk, sha, []byte("PACK-"+sha[:8]), []byte("IDX-"+sha[:8])); err != nil {
			t.Fatalf("PutPack(%s): %v", sha[:8], err)
		}
	}

	manifests.put(ddb.PackManifest{
		Repo:         rk,
		ULID:         "01J00000000000000000000001",
		PackID:       activeSHA,
		SizeBytes:    100,
		OriginRegion: "us-east-1",
		Status:       ddb.PackStatusActive,
		CreatedAt:    clk.Now(),
	})
	compactedManifest := ddb.PackManifest{
		Repo:         rk,
		ULID:         "01J00000000000000000000002",
		PackID:       compactedSHA,
		SizeBytes:    200,
		OriginRegion: "us-east-1",
		Status:       ddb.PackStatusCompacted,
		CreatedAt:    clk.Now(),
	}
	manifests.put(compactedManifest)
	_ = manifests.MarkGCEligible(ctx, rk, 2, []ddb.PackManifest{compactedManifest}, clk.Now(), clk.Now().Add(10*time.Millisecond))

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:         manifests,
		ManifestGC:        manifests,
		Objects:           objects,
		GraceWindow:       10 * time.Millisecond,
		OrphanGraceWindow: 10 * time.Millisecond,
		Clock:             clk,
		Metrics:           metrics,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	// Before grace window elapses: nothing should be reaped.
	res1, err := sweeper.SweepRepo(ctx, rk, 2)
	if err != nil {
		t.Fatalf("SweepRepo (before grace): %v", err)
	}
	if res1.CompactedReaped != 0 || res1.OrphanedReaped != 0 || res1.SkippedGrace != 2 {
		t.Fatalf("unexpected pre-grace GCResult: %+v", res1)
	}

	// Advance clock past grace window: both COMPACTED and orphaned packs must be reaped; ACTIVE must remain.
	clk.Advance(20 * time.Millisecond)

	res2, err := sweeper.SweepRepo(ctx, rk, 2)
	if err != nil {
		t.Fatalf("SweepRepo (after grace): %v", err)
	}
	if res2.CompactedReaped != 1 || res2.OrphanedReaped != 1 {
		t.Fatalf("unexpected post-grace GCResult: %+v", res2)
	}

	activeKey, _ := s3.PackKey(rk, activeSHA)
	compactedKey, _ := s3.PackKey(rk, compactedSHA)
	compactedIdx, _ := s3.IndexKey(rk, compactedSHA)
	orphanKey, _ := s3.PackKey(rk, orphanSHA)
	orphanIdx, _ := s3.IndexKey(rk, orphanSHA)

	if !objects.exists(activeKey) {
		t.Fatal("active pack must never be deleted by GC")
	}
	for _, gone := range []string{compactedKey, compactedIdx, orphanKey, orphanIdx} {
		if objects.exists(gone) {
			t.Fatalf("expected %s to be deleted by GC", gone)
		}
	}

	// Compacted manifest item must be cleaned up from DDB.
	remaining, err := manifests.ListManifestsSince(ctx, rk, 2, "", false)
	if err != nil {
		t.Fatalf("ListManifestsSince: %v", err)
	}
	if len(remaining) != 1 || remaining[0].PackID != activeSHA {
		t.Fatalf("expected only active manifest remaining, got %+v", remaining)
	}

	// Verify pack.gc.sweep span was emitted.
	var foundGCSpan bool
	for _, sp := range tel.SpanExporter.GetSpans() {
		if sp.Name == "pack.gc.sweep" {
			foundGCSpan = true
			break
		}
	}
	if !foundGCSpan {
		t.Fatal("expected pack.gc.sweep OTel span to be recorded")
	}
}

func TestGCSweeper_NeverDeletesPackReferencedByActiveManifest(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-safe", "repo-safe")
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)

	sharedSHA := strings.Repeat("d", 64)
	ctx := context.Background()
	if err := objects.PutPack(ctx, rk, sharedSHA, []byte("PACK-shared"), []byte("IDX-shared")); err != nil {
		t.Fatalf("PutPack: %v", err)
	}

	// Same PackID referenced by both an old COMPACTED entry and a current ACTIVE entry.
	manifests.put(ddb.PackManifest{
		Repo:         rk,
		ULID:         "01J00000000000000000000001",
		PackID:       sharedSHA,
		SizeBytes:    50,
		OriginRegion: "us-east-1",
		Status:       ddb.PackStatusCompacted,
		CreatedAt:    clk.Now().Add(-48 * time.Hour),
	})
	manifests.put(ddb.PackManifest{
		Repo:         rk,
		ULID:         "01J00000000000000000000002",
		PackID:       sharedSHA,
		SizeBytes:    50,
		OriginRegion: "us-east-1",
		Status:       ddb.PackStatusActive,
		CreatedAt:    clk.Now(),
	})

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:   manifests,
		ManifestGC:  manifests,
		Objects:     objects,
		GraceWindow: 10 * time.Millisecond,
		Clock:       clk,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	res, err := sweeper.SweepRepo(ctx, rk, 1)
	if err != nil {
		t.Fatalf("SweepRepo: %v", err)
	}
	if res.CompactedReaped != 0 || res.OrphanedReaped != 0 || res.SkippedActive == 0 {
		t.Fatalf("expected active-referenced pack to be skipped, got %+v", res)
	}

	packKey, _ := s3.PackKey(rk, sharedSHA)
	if !objects.exists(packKey) {
		t.Fatal("pack referenced by ACTIVE manifest must not be deleted")
	}

	// Verify invalid config errors.
	if _, err := NewGCSweeper(GCSweeperConfig{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("NewGCSweeper empty config = %v, want ErrInvalidConfig", err)
	}
}
