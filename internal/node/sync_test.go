package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type fakeMetadataStore struct {
	mu               sync.Mutex
	metas            map[tenant.RepoKey]ddb.RepoMeta
	refs             map[tenant.RepoKey][]ddb.RefState
	manifestsByShard map[tenant.RepoKey]map[int][]ddb.PackManifest
	seenWatermarks   []string
	err              error
}

func newFakeMetadataStore() *fakeMetadataStore {
	return &fakeMetadataStore{
		metas:            make(map[tenant.RepoKey]ddb.RepoMeta),
		refs:             make(map[tenant.RepoKey][]ddb.RefState),
		manifestsByShard: make(map[tenant.RepoKey]map[int][]ddb.PackManifest),
	}
}

func (f *fakeMetadataStore) PutMeta(m ddb.RepoMeta) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.metas[m.Repo] = m
}

func (f *fakeMetadataStore) SetRefs(repo tenant.RepoKey, refs []ddb.RefState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]ddb.RefState, len(refs))
	copy(cp, refs)
	f.refs[repo] = cp
}

func (f *fakeMetadataStore) AddManifest(repo tenant.RepoKey, shard int, pm ddb.PackManifest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.manifestsByShard[repo] == nil {
		f.manifestsByShard[repo] = make(map[int][]ddb.PackManifest)
	}
	f.manifestsByShard[repo][shard] = append(f.manifestsByShard[repo][shard], pm)
}

func (f *fakeMetadataStore) GetMeta(_ context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return ddb.RepoMeta{}, f.err
	}
	m, ok := f.metas[repo]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return m, nil
}

func (f *fakeMetadataStore) ListRefs(_ context.Context, repo tenant.RepoKey, _ int) ([]ddb.RefState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	refs := append([]ddb.RefState(nil), f.refs[repo]...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].RefName < refs[j].RefName })
	return refs, nil
}

func (f *fakeMetadataStore) QueryManifestShard(_ context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	f.seenWatermarks = append(f.seenWatermarks, afterULID)
	var out []ddb.PackManifest
	for _, pm := range f.manifestsByShard[repo][shard] {
		if afterULID != "" && pm.ULID <= afterULID {
			continue
		}
		if activeOnly && pm.Status != ddb.PackStatusActive {
			continue
		}
		out = append(out, pm)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ULID < out[j].ULID })
	return out, nil
}

type fakePackFetcher struct {
	mu      sync.Mutex
	packs   map[string][]byte
	idxs    map[string][]byte
	fetches map[string]int
	failIDs map[string]error
}

func newFakePackFetcher() *fakePackFetcher {
	return &fakePackFetcher{
		packs:   make(map[string][]byte),
		idxs:    make(map[string][]byte),
		fetches: make(map[string]int),
		failIDs: make(map[string]error),
	}
}

func (f *fakePackFetcher) Put(packID string, packData, idxData []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.packs[packID] = append([]byte(nil), packData...)
	f.idxs[packID] = append([]byte(nil), idxData...)
}

func (f *fakePackFetcher) GetPack(_ context.Context, _ tenant.RepoKey, packSHA256 string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetches[packSHA256]++
	if err, ok := f.failIDs[packSHA256]; ok {
		return nil, err
	}
	data, ok := f.packs[packSHA256]
	if !ok {
		return nil, s3.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakePackFetcher) GetIndex(_ context.Context, _ tenant.RepoKey, packSHA256 string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.failIDs[packSHA256]; ok {
		return nil, err
	}
	data, ok := f.idxs[packSHA256]
	if !ok {
		return nil, s3.ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (f *fakePackFetcher) FetchCount(packID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.fetches[packID]
}

func TestMaterializer_ColdCacheAndIncrementalWatermarkSync(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	oid1 := strings.Repeat("1", 40)
	oid2 := strings.Repeat("2", 40)
	pack1 := strings.Repeat("a", 64)
	pack2 := strings.Repeat("b", 64)
	packOld := strings.Repeat("c", 64)
	pack3 := strings.Repeat("d", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid1},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA1", PackID: pack1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA0", PackID: packOld, Status: ddb.PackStatusCompacted, OriginRegion: "us-east-1",
	})
	metaStore.AddManifest(rk, 1, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA2", PackID: pack2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})

	packStore.Put(pack1, []byte("PACK-1"), []byte("IDX-1"))
	packStore.Put(pack2, []byte("PACK-2"), []byte("IDX-2"))
	packStore.Put(packOld, []byte("PACK-OLD"), []byte("IDX-OLD"))
	packStore.Put(pack3, []byte("PACK-3"), []byte("IDX-3"))

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:      metaStore,
		Packs:     packStore,
		Cache:     c,
		Telemetry: tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	// 1. Cold-cache sync pulls pack1 and pack2, skips COMPACTED packOld, and materializes disk refs.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin (cold): %v", err)
	}
	if res1.PacksFetched != 2 {
		t.Fatalf("cold PacksFetched = %d, want 2", res1.PacksFetched)
	}
	if packStore.FetchCount(packOld) != 0 {
		t.Fatalf("COMPACTED pack should not be fetched, got %d", packStore.FetchCount(packOld))
	}
	if !c.HasPack(rk, pack1) || !c.HasPack(rk, pack2) {
		t.Fatal("expected pack1 and pack2 installed in cache")
	}
	if got := c.Watermark(rk, 0); got != "01ARZ3NDEKTSV4RRFFQ69G5FA1" {
		t.Fatalf("shard 0 watermark = %q, want 01ARZ3NDEKTSV4RRFFQ69G5FA1", got)
	}
	if got := c.Watermark(rk, 1); got != "01ARZ3NDEKTSV4RRFFQ69G5FA2" {
		t.Fatalf("shard 1 watermark = %q, want 01ARZ3NDEKTSV4RRFFQ69G5FA2", got)
	}

	// Verify disposable disk ref matches DDB.
	refBytes, err := os.ReadFile(filepath.Join(res1.Handle.Path(), "refs", "heads", "main"))
	if err != nil || strings.TrimSpace(string(refBytes)) != oid1 {
		t.Fatalf("disk refs/heads/main = %q (err=%v), want %s", string(refBytes), err, oid1)
	}

	// Plant a stale disk-only ref that must be purged on the next sync.
	stalePath := filepath.Join(res1.Handle.Path(), "refs", "heads", "stale")
	if err := os.WriteFile(stalePath, []byte(oid1+"\n"), 0o600); err != nil {
		t.Fatalf("write stale ref: %v", err)
	}
	res1.Handle.Release()

	// 2. Incremental sync: add pack3 to shard 0 and update refs/heads/main to oid2.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA9", PackID: pack3, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid2},
	})

	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin (warm): %v", err)
	}
	defer res2.Handle.Release()

	if res2.PacksFetched != 1 {
		t.Fatalf("warm PacksFetched = %d, want 1", res2.PacksFetched)
	}
	if packStore.FetchCount(pack1) != 1 || packStore.FetchCount(pack2) != 1 || packStore.FetchCount(pack3) != 1 {
		t.Fatalf("unexpected fetch counts: pack1=%d pack2=%d pack3=%d",
			packStore.FetchCount(pack1), packStore.FetchCount(pack2), packStore.FetchCount(pack3))
	}
	if got := c.Watermark(rk, 0); got != "01ARZ3NDEKTSV4RRFFQ69G5FA9" {
		t.Fatalf("shard 0 updated watermark = %q, want 01ARZ3NDEKTSV4RRFFQ69G5FA9", got)
	}

	// Stale disk ref must be gone; refs/heads/main must be updated to oid2.
	if _, err := os.Stat(stalePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected stale disk ref to be removed, stat err=%v", err)
	}
	refBytes, err = os.ReadFile(filepath.Join(res2.Handle.Path(), "refs", "heads", "main"))
	if err != nil || strings.TrimSpace(string(refBytes)) != oid2 {
		t.Fatalf("updated disk refs/heads/main = %q (err=%v), want %s", string(refBytes), err, oid2)
	}
}

func TestMaterializer_DoesNotAdvanceWatermarkOnS3Failure(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "broken")
	c, _ := cache.New(cache.Config{Root: t.TempDir()})
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	packID := strings.Repeat("e", 64)
	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FB1", PackID: packID, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.failIDs[packID] = errors.New("s3 transient failure")

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: packStore,
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	if _, err := mat.SyncAndPin(ctx, rk); err == nil {
		t.Fatal("expected SyncAndPin to fail when S3 fetch fails")
	}
	if got := c.Watermark(rk, 0); got != "" {
		t.Fatalf("watermark advanced despite S3 failure: got %q, want empty", got)
	}
}

func TestIsSafeRefName(t *testing.T) {
	valid := []string{
		"refs/heads/main",
		"refs/heads/feature/foo-bar_1",
		"refs/tags/v1.0.0",
		"refs/heads/a.b",
		"refs/heads/a@b",
		"refs/pull/42/head",
		"refs/heads/café",
	}
	for _, ref := range valid {
		if !isSafeRefName(ref) {
			t.Errorf("isSafeRefName(%q) = false, want true", ref)
		}
	}

	invalid := []string{
		"",
		"HEAD",
		"heads/main",
		"refs/",
		"/refs/heads/main",
		"refs/heads/",
		"refs/heads//a",
		"refs/heads/.hidden",
		"refs/heads/.",
		"refs/heads/..",
		"refs/heads/foo.lock",
		"refs/heads/a.lock/b",
		"refs/heads/a..b",
		"refs/heads/a@{b}",
		"refs/heads/@",
		"refs/heads/a.",
		"refs/heads/a b",
		"refs/heads/a~1",
		"refs/heads/a^1",
		"refs/heads/a:b",
		"refs/heads/a?b",
		"refs/heads/a*",
		"refs/heads/a[0]",
		`refs/heads/a\b`,
		"refs/heads/a\x00b",
		"refs/heads/a\x01b",
		"refs/heads/a\x1fb",
		"refs/heads/a\x7fb",
		"refs/heads/a\tb",
		"refs/heads/a\nb",
		"refs/heads/a\rb",
	}
	for _, ref := range invalid {
		if isSafeRefName(ref) {
			t.Errorf("isSafeRefName(%q) = true, want false", ref)
		}
	}
}

func TestMaterializeDisposableRefs_PrunesEmptyDirsOnRefReplacement(t *testing.T) {
	repoPath := t.TempDir()
	oid1 := strings.Repeat("1", 40)
	oid2 := strings.Repeat("2", 40)

	if err := materializeDisposableRefs(repoPath, []ddb.RefState{
		{RefName: "refs/heads/a/b", OID: oid1},
	}, "refs/heads/main"); err != nil {
		t.Fatalf("first materializeDisposableRefs: %v", err)
	}

	// Replace refs/heads/a/b with refs/heads/a (requires pruning empty dir refs/heads/a).
	if err := materializeDisposableRefs(repoPath, []ddb.RefState{
		{RefName: "refs/heads/a", OID: oid2},
	}, "refs/heads/main"); err != nil {
		t.Fatalf("second materializeDisposableRefs (replace dir with file): %v", err)
	}

	got, err := os.ReadFile(filepath.Join(repoPath, "refs", "heads", "a"))
	if err != nil || strings.TrimSpace(string(got)) != oid2 {
		t.Fatalf("refs/heads/a = %q (err=%v), want %s", string(got), err, oid2)
	}
}
