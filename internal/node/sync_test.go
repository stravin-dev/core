package node

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
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

func (f *fakeMetadataStore) SetManifestStatus(repo tenant.RepoKey, shard int, ulid string, status ddb.PackStatus) {
	f.mu.Lock()
	defer f.mu.Unlock()
	items := f.manifestsByShard[repo][shard]
	for i := range items {
		if items[i].ULID == ulid {
			items[i].Status = status
		}
	}
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

func buildSyntheticCommitPackForSync(t *testing.T, algo pack.HashAlgorithm, blobContent, commitMsg string) (commitOID string, packBytes, idxBytes []byte) {
	t.Helper()
	blobData := []byte(blobContent)
	blobOID := pack.ComputeOIDWith(algo, pack.ObjBlob, blobData)

	var treeBuf bytes.Buffer
	fmt.Fprintf(&treeBuf, "100644 file.txt\x00")
	treeBuf.Write(blobOID.Bytes())
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOIDWith(algo, pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\n", treeOID.String())
	commitBuf.WriteString("author Stravin Test <test@stravin.dev> 1700000000 +0000\n")
	commitBuf.WriteString("committer Stravin Test <test@stravin.dev> 1700000000 +0000\n\n")
	commitBuf.WriteString(commitMsg)
	commitData := commitBuf.Bytes()
	cOID := pack.ComputeOIDWith(algo, pack.ObjCommit, commitData)

	entries := []struct {
		typ  pack.ObjectType
		data []byte
	}{
		{pack.ObjBlob, blobData},
		{pack.ObjTree, treeData},
		{pack.ObjCommit, commitData},
	}

	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))
	for _, e := range entries {
		size := uint64(len(e.data))
		b := ((uint8(e.typ) & 0x07) << 4) | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			buf.WriteByte(b | 0x80)
			b = byte(size & 0x7f)
			size >>= 7
		}
		buf.WriteByte(b)
		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write(e.data); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sum := algo.Sum(buf.Bytes())
	buf.Write(sum.Bytes())
	packBytes = buf.Bytes()

	parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{HashAlgorithm: algo})
	if err != nil {
		t.Fatalf("pack.Parse: %v", err)
	}
	idxBytes, err = pack.BuildIdx(parsed)
	if err != nil {
		t.Fatalf("pack.BuildIdx: %v", err)
	}
	return cOID.String(), packBytes, idxBytes
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

	oid1, pack1Bytes, idx1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "hello-1\n", "commit 1\n")
	oid2, pack2Bytes, idx2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "hello-2\n", "commit 2\n")
	_, pack3Bytes, idx3Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "hello-3\n", "commit 3\n")
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

	packStore.Put(pack1, pack1Bytes, idx1Bytes)
	packStore.Put(pack2, pack2Bytes, idx2Bytes)
	packStore.Put(packOld, []byte("PACK-OLD"), []byte("IDX-OLD"))
	packStore.Put(pack3, pack3Bytes, idx3Bytes)

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packStore,
		Cache:       c,
		GitExecutor: gitExec,
		Telemetry:   tp,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	// 1. Cold-cache sync pulls pack1 and pack2, skips COMPACTED packOld, and materializes reftable refs.
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

	// Verify reftable storage and authoritative ref via git for-each-ref.
	if _, err := os.Stat(filepath.Join(res1.Handle.Path(), "reftable", "tables.list")); err != nil {
		t.Fatalf("expected reftable/tables.list in NVMe bare repo: %v", err)
	}
	gotRefs1, err := gitExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{RepoDir: res1.Handle.Path()})
	if err != nil || gotRefs1["refs/heads/main"] != oid1 {
		t.Fatalf("reftable refs/heads/main = %q (err=%v), want %s", gotRefs1["refs/heads/main"], err, oid1)
	}

	// Plant a stale ref in reftable that must be purged on the next sync.
	if err := gitExec.ExecUpdateRef(ctx, gitexec.UpdateRefRequest{
		RepoDir: res1.Handle.Path(),
		Updates: []gitexec.RefUpdateSpec{{RefName: "refs/heads/stale", NewOID: oid1}},
	}); err != nil {
		t.Fatalf("plant stale ref: %v", err)
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

	// Stale ref must be gone; refs/heads/main must be updated to oid2.
	gotRefs2, err := gitExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{RepoDir: res2.Handle.Path()})
	if err != nil {
		t.Fatalf("ExecForEachRef(2): %v", err)
	}
	if _, exists := gotRefs2["refs/heads/stale"]; exists {
		t.Fatalf("expected stale ref to be removed from reftable, got %+v", gotRefs2)
	}
	if gotRefs2["refs/heads/main"] != oid2 {
		t.Fatalf("updated reftable refs/heads/main = %q, want %s", gotRefs2["refs/heads/main"], oid2)
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

func TestMaterializeReftableRefs_HandlesDirectoryFileRefReplacement(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "df-refs")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "df-1\n", "commit 1\n")
	oid2, p2Bytes, i2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "df-2\n", "commit 2\n")
	p1 := strings.Repeat("1", 64)
	p2 := strings.Repeat("2", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FE1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FE2", PackID: p2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	packStore.Put(p1, p1Bytes, i1Bytes)
	packStore.Put(p2, p2Bytes, i2Bytes)

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packStore,
		Cache:       c,
		GitExecutor: gitExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/a/b", OID: oid1}})
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("first SyncAndPin: %v", err)
	}
	res1.Handle.Release()

	// Replace refs/heads/a/b with refs/heads/a in reftable.
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/a", OID: oid2}})
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("second SyncAndPin (replace a/b with a): %v", err)
	}
	defer res2.Handle.Release()

	refs, err := gitExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{RepoDir: res2.Handle.Path()})
	if err != nil {
		t.Fatalf("ExecForEachRef: %v", err)
	}
	if refs["refs/heads/a"] != oid2 || refs["refs/heads/a/b"] != "" {
		t.Fatalf("unexpected refs after D/F replacement: %+v", refs)
	}
}

func TestMaterializer_PrunesSupersededPacksAfterCompaction(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "compacted")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	pack1 := strings.Repeat("1", 64)
	pack2 := strings.Repeat("2", 64)
	packMerged := strings.Repeat("3", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FC1", PackID: pack1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FC2", PackID: pack2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.Put(pack1, []byte("PACK-1"), []byte("IDX-1"))
	packStore.Put(pack2, []byte("PACK-2"), []byte("IDX-2"))
	packStore.Put(packMerged, []byte("PACK-MERGED"), []byte("IDX-MERGED"))

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: packStore,
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	res1.Handle.Release()

	// Simulate compaction: publish packMerged ACTIVE and supersede pack1 & pack2 to COMPACTED.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FC3", PackID: packMerged, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FC1", ddb.PackStatusCompacted)
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FC2", ddb.PackStatusCompacted)

	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}

	// While pinned, superseded pack files remain on disk for in-flight git-upload-pack readers.
	pack1Path := filepath.Join(res2.Handle.Path(), "objects", "pack", "pack-"+pack1+".pack")
	if _, statErr := os.Stat(pack1Path); statErr != nil {
		t.Fatalf("expected pinned pack1 to remain on disk before release, err=%v", statErr)
	}
	res2.Handle.Release()

	// Once released, superseded packs must be unlinked and only packMerged should remain.
	if _, statErr := os.Stat(pack1Path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected superseded pack1 to be unlinked after release, stat err=%v", statErr)
	}
	if c.HasPack(rk, pack1) || c.HasPack(rk, pack2) {
		t.Fatalf("expected pack1 and pack2 pruned, cached=%v", c.CachedPackIDs(rk))
	}
	if !c.HasPack(rk, packMerged) {
		t.Fatalf("expected packMerged to remain cached, cached=%v", c.CachedPackIDs(rk))
	}
}

func TestMaterializer_PrunesSupersededPacksWhileNewerReadersPinned(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "sustained-prune")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	pack1 := strings.Repeat("a", 64)
	pack2 := strings.Repeat("b", 64)
	packMerged := strings.Repeat("c", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD1", PackID: pack1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD2", PackID: pack2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.Put(pack1, []byte("PACK-1"), []byte("IDX-1"))
	packStore.Put(pack2, []byte("PACK-2"), []byte("IDX-2"))
	packStore.Put(packMerged, []byte("PACK-MERGED"), []byte("IDX-MERGED"))

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: packStore,
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	// Reader 1 holds a pre-compaction lease.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}

	// Compaction supersedes pack1 & pack2 with packMerged.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD3", PackID: packMerged, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FD1", ddb.PackStatusCompacted)
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FD2", ddb.PackStatusCompacted)

	// Reader 2 triggers sync & PrunePacks while Reader 1 is still pinned.
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		res1.Handle.Release()
		t.Fatalf("SyncAndPin(2): %v", err)
	}

	// Reader 3 acquires a post-prune lease so pins never drop to 0.
	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("SyncAndPin(3): %v", err)
	}
	defer res3.Handle.Release()

	pack1Path := filepath.Join(res1.Handle.Path(), "objects", "pack", "pack-"+pack1+".pack")
	if _, statErr := os.Stat(pack1Path); statErr != nil {
		t.Fatalf("expected pack1 on disk while prior-gen readers are pinned, err=%v", statErr)
	}

	// Release prior-generation readers while res3 remains pinned (pins == 1).
	res1.Handle.Release()
	res2.Handle.Release()

	if _, statErr := os.Stat(pack1Path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("expected superseded pack1 to be unlinked while res3 remains pinned, err=%v", statErr)
	}
	if got := c.CachedPackIDs(rk); len(got) != 1 || got[0] != packMerged {
		t.Fatalf("CachedPackIDs = %v, want [%s]", got, packMerged)
	}
}

func TestMaterializer_PrunesSupersededPacksOnPeriodicReconcile(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "reconcile")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	pack1 := strings.Repeat("4", 64)
	pack2 := strings.Repeat("5", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive,
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD1", PackID: pack1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FD2", PackID: pack2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.Put(pack1, []byte("PACK-1"), []byte("IDX-1"))
	packStore.Put(pack2, []byte("PACK-2"), []byte("IDX-2"))

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                   metaStore,
		Packs:                  packStore,
		Cache:                  c,
		PruneReconcileInterval: time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	res1.Handle.Release()

	// Supersede pack1 without publishing a new pack (simulates Phase 2 finishing after an earlier sync).
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FD1", ddb.PackStatusCompacted)

	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	res2.Handle.Release()

	if c.HasPack(rk, pack1) {
		t.Fatalf("expected pack1 pruned on periodic reconcile, cached=%v", c.CachedPackIDs(rk))
	}
	if !c.HasPack(rk, pack2) {
		t.Fatalf("expected pack2 to remain cached, cached=%v", c.CachedPackIDs(rk))
	}
}

type hookMetadataStore struct {
	*fakeMetadataStore
	onListRefs func(callNum int)
	listCalls  int
	hookMu     sync.Mutex
}

func (h *hookMetadataStore) ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error) {
	h.hookMu.Lock()
	h.listCalls++
	n := h.listCalls
	cb := h.onListRefs
	h.hookMu.Unlock()
	if cb != nil {
		cb(n)
	}
	return h.fakeMetadataStore.ListRefs(ctx, repo, refShards)
}

func TestMaterializer_SyncAndPinPreventsStaleRefOverwriteAndCoalescesLinearizably(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "linearizable-sync")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	baseMeta := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v1\n", "commit 1\n")
	oid2, p2Bytes, i2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v2\n", "commit 2\n")
	p1 := strings.Repeat("a", 64)
	p2 := strings.Repeat("b", 64)

	baseMeta.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	baseMeta.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid1},
	})
	baseMeta.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FZ1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.Put(p1, p1Bytes, i1Bytes)
	packStore.Put(p2, p2Bytes, i2Bytes)

	flight1InListRefs := make(chan struct{})
	releaseFlight1 := make(chan struct{})

	metaStore := &hookMetadataStore{
		fakeMetadataStore: baseMeta,
		onListRefs: func(callNum int) {
			if callNum == 1 {
				close(flight1InListRefs)
				<-releaseFlight1
			}
		},
	}

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packStore,
		Cache:       c,
		GitExecutor: gitExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	// Start Sync 1 (reads v1 refs).
	type syncOutcome struct {
		res SyncResult
		err error
	}
	sync1Ch := make(chan syncOutcome, 1)
	go func() {
		res, err := mat.SyncAndPin(ctx, rk)
		sync1Ch <- syncOutcome{res: res, err: err}
	}()

	<-flight1InListRefs

	// Commit a push to DynamoDB (pack2 + oid2 + new branch) while Sync 1 is in flight.
	baseMeta.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FZ2", PackID: p2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	baseMeta.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid2},
		{Repo: rk, RefName: "refs/heads/agent/shared-race", OID: oid2},
	})

	// Launch two concurrent fetches (Sync 2 & Sync 3) after the push committed.
	// Neither may join Sync 1's stale flight, and neither may read stale refs outside Materialize.
	sync2Ch := make(chan syncOutcome, 1)
	sync3Ch := make(chan syncOutcome, 1)
	go func() {
		res, err := mat.SyncAndPin(ctx, rk)
		sync2Ch <- syncOutcome{res: res, err: err}
	}()
	go func() {
		res, err := mat.SyncAndPin(ctx, rk)
		sync3Ch <- syncOutcome{res: res, err: err}
	}()

	// Give Sync 2 & Sync 3 time to enter SyncAndPin and block behind Flight 1.
	time.Sleep(30 * time.Millisecond)

	// Because LoadAuthoritativeRefs runs inside Materialize, ListRefs must still
	// have been called only once (by Flight 1) while Flight 1 is in progress.
	metaStore.hookMu.Lock()
	callsDuringFlight1 := metaStore.listCalls
	metaStore.hookMu.Unlock()
	if callsDuringFlight1 != 1 {
		close(releaseFlight1)
		t.Fatalf("ListRefs called %d times while Flight 1 active, want 1 (must run inside Materialize)", callsDuringFlight1)
	}

	close(releaseFlight1)

	out1 := <-sync1Ch
	out2 := <-sync2Ch
	out3 := <-sync3Ch
	if out1.err != nil {
		t.Fatalf("sync 1 error: %v", out1.err)
	}
	defer out1.res.Handle.Release()
	if out2.err != nil {
		t.Fatalf("sync 2 error: %v", out2.err)
	}
	defer out2.res.Handle.Release()
	if out3.err != nil {
		t.Fatalf("sync 3 error: %v", out3.err)
	}
	defer out3.res.Handle.Release()

	// Both post-push callers (including any coalesced follower) must receive valid Meta & updated Refs.
	for idx, out := range []syncOutcome{out2, out3} {
		if out.res.Meta.HomeRegion != "us-east-1" || out.res.Meta.FailoverEpoch != 1 {
			t.Fatalf("sync %d Meta = %+v, want HomeRegion=us-east-1 FailoverEpoch=1", idx+2, out.res.Meta)
		}
		gotByName := make(map[string]string, len(out.res.Refs))
		for _, r := range out.res.Refs {
			gotByName[r.RefName] = r.OID
		}
		if gotByName["refs/heads/main"] != oid2 || gotByName["refs/heads/agent/shared-race"] != oid2 {
			t.Fatalf("sync %d Refs = %+v, want main=%s and agent/shared-race=%s", idx+2, gotByName, oid2, oid2)
		}
	}

	// Reftable on disk must reflect the newer v2 refs (never overwritten by Flight 1's v1 snapshot).
	diskRefs, err := gitExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{RepoDir: out2.res.Handle.Path()})
	if err != nil {
		t.Fatalf("ExecForEachRef: %v", err)
	}
	if diskRefs["refs/heads/main"] != oid2 {
		t.Fatalf("disk refs/heads/main = %q, want %s", diskRefs["refs/heads/main"], oid2)
	}
	if diskRefs["refs/heads/agent/shared-race"] != oid2 {
		t.Fatalf("disk refs/heads/agent/shared-race = %q, want %s", diskRefs["refs/heads/agent/shared-race"], oid2)
	}
}

func TestMaterializer_RefreshAuxIndexesRecoversFromStaleLocks(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "stale-locks")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "lock-1\n", "commit 1\n")
	oid2, p2Bytes, i2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "lock-2\n", "commit 2\n")
	p1 := strings.Repeat("a", 64)
	p2 := strings.Repeat("b", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid1},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FL1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore.Put(p1, p1Bytes, i1Bytes)
	packStore.Put(p2, p2Bytes, i2Bytes)

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packStore,
		Cache:       c,
		GitExecutor: gitExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	// Initial sync creates multi-pack-index, .bitmap, and commit-graph.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	repoPath := res1.Handle.Path()
	res1.Handle.Release()

	midxPath := filepath.Join(repoPath, "objects", "pack", "multi-pack-index")
	midxLockPath := filepath.Join(repoPath, "objects", "pack", "multi-pack-index.lock")
	cgPath := filepath.Join(repoPath, "objects", "info", "commit-graph")
	cgLockPath := filepath.Join(repoPath, "objects", "info", "commit-graph.lock")

	// Simulate interrupted MIDX & commit-graph writes: remove indexes and leave stale lockfiles.
	_ = os.Remove(midxPath)
	bitmaps, _ := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "multi-pack-index-*.bitmap"))
	for _, b := range bitmaps {
		_ = os.Remove(b)
	}
	_ = os.Remove(cgPath)
	if err := os.WriteFile(midxLockPath, []byte("stale-midx-lock"), 0o600); err != nil {
		t.Fatalf("write stale multi-pack-index.lock: %v", err)
	}
	if err := os.WriteFile(cgLockPath, []byte("stale-cg-lock"), 0o600); err != nil {
		t.Fatalf("write stale commit-graph.lock: %v", err)
	}

	// Next sync must clean up stale locks and rebuild multi-pack-index, .bitmap, and commit-graph.
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	res2.Handle.Release()

	if _, err := os.Stat(midxLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected multi-pack-index.lock to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(cgLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected commit-graph.lock to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(midxPath); err != nil {
		t.Fatalf("expected multi-pack-index to be rebuilt after stale lock cleanup: %v", err)
	}
	rebuiltBitmaps, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "multi-pack-index-*.bitmap"))
	if err != nil || len(rebuiltBitmaps) == 0 {
		t.Fatalf("expected multi-pack-index .bitmap to be rebuilt, glob=%v err=%v", rebuiltBitmaps, err)
	}
	if _, err := os.Stat(cgPath); err != nil {
		t.Fatalf("expected commit-graph to be rebuilt after stale lock cleanup: %v", err)
	}

	// Also verify recovery when indexes already exist but stale lockfiles are present on a new change.
	if err := os.WriteFile(midxLockPath, []byte("stale-midx-lock-2"), 0o600); err != nil {
		t.Fatalf("write stale multi-pack-index.lock (2): %v", err)
	}
	if err := os.WriteFile(cgLockPath, []byte("stale-cg-lock-2"), 0o600); err != nil {
		t.Fatalf("write stale commit-graph.lock (2): %v", err)
	}
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FL2", PackID: p2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid2},
	})

	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(3): %v", err)
	}
	res3.Handle.Release()

	if _, err := os.Stat(midxLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected multi-pack-index.lock (2) to be removed, stat err=%v", err)
	}
	if _, err := os.Stat(cgLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected commit-graph.lock (2) to be removed, stat err=%v", err)
	}
}

type stubRepackExecutor struct {
	*gitexec.SubprocessExecutor
	onRepack func(ctx context.Context, req gitexec.RepackRequest) error
}

func (s *stubRepackExecutor) ExecRepack(ctx context.Context, req gitexec.RepackRequest) error {
	if s.onRepack != nil {
		return s.onRepack(ctx, req)
	}
	return s.SubprocessExecutor.ExecRepack(ctx, req)
}

func TestMaterializer_RefreshAuxIndexesUsesDetachedBoundedContext(t *testing.T) {
	rk, err := tenant.NewRepoKey("acme", "detached-aux")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repoPath := t.TempDir()
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o750); err != nil {
		t.Fatalf("mkdir packDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-dummy.pack"), []byte("PACK"), 0o600); err != nil {
		t.Fatalf("write dummy pack: %v", err)
	}

	var calls int
	execStub := &stubRepackExecutor{
		SubprocessExecutor: gitexec.NewSubprocessExecutor(gitexec.Config{}),
		onRepack: func(ctx context.Context, _ gitexec.RepackRequest) error {
			calls++
			if ctx.Err() != nil {
				t.Errorf("ExecRepack received canceled context: %v", ctx.Err())
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Errorf("ExecRepack context missing bounded timeout deadline")
			}
			return nil
		},
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        newFakeMetadataStore(),
		Packs:       newFakePackFetcher(),
		Cache:       c,
		GitExecutor: execStub,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	parentCtx, cancel := context.WithCancel(context.Background())
	cancel()

	mat.refreshAuxIndexes(parentCtx, rk, repoPath, true, true)
	if calls != 2 {
		t.Fatalf("ExecRepack calls = %d, want 2 (MIDX+bitmap and commit-graph)", calls)
	}
}

func TestMaterializer_RefreshAuxIndexesLogsWarnRecordsMetricAndCleansLockOnFailure(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "aux-failure-obs")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	repoPath := t.TempDir()
	packDir := filepath.Join(repoPath, "objects", "pack")
	infoDir := filepath.Join(repoPath, "objects", "info")
	if err := os.MkdirAll(packDir, 0o750); err != nil {
		t.Fatalf("mkdir packDir: %v", err)
	}
	if err := os.MkdirAll(infoDir, 0o750); err != nil {
		t.Fatalf("mkdir infoDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-dummy.pack"), []byte("PACK"), 0o600); err != nil {
		t.Fatalf("write dummy pack: %v", err)
	}

	midxLockPath := filepath.Join(packDir, "multi-pack-index.lock")
	cgLockPath := filepath.Join(infoDir, "commit-graph.lock")

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	metrics := NewMetrics(tp, "us-east-1")

	execStub := &stubRepackExecutor{
		SubprocessExecutor: gitexec.NewSubprocessExecutor(gitexec.Config{}),
		onRepack: func(_ context.Context, req gitexec.RepackRequest) error {
			if req.WriteMIDX {
				_ = os.WriteFile(midxLockPath, []byte("aborted-midx"), 0o600)
				return errors.New("simulated midx failure")
			}
			if req.WriteCommitGraph {
				_ = os.WriteFile(cgLockPath, []byte("aborted-cg"), 0o600)
				return errors.New("simulated commit-graph failure")
			}
			return nil
		},
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        newFakeMetadataStore(),
		Packs:       newFakePackFetcher(),
		Cache:       c,
		GitExecutor: execStub,
		Telemetry:   tp,
		Metrics:     metrics,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	mat.refreshAuxIndexes(ctx, rk, repoPath, true, true)

	// Lockfiles left by failing ExecRepack must be cleaned up.
	if _, err := os.Stat(midxLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected multi-pack-index.lock cleaned after failure, stat err=%v", err)
	}
	if _, err := os.Stat(cgLockPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected commit-graph.lock cleaned after failure, stat err=%v", err)
	}

	// Both failures must be logged at WARN level.
	logs := logBuf.String()
	if !strings.Contains(logs, `"level":"WARN"`) || !strings.Contains(logs, "failed to refresh multi-pack-index") {
		t.Fatalf("expected WARN log for multi-pack-index failure, got: %s", logs)
	}
	if !strings.Contains(logs, "failed to refresh commit-graph") {
		t.Fatalf("expected WARN log for commit-graph failure, got: %s", logs)
	}

	// OTel counter stravin_node_aux_index_refresh_failures_total must record both failures.
	var rm metricdata.ResourceMetrics
	if err := tp.MetricReader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	gotByType := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, item := range sm.Metrics {
			if item.Name != "stravin_node_aux_index_refresh_failures_total" {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("data type = %T, want metricdata.Sum[int64]", item.Data)
			}
			for _, dp := range sum.DataPoints {
				if v, ok := dp.Attributes.Value("index_type"); ok {
					gotByType[v.AsString()] += dp.Value
				}
			}
		}
	}
	if gotByType["midx"] != 1 || gotByType["commit_graph"] != 1 {
		t.Fatalf("aux index failure counts = %+v, want midx=1 commit_graph=1", gotByType)
	}
}

func TestMaterializer_RefreshAuxIndexesZeroCommitFallbackStaysQuiet(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "aux-zero-commit")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	tp, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	repoPath := t.TempDir()
	packDir := filepath.Join(repoPath, "objects", "pack")
	if err := os.MkdirAll(packDir, 0o750); err != nil {
		t.Fatalf("mkdir packDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(packDir, "pack-dummy.pack"), []byte("PACK"), 0o600); err != nil {
		t.Fatalf("write dummy pack: %v", err)
	}

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	metrics := NewMetrics(tp, "us-east-1")

	execStub := &stubRepackExecutor{
		SubprocessExecutor: gitexec.NewSubprocessExecutor(gitexec.Config{}),
		onRepack: func(_ context.Context, req gitexec.RepackRequest) error {
			if req.WriteMIDX && req.WriteBitmap {
				return errors.New("no commits for bitmap")
			}
			return nil
		},
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        newFakeMetadataStore(),
		Packs:       newFakePackFetcher(),
		Cache:       c,
		GitExecutor: execStub,
		Telemetry:   tp,
		Metrics:     metrics,
		Logger:      logger,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}

	mat.refreshAuxIndexes(ctx, rk, repoPath, true, true)

	if strings.Contains(logBuf.String(), `"level":"WARN"`) {
		t.Fatalf("expected no WARN log on zero-commit bitmap fallback, got: %s", logBuf.String())
	}
	if got := metrics.promAuxIndexRefreshFailures.Load(); got != 0 {
		t.Fatalf("promAuxIndexRefreshFailures = %d, want 0", got)
	}
}
