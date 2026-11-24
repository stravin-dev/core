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
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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
	mu                 sync.Mutex
	metas              map[tenant.RepoKey]ddb.RepoMeta
	refs               map[tenant.RepoKey][]ddb.RefState
	manifestsByShard   map[tenant.RepoKey]map[int][]ddb.PackManifest
	seenWatermarks     []string
	getMetaCalls       int
	listRefsCalls      int
	getRefCalls        int
	queryManifestCalls int
	listRefsHook       func(ctx context.Context, repo tenant.RepoKey)
	queryManifestHook  func(ctx context.Context, shard int)
	err                error
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
	f.getMetaCalls++
	if f.err != nil {
		return ddb.RepoMeta{}, f.err
	}
	m, ok := f.metas[repo]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return m, nil
}

func (f *fakeMetadataStore) ListRefs(ctx context.Context, repo tenant.RepoKey, _ int) ([]ddb.RefState, error) {
	f.mu.Lock()
	hook := f.listRefsHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, repo)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listRefsCalls++
	if f.err != nil {
		return nil, f.err
	}
	refs := append([]ddb.RefState(nil), f.refs[repo]...)
	sort.Slice(refs, func(i, j int) bool { return refs[i].RefName < refs[j].RefName })
	return refs, nil
}

func (f *fakeMetadataStore) GetRef(_ context.Context, repo tenant.RepoKey, refName string, _ int) (ddb.RefState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getRefCalls++
	if f.err != nil {
		return ddb.RefState{}, f.err
	}
	for _, r := range f.refs[repo] {
		if r.RefName == refName {
			return r, nil
		}
	}
	return ddb.RefState{}, ddb.ErrNotFound
}

func (f *fakeMetadataStore) GetMetaCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getMetaCalls
}

func (f *fakeMetadataStore) ListRefsCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listRefsCalls
}

func (f *fakeMetadataStore) GetRefCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getRefCalls
}

func (f *fakeMetadataStore) QueryManifestCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.queryManifestCalls
}

func (f *fakeMetadataStore) QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error) {
	f.mu.Lock()
	hook := f.queryManifestHook
	f.mu.Unlock()
	if hook != nil {
		hook(ctx, shard)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queryManifestCalls++
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
	t.Cleanup(mat.Close)

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
	t.Cleanup(mat.Close)

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
	t.Cleanup(mat.Close)

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
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes: %v", err)
	}

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
	t.Cleanup(mat.Close)

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
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes: %v", err)
	}

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
	t.Cleanup(mat.Close)

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
	t.Cleanup(mat.Close)

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
		Meta:                    metaStore,
		Packs:                   packStore,
		Cache:                   c,
		GitExecutor:             gitExec,
		AuxIndexRefreshInterval: -1,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// Initial sync creates multi-pack-index, .bitmap, and commit-graph.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	repoPath := res1.Handle.Path()
	res1.Handle.Release()
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(1): %v", err)
	}

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
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(2): %v", err)
	}

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
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(3): %v", err)
	}

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

func TestMaterializer_MIDXAndBitmapStayActiveDuringDeferredPruneAndUnlink(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "midx-deferred-prune")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v1\n", "commit 1\n")
	oid2, p2Bytes, i2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v2\n", "commit 2\n")
	oid3, p3Bytes, i3Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v3\n", "commit 3\n")
	p1 := strings.Repeat("1", 64)
	p2 := strings.Repeat("2", 64)
	p3 := strings.Repeat("3", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid2},
		{Repo: rk, RefName: "refs/heads/old", OID: oid1},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FM1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FM2", PackID: p2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	packStore.Put(p1, p1Bytes, i1Bytes)
	packStore.Put(p2, p2Bytes, i2Bytes)
	packStore.Put(p3, p3Bytes, i3Bytes)

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                    metaStore,
		Packs:                   packStore,
		Cache:                   c,
		GitExecutor:             gitExec,
		AuxIndexRefreshInterval: -1,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// Reader 1 pins gen-0.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		res1.Handle.Release()
		t.Fatalf("WaitAuxIndexes(1): %v", err)
	}
	repoPath := res1.Handle.Path()
	packDir := filepath.Join(repoPath, "objects", "pack")

	// Compaction adds p3 and supersedes p1 while Reader 1 is still pinned.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FM3", PackID: p3, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	metaStore.SetManifestStatus(rk, 0, "01ARZ3NDEKTSV4RRFFQ69G5FM1", ddb.PackStatusCompacted)
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid3},
	})

	// Reader 2 syncs and schedules p1 for deferred prune (PendingPruneCount == 1).
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		res1.Handle.Release()
		t.Fatalf("SyncAndPin(2): %v", err)
	}

	// Reader 3 acquires a post-prune (gen-1) lease so pins never drop to 0.
	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("SyncAndPin(3): %v", err)
	}
	defer res3.Handle.Release()

	if got := c.PendingPruneCount(rk); got != 1 {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("PendingPruneCount = %d, want 1", got)
	}
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("WaitAuxIndexes(2): %v", err)
	}

	// MIDX and .bitmap MUST be present and valid while PendingPruneCount > 0.
	midxPath := filepath.Join(packDir, "multi-pack-index")
	if _, err := os.Stat(midxPath); err != nil {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("expected multi-pack-index present while PendingPruneCount > 0: %v", err)
	}
	bitmaps, err := filepath.Glob(filepath.Join(packDir, "multi-pack-index-*.bitmap"))
	if err != nil || len(bitmaps) == 0 {
		res1.Handle.Release()
		res2.Handle.Release()
		t.Fatalf("expected .bitmap present while PendingPruneCount > 0: glob=%v err=%v", bitmaps, err)
	}

	// Release prior-generation readers (res1 & res2) while post-prune res3 stays pinned ->
	// p1 is unlinked, and MIDX & .bitmap built via --stdin-packs MUST survive!
	res1.Handle.Release()
	res2.Handle.Release()
	if got := c.PendingPruneCount(rk); got != 0 {
		t.Fatalf("PendingPruneCount after gen-0 release = %d, want 0", got)
	}
	if _, err := os.Stat(filepath.Join(packDir, "pack-"+p1+".pack")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected p1 unlinked, stat err=%v", err)
	}
	if _, err := os.Stat(midxPath); err != nil {
		t.Fatalf("expected multi-pack-index preserved after deferred unlink: %v", err)
	}
	bitmapsAfter, err := filepath.Glob(filepath.Join(packDir, "multi-pack-index-*.bitmap"))
	if err != nil || len(bitmapsAfter) == 0 {
		t.Fatalf("expected .bitmap preserved after deferred unlink: glob=%v err=%v", bitmapsAfter, err)
	}

	// git multi-pack-index verify and git upload-pack must both succeed with zero errors.
	verifyCmd := exec.Command("git", "--git-dir", repoPath, "multi-pack-index", "verify")
	if out, err := verifyCmd.CombinedOutput(); err != nil {
		t.Fatalf("git multi-pack-index verify failed after deferred unlink: %v\n%s", err, string(out))
	}
	var upOut, upErr bytes.Buffer
	wantReq := fmt.Sprintf("0032want %s\n00000009done\n", oid3)
	if err := gitExec.ExecUploadPack(ctx, gitexec.UploadPackRequest{
		Repo:         rk,
		RepoDir:      repoPath,
		StatelessRPC: true,
		Stdin:        strings.NewReader(wantReq),
		Stdout:       &upOut,
		Stderr:       &upErr,
	}); err != nil {
		t.Fatalf("ExecUploadPack failed after deferred unlink: %v (stderr=%s)", err, upErr.String())
	}
	if !bytes.Contains(upOut.Bytes(), []byte("PACK")) {
		t.Fatalf("expected PACK payload from ExecUploadPack, got %d bytes", upOut.Len())
	}
}

func TestMaterializer_SyncAndPinDoesNotBlockOnSlowAuxIndexAndCoalesces(t *testing.T) {
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "async-coalesce")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "a1\n", "c1\n")
	oid2, p2Bytes, i2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "a2\n", "c2\n")
	p1 := strings.Repeat("a", 64)
	p2 := strings.Repeat("b", 64)

	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: oid1}})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FN1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	packStore.Put(p1, p1Bytes, i1Bytes)
	packStore.Put(p2, p2Bytes, i2Bytes)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var midxCalls atomic.Int32

	execStub := &stubRepackExecutor{
		SubprocessExecutor: gitexec.NewSubprocessExecutor(gitexec.Config{}),
		onRepack: func(ctx context.Context, req gitexec.RepackRequest) error {
			if req.WriteMIDX {
				if midxCalls.Add(1) == 1 {
					close(firstStarted)
					select {
					case <-releaseFirst:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		},
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                    metaStore,
		Packs:                   packStore,
		Cache:                   c,
		GitExecutor:             execStub,
		AuxIndexRefreshInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// SyncAndPin must return promptly even though ExecRepack is blocked on releaseFirst.
	start := time.Now()
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	res1.Handle.Release()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("SyncAndPin blocked for %v on slow refreshAuxIndexes", elapsed)
	}

	select {
	case <-firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("background refreshAuxIndexes did not start")
	}

	// Trigger 5 rapid syncs with ref changes while the first refresh is blocked.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FN2", PackID: p2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	for i := 0; i < 5; i++ {
		oid := oid2
		if i%2 == 0 {
			oid = oid1
		}
		metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: oid}})
		res, err := mat.SyncAndPin(ctx, rk)
		if err != nil {
			close(releaseFirst)
			t.Fatalf("SyncAndPin(burst %d): %v", i, err)
		}
		res.Handle.Release()
	}

	close(releaseFirst)
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes: %v", err)
	}

	if got := midxCalls.Load(); got != 2 {
		t.Fatalf("midxCalls = %d, want 2 (1 initial + 1 coalesced follow-up for 5 burst syncs)", got)
	}
}

func TestMaterializer_UnchangedFlightsDoNotRebuildCommitGraph(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}
	ctx := context.Background()
	rk, err := tenant.NewRepoKey("acme", "cg-unchanged")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	metaStore := newFakeMetadataStore()
	packStore := newFakePackFetcher()

	oid1, p1Bytes, i1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "cg1\n", "commit 1\n")
	p1 := strings.Repeat("7", 64)
	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1, RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	})
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: oid1}})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FP1", PackID: p1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1"})
	packStore.Put(p1, p1Bytes, i1Bytes)

	var midxCalls, cgCalls atomic.Int32
	baseExec := gitexec.NewSubprocessExecutor(gitexec.Config{})
	execStub := &stubRepackExecutor{
		SubprocessExecutor: baseExec,
		onRepack: func(ctx context.Context, req gitexec.RepackRequest) error {
			if req.WriteMIDX {
				midxCalls.Add(1)
			}
			if req.WriteCommitGraph {
				cgCalls.Add(1)
			}
			return baseExec.ExecRepack(ctx, req)
		},
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                    metaStore,
		Packs:                   packStore,
		Cache:                   c,
		GitExecutor:             execStub,
		AuxIndexRefreshInterval: -1,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// 1. Initial sync builds both MIDX and commit-graph.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	repoPath := res1.Handle.Path()
	res1.Handle.Release()
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(1): %v", err)
	}
	if midxCalls.Load() != 1 || cgCalls.Load() != 1 {
		t.Fatalf("after initial sync: midx=%d cg=%d, want 1, 1", midxCalls.Load(), cgCalls.Load())
	}

	// 2. Unchanged sync must not rebuild either MIDX or commit-graph.
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	res2.Handle.Release()
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(2): %v", err)
	}
	if midxCalls.Load() != 1 || cgCalls.Load() != 1 {
		t.Fatalf("after unchanged sync: midx=%d cg=%d, want 1, 1", midxCalls.Load(), cgCalls.Load())
	}

	// 3. Remove only multi-pack-index; unchanged sync must rebuild ONLY MIDX, not commit-graph.
	if err := os.Remove(filepath.Join(repoPath, "objects", "pack", "multi-pack-index")); err != nil {
		t.Fatalf("remove multi-pack-index: %v", err)
	}
	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(3): %v", err)
	}
	res3.Handle.Release()
	if err := mat.WaitAuxIndexes(ctx); err != nil {
		t.Fatalf("WaitAuxIndexes(3): %v", err)
	}
	if midxCalls.Load() != 2 || cgCalls.Load() != 1 {
		t.Fatalf("after MIDX-only rebuild: midx=%d cg=%d, want 2, 1", midxCalls.Load(), cgCalls.Load())
	}
}

type spyBareRepoExecutor struct {
	*gitexec.SubprocessExecutor

	initBareCalls    atomic.Int32
	forEachRefCalls  atomic.Int32
	updateRefCalls   atomic.Int32
	symbolicRefCalls atomic.Int32

	mu           sync.Mutex
	updateRefErr error
}

func newSpyBareRepoExecutor() *spyBareRepoExecutor {
	return &spyBareRepoExecutor{
		SubprocessExecutor: gitexec.NewSubprocessExecutor(gitexec.Config{}),
	}
}

func (s *spyBareRepoExecutor) ExecInitBare(ctx context.Context, req gitexec.InitBareRequest) error {
	s.initBareCalls.Add(1)
	return s.SubprocessExecutor.ExecInitBare(ctx, req)
}

func (s *spyBareRepoExecutor) ExecForEachRef(ctx context.Context, req gitexec.ForEachRefRequest) (map[string]string, error) {
	s.forEachRefCalls.Add(1)
	return s.SubprocessExecutor.ExecForEachRef(ctx, req)
}

func (s *spyBareRepoExecutor) ExecUpdateRef(ctx context.Context, req gitexec.UpdateRefRequest) error {
	s.updateRefCalls.Add(1)
	s.mu.Lock()
	err := s.updateRefErr
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return s.SubprocessExecutor.ExecUpdateRef(ctx, req)
}

func (s *spyBareRepoExecutor) ExecSymbolicRef(ctx context.Context, req gitexec.SymbolicRefRequest) error {
	s.symbolicRefCalls.Add(1)
	return s.SubprocessExecutor.ExecSymbolicRef(ctx, req)
}

func (s *spyBareRepoExecutor) resetCounts() {
	s.initBareCalls.Store(0)
	s.forEachRefCalls.Store(0)
	s.updateRefCalls.Store(0)
	s.symbolicRefCalls.Store(0)
}

func TestMaterializer_ReftableSyncStateSkipsSubprocessesWhenUnchanged(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "reftable-cache")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	oid1, pack1Bytes, idx1Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v1\n", "commit 1\n")
	oid2, pack2Bytes, idx2Bytes := buildSyntheticCommitPackForSync(t, pack.SHA1, "v2\n", "commit 2\n")
	pack1 := strings.Repeat("1", 64)
	pack2 := strings.Repeat("2", 64)

	metaStore := newFakeMetadataStore()
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
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FC1", PackID: pack1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})

	packStore := newFakePackFetcher()
	packStore.Put(pack1, pack1Bytes, idx1Bytes)
	packStore.Put(pack2, pack2Bytes, idx2Bytes)

	spyExec := newSpyBareRepoExecutor()
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:        metaStore,
		Packs:       packStore,
		Cache:       c,
		GitExecutor: spyExec,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// 1. Initial sync initializes reftable and syncs refs + HEAD.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	res1.Handle.Release()
	if spyExec.initBareCalls.Load() != 1 || spyExec.forEachRefCalls.Load() != 1 || spyExec.updateRefCalls.Load() != 1 || spyExec.symbolicRefCalls.Load() != 1 {
		t.Fatalf("initial sync counts = init:%d foreach:%d update:%d symref:%d, want 1,1,1,1",
			spyExec.initBareCalls.Load(), spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}

	// 2. Second sync with unchanged refs + HEAD must spawn ZERO git ref subprocesses.
	spyExec.resetCounts()
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	res2.Handle.Release()
	if spyExec.initBareCalls.Load() != 0 || spyExec.forEachRefCalls.Load() != 0 || spyExec.updateRefCalls.Load() != 0 || spyExec.symbolicRefCalls.Load() != 0 {
		t.Fatalf("unchanged sync counts = init:%d foreach:%d update:%d symref:%d, want 0,0,0,0",
			spyExec.initBareCalls.Load(), spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}

	// 3. Ref update with unchanged DefaultBranch skips ExecForEachRef and ExecSymbolicRef.
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FC2", PackID: pack2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid2},
	})
	spyExec.resetCounts()
	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(3): %v", err)
	}
	res3.Handle.Release()
	if spyExec.forEachRefCalls.Load() != 0 || spyExec.updateRefCalls.Load() != 1 || spyExec.symbolicRefCalls.Load() != 0 {
		t.Fatalf("ref-change sync counts = foreach:%d update:%d symref:%d, want 0,1,0",
			spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}

	// 4. DefaultBranch change with unchanged refs calls only ExecSymbolicRef.
	metaStore.PutMeta(ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/trunk",
	})
	spyExec.resetCounts()
	res4, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(4): %v", err)
	}
	res4.Handle.Release()
	if spyExec.forEachRefCalls.Load() != 0 || spyExec.updateRefCalls.Load() != 0 || spyExec.symbolicRefCalls.Load() != 1 {
		t.Fatalf("head-change sync counts = foreach:%d update:%d symref:%d, want 0,0,1",
			spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}

	// 5. ExecUpdateRef error invalidates in-memory state so next sync re-reads from disk.
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: oid1},
	})
	spyExec.mu.Lock()
	spyExec.updateRefErr = errors.New("simulated update-ref failure")
	spyExec.mu.Unlock()
	if _, err := mat.SyncAndPin(ctx, rk); err == nil {
		t.Fatal("expected SyncAndPin error when ExecUpdateRef fails")
	}

	spyExec.mu.Lock()
	spyExec.updateRefErr = nil
	spyExec.mu.Unlock()
	spyExec.resetCounts()
	res5, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(5): %v", err)
	}
	res5.Handle.Release()
	if spyExec.forEachRefCalls.Load() != 1 || spyExec.updateRefCalls.Load() != 1 || spyExec.symbolicRefCalls.Load() != 1 {
		t.Fatalf("post-error recovery counts = foreach:%d update:%d symref:%d, want 1,1,1",
			spyExec.forEachRefCalls.Load(), spyExec.updateRefCalls.Load(), spyExec.symbolicRefCalls.Load())
	}
}

func TestMaterializer_LoadAuthoritativeRefsLinearizableCoalescing(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "coalesce-refs")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	oid1 := strings.Repeat("1", 40)
	oid2 := strings.Repeat("2", 40)
	meta := ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	}
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(meta)
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: oid1}})

	flight1Entered := make(chan struct{})
	releaseFlight1 := make(chan struct{})
	var callNum atomic.Int32
	metaStore.listRefsHook = func(_ context.Context, _ tenant.RepoKey) {
		if callNum.Add(1) == 1 {
			close(flight1Entered)
			<-releaseFlight1
		}
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  metaStore,
		Packs: newFakePackFetcher(),
		Cache: c,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// Start Flight 1.
	flight1Done := make(chan []ddb.RefState, 1)
	go func() {
		_, refs, loadErr := mat.LoadAuthoritativeRefs(ctx, rk)
		if loadErr != nil {
			t.Errorf("flight 1 LoadAuthoritativeRefs: %v", loadErr)
		}
		flight1Done <- refs
	}()
	<-flight1Entered

	// Commit new ref while Flight 1 is in-flight.
	metaStore.SetRefs(rk, []ddb.RefState{{Repo: rk, RefName: "refs/heads/main", OID: oid2}})

	// Launch 4 concurrent readers after the commit; they must NOT join Flight 1,
	// and must coalesce into a single Flight 2 that observes oid2.
	const readers = 4
	var wg sync.WaitGroup
	results := make([][]ddb.RefState, readers)
	for i := 0; i < readers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, refs, loadErr := mat.LoadAuthoritativeRefs(ctx, rk)
			if loadErr != nil {
				t.Errorf("reader %d LoadAuthoritativeRefs: %v", i, loadErr)
				return
			}
			results[i] = refs
		}()
	}

	// Give readers a moment to enqueue into the next-generation batch.
	time.Sleep(25 * time.Millisecond)
	close(releaseFlight1)

	<-flight1Done
	wg.Wait()

	for i, refs := range results {
		if len(refs) != 1 || refs[0].OID != oid2 {
			t.Fatalf("reader %d saw stale refs %+v, want oid2=%s", i, refs, oid2)
		}
	}
	// Expect exactly 2 ListRefs calls total: Flight 1 + one coalesced Flight 2.
	if got := metaStore.ListRefsCalls(); got != 2 {
		t.Fatalf("ListRefsCalls = %d, want 2 (1 for flight 1 + 1 coalesced flight 2)", got)
	}
}

func TestMaterializer_SyncManifestPacksParallelShards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rk, _ := tenant.NewRepoKey("acme", "parallel-manifest")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	const numShards = 4
	meta := ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 1, ManifestShards: numShards, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
	}
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(meta)
	packStore := newFakePackFetcher()

	packIDs := make([]string, numShards)
	for s := 0; s < numShards; s++ {
		_, pb, ib := buildSyntheticCommitPackForSync(t, pack.SHA1, fmt.Sprintf("shard-%d\n", s), fmt.Sprintf("commit %d\n", s))
		pid := strings.Repeat(fmt.Sprintf("%x", s+1), 64)
		packIDs[s] = pid
		packStore.Put(pid, pb, ib)
		metaStore.AddManifest(rk, s, ddb.PackManifest{
			Repo: rk, ULID: fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G5FD%d", s), PackID: pid, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
		})
	}

	// Barrier proves all 4 shards are queried concurrently; sequential execution will deadlock until ctx expires.
	var arrived atomic.Int32
	allArrived := make(chan struct{})
	metaStore.queryManifestHook = func(qCtx context.Context, _ int) {
		if arrived.Add(1) == numShards {
			close(allArrived)
		}
		select {
		case <-allArrived:
		case <-qCtx.Done():
		}
	}

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                   metaStore,
		Packs:                  packStore,
		Cache:                  c,
		PruneReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	res, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin: %v", err)
	}
	defer res.Handle.Release()

	if arrived.Load() < numShards {
		t.Fatalf("expected all %d manifest shards to query concurrently, got %d", numShards, arrived.Load())
	}
	for s, pid := range packIDs {
		if !c.HasPack(rk, pid) {
			t.Fatalf("expected shard %d pack %s installed in cache", s, pid)
		}
		wantWM := fmt.Sprintf("01ARZ3NDEKTSV4RRFFQ69G5FD%d", s)
		if gotWM := c.Watermark(rk, s); gotWM != wantWM {
			t.Fatalf("shard %d watermark = %q, want %q", s, gotWM, wantWM)
		}
	}
}

func TestMaterializer_VersionGatedCache_ZeroListRefsAndManifestQueries(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "versioned-cache")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	commit1, pack1, idx1 := buildSyntheticCommitPackForSync(t, pack.SHA1, "v1\n", "commit 1\n")
	commit2, pack2, idx2 := buildSyntheticCommitPackForSync(t, pack.SHA1, "v2\n", "commit 2\n")
	packID1 := strings.Repeat("1", 64)
	packID2 := strings.Repeat("2", 64)

	meta := ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 2, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
		RefVersion: 1, ManifestVersion: 1,
	}
	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(meta)
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: commit1},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5F01", PackID: packID1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore := newFakePackFetcher()
	packStore.Put(packID1, pack1, idx1)
	packStore.Put(packID2, pack2, idx2)

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                   metaStore,
		Packs:                  packStore,
		Cache:                  c,
		PruneReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	// 1. First LoadAuthoritativeRefs populates refCache (1 ListRefs).
	gotMeta, gotRefs, err := mat.LoadAuthoritativeRefs(ctx, rk)
	if err != nil {
		t.Fatalf("LoadAuthoritativeRefs(1): %v", err)
	}
	if gotMeta.RefVersion != 1 || len(gotRefs) != 1 || gotRefs[0].OID != commit1 {
		t.Fatalf("unexpected first load: meta=%+v refs=%+v", gotMeta, gotRefs)
	}
	if metaStore.ListRefsCalls() != 1 {
		t.Fatalf("ListRefsCalls = %d, want 1", metaStore.ListRefsCalls())
	}
	// Mutating returned slice must not corrupt cache.
	gotRefs[0].OID = "corrupted"

	// 2. Repeated LoadAuthoritativeRefs with unchanged RefVersion makes 0 ListRefs calls.
	_, gotRefs2, err := mat.LoadAuthoritativeRefs(ctx, rk)
	if err != nil {
		t.Fatalf("LoadAuthoritativeRefs(2): %v", err)
	}
	if len(gotRefs2) != 1 || gotRefs2[0].OID != commit1 {
		t.Fatalf("cached refs corrupted or wrong: %+v", gotRefs2)
	}
	if metaStore.ListRefsCalls() != 1 {
		t.Fatalf("ListRefsCalls after cached read = %d, want 1", metaStore.ListRefsCalls())
	}

	// 3. First SyncAndPin reuses refCache (0 extra ListRefs), syncs manifest shards, materializes reftable.
	res1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	res1.Handle.Release()
	if metaStore.ListRefsCalls() != 1 {
		t.Fatalf("ListRefsCalls after first SyncAndPin = %d, want 1", metaStore.ListRefsCalls())
	}
	manifestCallsAfterFirst := metaStore.QueryManifestCalls()
	if manifestCallsAfterFirst == 0 {
		t.Fatalf("expected initial manifest queries > 0")
	}

	// 4. Repeated SyncAndPin with unchanged versions hits fast path: 0 ListRefs, 0 QueryManifestShard.
	res2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	if res2.Meta.RefVersion != 1 || len(res2.Refs) != 1 || res2.Refs[0].OID != commit1 {
		res2.Handle.Release()
		t.Fatalf("unexpected fast-path SyncAndPin result: %+v", res2)
	}
	res2.Handle.Release()
	if metaStore.ListRefsCalls() != 1 {
		t.Fatalf("ListRefsCalls after fast-path SyncAndPin = %d, want 1", metaStore.ListRefsCalls())
	}
	if metaStore.QueryManifestCalls() != manifestCallsAfterFirst {
		t.Fatalf("QueryManifestCalls after fast-path SyncAndPin = %d, want %d", metaStore.QueryManifestCalls(), manifestCallsAfterFirst)
	}

	// 5. External RefVersion bump (1 -> 2, ManifestVersion unchanged) immediately reloads ListRefs & updates reftable with 0 QueryManifestShard.
	meta.RefVersion = 2
	metaStore.PutMeta(meta)
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: commit1},
		{Repo: rk, RefName: "refs/heads/release", OID: commit1},
	})

	_, gotRefs3, err := mat.LoadAuthoritativeRefs(ctx, rk)
	if err != nil {
		t.Fatalf("LoadAuthoritativeRefs(after external bump): %v", err)
	}
	if len(gotRefs3) != 2 || gotRefs3[1].RefName != "refs/heads/release" || gotRefs3[1].OID != commit1 {
		t.Fatalf("expected refs/heads/release=%s, got %+v", commit1, gotRefs3)
	}
	if metaStore.ListRefsCalls() != 2 {
		t.Fatalf("ListRefsCalls after external bump = %d, want 2", metaStore.ListRefsCalls())
	}

	res3, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(after external RefVersion bump): %v", err)
	}
	out, err := exec.Command("git", "-C", res3.Handle.Path(), "rev-parse", "refs/heads/release").CombinedOutput()
	res3.Handle.Release()
	if err != nil {
		t.Fatalf("git rev-parse: %v (%s)", err, string(out))
	}
	if strings.TrimSpace(string(out)) != commit1 {
		t.Fatalf("reftable refs/heads/release = %q, want %q", strings.TrimSpace(string(out)), commit1)
	}
	if metaStore.ListRefsCalls() != 2 {
		t.Fatalf("ListRefsCalls after SyncAndPin = %d, want 2", metaStore.ListRefsCalls())
	}
	if metaStore.QueryManifestCalls() != manifestCallsAfterFirst {
		t.Fatalf("QueryManifestCalls after ref-only bump = %d, want %d", metaStore.QueryManifestCalls(), manifestCallsAfterFirst)
	}

	// 6. External ManifestVersion bump (1 -> 2) triggers manifest sync and installs pack2.
	meta.ManifestVersion = 2
	metaStore.PutMeta(meta)
	metaStore.AddManifest(rk, 1, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5F02", PackID: packID2, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	res4, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(after ManifestVersion bump): %v", err)
	}
	res4.Handle.Release()
	if !c.HasPack(rk, packID2) {
		t.Fatalf("expected pack2 installed after ManifestVersion bump")
	}
	if metaStore.QueryManifestCalls() <= manifestCallsAfterFirst {
		t.Fatalf("expected QueryManifestCalls to increase after ManifestVersion bump")
	}
	if metaStore.ListRefsCalls() != 2 {
		t.Fatalf("ListRefsCalls after manifest-only bump = %d, want 2", metaStore.ListRefsCalls())
	}
	_ = commit2
}

func TestMaterializer_VersionZeroAlwaysQueries(t *testing.T) {
	ctx := context.Background()
	rk, _ := tenant.NewRepoKey("acme", "version-zero")
	c, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	commit1, pack1, idx1 := buildSyntheticCommitPackForSync(t, pack.SHA1, "v0\n", "commit 0\n")
	packID1 := strings.Repeat("a", 64)

	metaStore := newFakeMetadataStore()
	metaStore.PutMeta(ddb.RepoMeta{
		Repo: rk, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 1, ManifestShards: 1, WriteState: lease.ModeActive, DefaultBranch: "refs/heads/main",
		RefVersion: 0, ManifestVersion: 0,
	})
	metaStore.SetRefs(rk, []ddb.RefState{
		{Repo: rk, RefName: "refs/heads/main", OID: commit1},
	})
	metaStore.AddManifest(rk, 0, ddb.PackManifest{
		Repo: rk, ULID: "01ARZ3NDEKTSV4RRFFQ69G5F01", PackID: packID1, Status: ddb.PackStatusActive, OriginRegion: "us-east-1",
	})
	packStore := newFakePackFetcher()
	packStore.Put(packID1, pack1, idx1)

	mat, err := NewMaterializer(MaterializerConfig{
		Meta:                   metaStore,
		Packs:                  packStore,
		Cache:                  c,
		PruneReconcileInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	t.Cleanup(mat.Close)

	for i := 1; i <= 2; i++ {
		if _, _, err := mat.LoadAuthoritativeRefs(ctx, rk); err != nil {
			t.Fatalf("LoadAuthoritativeRefs(%d): %v", i, err)
		}
		if metaStore.ListRefsCalls() != i {
			t.Fatalf("ListRefsCalls = %d, want %d for version 0", metaStore.ListRefsCalls(), i)
		}
	}

	r1, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(1): %v", err)
	}
	r1.Handle.Release()
	qAfter1 := metaStore.QueryManifestCalls()

	r2, err := mat.SyncAndPin(ctx, rk)
	if err != nil {
		t.Fatalf("SyncAndPin(2): %v", err)
	}
	r2.Handle.Release()
	if metaStore.ListRefsCalls() != 4 {
		t.Fatalf("ListRefsCalls = %d, want 4 for version 0", metaStore.ListRefsCalls())
	}
	if metaStore.QueryManifestCalls() <= qAfter1 {
		t.Fatalf("QueryManifestCalls = %d, want > %d for version 0", metaStore.QueryManifestCalls(), qAfter1)
	}
}
