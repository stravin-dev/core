package cache_test

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/tenant"
)

func mustRepoKey(t *testing.T, tid, rid string) tenant.RepoKey {
	t.Helper()
	k, err := tenant.NewRepoKey(tid, rid)
	if err != nil {
		t.Fatalf("NewRepoKey(%q, %q): %v", tid, rid, err)
	}
	return k
}

type testPackEntry struct {
	typ     pack.ObjectType
	refBase pack.OID
	data    []byte
}

func buildTestPackAndIdx(t *testing.T, entries []testPackEntry) ([]byte, []byte, *pack.ParsedPack) {
	t.Helper()
	packBytes := buildTestRawPack(t, entries)
	parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("pack.Parse: %v", err)
	}
	idxBytes, err := pack.BuildIdx(parsed)
	if err != nil {
		t.Fatalf("pack.BuildIdx: %v", err)
	}
	return packBytes, idxBytes, parsed
}

func buildTestRawPack(t *testing.T, entries []testPackEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))

	for _, e := range entries {
		size := len(e.data)
		b := (byte(e.typ)&0x07)<<4 | byte(size&0x0f)
		size >>= 4
		for size > 0 {
			buf.WriteByte(b | 0x80)
			b = byte(size & 0x7f)
			size >>= 7
		}
		buf.WriteByte(b)

		if e.typ == pack.ObjRefDelta {
			buf.Write(e.refBase.Bytes())
		}

		zw := zlib.NewWriter(&buf)
		if _, err := zw.Write(e.data); err != nil {
			t.Fatalf("zlib write: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("zlib close: %v", err)
		}
	}

	sum := sha1.Sum(buf.Bytes())
	buf.Write(sum[:])
	return buf.Bytes()
}

func buildDeltaCopyInsert(baseLen, copyLen int, insert []byte) []byte {
	var buf bytes.Buffer
	writeVarint := func(v int) {
		for {
			b := byte(v & 0x7f)
			v >>= 7
			if v > 0 {
				b |= 0x80
			}
			buf.WriteByte(b)
			if v == 0 {
				break
			}
		}
	}
	writeVarint(baseLen)
	writeVarint(copyLen + len(insert))

	if copyLen > 0 {
		cmd := byte(0x80)
		var args []byte
		if copyLen&0xff != 0 {
			cmd |= 0x10
			args = append(args, byte(copyLen&0xff))
		}
		if (copyLen>>8)&0xff != 0 {
			cmd |= 0x20
			args = append(args, byte((copyLen>>8)&0xff))
		}
		buf.WriteByte(cmd)
		buf.Write(args)
	}
	if len(insert) > 0 {
		buf.WriteByte(byte(len(insert)))
		buf.Write(insert)
	}
	return buf.Bytes()
}

func TestBareRepoMaterializationLayout(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.20,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "acme", "core")
	handle, err := c.Materialize(context.Background(), repo, func(ctx context.Context, repoPath string) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Materialize: %v", err)
	}
	defer handle.Release()

	wantPath := filepath.Join(root, "t", "acme", "core")
	if handle.Path() != wantPath {
		t.Fatalf("handle.Path() = %q, want %q", handle.Path(), wantPath)
	}

	for _, rel := range []string{"HEAD", "config"} {
		info, err := os.Stat(filepath.Join(wantPath, rel))
		if err != nil || info.IsDir() {
			t.Fatalf("expected file %s: err=%v", rel, err)
		}
	}
	for _, rel := range []string{"objects/pack", "refs", "refs/heads", "refs/tags"} {
		info, err := os.Stat(filepath.Join(wantPath, rel))
		if err != nil || !info.IsDir() {
			t.Fatalf("expected dir %s: err=%v", rel, err)
		}
	}

	// Verify invalid RepoKey and packID are rejected.
	if err := c.InstallPack(tenant.RepoKey{TenantID: "../bad", RepoID: "r"}, "abc123", []byte("p"), []byte("i")); err == nil {
		t.Fatal("expected error for invalid tenant id")
	}
	if err := c.InstallPack(repo, "../escape", []byte("p"), []byte("i")); !errors.Is(err, cache.ErrInvalidPackID) {
		t.Fatalf("expected ErrInvalidPackID, got %v", err)
	}
}

func TestAtomicInstallPackAndAccounting(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "tenant-a", "repo-1")
	packData := bytes.Repeat([]byte("P"), 128)
	idxData := bytes.Repeat([]byte("I"), 64)

	if err := c.InstallPack(repo, "pack001", packData, idxData); err != nil {
		t.Fatalf("InstallPack: %v", err)
	}

	packDir := filepath.Join(root, "t", "tenant-a", "repo-1", "objects", "pack")
	gotPack, err := os.ReadFile(filepath.Join(packDir, "pack-pack001.pack"))
	if err != nil || !bytes.Equal(gotPack, packData) {
		t.Fatalf("pack file mismatch: err=%v len=%d", err, len(gotPack))
	}
	gotIdx, err := os.ReadFile(filepath.Join(packDir, "pack-pack001.idx"))
	if err != nil || !bytes.Equal(gotIdx, idxData) {
		t.Fatalf("idx file mismatch: err=%v len=%d", err, len(gotIdx))
	}

	// No temp files left behind.
	entries, err := os.ReadDir(packDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected exactly 2 files in objects/pack, got %d", len(entries))
	}

	stats := c.Stats()
	if stats.TotalBytes != 192 || stats.TenantBytes["tenant-a"] != 192 {
		t.Fatalf("unexpected byte stats: %+v", stats)
	}
	if !c.HasPack(repo, "pack001") {
		t.Fatal("HasPack returned false for installed pack")
	}

	// Reinstalling the same packID must be idempotent in byte accounting.
	if err := c.InstallPack(repo, "pack001", packData, idxData); err != nil {
		t.Fatalf("re-InstallPack: %v", err)
	}
	stats = c.Stats()
	if stats.TotalBytes != 192 {
		t.Fatalf("double-counted bytes on reinstall: got %d, want 192", stats.TotalBytes)
	}
}

func TestConcurrentColdMaterializationSingleflight(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "acme", "hot-repo")
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})

	const concurrency = 16
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	paths := make(chan string, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := c.Materialize(context.Background(), repo, func(ctx context.Context, repoPath string) error {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return c.InstallPack(repo, "p1", []byte("pack-bytes"), []byte("idx-bytes"))
			})
			if err != nil {
				errs <- err
				return
			}
			paths <- h.Path()
			h.Release()
		}()
	}

	<-started
	// Give other goroutines time to join the in-flight call.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	close(paths)

	for err := range errs {
		t.Fatalf("concurrent Materialize error: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("materializer ran %d times, want 1 (singleflight coalescing)", got)
	}
	wantPath := filepath.Join(root, "t", "acme", "hot-repo")
	count := 0
	for p := range paths {
		if p != wantPath {
			t.Fatalf("path = %q, want %q", p, wantPath)
		}
		count++
	}
	if count != concurrency {
		t.Fatalf("got %d handles, want %d", count, concurrency)
	}
}

func TestPerTenantQuotaEvictionIsolation(t *testing.T) {
	root := t.TempDir()
	// Global cap = 1000 bytes, per-tenant cap = 20% = 200 bytes.
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1000,
		MaxTenantFraction: 0.20,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	// Quiet tenant B installs a 150-byte repo first (oldest globally!).
	repoB := mustRepoKey(t, "tenant-b", "quiet-repo")
	if err := c.InstallPack(repoB, "b1", make([]byte, 100), make([]byte, 50)); err != nil {
		t.Fatalf("InstallPack B: %v", err)
	}

	// Noisy tenant A installs 3 repos of 100 bytes each (300 > 200 cap).
	repoA1 := mustRepoKey(t, "tenant-a", "mono-1")
	repoA2 := mustRepoKey(t, "tenant-a", "mono-2")
	repoA3 := mustRepoKey(t, "tenant-a", "mono-3")

	if err := c.InstallPack(repoA1, "a1", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack A1: %v", err)
	}
	if err := c.InstallPack(repoA2, "a2", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack A2: %v", err)
	}
	if err := c.InstallPack(repoA3, "a3", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack A3: %v", err)
	}

	stats := c.Stats()
	if stats.TenantBytes["tenant-a"] > 200 {
		t.Fatalf("tenant-a exceeded 20%% quota: got %d bytes, cap 200", stats.TenantBytes["tenant-a"])
	}
	// Tenant A's oldest repo (mono-1) must be evicted, while mono-2 and mono-3 stay.
	if c.HasPack(repoA1, "a1") {
		t.Fatal("expected tenant-a/mono-1 to be evicted")
	}
	if _, err := os.Stat(filepath.Join(root, "t", "tenant-a", "mono-1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected evicted repo dir to be removed, stat err=%v", err)
	}
	if !c.HasPack(repoA2, "a2") || !c.HasPack(repoA3, "a3") {
		t.Fatal("expected tenant-a/mono-2 and mono-3 to remain cached")
	}
	// Quiet tenant B MUST NOT be evicted even though it was older than tenant A's repos.
	if !c.HasPack(repoB, "b1") {
		t.Fatal("noisy neighbor tenant-a evicted quiet tenant-b!")
	}
}

func TestReaderLeasePinPreventsEvictionAndDefersPackPruning(t *testing.T) {
	root := t.TempDir()
	// Global cap = 500 bytes, tenant cap = 20% = 100 bytes.
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          500,
		MaxTenantFraction: 0.20,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo1 := mustRepoKey(t, "tenant-a", "repo-1")
	repo2 := mustRepoKey(t, "tenant-a", "repo-2")

	if err := c.InstallPack(repo1, "p1", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack repo1: %v", err)
	}

	// Pin repo1 simulating an in-flight git-upload-pack reader.
	lease, err := c.Pin(repo1)
	if err != nil {
		t.Fatalf("Pin repo1: %v", err)
	}

	// Installing repo2 pushes tenant-a to 200 > 100, but repo1 is pinned and MUST NOT be evicted.
	if err := c.InstallPack(repo2, "p2", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack repo2: %v", err)
	}
	if !c.HasPack(repo1, "p1") {
		t.Fatal("pinned repo1 was evicted mid-read!")
	}

	// Queue superseded pack pruning while pinned — file must remain on disk until Release.
	if err := c.PrunePacks(repo1, []string{"p1"}); err != nil {
		t.Fatalf("PrunePacks while pinned: %v", err)
	}
	packDir := filepath.Join(root, "t", "tenant-a", "repo-1", "objects", "pack")
	packPath := filepath.Join(packDir, "pack-p1.pack")
	midxPath := filepath.Join(packDir, "multi-pack-index")
	bitmapPath := filepath.Join(packDir, "multi-pack-index-deadbeef.bitmap")
	if err := os.WriteFile(midxPath, []byte("MIDX"), 0o600); err != nil {
		t.Fatalf("write midx: %v", err)
	}
	if err := os.WriteFile(bitmapPath, []byte("BITM"), 0o600); err != nil {
		t.Fatalf("write bitmap: %v", err)
	}
	if _, err := os.Stat(packPath); err != nil {
		t.Fatalf("pinned pack unlinked before lease release: %v", err)
	}

	// Release lease -> deferred pack prune executes immediately and invalidates stale MIDX/bitmap.
	lease.Release()
	if _, err := os.Stat(packPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deferred pack prune did not unlink pack after Release: err=%v", err)
	}
	if _, err := os.Stat(midxPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deferred pack prune did not invalidate stale multi-pack-index: err=%v", err)
	}
	if _, err := os.Stat(bitmapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deferred pack prune did not invalidate stale midx bitmap: err=%v", err)
	}

	// Reinstall p1 while repo2 is pinned so both remain > quota, then unpin both and call Evict().
	h2, err := c.Pin(repo2)
	if err != nil {
		t.Fatalf("Pin repo2: %v", err)
	}
	if err := c.InstallPack(repo1, "p1", make([]byte, 60), make([]byte, 40)); err != nil {
		t.Fatalf("re-InstallPack repo1: %v", err)
	}
	// Touch repo2 again so repo1 is strictly older, then release repo2.
	h2Again, err := c.Pin(repo2)
	if err != nil {
		t.Fatalf("Pin repo2 again: %v", err)
	}
	h2.Release()
	h2Again.Release()

	evicted := c.Evict()
	if len(evicted) != 1 || evicted[0] != repo1 {
		t.Fatalf("Evict() = %v, want [%v]", evicted, repo1)
	}
}

func TestGlobalCapLRUEvictionAcrossTenants(t *testing.T) {
	root := t.TempDir()
	// Global cap = 500 bytes, per-tenant cap = 20% = 100 bytes.
	// 6 tenants each install 100 bytes (600 > 500 global cap, all within per-tenant cap).
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          500,
		MaxTenantFraction: 0.20,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	tenants := []string{"t1", "t2", "t3", "t4", "t5", "t6"}
	for _, tid := range tenants {
		repo := mustRepoKey(t, tid, "repo")
		if err := c.InstallPack(repo, "p1", make([]byte, 60), make([]byte, 40)); err != nil {
			t.Fatalf("InstallPack %s: %v", tid, err)
		}
	}

	stats := c.Stats()
	if stats.TotalBytes > 500 {
		t.Fatalf("TotalBytes = %d, want <= 500", stats.TotalBytes)
	}
	// Oldest tenant (t1) must have been evicted; newest (t6) must remain.
	if c.HasPack(mustRepoKey(t, "t1", "repo"), "p1") {
		t.Fatal("expected oldest tenant t1 to be evicted under global cap")
	}
	if !c.HasPack(mustRepoKey(t, "t6", "repo"), "p1") {
		t.Fatal("expected newest tenant t6 to remain cached")
	}
}

func TestSingleflightWaiterContextCancelDoesNotAbortLeader(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "acme", "slow-sync")
	leaderStarted := make(chan struct{})
	allowFinish := make(chan struct{})

	leaderDone := make(chan error, 1)
	go func() {
		h, err := c.Materialize(context.Background(), repo, func(ctx context.Context, repoPath string) error {
			close(leaderStarted)
			<-allowFinish
			return c.InstallPack(repo, "p1", []byte("pack"), []byte("idx"))
		})
		if h != nil {
			h.Release()
		}
		leaderDone <- err
	}()

	<-leaderStarted

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Materialize(waitCtx, repo, func(ctx context.Context, repoPath string) error {
		t.Error("waiter callback should never execute")
		return nil
	}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err = %v, want context.DeadlineExceeded", err)
	}

	close(allowFinish)
	if err := <-leaderDone; err != nil {
		t.Fatalf("leader Materialize failed: %v", err)
	}
	if !c.HasPack(repo, "p1") {
		t.Fatal("expected leader pack p1 to be installed")
	}
}

func TestConfigValidation(t *testing.T) {
	if _, err := cache.New(cache.Config{}); !errors.Is(err, cache.ErrEmptyRoot) {
		t.Fatalf("empty root: got %v, want ErrEmptyRoot", err)
	}
	if _, err := cache.New(cache.Config{Root: t.TempDir(), MaxBytes: -1}); !errors.Is(err, cache.ErrInvalidConfig) {
		t.Fatalf("negative max bytes: got %v, want ErrInvalidConfig", err)
	}
	if _, err := cache.New(cache.Config{Root: t.TempDir(), MaxTenantFraction: 1.5}); !errors.Is(err, cache.ErrInvalidConfig) {
		t.Fatalf("invalid fraction: got %v, want ErrInvalidConfig", err)
	}
}

func TestWatermarkMonotonicPersistence(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "acme", "payments")
	if err := c.InstallPack(repo, "p1", []byte("pack"), []byte("idx")); err != nil {
		t.Fatalf("InstallPack: %v", err)
	}

	if got := c.Watermark(repo, 0); got != "" {
		t.Fatalf("initial watermark = %q, want empty", got)
	}
	if !c.AdvanceWatermark(repo, 0, "01J00000000000000000000001") {
		t.Fatal("expected first AdvanceWatermark to succeed")
	}
	if !c.AdvanceWatermark(repo, 0, "01J00000000000000000000005") {
		t.Fatal("expected monotonic AdvanceWatermark to succeed")
	}
	// Stale and equal ULIDs must be rejected.
	if c.AdvanceWatermark(repo, 0, "01J00000000000000000000003") {
		t.Fatal("stale ULID must not regress watermark")
	}
	if c.AdvanceWatermark(repo, 0, "01J00000000000000000000005") {
		t.Fatal("equal ULID must return false")
	}
	if c.AdvanceWatermark(repo, 0, "") {
		t.Fatal("empty ULID must return false")
	}
	if !c.AdvanceWatermark(repo, 2, "01J00000000000000000000009") {
		t.Fatal("expected shard 2 AdvanceWatermark to succeed")
	}

	// Re-open cache on the same root directory and verify watermarks + packs persisted.
	c2, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("reopen cache.New: %v", err)
	}
	if got := c2.Watermark(repo, 0); got != "01J00000000000000000000005" {
		t.Fatalf("persisted shard 0 watermark = %q, want 01J00000000000000000000005", got)
	}
	if got := c2.Watermark(repo, 2); got != "01J00000000000000000000009" {
		t.Fatalf("persisted shard 2 watermark = %q, want 01J00000000000000000000009", got)
	}
	if !c2.HasPack(repo, "p1") {
		t.Fatal("expected persisted pack p1 to be discovered on reopen")
	}
	wms := c2.Watermarks(repo)
	if len(wms) != 2 {
		t.Fatalf("Watermarks len = %d, want 2", len(wms))
	}
}

func TestBaseResolverAndThinPackCompletion(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "acme", "git-engine")
	baseBlob := []byte("hello from warm nvme cache base pack!\n")
	baseOID := pack.ComputeOID(pack.ObjBlob, baseBlob)

	basePack, baseIdx, _ := buildTestPackAndIdx(t, []testPackEntry{
		{typ: pack.ObjBlob, data: baseBlob},
	})
	if err := c.InstallPack(repo, "base01", basePack, baseIdx); err != nil {
		t.Fatalf("InstallPack base: %v", err)
	}

	// Verify direct ResolveBase.
	resolver := c.BaseResolver(repo)
	var _ pack.BaseResolver = resolver

	gotType, gotData, err := resolver.ResolveBase(baseOID)
	if err != nil {
		t.Fatalf("ResolveBase: %v", err)
	}
	if gotType != pack.ObjBlob || !bytes.Equal(gotData, baseBlob) {
		t.Fatalf("ResolveBase mismatch: type=%v data=%q", gotType, gotData)
	}

	// Unknown OID returns pack.ErrObjectNotFound.
	missingOID := pack.ComputeOID(pack.ObjBlob, []byte("nonexistent"))
	if _, _, err := resolver.ResolveBase(missingOID); !errors.Is(err, pack.ErrObjectNotFound) {
		t.Fatalf("ResolveBase(missing) = %v, want pack.ErrObjectNotFound", err)
	}

	// End-to-end thin-pack completion (Issue #11 write path contract).
	suffix := []byte("pushed delta line\n")
	delta := buildDeltaCopyInsert(len(baseBlob), len(baseBlob), suffix)
	expectedDerived := append(append([]byte(nil), baseBlob...), suffix...)
	derivedOID := pack.ComputeOID(pack.ObjBlob, expectedDerived)

	thinPack := buildTestRawPack(t, []testPackEntry{
		{typ: pack.ObjRefDelta, refBase: baseOID, data: delta},
	})
	completer := pack.NewPureGoThinCompleter(pack.DefaultLimits())
	completed, err := completer.Complete(context.Background(), thinPack, resolver)
	if err != nil {
		t.Fatalf("Complete thin pack via cache BaseResolver: %v", err)
	}

	parsed, err := pack.Parse(bytes.NewReader(completed), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("Parse completed pack: %v", err)
	}
	if obj := parsed.Objects[derivedOID]; obj == nil || !bytes.Equal(obj.Data, expectedDerived) {
		t.Fatalf("completed thin pack missing expected derived blob %s", derivedOID)
	}
}
