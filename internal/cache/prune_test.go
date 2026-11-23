package cache_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/telemetry"
)

func TestPrunePacksReclaimsUnderContinuousPinTraffic(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "demo")
	if err := c.InstallPack(repo, "p1", make([]byte, 100), make([]byte, 20)); err != nil {
		t.Fatalf("InstallPack p1: %v", err)
	}
	if err := c.InstallPack(repo, "p2", make([]byte, 200), make([]byte, 40)); err != nil {
		t.Fatalf("InstallPack p2: %v", err)
	}

	// Acquire a gen-0 pin simulating an in-flight reader before PrunePacks.
	gen0Lease, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin gen0: %v", err)
	}

	// Schedule p1 for pruning while gen0Lease is active.
	if err := c.PrunePacks(repo, []string{"p1"}); err != nil {
		t.Fatalf("PrunePacks p1: %v", err)
	}

	packDir := filepath.Join(root, "t", "local", "demo", "objects", "pack")
	p1Pack := filepath.Join(packDir, "pack-p1.pack")
	p1Idx := filepath.Join(packDir, "pack-p1.idx")
	p2Pack := filepath.Join(packDir, "pack-p2.pack")

	// While gen0Lease is still held, p1 must remain on disk so the reader never sees ENOENT.
	if _, err := os.Stat(p1Pack); err != nil {
		t.Fatalf("p1.pack unlinked while gen-0 reader still pinned: %v", err)
	}

	// A new reader arrives after PrunePacks (gen-1) so total pins never drop to 0.
	gen1Lease, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin gen1: %v", err)
	}
	defer gen1Lease.Release()

	// Release gen0Lease -> all prior-generation pins are now gone, even though gen1Lease keeps pins == 1.
	gen0Lease.Release()

	if _, err := os.Stat(p1Pack); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("p1.pack was not unlinked after gen-0 released (with gen-1 still pinned): err=%v", err)
	}
	if _, err := os.Stat(p1Idx); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("p1.idx was not unlinked after gen-0 released (with gen-1 still pinned): err=%v", err)
	}
	if _, err := os.Stat(p2Pack); err != nil {
		t.Fatalf("active pack p2.pack unexpectedly removed: %v", err)
	}
	if stats := c.Stats(); stats.TotalBytes != 240 {
		t.Fatalf("TotalBytes = %d, want 240", stats.TotalBytes)
	}
}

func TestPrunePacksMultiGenerationOrdering(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "multi-gen")
	for _, id := range []string{"p1", "p2", "p3"} {
		if err := c.InstallPack(repo, id, make([]byte, 50), make([]byte, 10)); err != nil {
			t.Fatalf("InstallPack %s: %v", id, err)
		}
	}

	packDir := filepath.Join(root, "t", "local", "multi-gen", "objects", "pack")
	p1Path := filepath.Join(packDir, "pack-p1.pack")
	p2Path := filepath.Join(packDir, "pack-p2.pack")
	p3Path := filepath.Join(packDir, "pack-p3.pack")

	g0, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin g0: %v", err)
	}
	if err := c.PrunePacks(repo, []string{"p1"}); err != nil {
		t.Fatalf("PrunePacks p1: %v", err)
	}

	g1, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin g1: %v", err)
	}
	if err := c.PrunePacks(repo, []string{"p2"}); err != nil {
		t.Fatalf("PrunePacks p2: %v", err)
	}

	g2, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin g2: %v", err)
	}
	defer g2.Release()

	// Re-pruning an already-pending pack must not advance its cutoff generation.
	if err := c.PrunePacks(repo, []string{"p2"}); err != nil {
		t.Fatalf("re-PrunePacks p2: %v", err)
	}

	// Releasing g0 unlinks p1, but p2 stays on disk because g1 is still held.
	g0.Release()
	if _, err := os.Stat(p1Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("p1 should be unlinked after g0 release, err=%v", err)
	}
	if _, err := os.Stat(p2Path); err != nil {
		t.Fatalf("p2 must remain on disk while g1 is pinned, err=%v", err)
	}

	// Releasing g1 unlinks p2 even while g2 remains pinned.
	g1.Release()
	if _, err := os.Stat(p2Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("p2 should be unlinked after g1 release (with g2 still pinned), err=%v", err)
	}
	if _, err := os.Stat(p3Path); err != nil {
		t.Fatalf("p3 must remain on disk, err=%v", err)
	}
}

func TestMaterializePruneUnlinksWhileNewerReaderPinned(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 0.50,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "mat-prune")
	if err := c.InstallPack(repo, "old1", make([]byte, 80), make([]byte, 20)); err != nil {
		t.Fatalf("InstallPack old1: %v", err)
	}

	// Materialize's syncFn installs new1 and schedules old1 for pruning.
	h1, err := c.Materialize(context.Background(), repo, func(ctx context.Context, repoPath string) error {
		if err := c.InstallPack(repo, "new1", make([]byte, 90), make([]byte, 20)); err != nil {
			return err
		}
		return c.PrunePacks(repo, []string{"old1"})
	})
	if err != nil {
		t.Fatalf("Materialize h1: %v", err)
	}

	// A newer reader materializes after PrunePacks so total pins stay > 0.
	h2, err := c.Materialize(context.Background(), repo, nil)
	if err != nil {
		h1.Release()
		t.Fatalf("Materialize h2: %v", err)
	}
	defer h2.Release()

	oldPath := filepath.Join(h1.Path(), "objects", "pack", "pack-old1.pack")
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("old1 should remain on disk while gen-0 h1 is still pinned: %v", err)
	}

	// Releasing h1 unlinks old1 even while newer-generation h2 stays pinned.
	h1.Release()
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old1 should be unlinked once prior-gen h1 releases (while h2 stays pinned), err=%v", err)
	}
}

func TestPrunePacksConcurrentSustainedLoadRace(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          10 << 20,
		MaxTenantFraction: 1.0,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "sustained-load")
	if err := c.InstallPack(repo, "gen0", []byte("pack-gen0"), []byte("idx-gen0")); err != nil {
		t.Fatalf("InstallPack gen0: %v", err)
	}

	packDir := filepath.Join(root, "t", "local", "sustained-load", "objects", "pack")
	stop := make(chan struct{})
	var readerErr atomic.Value
	var readerWG sync.WaitGroup

	// Keep continuous overlapping Pin/Release traffic so pins > 0 throughout the test.
	const numReaders = 8
	for i := 0; i < numReaders; i++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				h, err := c.Pin(repo)
				if err != nil {
					readerErr.Store(err)
					return
				}
				// Snapshot currently non-pending packs at the moment of pinning.
				ids := c.CachedPackIDs(repo)
				var visible []string
				for _, id := range ids {
					if c.HasPack(repo, id) {
						visible = append(visible, id)
					}
				}
				// Simulate reader work: every pack visible when we pinned must remain readable.
				time.Sleep(time.Millisecond)
				for _, id := range visible {
					p := filepath.Join(packDir, "pack-"+id+".pack")
					if _, err := os.ReadFile(p); err != nil {
						readerErr.Store(fmt.Errorf("pinned reader hit error reading %s: %w", id, err))
						h.Release()
						return
					}
				}
				h.Release()
			}
		}()
	}

	// Simulate 25 compaction cycles installing a replacement pack and pruning the previous pack.
	const rounds = 25
	prevID := "gen0"
	for r := 1; r <= rounds; r++ {
		nextID := fmt.Sprintf("gen%d", r)
		if err := c.InstallPack(repo, nextID, []byte("pack-"+nextID), []byte("idx-"+nextID)); err != nil {
			close(stop)
			readerWG.Wait()
			t.Fatalf("InstallPack %s: %v", nextID, err)
		}
		if err := c.PrunePacks(repo, []string{prevID}); err != nil {
			close(stop)
			readerWG.Wait()
			t.Fatalf("PrunePacks %s: %v", prevID, err)
		}
		prevID = nextID
		time.Sleep(2 * time.Millisecond)
	}

	// While readers are STILL running continuously (pins > 0), wait briefly for prior-gen pins
	// to drain and verify superseded packs are reclaimed without stopping traffic.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ids := c.CachedPackIDs(repo)
		if len(ids) == 1 && ids[0] == prevID {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	gotIDs := c.CachedPackIDs(repo)
	close(stop)
	readerWG.Wait()

	if v := readerErr.Load(); v != nil {
		t.Fatalf("in-flight reader failed during concurrent pruning: %v", v)
	}
	if len(gotIDs) != 1 || gotIDs[0] != prevID {
		t.Fatalf("superseded packs were not reclaimed under sustained load: cached=%v, want [%s]", gotIDs, prevID)
	}
}

func collectInt64Metrics(t *testing.T, prov *telemetry.Provider) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := prov.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	out := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				if len(data.DataPoints) > 0 {
					out[m.Name] = data.DataPoints[len(data.DataPoints)-1].Value
				}
			case metricdata.Sum[int64]:
				if len(data.DataPoints) > 0 {
					out[m.Name] = data.DataPoints[len(data.DataPoints)-1].Value
				}
			}
		}
	}
	return out
}

func TestCacheMetricsActiveAndPendingPrunePacks(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-cache-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	c, err := cache.New(cache.Config{
		Root:              t.TempDir(),
		MaxBytes:          1 << 20,
		MaxTenantFraction: 1.0,
		Telemetry:         prov,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repoA := mustRepoKey(t, "local", "repo-a")
	repoB := mustRepoKey(t, "local", "repo-b")
	for _, id := range []string{"a1", "a2", "a3"} {
		if err := c.InstallPack(repoA, id, []byte("pack"), []byte("idx")); err != nil {
			t.Fatalf("InstallPack %s: %v", id, err)
		}
	}
	if err := c.InstallPack(repoB, "b1", []byte("pack"), []byte("idx")); err != nil {
		t.Fatalf("InstallPack b1: %v", err)
	}

	// Pin repoA and schedule 2 packs for deferred pruning.
	gen0, err := c.Pin(repoA)
	if err != nil {
		t.Fatalf("Pin repoA: %v", err)
	}
	if err := c.PrunePacks(repoA, []string{"a1", "a2"}); err != nil {
		t.Fatalf("PrunePacks repoA: %v", err)
	}

	// Verify per-repo Stats and OTel gauges while pending prune is active.
	st := c.Stats()
	if st.ActivePacks != 4 || st.PendingPrunePacks != 2 {
		t.Fatalf("Stats totals = (active=%d, pending=%d), want (4, 2)", st.ActivePacks, st.PendingPrunePacks)
	}
	if rsA := st.RepoStats[repoA]; rsA.ActivePacks != 3 || rsA.PendingPrune != 2 || rsA.Pins != 1 {
		t.Fatalf("RepoStats[repoA] = %+v, want active=3 pending=2 pins=1", rsA)
	}
	if rsB := st.RepoStats[repoB]; rsB.ActivePacks != 1 || rsB.PendingPrune != 0 {
		t.Fatalf("RepoStats[repoB] = %+v, want active=1 pending=0", rsB)
	}

	got := collectInt64Metrics(t, prov)
	if got["stravin_cache_active_packs"] != 4 {
		t.Fatalf("stravin_cache_active_packs = %d, want 4", got["stravin_cache_active_packs"])
	}
	if got["stravin_cache_pending_prune_packs"] != 2 {
		t.Fatalf("stravin_cache_pending_prune_packs = %d, want 2", got["stravin_cache_pending_prune_packs"])
	}
	if got["stravin_cache_repo_active_packs_max"] != 3 {
		t.Fatalf("stravin_cache_repo_active_packs_max = %d, want 3", got["stravin_cache_repo_active_packs_max"])
	}
	if got["stravin_cache_repo_pending_prune_packs_max"] != 2 {
		t.Fatalf("stravin_cache_repo_pending_prune_packs_max = %d, want 2", got["stravin_cache_repo_pending_prune_packs_max"])
	}

	// Acquire gen-1 pin so repoA stays pinned, then release gen-0 -> a1, a2 unlink immediately.
	gen1, err := c.Pin(repoA)
	if err != nil {
		t.Fatalf("Pin gen1 repoA: %v", err)
	}
	defer gen1.Release()
	gen0.Release()

	got = collectInt64Metrics(t, prov)
	if got["stravin_cache_active_packs"] != 2 {
		t.Fatalf("after prune stravin_cache_active_packs = %d, want 2", got["stravin_cache_active_packs"])
	}
	if got["stravin_cache_pending_prune_packs"] != 0 {
		t.Fatalf("after prune stravin_cache_pending_prune_packs = %d, want 0", got["stravin_cache_pending_prune_packs"])
	}
	if got["stravin_cache_repo_active_packs_max"] != 1 {
		t.Fatalf("after prune stravin_cache_repo_active_packs_max = %d, want 1", got["stravin_cache_repo_active_packs_max"])
	}
	if got["stravin_cache_repo_pending_prune_packs_max"] != 0 {
		t.Fatalf("after prune stravin_cache_repo_pending_prune_packs_max = %d, want 0", got["stravin_cache_repo_pending_prune_packs_max"])
	}
	if got["stravin_cache_packs_pruned_total"] != 2 {
		t.Fatalf("after prune stravin_cache_packs_pruned_total = %d, want 2", got["stravin_cache_packs_pruned_total"])
	}
}

func TestActivePackIdxNamesAndMarkMIDXBuiltPreservesMIDXOnDeferredUnlink(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 1.0,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "midx-keep")
	if err := c.InstallPack(repo, "p1", make([]byte, 64), make([]byte, 16)); err != nil {
		t.Fatalf("InstallPack p1: %v", err)
	}
	if err := c.InstallPack(repo, "p2", make([]byte, 64), make([]byte, 16)); err != nil {
		t.Fatalf("InstallPack p2: %v", err)
	}

	// Gen-0 reader pins repo before prune.
	gen0, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin gen0: %v", err)
	}
	if err := c.PrunePacks(repo, []string{"p1"}); err != nil {
		t.Fatalf("PrunePacks p1: %v", err)
	}

	// ActivePackIdxNames must exclude pending-prune pack p1 and return gen >= 1.
	idxNames, gen := c.ActivePackIdxNames(repo)
	if len(idxNames) != 1 || idxNames[0] != "pack-p2.idx" {
		t.Fatalf("ActivePackIdxNames = %v, want [pack-p2.idx]", idxNames)
	}
	if gen == 0 {
		t.Fatalf("expected prune generation > 0 after PrunePacks, got %d", gen)
	}

	// Simulate building MIDX + .bitmap using --stdin-packs at gen while p1 is still pending prune.
	packDir := filepath.Join(root, "t", "local", "midx-keep", "objects", "pack")
	midxPath := filepath.Join(packDir, "multi-pack-index")
	bitmapPath := filepath.Join(packDir, "multi-pack-index-deadbeef.bitmap")
	if err := os.WriteFile(midxPath, []byte("MIDX-V1"), 0o600); err != nil {
		t.Fatalf("write midx: %v", err)
	}
	if err := os.WriteFile(bitmapPath, []byte("BITM-V1"), 0o600); err != nil {
		t.Fatalf("write bitmap: %v", err)
	}
	if ok := c.MarkMIDXBuilt(repo, gen); !ok {
		t.Fatalf("MarkMIDXBuilt(gen=%d) = false, want true", gen)
	}

	// Gen-1 reader keeps repo pinned while gen-0 releases -> p1 unlinks, MIDX/bitmap stay intact!
	gen1, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin gen1: %v", err)
	}
	defer gen1.Release()

	gen0.Release()

	if _, err := os.Stat(filepath.Join(packDir, "pack-p1.pack")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected pack-p1.pack unlinked after gen0 release, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(packDir, "pack-p1.idx")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected pack-p1.idx unlinked after gen0 release, err=%v", err)
	}
	if _, err := os.Stat(midxPath); err != nil {
		t.Fatalf("expected multi-pack-index preserved after safe deferred unlink, err=%v", err)
	}
	if _, err := os.Stat(bitmapPath); err != nil {
		t.Fatalf("expected .bitmap preserved after safe deferred unlink, err=%v", err)
	}
}

func TestMarkMIDXBuiltInvalidatesStaleGeneration(t *testing.T) {
	root := t.TempDir()
	c, err := cache.New(cache.Config{
		Root:              root,
		MaxBytes:          1 << 20,
		MaxTenantFraction: 1.0,
	})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}

	repo := mustRepoKey(t, "local", "midx-stale")
	if err := c.InstallPack(repo, "p1", make([]byte, 64), make([]byte, 16)); err != nil {
		t.Fatalf("InstallPack p1: %v", err)
	}
	if err := c.InstallPack(repo, "p2", make([]byte, 64), make([]byte, 16)); err != nil {
		t.Fatalf("InstallPack p2: %v", err)
	}

	gen0, err := c.Pin(repo)
	if err != nil {
		t.Fatalf("Pin gen0: %v", err)
	}
	defer gen0.Release()

	_, staleGen := c.ActivePackIdxNames(repo)
	if err := c.PrunePacks(repo, []string{"p1"}); err != nil {
		t.Fatalf("PrunePacks p1: %v", err)
	}

	packDir := filepath.Join(root, "t", "local", "midx-stale", "objects", "pack")
	midxPath := filepath.Join(packDir, "multi-pack-index")
	bitmapPath := filepath.Join(packDir, "multi-pack-index-cafe.bitmap")
	if err := os.WriteFile(midxPath, []byte("STALE-MIDX"), 0o600); err != nil {
		t.Fatalf("write midx: %v", err)
	}
	if err := os.WriteFile(bitmapPath, []byte("STALE-BITM"), 0o600); err != nil {
		t.Fatalf("write bitmap: %v", err)
	}

	if ok := c.MarkMIDXBuilt(repo, staleGen); ok {
		t.Fatalf("MarkMIDXBuilt(staleGen=%d) = true, want false after prune", staleGen)
	}
	if _, err := os.Stat(midxPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected stale multi-pack-index invalidated, err=%v", err)
	}
	if _, err := os.Stat(bitmapPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected stale .bitmap invalidated, err=%v", err)
	}
}
