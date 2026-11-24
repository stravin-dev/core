package compaction

import (
	"context"
	"fmt"
	"testing"
	"time"

	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

func seedTestPacks(t *testing.T, clk *staticClock, rk tenant.RepoKey, manifests *fakeManifestStore, objects *fakePackObjectStore, count int) {
	t.Helper()
	ctx := context.Background()
	var parent *pack.OID
	for i := 0; i < count; i++ {
		objs, commitOID := buildCommitTreeBlob(
			t,
			fmt.Sprintf("file-%03d.txt", i),
			fmt.Sprintf("content-%03d\n", i),
			fmt.Sprintf("commit %d", i),
			parent,
		)
		c := commitOID
		parent = &c
		packBytes, idxBytes, packSHA := buildTestGitPack(t, objs...)
		if err := objects.PutPack(ctx, rk, packSHA, packBytes, idxBytes); err != nil {
			t.Fatalf("PutPack(%d): %v", i, err)
		}
		manifests.put(ddb.PackManifest{
			Repo:         rk,
			ULID:         fmt.Sprintf("01J0000000000000000000%04d", i+1),
			PackID:       packSHA,
			SizeBytes:    int64(len(packBytes)),
			OriginRegion: "us-east-1",
			Status:       ddb.PackStatusActive,
			CreatedAt:    clk.Now(),
		})
	}
}

type countingStrategy struct {
	inner CompactionStrategy
	calls int
}

func (c *countingStrategy) Tier() Tier { return c.inner.Tier() }

func (c *countingStrategy) Compact(ctx context.Context, req StrategyRequest) (StrategyResult, error) {
	c.calls++
	return c.inner.Compact(ctx, req)
}

func TestWorker_StepCompactionDrainsConsecutiveTier0Batches(t *testing.T) {
	t.Run("drains until partial batch", func(t *testing.T) {
		clk := newStaticClock(time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC))
		rk := mustTestRepoKey(t, "tenant-drain", "repo-partial")
		leaseStore := newFakeLeaseStore()
		manifests := newFakeManifestStore(1)
		objects := newFakePackObjectStore(clk)

		// MaxBatchSize=3 with 4 packs:
		// Batch 1: 3 packs -> 1 merged pack (2 active remain: 1 old + 1 merged)
		// Batch 2: 2 packs (< MaxBatchSize) -> 1 merged pack (1 active remains) -> stops immediately.
		seedTestPacks(t, clk, rk, manifests, objects, 4)

		leaseMgr, err := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w-drain-1", TTL: 5 * time.Second})
		if err != nil {
			t.Fatalf("NewLeaseManager: %v", err)
		}
		tier0, err := NewTier0Strategy(objects, pack.DefaultLimits())
		if err != nil {
			t.Fatalf("NewTier0Strategy: %v", err)
		}
		counted := &countingStrategy{inner: tier0}
		engine, err := NewEngine(EngineConfig{
			Leases:        leaseMgr,
			Manifests:     manifests,
			ManifestGC:    manifests,
			MinPacksTier0: 2,
			MaxBatchSize:  3,
			Clock:         clk,
			Strategies:    map[Tier]CompactionStrategy{Tier0: counted},
		})
		if err != nil {
			t.Fatalf("NewEngine: %v", err)
		}
		worker, err := NewWorker(WorkerConfig{
			Engine: engine,
			Queue:  NewFairQueue(),
		})
		if err != nil {
			t.Fatalf("NewWorker: %v", err)
		}
		worker.Queue().Enqueue(ShardTarget{
			Repo:           rk,
			Shard:          0,
			ManifestShards: 1,
			OriginRegion:   "us-east-1",
			Tier:           Tier0,
		})

		res, ran, err := worker.StepCompaction(context.Background())
		if err != nil {
			t.Fatalf("StepCompaction: %v", err)
		}
		if !ran || res.Skipped {
			t.Fatalf("expected ran=true, skipped=false, got ran=%v res=%+v", ran, res)
		}
		if counted.calls != 2 {
			t.Fatalf("strategy calls = %d, want 2", counted.calls)
		}
		if res.InputPacks != 5 {
			t.Fatalf("total InputPacks = %d, want 5 (3 + 2)", res.InputPacks)
		}
		active, err := manifests.QueryManifestShard(context.Background(), rk, 0, "", true)
		if err != nil {
			t.Fatalf("QueryManifestShard: %v", err)
		}
		if len(active) != 1 {
			t.Fatalf("active packs remaining = %d, want 1", len(active))
		}
	})

	t.Run("stops when follow-up batch is skipped", func(t *testing.T) {
		clk := newStaticClock(time.Date(2026, 11, 23, 12, 5, 0, 0, time.UTC))
		rk := mustTestRepoKey(t, "tenant-drain", "repo-skipped")
		leaseStore := newFakeLeaseStore()
		manifests := newFakeManifestStore(1)
		objects := newFakePackObjectStore(clk)

		// MaxBatchSize=3 with 3 packs:
		// Batch 1: 3 packs (full batch) -> 1 merged pack
		// Batch 2: 1 pack (< MinPacksTier0=2) -> Skipped -> stops and preserves Batch 1 result.
		seedTestPacks(t, clk, rk, manifests, objects, 3)

		leaseMgr, _ := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w-drain-2", TTL: 5 * time.Second})
		tier0, _ := NewTier0Strategy(objects, pack.DefaultLimits())
		counted := &countingStrategy{inner: tier0}
		engine, err := NewEngine(EngineConfig{
			Leases:        leaseMgr,
			Manifests:     manifests,
			ManifestGC:    manifests,
			MinPacksTier0: 2,
			MaxBatchSize:  3,
			Clock:         clk,
			Strategies:    map[Tier]CompactionStrategy{Tier0: counted},
		})
		if err != nil {
			t.Fatalf("NewEngine: %v", err)
		}
		worker, err := NewWorker(WorkerConfig{
			Engine: engine,
			Queue:  NewFairQueue(),
		})
		if err != nil {
			t.Fatalf("NewWorker: %v", err)
		}
		worker.Queue().Enqueue(ShardTarget{
			Repo:           rk,
			Shard:          0,
			ManifestShards: 1,
			OriginRegion:   "us-east-1",
			Tier:           Tier0,
		})

		res, ran, err := worker.StepCompaction(context.Background())
		if err != nil {
			t.Fatalf("StepCompaction: %v", err)
		}
		if !ran || res.Skipped {
			t.Fatalf("expected non-skipped result from batch 1, got ran=%v res=%+v", ran, res)
		}
		if counted.calls != 1 {
			t.Fatalf("strategy calls = %d, want 1", counted.calls)
		}
		if res.InputPacks != 3 || res.OutputPackID == "" {
			t.Fatalf("unexpected result: %+v", res)
		}
	})

	t.Run("caps consecutive full batches at maxConsecutiveTier0Batches", func(t *testing.T) {
		clk := newStaticClock(time.Date(2026, 11, 23, 12, 10, 0, 0, time.UTC))
		rk := mustTestRepoKey(t, "tenant-drain", "repo-capped")
		leaseStore := newFakeLeaseStore()
		manifests := newFakeManifestStore(1)
		objects := newFakePackObjectStore(clk)

		// MaxBatchSize=3 reduces active packs by 2 per batch.
		// With 25 packs, 10 batches reduce active packs by 20 -> 5 remain, capped at 10 calls.
		seedTestPacks(t, clk, rk, manifests, objects, 25)

		leaseMgr, _ := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w-drain-3", TTL: 5 * time.Second})
		tier0, _ := NewTier0Strategy(objects, pack.DefaultLimits())
		counted := &countingStrategy{inner: tier0}
		engine, err := NewEngine(EngineConfig{
			Leases:        leaseMgr,
			Manifests:     manifests,
			ManifestGC:    manifests,
			MinPacksTier0: 2,
			MaxBatchSize:  3,
			Clock:         clk,
			Strategies:    map[Tier]CompactionStrategy{Tier0: counted},
		})
		if err != nil {
			t.Fatalf("NewEngine: %v", err)
		}
		worker, err := NewWorker(WorkerConfig{
			Engine: engine,
			Queue:  NewFairQueue(),
		})
		if err != nil {
			t.Fatalf("NewWorker: %v", err)
		}
		worker.Queue().Enqueue(ShardTarget{
			Repo:           rk,
			Shard:          0,
			ManifestShards: 1,
			OriginRegion:   "us-east-1",
			Tier:           Tier0,
		})

		res, ran, err := worker.StepCompaction(context.Background())
		if err != nil {
			t.Fatalf("StepCompaction: %v", err)
		}
		if !ran || res.Skipped {
			t.Fatalf("expected ran=true, skipped=false, got ran=%v res=%+v", ran, res)
		}
		if counted.calls != maxConsecutiveTier0Batches {
			t.Fatalf("strategy calls = %d, want %d", counted.calls, maxConsecutiveTier0Batches)
		}
		active, err := manifests.QueryManifestShard(context.Background(), rk, 0, "", true)
		if err != nil {
			t.Fatalf("QueryManifestShard: %v", err)
		}
		if len(active) != 5 {
			t.Fatalf("active packs remaining = %d, want 5", len(active))
		}
	})
}
