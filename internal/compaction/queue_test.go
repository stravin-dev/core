package compaction

import (
	"fmt"
	"sync"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func mustTestRepoKey(t *testing.T, tid, rid string) tenant.RepoKey {
	t.Helper()
	rk, err := tenant.NewRepoKey(tid, rid)
	if err != nil {
		t.Fatalf("NewRepoKey(%q, %q): %v", tid, rid, err)
	}
	return rk
}

func TestFairQueue_RoundRobinPreventsStarvation(t *testing.T) {
	q := NewFairQueue()

	noisy := mustTestRepoKey(t, "tenant-noisy", "monorepo")
	quiet := mustTestRepoKey(t, "tenant-quiet", "service")

	// Enqueue 10 shards for noisy tenant first, then 2 shards for quiet tenant.
	for shard := 0; shard < 10; shard++ {
		if !q.Enqueue(ShardTarget{Repo: noisy, Shard: shard, ManifestShards: 16, Tier: Tier0}) {
			t.Fatalf("expected enqueue for noisy shard %d to succeed", shard)
		}
	}
	for shard := 0; shard < 2; shard++ {
		if !q.Enqueue(ShardTarget{Repo: quiet, Shard: shard, ManifestShards: 4, Tier: Tier0}) {
			t.Fatalf("expected enqueue for quiet shard %d to succeed", shard)
		}
	}

	if q.Len() != 12 || q.TenantCount() != 2 {
		t.Fatalf("Len=%d TenantCount=%d, want 12 and 2", q.Len(), q.TenantCount())
	}

	// First 4 dequeues must interleave noisy and quiet (no starvation).
	var order []tenant.TenantID
	for i := 0; i < 4; i++ {
		item, ok := q.Dequeue()
		if !ok {
			t.Fatalf("unexpected empty dequeue at step %d", i)
		}
		order = append(order, item.Repo.TenantID)
	}

	want := []tenant.TenantID{"tenant-noisy", "tenant-quiet", "tenant-noisy", "tenant-quiet"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("dequeue order = %v, want prefix %v", order, want)
		}
	}

	// Drain remaining noisy items.
	for i := 0; i < 8; i++ {
		item, ok := q.Dequeue()
		if !ok || item.Repo.TenantID != "tenant-noisy" {
			t.Fatalf("step %d: got (%+v, %v), want tenant-noisy", i, item, ok)
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("expected empty queue after draining all items")
	}
}

func TestFairQueue_WeightedSchedulingAndDedup(t *testing.T) {
	q := NewFairQueue()
	if err := q.SetTenantWeight("tenant-pro", 2); err != nil {
		t.Fatalf("SetTenantWeight: %v", err)
	}
	if err := q.SetTenantWeight("tenant-pro", 0); err == nil {
		t.Fatal("expected error for zero weight")
	}

	pro := mustTestRepoKey(t, "tenant-pro", "app")
	free := mustTestRepoKey(t, "tenant-free", "lib")

	target := ShardTarget{Repo: pro, Shard: 0, ManifestShards: 4, Tier: Tier0}
	if !q.Enqueue(target) {
		t.Fatal("first enqueue should succeed")
	}
	if q.Enqueue(target) {
		t.Fatal("duplicate pending target should be deduplicated")
	}

	q.Enqueue(ShardTarget{Repo: pro, Shard: 1, ManifestShards: 4, Tier: Tier0})
	q.Enqueue(ShardTarget{Repo: pro, Shard: 2, ManifestShards: 4, Tier: Tier0})
	q.Enqueue(ShardTarget{Repo: free, Shard: 0, ManifestShards: 4, Tier: Tier0})

	// Pro has weight 2, free has weight 1 -> pro, pro, free, pro.
	want := []tenant.TenantID{"tenant-pro", "tenant-pro", "tenant-free", "tenant-pro"}
	for i, w := range want {
		got, ok := q.Dequeue()
		if !ok || got.Repo.TenantID != w {
			t.Fatalf("step %d: got (%v, %v), want %v", i, got.Repo.TenantID, ok, w)
		}
	}
}

func TestFairQueue_ConcurrentAccess(t *testing.T) {
	q := NewFairQueue()
	var wg sync.WaitGroup

	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			rk := mustTestRepoKey(t, fmt.Sprintf("tenant-%d", worker), "repo")
			for s := 0; s < 25; s++ {
				q.Enqueue(ShardTarget{Repo: rk, Shard: s, ManifestShards: 32, Tier: Tier0})
			}
		}(g)
	}
	wg.Wait()

	if q.Len() != 200 {
		t.Fatalf("Len() = %d, want 200", q.Len())
	}

	var count int
	var mu sync.Mutex
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if _, ok := q.Dequeue(); !ok {
					return
				}
				mu.Lock()
				count++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if count != 200 {
		t.Fatalf("dequeued %d items, want 200", count)
	}
}
