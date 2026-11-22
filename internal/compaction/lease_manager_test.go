package compaction

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type fakeLeaseStore struct {
	mu           sync.Mutex
	leases       map[string]ddb.ShardLease
	renewCalls   atomic.Int64
	forceLost    atomic.Bool
	releaseCalls atomic.Int64
}

func newFakeLeaseStore() *fakeLeaseStore {
	return &fakeLeaseStore{
		leases: make(map[string]ddb.ShardLease),
	}
}

func leaseMapKey(repo tenant.RepoKey, shard int) string {
	return fmt.Sprintf("%s#%d", repo.DynamoPrefix(), shard)
}

func (f *fakeLeaseStore) AcquireShardLease(_ context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ddb.ShardLease, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := leaseMapKey(repo, shard)
	now := time.Now().UTC()
	if cur, ok := f.leases[k]; ok && cur.OwnerID != ownerID && cur.ExpiresAt.After(now) {
		return ddb.ShardLease{}, ddb.ErrLeaseHeld
	}
	l := ddb.ShardLease{
		Repo:      repo,
		Shard:     shard,
		OwnerID:   ownerID,
		ExpiresAt: now.Add(ttl),
	}
	f.leases[k] = l
	return l, nil
}

func (f *fakeLeaseStore) RenewShardLease(_ context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ddb.ShardLease, error) {
	f.renewCalls.Add(1)
	if f.forceLost.Load() {
		return ddb.ShardLease{}, ddb.ErrLeaseLost
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := leaseMapKey(repo, shard)
	cur, ok := f.leases[k]
	if !ok || cur.OwnerID != ownerID {
		return ddb.ShardLease{}, ddb.ErrLeaseLost
	}
	cur.ExpiresAt = time.Now().UTC().Add(ttl)
	f.leases[k] = cur
	return cur, nil
}

func (f *fakeLeaseStore) ReleaseShardLease(_ context.Context, repo tenant.RepoKey, shard int, ownerID string) error {
	f.releaseCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	k := leaseMapKey(repo, shard)
	cur, ok := f.leases[k]
	if !ok || cur.OwnerID != ownerID {
		return ddb.ErrLeaseLost
	}
	delete(f.leases, k)
	return nil
}

func TestLeaseManager_ContentionRenewalAndRelease(t *testing.T) {
	store := newFakeLeaseStore()
	rk := mustTestRepoKey(t, "t1", "repo1")

	mgr1, err := NewLeaseManager(store, LeaseManagerConfig{
		OwnerID:       "worker-1",
		TTL:           120 * time.Millisecond,
		RenewInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager(1): %v", err)
	}
	mgr2, err := NewLeaseManager(store, LeaseManagerConfig{
		OwnerID:       "worker-2",
		TTL:           120 * time.Millisecond,
		RenewInterval: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager(2): %v", err)
	}

	ctx := context.Background()
	held1, err := mgr1.Acquire(ctx, rk, 0)
	if err != nil {
		t.Fatalf("mgr1.Acquire: %v", err)
	}

	// Competing worker must hit ErrLeaseContention (wrapping ddb.ErrLeaseHeld).
	if _, err := mgr2.Acquire(ctx, rk, 0); !errors.Is(err, ErrLeaseContention) || !errors.Is(err, ddb.ErrLeaseHeld) {
		t.Fatalf("competing Acquire = %v, want ErrLeaseContention/ddb.ErrLeaseHeld", err)
	}

	// Wait for at least one renewal tick.
	deadline := time.Now().Add(300 * time.Millisecond)
	for store.renewCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if store.renewCalls.Load() == 0 {
		t.Fatal("expected background lease renewal to fire")
	}

	// Release lease and verify worker-2 can now acquire it.
	if err := held1.Release(ctx); err != nil {
		t.Fatalf("held1.Release: %v", err)
	}
	if err := held1.Release(ctx); err != nil {
		t.Fatalf("second held1.Release should be idempotent, got: %v", err)
	}

	held2, err := mgr2.Acquire(ctx, rk, 0)
	if err != nil {
		t.Fatalf("mgr2.Acquire after release: %v", err)
	}
	if err := held2.Release(ctx); err != nil {
		t.Fatalf("held2.Release: %v", err)
	}
}

func TestLeaseManager_CancelsContextOnLeaseLoss(t *testing.T) {
	store := newFakeLeaseStore()
	rk := mustTestRepoKey(t, "t1", "repo-loss")

	mgr, err := NewLeaseManager(store, LeaseManagerConfig{
		OwnerID:       "worker-1",
		TTL:           100 * time.Millisecond,
		RenewInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	held, err := mgr.Acquire(context.Background(), rk, 1)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer func() {
		_ = held.Release(context.Background())
	}()

	store.forceLost.Store(true)

	select {
	case <-held.Context().Done():
		cause := context.Cause(held.Context())
		if !errors.Is(cause, ErrLeaseLost) {
			t.Fatalf("context cause = %v, want ErrLeaseLost", cause)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for HeldLease context cancellation on lease loss")
	}
}
