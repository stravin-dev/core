package compaction

import (
	"fmt"
	"sync"

	"stravin.dev/core/internal/tenant"
)

type dedupKey struct {
	tenantID tenant.TenantID
	repoID   tenant.RepoID
	shard    int
	tier     Tier
}

type tenantSlot struct {
	tenantID  tenant.TenantID
	items     []ShardTarget
	weight    int
	remaining int
}

// FairQueue is a thread-safe weighted round-robin compaction queue partitioned
// by tenant_id so a single high-throughput tenant cannot starve compaction for
// quiet tenants.
type FairQueue struct {
	mu      sync.Mutex
	order   []tenant.TenantID
	slots   map[tenant.TenantID]*tenantSlot
	weights map[tenant.TenantID]int
	queued  map[dedupKey]struct{}
	cursor  int
	total   int
}

// NewFairQueue constructs an empty FairQueue.
func NewFairQueue() *FairQueue {
	return &FairQueue{
		slots:   make(map[tenant.TenantID]*tenantSlot),
		weights: make(map[tenant.TenantID]int),
		queued:  make(map[dedupKey]struct{}),
	}
}

// SetTenantWeight configures the weighted round-robin quantum (>= 1) for a tenant.
func (q *FairQueue) SetTenantWeight(tenantID tenant.TenantID, weight int) error {
	if err := tenantID.Validate(); err != nil {
		return err
	}
	if weight < 1 {
		return fmt.Errorf("%w: tenant weight must be >= 1, got %d", ErrInvalidConfig, weight)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.weights[tenantID] = weight
	if slot, ok := q.slots[tenantID]; ok {
		slot.weight = weight
		if slot.remaining > weight {
			slot.remaining = weight
		}
	}
	return nil
}

// Enqueue adds target to its owning tenant's FIFO queue. Duplicate pending
// targets with the same (tenant, repo, shard, tier) are ignored and return false.
func (q *FairQueue) Enqueue(target ShardTarget) bool {
	if err := target.Repo.Validate(); err != nil {
		return false
	}
	if target.Tier == "" {
		target.Tier = Tier0
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	dk := dedupKey{
		tenantID: target.Repo.TenantID,
		repoID:   target.Repo.RepoID,
		shard:    target.Shard,
		tier:     target.Tier,
	}
	if _, exists := q.queued[dk]; exists {
		return false
	}
	q.queued[dk] = struct{}{}

	tid := target.Repo.TenantID
	slot, ok := q.slots[tid]
	if !ok {
		w := q.weights[tid]
		if w < 1 {
			w = 1
		}
		slot = &tenantSlot{
			tenantID:  tid,
			weight:    w,
			remaining: w,
		}
		q.slots[tid] = slot
		q.order = append(q.order, tid)
	}
	slot.items = append(slot.items, target)
	q.total++
	return true
}

// Dequeue pops the next ShardTarget using weighted round-robin across tenants.
func (q *FairQueue) Dequeue() (ShardTarget, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.total == 0 || len(q.order) == 0 {
		return ShardTarget{}, false
	}

	if q.cursor >= len(q.order) {
		q.cursor = 0
	}

	tid := q.order[q.cursor]
	slot := q.slots[tid]
	target := slot.items[0]
	slot.items = slot.items[1:]
	q.total--

	dk := dedupKey{
		tenantID: target.Repo.TenantID,
		repoID:   target.Repo.RepoID,
		shard:    target.Shard,
		tier:     target.Tier,
	}
	delete(q.queued, dk)

	if len(slot.items) == 0 {
		delete(q.slots, tid)
		q.order = append(q.order[:q.cursor], q.order[q.cursor+1:]...)
		if len(q.order) > 0 && q.cursor >= len(q.order) {
			q.cursor = 0
		}
		return target, true
	}

	slot.remaining--
	if slot.remaining <= 0 {
		slot.remaining = slot.weight
		q.cursor = (q.cursor + 1) % len(q.order)
	}
	return target, true
}

// Len returns the total number of queued shard targets across all tenants.
func (q *FairQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.total
}

// TenantCount returns the number of distinct tenants with pending work.
func (q *FairQueue) TenantCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}
