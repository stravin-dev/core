package cache

import (
	"sort"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultMaxBytes is the default global NVMe cache capacity (10 GiB).
	DefaultMaxBytes int64 = 10 << 30
	// DefaultMaxTenantFraction is the default per-tenant share of global cache capacity (20%).
	DefaultMaxTenantFraction = 0.20
)

// EvictionCandidate describes a single cached repository considered for eviction.
type EvictionCandidate struct {
	Repo       tenant.RepoKey
	SizeBytes  int64
	LastAccess uint64
	Pinned     bool
}

// EvictionState captures the point-in-time cache accounting snapshot passed to an EvictionPolicy.
type EvictionState struct {
	TotalBytes  int64
	MaxBytes    int64
	TenantBytes map[tenant.TenantID]int64
	Candidates  []EvictionCandidate
}

// EvictionPolicy is a GoF Strategy interface deciding which unpinned repositories to evict.
type EvictionPolicy interface {
	SelectEvictions(state EvictionState) []tenant.RepoKey
}

// PartitionedLRUPolicy enforces a per-tenant byte ceiling (MaxTenantFraction * MaxBytes)
// before falling back to global LRU eviction, preventing a single noisy-neighbor tenant
// from evicting hot packs belonging to other tenants.
type PartitionedLRUPolicy struct {
	MaxTenantFraction float64
}

// NewPartitionedLRUPolicy constructs a PartitionedLRUPolicy with the given per-tenant cap fraction.
func NewPartitionedLRUPolicy(maxTenantFraction float64) *PartitionedLRUPolicy {
	if maxTenantFraction <= 0 || maxTenantFraction > 1 {
		maxTenantFraction = DefaultMaxTenantFraction
	}
	return &PartitionedLRUPolicy{MaxTenantFraction: maxTenantFraction}
}

// SelectEvictions returns an ordered list of unpinned repositories to evict.
func (p *PartitionedLRUPolicy) SelectEvictions(state EvictionState) []tenant.RepoKey {
	if state.MaxBytes <= 0 {
		return nil
	}

	tenantCap := int64(float64(state.MaxBytes) * p.MaxTenantFraction)
	if tenantCap < 1 {
		tenantCap = 1
	}

	tenantBytes := make(map[tenant.TenantID]int64, len(state.TenantBytes))
	for t, b := range state.TenantBytes {
		tenantBytes[t] = b
	}
	totalBytes := state.TotalBytes

	byTenant := make(map[tenant.TenantID][]EvictionCandidate)
	unpinned := make([]EvictionCandidate, 0, len(state.Candidates))
	for _, c := range state.Candidates {
		if c.Pinned {
			continue
		}
		byTenant[c.Repo.TenantID] = append(byTenant[c.Repo.TenantID], c)
		unpinned = append(unpinned, c)
	}

	evictedSet := make(map[tenant.RepoKey]struct{})
	var evicted []tenant.RepoKey

	// Phase 1: enforce per-tenant ceiling so noisy tenants evict their own cold repos first.
	tenants := make([]tenant.TenantID, 0, len(byTenant))
	for tid := range byTenant {
		if tenantBytes[tid] > tenantCap {
			tenants = append(tenants, tid)
		}
	}
	sort.Slice(tenants, func(i, j int) bool { return tenants[i] < tenants[j] })

	for _, tid := range tenants {
		cands := byTenant[tid]
		sortCandidatesLRU(cands)
		for _, c := range cands {
			if tenantBytes[tid] <= tenantCap {
				break
			}
			evicted = append(evicted, c.Repo)
			evictedSet[c.Repo] = struct{}{}
			tenantBytes[tid] -= c.SizeBytes
			totalBytes -= c.SizeBytes
		}
	}

	// Phase 2: enforce global byte cap using deterministic LRU across remaining unpinned repos.
	if totalBytes > state.MaxBytes {
		sortCandidatesLRU(unpinned)
		for _, c := range unpinned {
			if totalBytes <= state.MaxBytes {
				break
			}
			if _, already := evictedSet[c.Repo]; already {
				continue
			}
			evicted = append(evicted, c.Repo)
			evictedSet[c.Repo] = struct{}{}
			totalBytes -= c.SizeBytes
		}
	}

	return evicted
}

func sortCandidatesLRU(cands []EvictionCandidate) {
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].LastAccess != cands[j].LastAccess {
			return cands[i].LastAccess < cands[j].LastAccess
		}
		return cands[i].Repo.String() < cands[j].Repo.String()
	})
}
