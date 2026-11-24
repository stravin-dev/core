package edge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

const defaultMetaCacheTTL = 30 * time.Second

var (
	// ErrRepoNotFound indicates the repository META record does not exist.
	ErrRepoNotFound = errors.New("edge: repository not found")
	// ErrNonHomeRegion indicates a write targeted a repository whose home_region differs from local_region.
	ErrNonHomeRegion = errors.New("edge: local region is not repository home_region")
	// ErrWriteUnavailable indicates writes are currently blocked (expired lease, non-home, or read-only).
	ErrWriteUnavailable = errors.New("edge: write unavailable")
)

// MetaStore is the narrow read-only interface for looking up repository META records.
type MetaStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
}

// RouteDecision captures resolved repository metadata and write-lease verification results.
type RouteDecision struct {
	Meta       ddb.RepoMeta
	HomeRegion lease.Region
	IsHome     bool
	Lease      lease.Lease
}

type cachedMeta struct {
	meta      ddb.RepoMeta
	expiresAt time.Time
}

// Resolver caches repository META (home_region, shard counts) in memory with a short TTL
// and verifies regional write leases on write operations (failing closed).
type Resolver struct {
	localRegion   lease.Region
	metaStore     MetaStore
	leaseVerifier lease.Verifier
	ttl           time.Duration
	clock         lease.Clock
	metrics       *Metrics

	mu    sync.RWMutex
	cache map[tenant.RepoKey]cachedMeta
}

// NewResolver constructs a Resolver for localRegion.
func NewResolver(localRegion lease.Region, metaStore MetaStore, leaseVerifier lease.Verifier, ttl time.Duration, clock lease.Clock) (*Resolver, error) {
	if localRegion == "" {
		return nil, lease.ErrEmptyRegion
	}
	if metaStore == nil {
		return nil, errors.New("edge: meta store is required")
	}
	if leaseVerifier == nil {
		return nil, errors.New("edge: lease verifier is required")
	}
	if ttl <= 0 {
		ttl = defaultMetaCacheTTL
	}
	if clock == nil {
		clock = lease.RealClock{}
	}
	return &Resolver{
		localRegion:   localRegion,
		metaStore:     metaStore,
		leaseVerifier: leaseVerifier,
		ttl:           ttl,
		clock:         clock,
		cache:         make(map[tenant.RepoKey]cachedMeta),
	}, nil
}

// SetMetrics attaches edge metrics for recording META cache hit/miss outcomes.
func (r *Resolver) SetMetrics(m *Metrics) {
	r.mu.Lock()
	r.metrics = m
	r.mu.Unlock()
}

// LocalRegion returns the edge instance's configured region.
func (r *Resolver) LocalRegion() lease.Region {
	return r.localRegion
}

// ResolveRead resolves repository META using the TTL cache.
func (r *Resolver) ResolveRead(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	if err := repo.Validate(); err != nil {
		return ddb.RepoMeta{}, err
	}

	now := r.clock.Now()
	r.mu.RLock()
	entry, ok := r.cache[repo]
	metrics := r.metrics
	r.mu.RUnlock()
	if ok && now.Before(entry.expiresAt) {
		metrics.RecordMetaCacheLookup(ctx, true)
		return entry.meta, nil
	}
	metrics.RecordMetaCacheLookup(ctx, false)

	meta, err := r.metaStore.GetMeta(ctx, repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return ddb.RepoMeta{}, ErrRepoNotFound
		}
		return ddb.RepoMeta{}, err
	}
	if meta.Repo.TenantID != repo.TenantID || meta.Repo.RepoID != repo.RepoID {
		return ddb.RepoMeta{}, ErrForbidden
	}

	r.mu.Lock()
	r.cache[repo] = cachedMeta{
		meta:      meta,
		expiresAt: now.Add(r.ttl),
	}
	r.mu.Unlock()
	return meta, nil
}

// ResolveWrite resolves repository META, confirms local_region == home_region, and validates
// the regional write lease (failing closed with ErrWriteUnavailable if expired, non-home, or read-only).
func (r *Resolver) ResolveWrite(ctx context.Context, repo tenant.RepoKey) (RouteDecision, error) {
	meta, err := r.ResolveRead(ctx, repo)
	if err != nil {
		return RouteDecision{}, err
	}
	if meta.HomeRegion != r.localRegion {
		return RouteDecision{}, fmt.Errorf("%w: %w", ErrWriteUnavailable, ErrNonHomeRegion)
	}
	if meta.WriteState != lease.ModeActive {
		return RouteDecision{}, fmt.Errorf("%w: %w", ErrWriteUnavailable, lease.ErrReadOnlyMode)
	}
	l, err := r.leaseVerifier.VerifyWriteLease(ctx, repo, r.localRegion)
	if err != nil {
		return RouteDecision{}, fmt.Errorf("%w: %w", ErrWriteUnavailable, err)
	}
	return RouteDecision{
		Meta:       meta,
		HomeRegion: meta.HomeRegion,
		IsHome:     true,
		Lease:      l,
	}, nil
}

// Invalidate evicts repo from the in-memory META cache.
func (r *Resolver) Invalidate(repo tenant.RepoKey) {
	r.mu.Lock()
	delete(r.cache, repo)
	r.mu.Unlock()
}
