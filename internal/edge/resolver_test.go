package edge

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type stubMetaStore struct {
	calls atomic.Int64
	metas map[tenant.RepoKey]ddb.RepoMeta
}

func (s *stubMetaStore) GetMeta(_ context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	s.calls.Add(1)
	m, ok := s.metas[repo]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return m, nil
}

type stubLeaseStore struct {
	leases map[tenant.RepoKey]lease.Lease
}

func (s *stubLeaseStore) GetLease(_ context.Context, repo tenant.RepoKey) (lease.Lease, error) {
	l, ok := s.leases[repo]
	if !ok {
		return lease.Lease{}, lease.ErrLeaseNotFound
	}
	return l, nil
}

func TestResolver_TTLCacheAndWriteLeaseEnforcement(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: now}
	repo := tenant.RepoKey{TenantID: "acme", RepoID: "core"}
	remoteRepo := tenant.RepoKey{TenantID: "acme", RepoID: "west-only"}

	metaStore := &stubMetaStore{
		metas: map[tenant.RepoKey]ddb.RepoMeta{
			repo: {
				Repo:           repo,
				HomeRegion:     "us-east-1",
				FailoverEpoch:  1,
				RefShards:      4,
				ManifestShards: 4,
				WriteState:     lease.ModeActive,
			},
			remoteRepo: {
				Repo:           remoteRepo,
				HomeRegion:     "us-west-2",
				FailoverEpoch:  1,
				RefShards:      4,
				ManifestShards: 4,
				WriteState:     lease.ModeActive,
			},
		},
	}
	leaseStore := &stubLeaseStore{
		leases: map[tenant.RepoKey]lease.Lease{
			repo: {
				Repo:       repo,
				HomeRegion: "us-east-1",
				Epoch:      1,
				Mode:       lease.ModeActive,
				ExpiresAt:  now.Add(10 * time.Second),
				HolderID:   "ctrl-1",
			},
		},
	}

	verifier := lease.NewStoreVerifier(leaseStore, clk, 0)
	res, err := NewResolver("us-east-1", metaStore, verifier, 30*time.Second, clk)
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}

	ctx := context.Background()
	if _, err := res.ResolveRead(ctx, repo); err != nil {
		t.Fatalf("ResolveRead 1: %v", err)
	}
	if _, err := res.ResolveRead(ctx, repo); err != nil {
		t.Fatalf("ResolveRead 2: %v", err)
	}
	if got := metaStore.calls.Load(); got != 1 {
		t.Fatalf("expected 1 GetMeta call due to TTL cache, got %d", got)
	}

	// Write succeeds while lease is valid and region matches.
	dec, err := res.ResolveWrite(ctx, repo)
	if err != nil {
		t.Fatalf("ResolveWrite valid: %v", err)
	}
	if !dec.IsHome || dec.HomeRegion != "us-east-1" {
		t.Fatalf("unexpected RouteDecision: %+v", dec)
	}

	// Non-home repo write fails closed.
	if _, err := res.ResolveWrite(ctx, remoteRepo); !errors.Is(err, ErrWriteUnavailable) || !errors.Is(err, ErrNonHomeRegion) {
		t.Fatalf("expected ErrWriteUnavailable/ErrNonHomeRegion, got %v", err)
	}

	// Advance past lease expiry -> write fails closed.
	clk.Advance(15 * time.Second)
	if _, err := res.ResolveWrite(ctx, repo); !errors.Is(err, ErrWriteUnavailable) || !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("expected ErrWriteUnavailable/ErrLeaseExpired, got %v", err)
	}

	// Advance past META cache TTL -> refreshes from store (1 repo + 1 remoteRepo + 1 refresh = 3).
	clk.Advance(20 * time.Second)
	if _, err := res.ResolveRead(ctx, repo); err != nil {
		t.Fatalf("ResolveRead after TTL: %v", err)
	}
	if got := metaStore.calls.Load(); got != 3 {
		t.Fatalf("expected 3 GetMeta calls after TTL expiry, got %d", got)
	}
}
