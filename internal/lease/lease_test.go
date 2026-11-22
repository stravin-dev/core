package lease_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time { return c.now }

type memoryStore struct {
	leases map[tenant.RepoKey]lease.Lease
	err    error
}

func (s *memoryStore) GetLease(_ context.Context, repo tenant.RepoKey) (lease.Lease, error) {
	if s.err != nil {
		return lease.Lease{}, s.err
	}
	l, ok := s.leases[repo]
	if !ok {
		return lease.Lease{}, lease.ErrLeaseNotFound
	}
	return l, nil
}

func TestLeaseAllowsWrite(t *testing.T) {
	now := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	repo, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	active := lease.Lease{
		Repo:       repo,
		HomeRegion: "us-east-1",
		Epoch:      7,
		Mode:       lease.ModeActive,
		ExpiresAt:  now.Add(5 * time.Second),
		HolderID:   "node-1",
	}

	if err := active.AllowsWrite("us-east-1", now); err != nil {
		t.Fatalf("expected active lease to allow write, got %v", err)
	}

	cases := []struct {
		name        string
		mutate      func(l *lease.Lease)
		localRegion lease.Region
		at          time.Time
		wantErr     error
	}{
		{
			name:        "wrong region",
			localRegion: "us-west-2",
			at:          now,
			wantErr:     lease.ErrWrongRegion,
		},
		{
			name:        "expired at exact boundary",
			localRegion: "us-east-1",
			at:          active.ExpiresAt,
			wantErr:     lease.ErrLeaseExpired,
		},
		{
			name:        "expired past boundary",
			localRegion: "us-east-1",
			at:          active.ExpiresAt.Add(time.Second),
			wantErr:     lease.ErrLeaseExpired,
		},
		{
			name:        "draining mode rejects writes",
			mutate:      func(l *lease.Lease) { l.Mode = lease.ModeDraining },
			localRegion: "us-east-1",
			at:          now,
			wantErr:     lease.ErrReadOnlyMode,
		},
		{
			name:        "read-only mode rejects writes",
			mutate:      func(l *lease.Lease) { l.Mode = lease.ModeReadOnly },
			localRegion: "us-east-1",
			at:          now,
			wantErr:     lease.ErrReadOnlyMode,
		},
		{
			name:        "zero epoch invalid",
			mutate:      func(l *lease.Lease) { l.Epoch = 0 },
			localRegion: "us-east-1",
			at:          now,
			wantErr:     lease.ErrInvalidEpoch,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := active
			if tc.mutate != nil {
				tc.mutate(&l)
			}
			err := l.AllowsWrite(tc.localRegion, tc.at)
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("AllowsWrite() err = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

func TestStoreVerifier(t *testing.T) {
	now := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	repo, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	store := &memoryStore{
		leases: map[tenant.RepoKey]lease.Lease{
			repo: {
				Repo:       repo,
				HomeRegion: "us-east-1",
				Epoch:      3,
				Mode:       lease.ModeActive,
				ExpiresAt:  now.Add(2 * time.Second),
				HolderID:   "node-1",
			},
		},
	}

	v := lease.NewStoreVerifier(store, fixedClock{now: now}, 500*time.Millisecond)
	got, err := v.VerifyWriteLease(context.Background(), repo, "us-east-1")
	if err != nil {
		t.Fatalf("VerifyWriteLease unexpected error: %v", err)
	}
	if got.Epoch != 3 {
		t.Fatalf("got epoch %d, want 3", got.Epoch)
	}

	// Within safety margin -> fail closed as expired.
	tightVerifier := lease.NewStoreVerifier(store, fixedClock{now: now.Add(1600 * time.Millisecond)}, 500*time.Millisecond)
	if _, err := tightVerifier.VerifyWriteLease(context.Background(), repo, "us-east-1"); !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired within safety margin, got %v", err)
	}

	// Missing lease -> ErrLeaseNotFound.
	missingRepo, _ := tenant.NewRepoKey("acme", "unknown")
	if _, err := v.VerifyWriteLease(context.Background(), missingRepo, "us-east-1"); !errors.Is(err, lease.ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound, got %v", err)
	}

	// Stale epoch fence check.
	if err := lease.VerifyEpochFence(3, 2); !errors.Is(err, lease.ErrStaleEpoch) {
		t.Fatalf("VerifyEpochFence(3, 2) = %v, want ErrStaleEpoch", err)
	}
	if err := lease.VerifyEpochFence(3, 3); err != nil {
		t.Fatalf("VerifyEpochFence(3, 3) unexpected error: %v", err)
	}
}
