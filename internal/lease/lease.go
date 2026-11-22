// Package lease defines the frozen regional write-lease and failover_epoch
// fencing token contract shared by stravin-edge, stravin-node, and stravin-ctrl.
package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"stravin.dev/core/internal/tenant"
)

// Epoch is a monotonically increasing failover fencing token stored in DynamoDB META.
type Epoch uint64

// Region identifies a deployment region (for example, "us-east-1").
type Region string

// Mode represents a region's operational write state for a repository.
type Mode string

const (
	ModeActive   Mode = "ACTIVE"
	ModeDraining Mode = "DRAINING"
	ModeReadOnly Mode = "READ_ONLY"
)

var (
	ErrLeaseNotFound = errors.New("lease: not found")
	ErrLeaseExpired  = errors.New("lease: expired")
	ErrWrongRegion   = errors.New("lease: local region is not home region")
	ErrReadOnlyMode  = errors.New("lease: region is not accepting writes")
	ErrInvalidEpoch  = errors.New("lease: failover_epoch must be greater than zero")
	ErrStaleEpoch    = errors.New("lease: observed failover_epoch is stale")
	ErrEmptyRegion   = errors.New("lease: empty region")
)

// Lease describes the regional write authorization and fencing epoch for a repository.
type Lease struct {
	Repo       tenant.RepoKey
	HomeRegion Region
	Epoch      Epoch
	Mode       Mode
	ExpiresAt  time.Time
	HolderID   string
}

// IsValidAt reports whether the lease is active and unexpired at now.
func (l Lease) IsValidAt(now time.Time) bool {
	return l.Mode == ModeActive && l.Epoch > 0 && now.Before(l.ExpiresAt)
}

// AllowsWrite verifies that l authorizes writes in localRegion at time now.
func (l Lease) AllowsWrite(localRegion Region, now time.Time) error {
	if err := l.Repo.Validate(); err != nil {
		return err
	}
	if localRegion == "" || l.HomeRegion == "" {
		return ErrEmptyRegion
	}
	if l.Epoch == 0 {
		return ErrInvalidEpoch
	}
	if l.Mode != ModeActive {
		return fmt.Errorf("%w: mode=%s", ErrReadOnlyMode, l.Mode)
	}
	if l.HomeRegion != localRegion {
		return fmt.Errorf("%w: home=%s local=%s", ErrWrongRegion, l.HomeRegion, localRegion)
	}
	if !now.Before(l.ExpiresAt) {
		return fmt.Errorf("%w: expired_at=%s", ErrLeaseExpired, l.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// VerifyEpochFence enforces that observed is at least required.
func VerifyEpochFence(required, observed Epoch) error {
	if required == 0 || observed == 0 {
		return ErrInvalidEpoch
	}
	if observed < required {
		return fmt.Errorf("%w: observed=%d required=%d", ErrStaleEpoch, observed, required)
	}
	return nil
}

// Clock abstracts wall-clock reads for deterministic lease verification in tests.
type Clock interface {
	Now() time.Time
}

// RealClock delegates to time.Now().
type RealClock struct{}

// Now returns the current UTC time.
func (RealClock) Now() time.Time { return time.Now().UTC() }

// Store is the narrow read-only seam for fetching repository write leases.
type Store interface {
	GetLease(ctx context.Context, repo tenant.RepoKey) (Lease, error)
}

// Verifier checks whether the local region holds a valid write lease for a repository.
type Verifier interface {
	VerifyWriteLease(ctx context.Context, repo tenant.RepoKey, localRegion Region) (Lease, error)
}

// StoreVerifier implements Verifier over a Store and Clock with an optional
// clock-skew safety margin subtracted from ExpiresAt.
type StoreVerifier struct {
	store        Store
	clock        Clock
	safetyMargin time.Duration
}

// NewStoreVerifier constructs a StoreVerifier. If clock is nil, RealClock is used.
func NewStoreVerifier(store Store, clock Clock, safetyMargin time.Duration) *StoreVerifier {
	if clock == nil {
		clock = RealClock{}
	}
	if safetyMargin < 0 {
		safetyMargin = 0
	}
	return &StoreVerifier{
		store:        store,
		clock:        clock,
		safetyMargin: safetyMargin,
	}
}

// VerifyWriteLease fetches and validates the write lease for repo in localRegion.
func (v *StoreVerifier) VerifyWriteLease(ctx context.Context, repo tenant.RepoKey, localRegion Region) (Lease, error) {
	if err := repo.Validate(); err != nil {
		return Lease{}, err
	}
	if localRegion == "" {
		return Lease{}, ErrEmptyRegion
	}
	l, err := v.store.GetLease(ctx, repo)
	if err != nil {
		return Lease{}, err
	}
	effectiveNow := v.clock.Now().Add(v.safetyMargin)
	if err := l.AllowsWrite(localRegion, effectiveNow); err != nil {
		return Lease{}, err
	}
	return l, nil
}
