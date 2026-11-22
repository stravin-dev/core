package node

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeRegionalLeaseStore struct {
	mu    sync.Mutex
	lease ddb.RegionalWriteLease
	err   error
	calls int
}

func (s *fakeRegionalLeaseStore) GetRegionalWriteLease(_ context.Context, _ lease.Region) (ddb.RegionalWriteLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.err != nil {
		return ddb.RegionalWriteLease{}, s.err
	}
	return s.lease, nil
}

func (s *fakeRegionalLeaseStore) Set(l ddb.RegionalWriteLease, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lease = l
	s.err = err
}

func (s *fakeRegionalLeaseStore) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func TestLeaseRefresher_ValidExpiredReadOnlyAndEpochFence(t *testing.T) {
	base := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	clk := &fakeClock{now: base}
	store := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region:    "us-east-1",
			Epoch:     5,
			Mode:      lease.ModeActive,
			HolderID:  "ctrl-1",
			ExpiresAt: base.Add(30 * time.Second),
		},
	}

	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:        store,
		Region:       "us-east-1",
		Interval:     20 * time.Millisecond,
		SafetyMargin: 2 * time.Second,
		Clock:        clk,
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}

	// Before first refresh, lease is not yet loaded.
	if err := refresher.CheckWriteAllowed(); !errors.Is(err, lease.ErrLeaseNotFound) {
		t.Fatalf("before refresh CheckWriteAllowed = %v, want ErrLeaseNotFound", err)
	}

	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}
	if err := refresher.CheckWriteAllowed(); err != nil {
		t.Fatalf("CheckWriteAllowed after valid refresh = %v, want nil", err)
	}
	if err := refresher.CheckEpochFence(5); err != nil {
		t.Fatalf("CheckEpochFence(5) = %v, want nil", err)
	}
	if err := refresher.CheckEpochFence(6); !errors.Is(err, lease.ErrStaleEpoch) {
		t.Fatalf("CheckEpochFence(6) = %v, want ErrStaleEpoch", err)
	}

	// Advance clock into safety margin -> fenced as expired.
	clk.Advance(29 * time.Second)
	if err := refresher.CheckWriteAllowed(); !errors.Is(err, lease.ErrLeaseExpired) {
		t.Fatalf("expired CheckWriteAllowed = %v, want ErrLeaseExpired", err)
	}

	// Switch regional mode to READ_ONLY -> fenced as read-only.
	clk.now = base
	store.Set(ddb.RegionalWriteLease{
		Region:    "us-east-1",
		Epoch:     6,
		Mode:      lease.ModeReadOnly,
		HolderID:  "ctrl-1",
		ExpiresAt: base.Add(60 * time.Second),
	}, nil)
	if err := refresher.RefreshOnce(context.Background()); err != nil {
		t.Fatalf("RefreshOnce read-only: %v", err)
	}
	if err := refresher.CheckWriteAllowed(); !errors.Is(err, lease.ErrReadOnlyMode) {
		t.Fatalf("read-only CheckWriteAllowed = %v, want ErrReadOnlyMode", err)
	}
}

func TestLeaseRefresher_BackgroundLoopStopsCleanly(t *testing.T) {
	base := time.Now().UTC()
	store := &fakeRegionalLeaseStore{
		lease: ddb.RegionalWriteLease{
			Region:    "us-east-1",
			Epoch:     1,
			Mode:      lease.ModeActive,
			HolderID:  "ctrl-1",
			ExpiresAt: base.Add(time.Minute),
		},
	}

	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:    store,
		Region:   "us-east-1",
		Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	refresher.Start(ctx)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if store.Calls() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	refresher.Stop()

	if store.Calls() < 2 {
		t.Fatalf("expected background refresher to poll at least twice, got %d", store.Calls())
	}
}
