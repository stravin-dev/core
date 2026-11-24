package ctrl

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
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

type memLeaseStore struct {
	mu     sync.Mutex
	leases map[lease.Region]ddb.RegionalWriteLease
	metas  map[string]ddb.RepoMeta
	clock  lease.Clock
}

func newMemLeaseStore(clk lease.Clock) *memLeaseStore {
	return &memLeaseStore{
		leases: make(map[lease.Region]ddb.RegionalWriteLease),
		metas:  make(map[string]ddb.RepoMeta),
		clock:  clk,
	}
}

func (s *memLeaseStore) GetRegionalWriteLease(_ context.Context, region lease.Region) (ddb.RegionalWriteLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[region]
	if !ok {
		return ddb.RegionalWriteLease{}, lease.ErrLeaseNotFound
	}
	return l, nil
}

func (s *memLeaseStore) AcquireRegionalWriteLease(_ context.Context, region lease.Region, holderID string, epoch lease.Epoch, mode lease.Mode, ttl time.Duration) (ddb.RegionalWriteLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now().UTC()
	if existing, ok := s.leases[region]; ok {
		ownerOK := existing.HolderID == holderID || !now.Before(existing.ExpiresAt)
		epochOK := existing.Epoch <= epoch
		if !ownerOK || !epochOK {
			return ddb.RegionalWriteLease{}, ddb.ErrLeaseHeld
		}
	}
	l := ddb.RegionalWriteLease{
		Region:    region,
		Epoch:     epoch,
		Mode:      mode,
		HolderID:  holderID,
		ExpiresAt: now.Add(ttl),
	}
	s.leases[region] = l
	return l, nil
}

func (s *memLeaseStore) PutRegionalWriteLease(_ context.Context, l ddb.RegionalWriteLease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[l.Region] = l
	return nil
}

func (s *memLeaseStore) GetMeta(_ context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.metas[repo.String()]
	if !ok {
		return ddb.RepoMeta{}, ddb.ErrNotFound
	}
	return m, nil
}

func (s *memLeaseStore) UpdateMeta(_ context.Context, meta ddb.RepoMeta, expectedEpoch lease.Epoch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.metas[meta.Repo.String()]
	if !ok {
		return ddb.ErrNotFound
	}
	if existing.FailoverEpoch != expectedEpoch {
		return ddb.ErrEpochConflict
	}
	s.metas[meta.Repo.String()] = meta
	return nil
}

func TestLeaseManager_RenewAndHeartbeat(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	store := newMemLeaseStore(clk)
	tel, err := telemetry.NewInMemoryProvider("stravin-ctrl-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases:            store,
		Meta:              store,
		Region:            "us-east-1",
		HolderID:          "ctrl-1",
		LeaseTTL:          5 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		Clock:             clk,
		Metrics:           metrics,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	l1, err := mgr.RenewOnce(context.Background())
	if err != nil {
		t.Fatalf("RenewOnce(1): %v", err)
	}
	if l1.Epoch != 1 || l1.Mode != lease.ModeActive || !l1.ExpiresAt.Equal(clk.Now().Add(5*time.Second)) {
		t.Fatalf("unexpected initial lease: %+v", l1)
	}

	st := mgr.Status()
	if !st.Valid || !st.AllowsWrites || !st.Initialized {
		t.Fatalf("expected valid active status, got %+v", st)
	}

	// Advance clock past TTL without renewal: status fails closed.
	clk.Advance(6 * time.Second)
	expired := mgr.Status()
	if expired.Valid || expired.AllowsWrites {
		t.Fatalf("expected expired lease to fail closed, got %+v", expired)
	}

	// Next renewal extends TTL while preserving epoch.
	l2, err := mgr.RenewOnce(context.Background())
	if err != nil {
		t.Fatalf("RenewOnce(2): %v", err)
	}
	if l2.Epoch != 1 || !l2.ExpiresAt.Equal(clk.Now().Add(5*time.Second)) {
		t.Fatalf("unexpected renewed lease: %+v", l2)
	}
}

func TestLeaseManager_FenceRegionAndMonotonicEpoch(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	store := newMemLeaseStore(clk)
	rk, _ := tenant.NewRepoKey("acme", "mono")
	store.metas[rk.String()] = ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      4,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
	}

	tel, err := telemetry.NewInMemoryProvider("stravin-ctrl-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases:            store,
		Meta:              store,
		Region:            "us-east-1",
		HolderID:          "ctrl-1",
		LeaseTTL:          5 * time.Second,
		HeartbeatInterval: 1 * time.Second,
		FenceWait:         5 * time.Millisecond,
		Clock:             clk,
		Metrics:           metrics,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	if _, err := mgr.RenewOnce(context.Background()); err != nil {
		t.Fatalf("RenewOnce: %v", err)
	}

	// Fence region to DRAINING and advance epoch on both regional lease and repo META.
	res, err := mgr.FenceRegion(context.Background(), FenceRequest{
		TargetMode:   lease.ModeDraining,
		AdvanceEpoch: true,
		Repo:         &rk,
	})
	if err != nil {
		t.Fatalf("FenceRegion(DRAINING): %v", err)
	}
	if res.PreviousEpoch != 1 || res.CurrentEpoch != 2 {
		t.Fatalf("expected epoch 1->2, got %d->%d", res.PreviousEpoch, res.CurrentEpoch)
	}
	if res.CurrentMode != lease.ModeDraining || res.FenceWaitApplied != 5*time.Millisecond {
		t.Fatalf("unexpected fence result: %+v", res)
	}
	if res.RepoMeta == nil || res.RepoMeta.FailoverEpoch != 2 || res.RepoMeta.WriteState != lease.ModeDraining {
		t.Fatalf("expected repo META updated to epoch 2 DRAINING, got %+v", res.RepoMeta)
	}
	if mgr.Status().AllowsWrites {
		t.Fatalf("expected DRAINING mode to disallow writes")
	}

	// Verify stale epoch is rejected by lease.VerifyEpochFence.
	if err := lease.VerifyEpochFence(res.CurrentEpoch, 1); !errors.Is(err, lease.ErrStaleEpoch) {
		t.Fatalf("expected ErrStaleEpoch for stale writer epoch 1 vs required 2, got %v", err)
	}

	// Non-monotonic MinEpoch must fail.
	if _, err := mgr.FenceRegion(context.Background(), FenceRequest{
		TargetMode: lease.ModeActive,
		MinEpoch:   2,
	}); !errors.Is(err, ErrNonMonotonicEpoch) {
		t.Fatalf("expected ErrNonMonotonicEpoch, got %v", err)
	}

	// Advance to READ_ONLY then back to ACTIVE at epoch 4.
	if _, err := mgr.FenceRegion(context.Background(), FenceRequest{
		TargetMode:   lease.ModeReadOnly,
		AdvanceEpoch: true,
	}); err != nil {
		t.Fatalf("FenceRegion(READ_ONLY): %v", err)
	}
	resActive, err := mgr.FenceRegion(context.Background(), FenceRequest{
		TargetMode:   lease.ModeActive,
		AdvanceEpoch: true,
	})
	if err != nil {
		t.Fatalf("FenceRegion(ACTIVE): %v", err)
	}
	if resActive.CurrentEpoch != 4 || !mgr.Status().AllowsWrites {
		t.Fatalf("expected ACTIVE epoch 4 allowing writes, got %+v", resActive)
	}

	// Verify ctrl.failover.transition span was emitted.
	spans := tel.SpanExporter.GetSpans()
	foundSpan := false
	for _, sp := range spans {
		if sp.Name == "ctrl.failover.transition" {
			foundSpan = true
			break
		}
	}
	if !foundSpan {
		t.Fatalf("expected ctrl.failover.transition span in exporter")
	}
}

func TestLeaseManager_RunCancelsCleanly(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	store := newMemLeaseStore(clk)
	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases:            store,
		Region:            "us-east-1",
		HolderID:          "ctrl-1",
		LeaseTTL:          100 * time.Millisecond,
		HeartbeatInterval: 10 * time.Millisecond,
		Clock:             clk,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- mgr.Run(ctx)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if !mgr.Status().Initialized {
		t.Fatalf("expected lease to be initialized by Run loop")
	}
}

func TestLeaseManager_OverflowEpochClampedInSpan(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)}
	store := newMemLeaseStore(clk)
	rk, err := tenant.NewRepoKey("acme", "overflow")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	store.metas[rk.String()] = ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      1,
		ManifestShards: 1,
		WriteState:     lease.ModeActive,
	}

	tel, err := telemetry.NewInMemoryProvider("stravin-ctrl")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases:   store,
		Meta:     store,
		Region:   "us-east-1",
		HolderID: "ctrl-1",
		Clock:    clk,
		Metrics:  metrics,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	overflowEpoch := lease.Epoch(math.MaxInt64) + 1
	if _, err := mgr.FenceRegion(context.Background(), FenceRequest{
		TargetMode: lease.ModeDraining,
		MinEpoch:   overflowEpoch,
	}); err != nil {
		t.Fatalf("FenceRegion: %v", err)
	}
	assertSpanFailoverEpoch(t, tel.SpanExporter.GetSpans(), "ctrl.failover.transition", math.MaxInt64)

	tel.SpanExporter.Reset()
	if _, err := mgr.UpdateRepoEpoch(context.Background(), rk, lease.Epoch(math.MaxUint64), lease.ModeDraining); err != nil {
		t.Fatalf("UpdateRepoEpoch: %v", err)
	}
	assertSpanFailoverEpoch(t, tel.SpanExporter.GetSpans(), "ctrl.failover.transition", math.MaxInt64)
}
