package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
)

const (
	defaultLeaseRefreshInterval = 5 * time.Second
	defaultLeaseRefreshTimeout  = 3 * time.Second
	defaultLeaseSafetyMargin    = 2 * time.Second
)

// RegionalLeaseStore is the narrow DynamoDB seam for reading regional write leases.
// *ddb.Repository satisfies this interface directly.
type RegionalLeaseStore interface {
	GetRegionalWriteLease(ctx context.Context, region lease.Region) (ddb.RegionalWriteLease, error)
}

// LeaseRefresherConfig configures the background regional write-lease and failover_epoch refresher.
type LeaseRefresherConfig struct {
	Store        RegionalLeaseStore
	Region       lease.Region
	Interval     time.Duration
	Timeout      time.Duration
	SafetyMargin time.Duration
	Clock        lease.Clock
	Logger       *slog.Logger
}

// LeaseRefresher periodically polls DynamoDB's regional write lease in the
// background so stravin-node validates failover_epoch and write fencing without
// ever including META in per-push DynamoDB transactions.
type LeaseRefresher struct {
	store        RegionalLeaseStore
	region       lease.Region
	interval     time.Duration
	timeout      time.Duration
	safetyMargin time.Duration
	clock        lease.Clock
	logger       *slog.Logger

	mu      sync.RWMutex
	loaded  bool
	current ddb.RegionalWriteLease
	lastErr error

	cancelMu sync.Mutex
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewLeaseRefresher creates a LeaseRefresher with safe defaults.
func NewLeaseRefresher(cfg LeaseRefresherConfig) (*LeaseRefresher, error) {
	if cfg.Store == nil {
		return nil, errors.New("node: lease store is required")
	}
	if strings.TrimSpace(string(cfg.Region)) == "" {
		return nil, lease.ErrEmptyRegion
	}
	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultLeaseRefreshInterval
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultLeaseRefreshTimeout
	}
	safetyMargin := cfg.SafetyMargin
	if safetyMargin < 0 {
		safetyMargin = 0
	}
	if safetyMargin == 0 && cfg.Clock == nil {
		safetyMargin = defaultLeaseSafetyMargin
	}
	clk := cfg.Clock
	if clk == nil {
		clk = lease.RealClock{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &LeaseRefresher{
		store:        cfg.Store,
		region:       cfg.Region,
		interval:     interval,
		timeout:      timeout,
		safetyMargin: safetyMargin,
		clock:        clk,
		logger:       logger,
	}, nil
}

// Start launches the background lease refresh loop until ctx is cancelled or Stop is called.
func (r *LeaseRefresher) Start(ctx context.Context) {
	r.cancelMu.Lock()
	defer r.cancelMu.Unlock()
	if r.cancel != nil {
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.wg.Add(1)
	go r.loop(runCtx)
}

// Stop halts the background refresh loop and waits for it to exit cleanly.
func (r *LeaseRefresher) Stop() {
	r.cancelMu.Lock()
	cancel := r.cancel
	r.cancel = nil
	r.cancelMu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
}

func (r *LeaseRefresher) loop(ctx context.Context) {
	defer r.wg.Done()
	_ = r.RefreshOnce(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.RefreshOnce(ctx); err != nil && ctx.Err() == nil {
				r.logger.Warn("node: regional write lease refresh failed",
					"region", string(r.region),
					"error", err,
				)
			}
		}
	}
}

// RefreshOnce fetches the regional write lease from DynamoDB and updates cached state.
func (r *LeaseRefresher) RefreshOnce(ctx context.Context) error {
	reqCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	regLease, err := r.store.GetRegionalWriteLease(reqCtx, r.region)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.lastErr = err
		return err
	}
	r.loaded = true
	r.current = regLease
	r.lastErr = nil
	return nil
}

// Current returns the most recently refreshed regional write lease.
func (r *LeaseRefresher) Current() (ddb.RegionalWriteLease, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.loaded {
		if r.lastErr != nil {
			return ddb.RegionalWriteLease{}, r.lastErr
		}
		return ddb.RegionalWriteLease{}, lease.ErrLeaseNotFound
	}
	return r.current, nil
}

// CheckWriteAllowed verifies that the cached regional write lease is active,
// has a positive failover_epoch, and is unexpired including the safety margin.
func (r *LeaseRefresher) CheckWriteAllowed() error {
	cur, err := r.Current()
	if err != nil {
		return err
	}
	if cur.Epoch == 0 {
		return lease.ErrInvalidEpoch
	}
	if cur.Mode != lease.ModeActive {
		return fmt.Errorf("%w: mode=%s", lease.ErrReadOnlyMode, cur.Mode)
	}
	effectiveNow := r.clock.Now().Add(r.safetyMargin)
	if !effectiveNow.Before(cur.ExpiresAt) {
		return fmt.Errorf("%w: expired_at=%s", lease.ErrLeaseExpired, cur.ExpiresAt.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// CheckEpochFence verifies that the cached regional lease epoch is at least required.
func (r *LeaseRefresher) CheckEpochFence(required lease.Epoch) error {
	cur, err := r.Current()
	if err != nil {
		return err
	}
	return lease.VerifyEpochFence(required, cur.Epoch)
}
