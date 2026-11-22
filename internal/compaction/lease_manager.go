package compaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	defaultLeaseTTL       = 30 * time.Second
	defaultReleaseTimeout = 5 * time.Second
	minLeaseRenewInterval = 10 * time.Millisecond
	renewIntervalFraction = 3
)

// LeaseManagerConfig configures sharded compaction lease acquisition and renewal.
type LeaseManagerConfig struct {
	OwnerID       string
	TTL           time.Duration
	RenewInterval time.Duration
	Metrics       *Metrics
	Logger        *slog.Logger
}

// LeaseManager claims, renews, and releases SHARD#{m} compaction leases in DynamoDB.
type LeaseManager struct {
	store         LeaseStore
	ownerID       string
	ttl           time.Duration
	renewInterval time.Duration
	metrics       *Metrics
	logger        *slog.Logger
}

// NewLeaseManager constructs a LeaseManager with validated defaults.
func NewLeaseManager(store LeaseStore, cfg LeaseManagerConfig) (*LeaseManager, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil lease store", ErrInvalidConfig)
	}
	ownerID := strings.TrimSpace(cfg.OwnerID)
	if ownerID == "" {
		return nil, fmt.Errorf("%w: empty owner_id", ErrInvalidConfig)
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultLeaseTTL
	}
	renewInterval := cfg.RenewInterval
	if renewInterval <= 0 {
		renewInterval = ttl / renewIntervalFraction
		if renewInterval < minLeaseRenewInterval {
			renewInterval = minLeaseRenewInterval
		}
	}
	if renewInterval >= ttl {
		return nil, fmt.Errorf("%w: renew_interval (%s) must be < ttl (%s)", ErrInvalidConfig, renewInterval, ttl)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &LeaseManager{
		store:         store,
		ownerID:       ownerID,
		ttl:           ttl,
		renewInterval: renewInterval,
		metrics:       cfg.Metrics,
		logger:        logger,
	}, nil
}

// HeldLease represents an active SHARD#{m} lease with automatic background renewal.
type HeldLease struct {
	manager *LeaseManager
	lease   ddb.ShardLease

	ctx         context.Context
	cancel      context.CancelCauseFunc
	stopRenew   chan struct{}
	doneRenew   chan struct{}
	releaseOnce sync.Once
	releaseErr  error
}

// Acquire claims the DynamoDB compaction lease for (repo, shard) and starts a
// background renewal goroutine that cancels the returned HeldLease context if
// lease ownership is lost.
func (m *LeaseManager) Acquire(ctx context.Context, repo tenant.RepoKey, shard int) (*HeldLease, error) {
	spanCtx, span := m.metrics.Tracer().Start(ctx, "ddb.lease_acquire",
		trace.WithAttributes(
			telemetry.NewSpanAttrs().
				TenantID(string(repo.TenantID)).
				RepoID(string(repo.RepoID)).
				ManifestShard(shard).
				Build()...,
		),
	)
	defer span.End()

	acquired, err := m.store.AcquireShardLease(spanCtx, repo, shard, m.ownerID, m.ttl)
	if err != nil {
		if errors.Is(err, ddb.ErrLeaseHeld) {
			m.metrics.recordLeaseContention(spanCtx, shard)
			span.RecordError(err)
			span.SetStatus(codes.Error, "lease held by another worker")
			return nil, fmt.Errorf("%w: %w", ErrLeaseContention, err)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	leaseCtx, cancel := context.WithCancelCause(ctx)
	held := &HeldLease{
		manager:   m,
		lease:     acquired,
		ctx:       leaseCtx,
		cancel:    cancel,
		stopRenew: make(chan struct{}),
		doneRenew: make(chan struct{}),
	}

	go held.renewLoop()
	return held, nil
}

// Context returns a context that is cancelled if the parent context cancels,
// the lease is released, or background lease renewal detects ErrLeaseLost.
func (h *HeldLease) Context() context.Context {
	return h.ctx
}

// Lease returns the initially acquired ShardLease metadata.
func (h *HeldLease) Lease() ddb.ShardLease {
	return h.lease
}

// Release stops background renewal and releases the DynamoDB shard lease.
// It is safe to call multiple times.
func (h *HeldLease) Release(ctx context.Context) error {
	h.releaseOnce.Do(func() {
		close(h.stopRenew)
		<-h.doneRenew
		h.cancel(nil)

		releaseCtx := ctx
		if releaseCtx == nil || releaseCtx.Err() != nil {
			var cancel context.CancelFunc
			releaseCtx, cancel = context.WithTimeout(context.Background(), defaultReleaseTimeout)
			defer cancel()
		}
		err := h.manager.store.ReleaseShardLease(releaseCtx, h.lease.Repo, h.lease.Shard, h.manager.ownerID)
		if err != nil && !errors.Is(err, ddb.ErrLeaseLost) {
			h.releaseErr = err
		}
	})
	return h.releaseErr
}

func (h *HeldLease) renewLoop() {
	defer close(h.doneRenew)
	ticker := time.NewTicker(h.manager.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.stopRenew:
			return
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(h.ctx, h.manager.renewInterval)
			_, err := h.manager.store.RenewShardLease(
				renewCtx,
				h.lease.Repo,
				h.lease.Shard,
				h.manager.ownerID,
				h.manager.ttl,
			)
			cancel()
			if err == nil {
				continue
			}
			if errors.Is(err, ddb.ErrLeaseLost) {
				h.manager.logger.Warn("compaction shard lease lost during renewal",
					"tenant_id", string(h.lease.Repo.TenantID),
					"repo_id", string(h.lease.Repo.RepoID),
					"shard", h.lease.Shard,
					"owner_id", h.manager.ownerID,
				)
				h.cancel(fmt.Errorf("%w: %w", ErrLeaseLost, err))
				return
			}
			h.manager.logger.Warn("transient compaction shard lease renewal error",
				"tenant_id", string(h.lease.Repo.TenantID),
				"repo_id", string(h.lease.Repo.RepoID),
				"shard", h.lease.Shard,
				"error", err,
			)
		}
	}
}
