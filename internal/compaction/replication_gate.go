package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultCRRBreakerThreshold is the default number of consecutive
	// non-COMPLETED S3 CRR replication checks before the GC circuit breaker trips.
	DefaultCRRBreakerThreshold = 5
	// DefaultCRRBreakerCooldown is the default duration GC deletions remain
	// paused after the CRR-lag circuit breaker trips.
	DefaultCRRBreakerCooldown = 15 * time.Minute
)

// ObjectHeadChecker is the narrow S3 HeadObject contract used by the CRR
// replication gate. *s3.Store satisfies this interface directly.
type ObjectHeadChecker interface {
	HeadObject(ctx context.Context, key string) (s3.ObjectMeta, error)
}

var _ ObjectHeadChecker = (*s3.Store)(nil)

// ReplicationDecision is the outcome of a CRR replication-gate evaluation.
type ReplicationDecision struct {
	Allowed     bool
	Reason      string
	BreakerOpen bool
}

// ReplicationGate decides whether a superseded COMPACTED pack may be safely
// deleted based on S3 Cross-Region Replication status and lag circuit-breaker state.
type ReplicationGate interface {
	AllowDeletion(ctx context.Context, repo tenant.RepoKey, replacementPackIDs []string) ReplicationDecision
	IsBreakerOpen() bool
}

// CRRCircuitBreakerConfig configures the CRR-lag circuit breaker.
type CRRCircuitBreakerConfig struct {
	Threshold int
	Cooldown  time.Duration
	Clock     lease.Clock
}

// CRRCircuitBreaker tracks consecutive non-COMPLETED / failed S3 CRR checks and
// pauses GC deletions for a cooldown window once the threshold is reached.
type CRRCircuitBreaker struct {
	mu                  sync.Mutex
	threshold           int
	cooldown            time.Duration
	clock               lease.Clock
	consecutiveFailures int
	isOpen              bool
	openUntil           time.Time
}

// NewCRRCircuitBreaker constructs a CRR-lag circuit breaker with safe defaults.
func NewCRRCircuitBreaker(cfg CRRCircuitBreakerConfig) *CRRCircuitBreaker {
	threshold := cfg.Threshold
	if threshold <= 0 {
		threshold = DefaultCRRBreakerThreshold
	}
	cooldown := cfg.Cooldown
	if cooldown <= 0 {
		cooldown = DefaultCRRBreakerCooldown
	}
	clk := cfg.Clock
	if clk == nil {
		clk = lease.RealClock{}
	}
	return &CRRCircuitBreaker{
		threshold: threshold,
		cooldown:  cooldown,
		clock:     clk,
	}
}

// IsOpen reports whether the circuit breaker is currently open, automatically
// closing and resetting consecutive failures once the cooldown window elapses.
func (b *CRRCircuitBreaker) IsOpen() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isOpenLocked(b.clock.Now())
}

// RecordSuccess resets the failure streak and closes the breaker.
func (b *CRRCircuitBreaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutiveFailures = 0
	b.isOpen = false
	b.openUntil = time.Time{}
}

// RecordFailure increments the consecutive failure count and trips the breaker
// once threshold is reached. It returns true if the breaker is open.
func (b *CRRCircuitBreaker) RecordFailure() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	if b.isOpenLocked(now) {
		return true
	}
	b.consecutiveFailures++
	if b.consecutiveFailures >= b.threshold {
		b.isOpen = true
		b.openUntil = now.Add(b.cooldown)
		return true
	}
	return false
}

// ConsecutiveFailures returns the current consecutive failure streak.
func (b *CRRCircuitBreaker) ConsecutiveFailures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.isOpenLocked(b.clock.Now())
	return b.consecutiveFailures
}

func (b *CRRCircuitBreaker) isOpenLocked(now time.Time) bool {
	if !b.isOpen {
		return false
	}
	if !now.Before(b.openUntil) {
		b.isOpen = false
		b.consecutiveFailures = 0
		b.openUntil = time.Time{}
		return false
	}
	return true
}

// S3ReplicationGateConfig configures an S3ReplicationGate.
type S3ReplicationGateConfig struct {
	Objects ObjectHeadChecker
	Breaker *CRRCircuitBreaker
	Metrics *Metrics
	Logger  *slog.Logger
}

// S3ReplicationGate verifies via S3 HeadObject that replacement compacted packs
// (.pack and .idx) report ReplicationStatus COMPLETED/COMPLETE before GC may
// delete superseded COMPACTED packs.
type S3ReplicationGate struct {
	objects ObjectHeadChecker
	breaker *CRRCircuitBreaker
	metrics *Metrics
	logger  *slog.Logger
}

var _ ReplicationGate = (*S3ReplicationGate)(nil)

// NewS3ReplicationGate constructs a fail-closed S3 CRR replication gate.
func NewS3ReplicationGate(cfg S3ReplicationGateConfig) (*S3ReplicationGate, error) {
	if cfg.Objects == nil {
		return nil, fmt.Errorf("%w: nil object head checker", ErrInvalidConfig)
	}
	breaker := cfg.Breaker
	if breaker == nil {
		breaker = NewCRRCircuitBreaker(CRRCircuitBreakerConfig{})
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &S3ReplicationGate{
		objects: cfg.Objects,
		breaker: breaker,
		metrics: cfg.Metrics,
		logger:  logger,
	}, nil
}

// IsBreakerOpen reports whether the underlying CRR-lag circuit breaker is open.
func (g *S3ReplicationGate) IsBreakerOpen() bool {
	return g.breaker.IsOpen()
}

// AllowDeletion checks the CRR-lag circuit breaker and verifies that every
// replacement pack (.pack and .idx) reports S3 ReplicationStatus COMPLETED
// (or COMPLETE). Any non-completed status or HeadObject error fails closed.
func (g *S3ReplicationGate) AllowDeletion(ctx context.Context, repo tenant.RepoKey, replacementPackIDs []string) ReplicationDecision {
	if g.breaker.IsOpen() {
		g.metrics.recordGCCRRBreakerOpen(ctx, true)
		g.metrics.recordGCCRRBlocked(ctx, "breaker_open")
		return ReplicationDecision{
			Allowed:     false,
			Reason:      "breaker_open",
			BreakerOpen: true,
		}
	}
	g.metrics.recordGCCRRBreakerOpen(ctx, false)

	if len(replacementPackIDs) == 0 {
		return g.recordBlocked(ctx, repo, "", "", "", "no_replacement", nil)
	}

	for _, packID := range replacementPackIDs {
		packKey, err := s3.PackKey(repo, packID)
		if err != nil {
			return g.recordBlocked(ctx, repo, packID, "", "", "head_error", err)
		}
		idxKey, err := s3.IndexKey(repo, packID)
		if err != nil {
			return g.recordBlocked(ctx, repo, packID, "", "", "head_error", err)
		}

		for _, key := range []string{packKey, idxKey} {
			meta, err := g.objects.HeadObject(ctx, key)
			if err != nil {
				return g.recordBlocked(ctx, repo, packID, key, "", "head_error", err)
			}
			ok, reason := classifyReplicationStatus(meta.ReplicationStatus)
			if !ok {
				return g.recordBlocked(ctx, repo, packID, key, meta.ReplicationStatus, reason, nil)
			}
		}
	}

	g.breaker.RecordSuccess()
	g.metrics.recordGCCRRBreakerOpen(ctx, false)
	return ReplicationDecision{
		Allowed:     true,
		Reason:      "completed",
		BreakerOpen: false,
	}
}

func (g *S3ReplicationGate) recordBlocked(
	ctx context.Context,
	repo tenant.RepoKey,
	packID, key, status, reason string,
	headErr error,
) ReplicationDecision {
	tripped := g.breaker.RecordFailure()
	g.metrics.recordGCCRRBlocked(ctx, reason)
	g.metrics.recordGCCRRBreakerOpen(ctx, tripped)

	attrs := []any{
		"tenant_id", string(repo.TenantID),
		"repo_id", string(repo.RepoID),
		"replacement_pack_id", packID,
		"key", key,
		"replication_status", status,
		"reason", reason,
		"consecutive_failures", g.breaker.ConsecutiveFailures(),
		"breaker_open", tripped,
	}
	if headErr != nil {
		attrs = append(attrs, "error", headErr)
	}
	if tripped {
		g.logger.Warn("stravin-pack GC CRR circuit breaker tripped; pausing GC deletions", attrs...)
	} else {
		g.logger.Warn("stravin-pack GC skipped deletion due to S3 CRR replication gate", attrs...)
	}

	return ReplicationDecision{
		Allowed:     false,
		Reason:      reason,
		BreakerOpen: tripped,
	}
}

func classifyReplicationStatus(raw string) (completed bool, reason string) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case s3.ReplicationStatusCompleted, s3.ReplicationStatusComplete:
		return true, "completed"
	case s3.ReplicationStatusPending:
		return false, "pending"
	case s3.ReplicationStatusFailed:
		return false, "failed"
	case s3.ReplicationStatusReplica:
		return false, "replica"
	case "":
		return false, "missing_status"
	default:
		return false, "unknown_status"
	}
}
