package edge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

var (
	// ErrRateLimited is returned when a tenant exceeds its ingress rate, concurrency, or bandwidth quota.
	ErrRateLimited = errors.New("edge: rate limit exceeded")
	// ErrConcurrencyLimited wraps ErrRateLimited when max concurrent clones are reached.
	ErrConcurrencyLimited = fmt.Errorf("%w: concurrent clones limit", ErrRateLimited)
	// ErrBandwidthLimited wraps ErrRateLimited when pack bytes/min budget is exceeded.
	ErrBandwidthLimited = fmt.Errorf("%w: pack bytes/min limit", ErrRateLimited)
)

// Limiter is the GoF Strategy interface for per-tenant ingress rate limiting.
type Limiter interface {
	AllowRequest(ctx context.Context, tenantID tenant.TenantID) error
	AcquireClone(ctx context.Context, tenantID tenant.TenantID) (release func(), err error)
	AllowPackBytes(ctx context.Context, tenantID tenant.TenantID, n int64) error
}

// LimiterConfig configures TokenBucketLimiter quotas per tenant.
// Zero values disable the corresponding quota.
type LimiterConfig struct {
	RequestsPerSecond   float64
	RequestBurst        int
	MaxConcurrentClones int
	PackBytesPerMin     int64
	Clock               lease.Clock
}

type tokenBucket struct {
	tokens     float64
	capacity   float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

func newTokenBucket(capacity, refillRate float64, now time.Time) *tokenBucket {
	return &tokenBucket{
		tokens:     capacity,
		capacity:   capacity,
		refillRate: refillRate,
		lastRefill: now,
	}
}

func (b *tokenBucket) take(cost float64, now time.Time) bool {
	if now.After(b.lastRefill) {
		elapsed := now.Sub(b.lastRefill).Seconds()
		b.tokens += elapsed * b.refillRate
		if b.tokens > b.capacity {
			b.tokens = b.capacity
		}
		b.lastRefill = now
	}
	if b.tokens < cost {
		return false
	}
	b.tokens -= cost
	return true
}

type tenantQuotaState struct {
	rpsBucket    *tokenBucket
	bytesBucket  *tokenBucket
	activeClones int
}

// TokenBucketLimiter is a thread-safe in-memory per-tenant Limiter enforcing
// RPS, concurrent clones, and pack bytes/min.
type TokenBucketLimiter struct {
	mu      sync.Mutex
	cfg     LimiterConfig
	clock   lease.Clock
	tenants map[tenant.TenantID]*tenantQuotaState
}

// NewTokenBucketLimiter constructs a per-tenant TokenBucketLimiter.
func NewTokenBucketLimiter(cfg LimiterConfig) *TokenBucketLimiter {
	if cfg.Clock == nil {
		cfg.Clock = lease.RealClock{}
	}
	if cfg.RequestsPerSecond > 0 && cfg.RequestBurst <= 0 {
		burst := int(cfg.RequestsPerSecond)
		if burst < 1 {
			burst = 1
		}
		cfg.RequestBurst = burst
	}
	return &TokenBucketLimiter{
		cfg:     cfg,
		clock:   cfg.Clock,
		tenants: make(map[tenant.TenantID]*tenantQuotaState),
	}
}

func (l *TokenBucketLimiter) stateLocked(tenantID tenant.TenantID, now time.Time) *tenantQuotaState {
	st, ok := l.tenants[tenantID]
	if ok {
		return st
	}
	st = &tenantQuotaState{}
	if l.cfg.RequestsPerSecond > 0 {
		st.rpsBucket = newTokenBucket(float64(l.cfg.RequestBurst), l.cfg.RequestsPerSecond, now)
	}
	if l.cfg.PackBytesPerMin > 0 {
		st.bytesBucket = newTokenBucket(float64(l.cfg.PackBytesPerMin), float64(l.cfg.PackBytesPerMin)/60.0, now)
	}
	l.tenants[tenantID] = st
	return st
}

// AllowRequest consumes one RPS token for tenantID or returns ErrRateLimited.
func (l *TokenBucketLimiter) AllowRequest(_ context.Context, tenantID tenant.TenantID) error {
	if l.cfg.RequestsPerSecond <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	st := l.stateLocked(tenantID, now)
	if !st.rpsBucket.take(1.0, now) {
		return ErrRateLimited
	}
	return nil
}

// AcquireClone claims a concurrent clone slot for tenantID, returning an idempotent release function.
func (l *TokenBucketLimiter) AcquireClone(_ context.Context, tenantID tenant.TenantID) (func(), error) {
	if l.cfg.MaxConcurrentClones <= 0 {
		return func() {}, nil
	}
	l.mu.Lock()
	now := l.clock.Now()
	st := l.stateLocked(tenantID, now)
	if st.activeClones >= l.cfg.MaxConcurrentClones {
		l.mu.Unlock()
		return nil, ErrConcurrencyLimited
	}
	st.activeClones++
	l.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			l.mu.Lock()
			if st.activeClones > 0 {
				st.activeClones--
			}
			l.mu.Unlock()
		})
	}
	return release, nil
}

// AllowPackBytes consumes n bytes from tenantID's pack bytes/min bucket or returns ErrBandwidthLimited.
func (l *TokenBucketLimiter) AllowPackBytes(_ context.Context, tenantID tenant.TenantID, n int64) error {
	if l.cfg.PackBytesPerMin <= 0 || n <= 0 {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.clock.Now()
	st := l.stateLocked(tenantID, now)
	if !st.bytesBucket.take(float64(n), now) {
		return ErrBandwidthLimited
	}
	return nil
}

// ByteLimitingReader wraps an io.Reader and enforces Limiter.AllowPackBytes incrementally
// on every Read without buffering the stream in memory.
type ByteLimitingReader struct {
	ctx      context.Context
	r        io.Reader
	limiter  Limiter
	tenantID tenant.TenantID
}

// NewByteLimitingReader returns a streaming reader enforcing per-tenant pack byte quotas.
func NewByteLimitingReader(ctx context.Context, r io.Reader, limiter Limiter, tenantID tenant.TenantID) io.Reader {
	if limiter == nil || r == nil {
		return r
	}
	return &ByteLimitingReader{
		ctx:      ctx,
		r:        r,
		limiter:  limiter,
		tenantID: tenantID,
	}
}

func (b *ByteLimitingReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		if limErr := b.limiter.AllowPackBytes(b.ctx, b.tenantID, int64(n)); limErr != nil {
			return 0, limErr
		}
	}
	return n, err
}
