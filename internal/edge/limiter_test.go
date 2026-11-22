package edge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
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

func TestTokenBucketLimiter_RPSIsolationAndRefill(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	lim := NewTokenBucketLimiter(LimiterConfig{
		RequestsPerSecond: 2,
		RequestBurst:      2,
		Clock:             clk,
	})

	ctx := context.Background()
	if err := lim.AllowRequest(ctx, "tenant-a"); err != nil {
		t.Fatalf("req 1 failed: %v", err)
	}
	if err := lim.AllowRequest(ctx, "tenant-a"); err != nil {
		t.Fatalf("req 2 failed: %v", err)
	}
	if err := lim.AllowRequest(ctx, "tenant-a"); !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited on burst exhaustion, got %v", err)
	}

	// Noisy tenant-a must not impact tenant-b.
	if err := lim.AllowRequest(ctx, "tenant-b"); err != nil {
		t.Fatalf("tenant-b should not be limited: %v", err)
	}

	// Advance 500ms -> 1 token refilled at 2 RPS.
	clk.Advance(500 * time.Millisecond)
	if err := lim.AllowRequest(ctx, "tenant-a"); err != nil {
		t.Fatalf("expected refill after 500ms, got %v", err)
	}
}

func TestTokenBucketLimiter_ConcurrentClones(t *testing.T) {
	lim := NewTokenBucketLimiter(LimiterConfig{
		MaxConcurrentClones: 1,
	})
	ctx := context.Background()

	rel, err := lim.AcquireClone(ctx, "acme")
	if err != nil {
		t.Fatalf("AcquireClone 1: %v", err)
	}
	if _, err := lim.AcquireClone(ctx, "acme"); !errors.Is(err, ErrConcurrencyLimited) {
		t.Fatalf("expected ErrConcurrencyLimited, got %v", err)
	}
	// Idempotent release.
	rel()
	rel()

	rel2, err := lim.AcquireClone(ctx, "acme")
	if err != nil {
		t.Fatalf("AcquireClone after release: %v", err)
	}
	rel2()
}

func TestTokenBucketLimiter_PackBytesStreaming(t *testing.T) {
	clk := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	lim := NewTokenBucketLimiter(LimiterConfig{
		PackBytesPerMin: 100,
		Clock:           clk,
	})

	r := NewByteLimitingReader(context.Background(), bytes.NewReader(make([]byte, 150)), lim, "acme")
	buf := make([]byte, 60)
	if _, err := r.Read(buf); err != nil {
		t.Fatalf("first 60 bytes should succeed: %v", err)
	}
	if _, err := r.Read(buf); !errors.Is(err, ErrBandwidthLimited) {
		t.Fatalf("expected ErrBandwidthLimited on exceeding 100 bytes/min, got %v", err)
	}

	// Refill 60s -> full 100-byte budget restored.
	clk.Advance(60 * time.Second)
	if _, err := io.ReadAll(NewByteLimitingReader(context.Background(), bytes.NewReader(make([]byte, 80)), lim, "acme")); err != nil {
		t.Fatalf("expected 80 bytes after refill, got %v", err)
	}
}
