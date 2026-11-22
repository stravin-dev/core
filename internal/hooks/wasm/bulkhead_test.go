package wasm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBulkheadEnforcesConcurrencyCap(t *testing.T) {
	const (
		capLimit = 2
		workers  = 8
	)
	bh := newBulkhead(capLimit)

	var active atomic.Int32
	var maxObserved atomic.Int32
	var wg sync.WaitGroup

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := bh.Acquire(context.Background()); err != nil {
				t.Errorf("Acquire failed: %v", err)
				return
			}
			defer bh.Release()

			cur := active.Add(1)
			for {
				prev := maxObserved.Load()
				if cur <= prev || maxObserved.CompareAndSwap(prev, cur) {
					break
				}
			}
			time.Sleep(15 * time.Millisecond)
			active.Add(-1)
		}()
	}
	wg.Wait()

	if got := maxObserved.Load(); got > capLimit {
		t.Fatalf("peak concurrency = %d, exceeds bulkhead cap %d", got, capLimit)
	}
	if got := maxObserved.Load(); got < 2 {
		t.Fatalf("expected concurrent execution up to cap %d, observed peak %d", capLimit, got)
	}
}

func TestBulkheadRespectsContextCancellation(t *testing.T) {
	bh := newBulkhead(1)
	if err := bh.Acquire(context.Background()); err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}
	defer bh.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := bh.Acquire(ctx); err == nil {
		t.Fatal("expected context deadline error while bulkhead full, got nil")
	}
}
