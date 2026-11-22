package wasm

import (
	"context"
	"errors"
	"testing"
	"time"
)

type recordingDiffTreeReader struct {
	stubObjectReader
	lastLimit    uint32
	lastCtx      context.Context
	blockForCtx  bool
	ctxDiffCalls int
}

func (r *recordingDiffTreeReader) DiffTree(oldTreeOID, newTreeOID [20]byte, limit uint32) ([]TreeChange, error) {
	r.mu.Lock()
	r.lastLimit = limit
	r.mu.Unlock()
	return r.stubObjectReader.DiffTree(oldTreeOID, newTreeOID, limit)
}

func (r *recordingDiffTreeReader) DiffTreeContext(ctx context.Context, oldTreeOID, newTreeOID [20]byte, limit uint32) ([]TreeChange, error) {
	r.mu.Lock()
	r.ctxDiffCalls++
	r.lastLimit = limit
	r.lastCtx = ctx
	block := r.blockForCtx
	r.mu.Unlock()

	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.stubObjectReader.DiffTree(oldTreeOID, newTreeOID, limit)
}

func TestBudgetedObjectReaderDiffTreeLimitClamping(t *testing.T) {
	cases := []struct {
		name      string
		reqLimit  uint32
		wantLimit uint32
	}{
		{name: "zero defaults to DefaultDiffTreeLimit", reqLimit: 0, wantLimit: DefaultDiffTreeLimit},
		{name: "within bounds preserved", reqLimit: 42, wantLimit: 42},
		{name: "excessive limit clamped to MaxDiffTreeLimit", reqLimit: MaxDiffTreeLimit + 99999, wantLimit: MaxDiffTreeLimit},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inner := &recordingDiffTreeReader{}
			budgeted := newBudgetedObjectReader(inner, DefaultMaxCommitsWalked, DefaultMaxBlobBytesRead, DefaultMaxHostCalls)

			if _, err := budgeted.DiffTree(context.Background(), oidByte(1), oidByte(2), tc.reqLimit); err != nil {
				t.Fatalf("DiffTree error: %v", err)
			}
			if inner.lastLimit != tc.wantLimit {
				t.Fatalf("forwarded limit = %d, want %d", inner.lastLimit, tc.wantLimit)
			}
		})
	}
}

func TestBudgetedObjectReaderDiffTreeContextCancellation(t *testing.T) {
	t.Run("pre-cancelled context returns immediately without calling inner", func(t *testing.T) {
		inner := &recordingDiffTreeReader{}
		budgeted := newBudgetedObjectReader(inner, DefaultMaxCommitsWalked, DefaultMaxBlobBytesRead, DefaultMaxHostCalls)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := budgeted.DiffTree(ctx, oidByte(1), oidByte(2), 10)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
		if inner.ctxDiffCalls != 0 || inner.diffCalls != 0 {
			t.Fatalf("expected 0 inner calls on cancelled ctx, got ctx=%d plain=%d", inner.ctxDiffCalls, inner.diffCalls)
		}
	})

	t.Run("execCtx timeout propagates to ContextDiffTreeReader", func(t *testing.T) {
		inner := &recordingDiffTreeReader{blockForCtx: true}
		budgeted := newBudgetedObjectReader(inner, DefaultMaxCommitsWalked, DefaultMaxBlobBytesRead, DefaultMaxHostCalls)

		execCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, err := budgeted.DiffTree(execCtx, oidByte(1), oidByte(2), 100)
		elapsed := time.Since(start)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v, want context.DeadlineExceeded", err)
		}
		if elapsed > time.Second {
			t.Fatalf("timeout propagation took too long: %v", elapsed)
		}
		if inner.ctxDiffCalls != 1 {
			t.Fatalf("expected DiffTreeContext called once, got %d", inner.ctxDiffCalls)
		}
	})
}
