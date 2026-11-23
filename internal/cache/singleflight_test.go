package cache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/tenant"
)

func mustTestRepoKey(t *testing.T, tid, rid string) tenant.RepoKey {
	t.Helper()
	k, err := tenant.NewRepoKey(tid, rid)
	if err != nil {
		t.Fatalf("NewRepoKey(%q, %q): %v", tid, rid, err)
	}
	return k
}

func TestRepoFlightGroup_LinearizableCoalescing(t *testing.T) {
	g := newRepoFlightGroup()
	repo := mustTestRepoKey(t, "acme", "linearizable")

	var version atomic.Int32
	version.Store(1)

	var executions atomic.Int32
	flight1Started := make(chan struct{})
	releaseFlight1 := make(chan struct{})

	// Caller 1 starts Flight 1 while version == 1.
	var caller1Version int32
	caller1Done := make(chan error, 1)
	go func() {
		caller1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			executions.Add(1)
			v := version.Load()
			caller1Version = v
			close(flight1Started)
			<-releaseFlight1
			return nil
		})
	}()

	<-flight1Started

	// Simulate a write committing after Flight 1 started reading state.
	version.Store(2)

	// Launch 5 callers arriving during Flight 1; they must NOT get Flight 1's
	// stale result, and they MUST coalesce into a single fresh Flight 2.
	const waiters = 5
	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	var flight2Observed atomic.Int32

	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := g.Do(context.Background(), repo, func(ctx context.Context) error {
				executions.Add(1)
				flight2Observed.Store(version.Load())
				return nil
			})
			errs <- err
		}()
	}

	// Wait deterministically until all waiters are queued on the next batch.
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		st := g.states[repo]
		queued := 0
		if st != nil && st.next != nil {
			queued = st.next.waiters
		}
		g.mu.Unlock()
		if queued == waiters {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waiters to queue (got %d)", waiters, queued)
		}
		time.Sleep(time.Millisecond)
	}

	close(releaseFlight1)

	if err := <-caller1Done; err != nil {
		t.Fatalf("caller 1 error: %v", err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("waiter error: %v", err)
		}
	}

	if caller1Version != 1 {
		t.Fatalf("caller 1 observed version %d, want 1", caller1Version)
	}
	if got := executions.Load(); got != 2 {
		t.Fatalf("total flight executions = %d, want 2 (Flight 1 + single coalesced Flight 2)", got)
	}
	if got := flight2Observed.Load(); got != 2 {
		t.Fatalf("Flight 2 observed version %d, want 2", got)
	}

	g.mu.Lock()
	remaining := len(g.states)
	g.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d flight state entries", remaining)
	}
}

func TestRepoFlightGroup_WaiterCancelDoesNotBlockSiblings(t *testing.T) {
	g := newRepoFlightGroup()
	repo := mustTestRepoKey(t, "acme", "cancel-sibling")

	flight1Started := make(chan struct{})
	releaseFlight1 := make(chan struct{})
	flight1Done := make(chan error, 1)

	go func() {
		flight1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			close(flight1Started)
			<-releaseFlight1
			return nil
		})
	}()
	<-flight1Started

	// First waiter cancels while Flight 1 is still running.
	cancelCtx, cancel := context.WithCancel(context.Background())
	waiter1Done := make(chan error, 1)
	go func() {
		waiter1Done <- g.Do(cancelCtx, repo, func(ctx context.Context) error {
			t.Error("cancelled waiter callback should not execute")
			return nil
		})
	}()

	// Second waiter stays active and must still execute Flight 2 after Flight 1 finishes.
	var flight2Ran atomic.Int32
	waiter2Done := make(chan error, 1)
	go func() {
		waiter2Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			flight2Ran.Add(1)
			return nil
		})
	}()

	// Wait for both waiters to queue.
	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		st := g.states[repo]
		queued := 0
		if st != nil && st.next != nil {
			queued = st.next.waiters
		}
		g.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 2 waiters (got %d)", queued)
		}
		time.Sleep(time.Millisecond)
	}

	cancel()
	if err := <-waiter1Done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter err = %v, want context.Canceled", err)
	}

	close(releaseFlight1)
	if err := <-flight1Done; err != nil {
		t.Fatalf("flight 1 err = %v", err)
	}
	if err := <-waiter2Done; err != nil {
		t.Fatalf("waiter 2 err = %v", err)
	}
	if got := flight2Ran.Load(); got != 1 {
		t.Fatalf("flight 2 ran %d times, want 1", got)
	}

	g.mu.Lock()
	remaining := len(g.states)
	g.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d flight state entries", remaining)
	}
}

func TestRepoFlightGroup_AllWaitersCancelSkipsNextFlight(t *testing.T) {
	g := newRepoFlightGroup()
	repo := mustTestRepoKey(t, "acme", "all-cancel")

	var executions atomic.Int32
	flight1Started := make(chan struct{})
	releaseFlight1 := make(chan struct{})
	flight1Done := make(chan error, 1)

	go func() {
		flight1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			executions.Add(1)
			close(flight1Started)
			<-releaseFlight1
			return nil
		})
	}()
	<-flight1Started

	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	w1 := make(chan error, 1)
	w2 := make(chan error, 1)
	go func() {
		w1 <- g.Do(ctx1, repo, func(ctx context.Context) error {
			executions.Add(1)
			return nil
		})
	}()
	go func() {
		w2 <- g.Do(ctx2, repo, func(ctx context.Context) error {
			executions.Add(1)
			return nil
		})
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		st := g.states[repo]
		queued := 0
		if st != nil && st.next != nil {
			queued = st.next.waiters
		}
		g.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for 2 waiters (got %d)", queued)
		}
		time.Sleep(time.Millisecond)
	}

	cancel1()
	cancel2()
	if err := <-w1; !errors.Is(err, context.Canceled) {
		t.Fatalf("w1 err = %v, want context.Canceled", err)
	}
	if err := <-w2; !errors.Is(err, context.Canceled) {
		t.Fatalf("w2 err = %v, want context.Canceled", err)
	}

	close(releaseFlight1)
	if err := <-flight1Done; err != nil {
		t.Fatalf("flight 1 err = %v", err)
	}
	if got := executions.Load(); got != 1 {
		t.Fatalf("executions = %d, want 1", got)
	}

	g.mu.Lock()
	remaining := len(g.states)
	g.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d flight state entries", remaining)
	}
}

func TestRepoFlightGroup_ErrorPropagationAndPanicSafety(t *testing.T) {
	g := newRepoFlightGroup()
	repo := mustTestRepoKey(t, "acme", "err-panic")

	// 1. Flight 1 returns an error; queued waiters run Flight 2 and succeed.
	errBoom := errors.New("boom")
	f1Started := make(chan struct{})
	f1Release := make(chan struct{})
	f1Done := make(chan error, 1)

	go func() {
		f1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			close(f1Started)
			<-f1Release
			return errBoom
		})
	}()
	<-f1Started

	w1Done := make(chan error, 1)
	w2Done := make(chan error, 1)
	go func() {
		w1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error { return nil })
	}()
	go func() {
		w2Done <- g.Do(context.Background(), repo, func(ctx context.Context) error { return nil })
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		st := g.states[repo]
		queued := 0
		if st != nil && st.next != nil {
			queued = st.next.waiters
		}
		g.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for waiters (got %d)", queued)
		}
		time.Sleep(time.Millisecond)
	}

	close(f1Release)
	if err := <-f1Done; !errors.Is(err, errBoom) {
		t.Fatalf("flight 1 err = %v, want %v", err, errBoom)
	}
	if err := <-w1Done; err != nil {
		t.Fatalf("waiter 1 err = %v, want nil", err)
	}
	if err := <-w2Done; err != nil {
		t.Fatalf("waiter 2 err = %v, want nil", err)
	}

	// 2. Panic in a coalesced batch does not deadlock or report nil to followers.
	p1Started := make(chan struct{})
	p1Release := make(chan struct{})
	p1Done := make(chan error, 1)
	go func() {
		p1Done <- g.Do(context.Background(), repo, func(ctx context.Context) error {
			close(p1Started)
			<-p1Release
			return nil
		})
	}()
	<-p1Started

	var panickedCount atomic.Int32
	var followerErrCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panickedCount.Add(1)
				}
			}()
			err := g.Do(context.Background(), repo, func(ctx context.Context) error {
				panic("simulated flight panic")
			})
			if err != nil && strings.Contains(err.Error(), "simulated flight panic") {
				followerErrCount.Add(1)
			}
		}()
	}

	deadline = time.Now().Add(2 * time.Second)
	for {
		g.mu.Lock()
		st := g.states[repo]
		queued := 0
		if st != nil && st.next != nil {
			queued = st.next.waiters
		}
		g.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for panic-test waiters (got %d)", queued)
		}
		time.Sleep(time.Millisecond)
	}

	close(p1Release)
	if err := <-p1Done; err != nil {
		t.Fatalf("p1 err = %v", err)
	}
	wg.Wait()

	if panickedCount.Load() != 1 || followerErrCount.Load() != 1 {
		t.Fatalf("panicked=%d followerErr=%d, want 1 and 1", panickedCount.Load(), followerErrCount.Load())
	}

	g.mu.Lock()
	remaining := len(g.states)
	g.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("leaked %d flight state entries after panic", remaining)
	}
}
