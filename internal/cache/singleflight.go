package cache

import (
	"context"
	"fmt"
	"sync"

	"stravin.dev/core/internal/tenant"
)

type flightCall struct {
	done    chan struct{}
	err     error
	started bool
	waiters int
}

type repoFlightState struct {
	active *flightCall
	next   *flightCall
}

// repoFlightGroup provides generation-batched (linearizable) singleflight
// coalescing for concurrent materialization calls sharing a RepoKey.
// Callers arriving while Flight N is already running wait for Flight N to
// complete and coalesce together into a single fresh Flight N+1 whose fn
// starts strictly after their arrival.
type repoFlightGroup struct {
	mu     sync.Mutex
	states map[tenant.RepoKey]*repoFlightState
}

func newRepoFlightGroup() *repoFlightGroup {
	return &repoFlightGroup{
		states: make(map[tenant.RepoKey]*repoFlightState),
	}
}

// Do guarantees every non-cancelled caller observes a flight whose fn started
// at or after its own Do call, while coalescing concurrent waiters into a
// single next-generation flight. Waiters respect ctx cancellation without
// aborting in-flight work or deadlocking sibling waiters.
func (g *repoFlightGroup) Do(ctx context.Context, key tenant.RepoKey, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	st := g.states[key]
	if st == nil {
		c := &flightCall{done: make(chan struct{}), started: true}
		g.states[key] = &repoFlightState{active: c}
		g.mu.Unlock()
		return g.runFlight(ctx, key, c, fn)
	}

	if st.active == nil {
		c := st.next
		if c == nil {
			c = &flightCall{done: make(chan struct{})}
		}
		st.next = nil
		c.started = true
		st.active = c
		g.mu.Unlock()
		return g.runFlight(ctx, key, c, fn)
	}

	if st.next == nil {
		st.next = &flightCall{done: make(chan struct{})}
	}
	myBatch := st.next
	myBatch.waiters++
	activeDone := st.active.done
	g.mu.Unlock()

	select {
	case <-activeDone:
	case <-ctx.Done():
		g.cancelWaiter(key, myBatch)
		return ctx.Err()
	}

	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.cancelWaiterLocked(key, myBatch)
		g.mu.Unlock()
		return err
	}
	if !myBatch.started {
		myBatch.started = true
		if cur := g.states[key]; cur != nil {
			if cur.next == myBatch {
				cur.next = nil
			}
			cur.active = myBatch
		} else {
			g.states[key] = &repoFlightState{active: myBatch}
		}
		g.mu.Unlock()
		return g.runFlight(ctx, key, myBatch, fn)
	}
	g.mu.Unlock()

	select {
	case <-myBatch.done:
		return myBatch.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (g *repoFlightGroup) runFlight(ctx context.Context, key tenant.RepoKey, c *flightCall, fn func(context.Context) error) error {
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("cache: materialization flight panicked: %v", r)
			g.finishActive(key, c)
			panic(r)
		}
		g.finishActive(key, c)
	}()

	c.err = fn(ctx)
	return c.err
}

func (g *repoFlightGroup) finishActive(key tenant.RepoKey, c *flightCall) {
	g.mu.Lock()
	if st := g.states[key]; st != nil && st.active == c {
		st.active = nil
		if st.next == nil {
			delete(g.states, key)
		}
	}
	g.mu.Unlock()
	close(c.done)
}

func (g *repoFlightGroup) cancelWaiter(key tenant.RepoKey, batch *flightCall) {
	g.mu.Lock()
	g.cancelWaiterLocked(key, batch)
	g.mu.Unlock()
}

func (g *repoFlightGroup) cancelWaiterLocked(key tenant.RepoKey, batch *flightCall) {
	if batch.started {
		return
	}
	batch.waiters--
	if batch.waiters > 0 {
		return
	}
	if st := g.states[key]; st != nil && st.next == batch {
		st.next = nil
		if st.active == nil {
			delete(g.states, key)
		}
	}
}
