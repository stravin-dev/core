package node

import (
	"context"
	"fmt"
	"sync"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type refFlightCall struct {
	done    chan struct{}
	meta    ddb.RepoMeta
	refs    []ddb.RefState
	err     error
	started bool
	waiters int
}

type refFlightState struct {
	active *refFlightCall
	next   *refFlightCall
}

// refLoadFlightGroup provides generation-batched (linearizable) singleflight
// coalescing for concurrent LoadAuthoritativeRefs calls sharing a RepoKey.
// Callers arriving while Flight N is already running wait for Flight N to
// complete and coalesce together into a single fresh Flight N+1 whose fn
// starts strictly after their arrival, preserving read-after-write linearizability.
type refLoadFlightGroup struct {
	mu     sync.Mutex
	states map[tenant.RepoKey]*refFlightState
}

func newRefLoadFlightGroup() *refLoadFlightGroup {
	return &refLoadFlightGroup{
		states: make(map[tenant.RepoKey]*refFlightState),
	}
}

func (g *refLoadFlightGroup) Do(
	ctx context.Context,
	key tenant.RepoKey,
	fn func(context.Context) (ddb.RepoMeta, []ddb.RefState, error),
) (ddb.RepoMeta, []ddb.RefState, error) {
	if err := ctx.Err(); err != nil {
		return ddb.RepoMeta{}, nil, err
	}

	g.mu.Lock()
	st := g.states[key]
	if st == nil {
		c := &refFlightCall{done: make(chan struct{}), started: true}
		g.states[key] = &refFlightState{active: c}
		g.mu.Unlock()
		return g.runFlight(ctx, key, c, fn)
	}

	if st.active == nil {
		c := st.next
		if c == nil {
			c = &refFlightCall{done: make(chan struct{})}
		}
		st.next = nil
		c.started = true
		st.active = c
		g.mu.Unlock()
		return g.runFlight(ctx, key, c, fn)
	}

	if st.next == nil {
		st.next = &refFlightCall{done: make(chan struct{})}
	}
	myBatch := st.next
	myBatch.waiters++
	activeDone := st.active.done
	g.mu.Unlock()

	select {
	case <-activeDone:
	case <-ctx.Done():
		g.cancelWaiter(key, myBatch)
		return ddb.RepoMeta{}, nil, ctx.Err()
	}

	g.mu.Lock()
	if err := ctx.Err(); err != nil {
		g.cancelWaiterLocked(key, myBatch)
		g.mu.Unlock()
		return ddb.RepoMeta{}, nil, err
	}
	if !myBatch.started {
		myBatch.started = true
		if cur := g.states[key]; cur != nil {
			if cur.next == myBatch {
				cur.next = nil
			}
			cur.active = myBatch
		} else {
			g.states[key] = &refFlightState{active: myBatch}
		}
		g.mu.Unlock()
		return g.runFlight(ctx, key, myBatch, fn)
	}
	g.mu.Unlock()

	select {
	case <-myBatch.done:
		return myBatch.meta, cloneRefStates(myBatch.refs), myBatch.err
	case <-ctx.Done():
		return ddb.RepoMeta{}, nil, ctx.Err()
	}
}

func (g *refLoadFlightGroup) runFlight(
	ctx context.Context,
	key tenant.RepoKey,
	c *refFlightCall,
	fn func(context.Context) (ddb.RepoMeta, []ddb.RefState, error),
) (ddb.RepoMeta, []ddb.RefState, error) {
	defer func() {
		if r := recover(); r != nil {
			c.err = fmt.Errorf("node: ref load flight panicked: %v", r)
			g.finishActive(key, c)
			panic(r)
		}
		g.finishActive(key, c)
	}()

	c.meta, c.refs, c.err = fn(ctx)
	return c.meta, cloneRefStates(c.refs), c.err
}

func (g *refLoadFlightGroup) finishActive(key tenant.RepoKey, c *refFlightCall) {
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

func (g *refLoadFlightGroup) cancelWaiter(key tenant.RepoKey, batch *refFlightCall) {
	g.mu.Lock()
	g.cancelWaiterLocked(key, batch)
	g.mu.Unlock()
}

func (g *refLoadFlightGroup) cancelWaiterLocked(key tenant.RepoKey, batch *refFlightCall) {
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

func cloneRefStates(refs []ddb.RefState) []ddb.RefState {
	if len(refs) == 0 {
		return nil
	}
	cp := make([]ddb.RefState, len(refs))
	copy(cp, refs)
	return cp
}
