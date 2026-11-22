package cache

import (
	"context"
	"sync"

	"stravin.dev/core/internal/tenant"
)

type flightCall struct {
	done chan struct{}
	err  error
}

// repoFlightGroup coalesces concurrent materialization calls for the same RepoKey.
type repoFlightGroup struct {
	mu    sync.Mutex
	calls map[tenant.RepoKey]*flightCall
}

func newRepoFlightGroup() *repoFlightGroup {
	return &repoFlightGroup{
		calls: make(map[tenant.RepoKey]*flightCall),
	}
}

// Do executes fn at most once for concurrent callers sharing key.
// Waiters respect ctx cancellation without aborting the leader's in-flight work.
func (g *repoFlightGroup) Do(ctx context.Context, key tenant.RepoKey, fn func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	g.mu.Lock()
	if c, ok := g.calls[key]; ok {
		g.mu.Unlock()
		select {
		case <-c.done:
			return c.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c := &flightCall{done: make(chan struct{})}
	g.calls[key] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, key)
		g.mu.Unlock()
		close(c.done)
	}()

	c.err = fn(ctx)
	return c.err
}
