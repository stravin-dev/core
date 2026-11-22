package wasm

import "context"

// bulkhead enforces a bounded concurrency cap on synchronous Wasm hook execution
// so hook-heavy pushes cannot starve Git pack reads or uploads on the same node.
type bulkhead struct {
	sem chan struct{}
}

func newBulkhead(maxConcurrency int) *bulkhead {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}
	return &bulkhead{
		sem: make(chan struct{}, maxConcurrency),
	}
}

func (b *bulkhead) Acquire(ctx context.Context) error {
	select {
	case b.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *bulkhead) Release() {
	<-b.sem
}
