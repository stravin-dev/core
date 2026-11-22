package wasm

import (
	"container/list"
	"context"
	"sync"

	"github.com/tetratelabs/wazero"
)

type moduleCacheKey struct {
	sha256           string
	memoryLimitPages uint32
}

type compileFlight struct {
	done chan struct{}
	err  error
}

// cachedModule reference-counts a compiled wazero module so concurrent invocations
// never race with LRU eviction calling Close.
type cachedModule struct {
	key      moduleCacheKey
	compiled wazero.CompiledModule
	refs     int
	evicted  bool
}

// moduleCache is a thread-safe LRU cache of pre-compiled wazero.CompiledModule instances
// with per-key singleflight deduplication so concurrent misses never compile or close duplicates.
type moduleCache struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List
	entries    map[moduleCacheKey]*list.Element
	inflight   map[moduleCacheKey]*compileFlight
}

func newModuleCache(maxEntries int) *moduleCache {
	if maxEntries <= 0 {
		maxEntries = DefaultModuleCacheSize
	}
	return &moduleCache{
		maxEntries: maxEntries,
		ll:         list.New(),
		entries:    make(map[moduleCacheKey]*list.Element, maxEntries),
		inflight:   make(map[moduleCacheKey]*compileFlight),
	}
}

// Get returns a ref-counted cachedModule if present. Callers must call Release when done.
func (c *moduleCache) Get(key moduleCacheKey) (*cachedModule, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	mod := elem.Value.(*cachedModule)
	mod.refs++
	return mod, true
}

// GetOrCompile returns a cached module or compiles it once across concurrent callers for the same key.
// Callers must call Release when done using the returned cachedModule.
func (c *moduleCache) GetOrCompile(
	ctx context.Context,
	key moduleCacheKey,
	compileFn func() (wazero.CompiledModule, error),
) (*cachedModule, bool, error) {
	for {
		c.mu.Lock()
		if elem, ok := c.entries[key]; ok {
			c.ll.MoveToFront(elem)
			mod := elem.Value.(*cachedModule)
			mod.refs++
			c.mu.Unlock()
			return mod, true, nil
		}
		if fl, ok := c.inflight[key]; ok {
			c.mu.Unlock()
			select {
			case <-fl.done:
				if fl.err != nil {
					return nil, false, fl.err
				}
				continue
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}

		fl := &compileFlight{done: make(chan struct{})}
		c.inflight[key] = fl
		c.mu.Unlock()

		compiled, err := compileFn()

		var toClose wazero.CompiledModule
		c.mu.Lock()
		delete(c.inflight, key)
		fl.err = err
		if err != nil {
			close(fl.done)
			c.mu.Unlock()
			return nil, false, err
		}

		entry := &cachedModule{
			key:      key,
			compiled: compiled,
			refs:     1,
		}
		elem := c.ll.PushFront(entry)
		c.entries[key] = elem

		if c.ll.Len() > c.maxEntries {
			if tail := c.ll.Back(); tail != nil {
				c.ll.Remove(tail)
				evicted := tail.Value.(*cachedModule)
				delete(c.entries, evicted.key)
				evicted.evicted = true
				if evicted.refs == 0 && c.inflight[evicted.key] == nil {
					toClose = evicted.compiled
				}
			}
		}
		close(fl.done)
		c.mu.Unlock()

		if toClose != nil {
			_ = toClose.Close(context.Background())
		}
		return entry, false, nil
	}
}

// Put stores compiled under key and returns a ref-counted handle. Callers must call Release when done.
func (c *moduleCache) Put(ctx context.Context, key moduleCacheKey, compiled wazero.CompiledModule) *cachedModule {
	var toClose wazero.CompiledModule

	c.mu.Lock()
	if elem, ok := c.entries[key]; ok {
		c.ll.MoveToFront(elem)
		existing := elem.Value.(*cachedModule)
		existing.refs++
		c.mu.Unlock()
		return existing
	}

	entry := &cachedModule{
		key:      key,
		compiled: compiled,
		refs:     1,
	}
	elem := c.ll.PushFront(entry)
	c.entries[key] = elem

	if c.ll.Len() > c.maxEntries {
		if tail := c.ll.Back(); tail != nil {
			c.ll.Remove(tail)
			evicted := tail.Value.(*cachedModule)
			delete(c.entries, evicted.key)
			evicted.evicted = true
			if evicted.refs == 0 && c.inflight[evicted.key] == nil {
				toClose = evicted.compiled
			}
		}
	}
	c.mu.Unlock()

	if toClose != nil {
		_ = toClose.Close(ctx)
	}
	return entry
}

// Release decrements the reference count on mod and closes it if it was evicted
// and no replacement entry exists in the cache for the same key.
func (c *moduleCache) Release(ctx context.Context, mod *cachedModule) {
	if mod == nil {
		return
	}
	var toClose wazero.CompiledModule
	c.mu.Lock()
	mod.refs--
	if mod.refs == 0 && mod.evicted && c.entries[mod.key] == nil && c.inflight[mod.key] == nil {
		toClose = mod.compiled
	}
	c.mu.Unlock()
	if toClose != nil {
		_ = toClose.Close(ctx)
	}
}

// Len returns the current number of entries in the cache.
func (c *moduleCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// Close evicts and closes all unreferenced cached modules.
func (c *moduleCache) Close(ctx context.Context) {
	c.mu.Lock()
	var toClose []wazero.CompiledModule
	for e := c.ll.Front(); e != nil; e = e.Next() {
		mod := e.Value.(*cachedModule)
		mod.evicted = true
		if mod.refs == 0 && mod.compiled != nil {
			toClose = append(toClose, mod.compiled)
		}
	}
	c.ll.Init()
	clear(c.entries)
	c.mu.Unlock()

	for _, cm := range toClose {
		_ = cm.Close(ctx)
	}
}
