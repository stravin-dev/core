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

// cachedModule reference-counts a compiled wazero module so concurrent invocations
// never race with LRU eviction calling Close.
type cachedModule struct {
	key      moduleCacheKey
	compiled wazero.CompiledModule

	mu      sync.Mutex
	refs    int
	evicted bool
}

func (m *cachedModule) acquire() {
	m.mu.Lock()
	m.refs++
	m.mu.Unlock()
}

func (m *cachedModule) release(ctx context.Context) {
	m.mu.Lock()
	m.refs--
	shouldClose := m.refs == 0 && m.evicted
	m.mu.Unlock()
	if shouldClose && m.compiled != nil {
		_ = m.compiled.Close(ctx)
	}
}

func (m *cachedModule) markEvicted(ctx context.Context) {
	m.mu.Lock()
	m.evicted = true
	shouldClose := m.refs == 0
	m.mu.Unlock()
	if shouldClose && m.compiled != nil {
		_ = m.compiled.Close(ctx)
	}
}

// moduleCache is a thread-safe LRU cache of pre-compiled wazero.CompiledModule instances.
type moduleCache struct {
	mu         sync.Mutex
	maxEntries int
	ll         *list.List
	entries    map[moduleCacheKey]*list.Element
}

func newModuleCache(maxEntries int) *moduleCache {
	if maxEntries <= 0 {
		maxEntries = DefaultModuleCacheSize
	}
	return &moduleCache{
		maxEntries: maxEntries,
		ll:         list.New(),
		entries:    make(map[moduleCacheKey]*list.Element, maxEntries),
	}
}

// Get returns a ref-counted cachedModule if present. Callers must call release when done.
func (c *moduleCache) Get(key moduleCacheKey) (*cachedModule, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	mod := elem.Value.(*cachedModule)
	mod.acquire()
	return mod, true
}

// Put stores compiled under key (or returns the already-cached entry if another goroutine
// raced to compile it first) and returns a ref-counted handle. Callers must call release when done.
func (c *moduleCache) Put(ctx context.Context, key moduleCacheKey, compiled wazero.CompiledModule) *cachedModule {
	var toClose wazero.CompiledModule
	var evicted *cachedModule

	c.mu.Lock()
	if elem, ok := c.entries[key]; ok {
		c.ll.MoveToFront(elem)
		existing := elem.Value.(*cachedModule)
		existing.acquire()
		toClose = compiled
		c.mu.Unlock()
		if toClose != nil {
			_ = toClose.Close(ctx)
		}
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
		tail := c.ll.Back()
		if tail != nil {
			c.ll.Remove(tail)
			evicted = tail.Value.(*cachedModule)
			delete(c.entries, evicted.key)
		}
	}
	c.mu.Unlock()

	if evicted != nil {
		evicted.markEvicted(ctx)
	}
	return entry
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
	evicted := make([]*cachedModule, 0, c.ll.Len())
	for e := c.ll.Front(); e != nil; e = e.Next() {
		evicted = append(evicted, e.Value.(*cachedModule))
	}
	c.ll.Init()
	clear(c.entries)
	c.mu.Unlock()

	for _, mod := range evicted {
		mod.markEvicted(ctx)
	}
}
