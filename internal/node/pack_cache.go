package node

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"sync"

	"stravin.dev/core/internal/tenant"
)

const (
	defaultPackCacheMaxBytes      = 64 << 20 // 64 MiB total response cache cap
	defaultPackCacheMaxEntryBytes = 8 << 20  // 8 MiB single response cap
)

// isCacheableV2FullCloneFetch returns true when raw is a complete Protocol v2
// `command=fetch` request ending in flush-pkt ("0000") with at least one
// immutable `want <oid>` line, a `done` line, and no `have ` or mutable
// `want-ref ` lines.
func isCacheableV2FullCloneFetch(raw []byte) bool {
	pktLen, payload, pos, ok := nextPktLine(raw, 0)
	if !ok || pktLen < 4 || strings.TrimSuffix(string(payload), "\n") != "command=fetch" {
		return false
	}

	// Phase 1: capabilities until delim-pkt ("0001").
	sawDelim := false
	for pos < len(raw) {
		pktLen, _, nextPos, ok := nextPktLine(raw, pos)
		if !ok {
			return false
		}
		pos = nextPos
		if pktLen == 1 {
			sawDelim = true
			break
		}
		if pktLen < 4 {
			return false
		}
	}
	if !sawDelim {
		return false
	}

	// Phase 2: fetch arguments until flush-pkt ("0000").
	var (
		hasWant bool
		hasDone bool
	)
	for pos < len(raw) {
		pktLen, payload, nextPos, ok := nextPktLine(raw, pos)
		if !ok {
			return false
		}
		pos = nextPos
		if pktLen == 0 {
			return pos == len(raw) && hasWant && hasDone
		}
		if pktLen < 4 {
			return false
		}
		line := strings.TrimSuffix(string(payload), "\n")
		switch {
		case strings.HasPrefix(line, "have "):
			return false
		case strings.HasPrefix(line, "want-ref "):
			return false
		case strings.HasPrefix(line, "want "):
			hasWant = true
		case line == "done":
			hasDone = true
		}
	}
	return false
}

func uploadPackCacheKey(repo tenant.RepoKey, gitProtocol string, requestBytes []byte) string {
	h := sha256.New()
	h.Write([]byte(repo.String()))
	h.Write([]byte{0})
	h.Write([]byte(gitProtocol))
	h.Write([]byte{0})
	h.Write(requestBytes)
	return hex.EncodeToString(h.Sum(nil))
}

type packCacheEntry struct {
	key  string
	data []byte
}

type packCacheFlight struct {
	done chan struct{}
	data []byte
}

// uploadPackResponseCache is a thread-safe bounded LRU cache with singleflight
// coalescing for immutable full-clone Protocol v2 upload-pack responses.
type uploadPackResponseCache struct {
	mu            sync.Mutex
	maxBytes      int
	maxEntryBytes int
	totalBytes    int
	lru           *list.List
	entries       map[string]*list.Element
	inflight      map[string]*packCacheFlight
}

func newUploadPackResponseCache(maxBytes, maxEntryBytes int) *uploadPackResponseCache {
	if maxBytes < 0 {
		maxBytes = 0
	}
	if maxEntryBytes < 0 {
		maxEntryBytes = 0
	}
	if maxEntryBytes > maxBytes {
		maxEntryBytes = maxBytes
	}
	return &uploadPackResponseCache{
		maxBytes:      maxBytes,
		maxEntryBytes: maxEntryBytes,
		lru:           list.New(),
		entries:       make(map[string]*list.Element),
		inflight:      make(map[string]*packCacheFlight),
	}
}

func (c *uploadPackResponseCache) Get(key string) ([]byte, bool) {
	if c == nil || c.maxBytes <= 0 {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	c.lru.MoveToFront(elem)
	return elem.Value.(*packCacheEntry).data, true
}

func (c *uploadPackResponseCache) Put(key string, val []byte) {
	if c == nil || c.maxBytes <= 0 || len(val) == 0 || len(val) > c.maxEntryBytes {
		return
	}
	cp := append([]byte(nil), val...)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.putLocked(key, cp)
}

func (c *uploadPackResponseCache) putLocked(key string, data []byte) {
	if len(data) == 0 || len(data) > c.maxEntryBytes || c.maxBytes <= 0 {
		return
	}
	if elem, ok := c.entries[key]; ok {
		entry := elem.Value.(*packCacheEntry)
		c.totalBytes -= len(entry.data)
		entry.data = data
		c.totalBytes += len(data)
		c.lru.MoveToFront(elem)
	} else {
		elem := c.lru.PushFront(&packCacheEntry{key: key, data: data})
		c.entries[key] = elem
		c.totalBytes += len(data)
	}
	for c.totalBytes > c.maxBytes && c.lru.Len() > 0 {
		back := c.lru.Back()
		c.lru.Remove(back)
		entry := back.Value.(*packCacheEntry)
		delete(c.entries, entry.key)
		c.totalBytes -= len(entry.data)
	}
}

// GetOrJoin returns cached bytes on hit, waits on an in-flight leader for the
// same key, or registers the caller as the leader and returns an idempotent
// finish callback that stores the response (when non-empty and within cap) and
// unblocks any waiting followers.
func (c *uploadPackResponseCache) GetOrJoin(ctx context.Context, key string) (data []byte, finish func([]byte), hit bool, err error) {
	if c == nil || c.maxBytes <= 0 {
		return nil, nil, false, nil
	}

	c.mu.Lock()
	if elem, ok := c.entries[key]; ok {
		c.lru.MoveToFront(elem)
		cached := elem.Value.(*packCacheEntry).data
		c.mu.Unlock()
		return cached, nil, true, nil
	}
	if flight, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-flight.done:
			if len(flight.data) > 0 {
				return flight.data, nil, true, nil
			}
			return nil, nil, false, nil
		case <-ctx.Done():
			return nil, nil, false, ctx.Err()
		}
	}

	flight := &packCacheFlight{done: make(chan struct{})}
	c.inflight[key] = flight
	c.mu.Unlock()

	var once sync.Once
	finish = func(resp []byte) {
		once.Do(func() {
			var stored []byte
			if len(resp) > 0 && len(resp) <= c.maxEntryBytes {
				stored = append([]byte(nil), resp...)
			}
			c.mu.Lock()
			if len(stored) > 0 {
				c.putLocked(key, stored)
				flight.data = stored
			}
			delete(c.inflight, key)
			close(flight.done)
			c.mu.Unlock()
		})
	}
	return nil, finish, false, nil
}

// boundedCaptureWriter tees writes to dst while buffering up to maxBytes in
// memory. If the stream exceeds maxBytes, it drops the buffer immediately and
// continues forwarding writes to dst without error.
type boundedCaptureWriter struct {
	dst      io.Writer
	maxBytes int
	buf      bytes.Buffer
	exceeded bool
}

func (w *boundedCaptureWriter) Write(p []byte) (int, error) {
	n, err := w.dst.Write(p)
	if !w.exceeded && n > 0 {
		if w.buf.Len()+n > w.maxBytes {
			w.exceeded = true
			w.buf.Reset()
		} else {
			w.buf.Write(p[:n])
		}
	}
	return n, err
}
