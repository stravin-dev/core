package edge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultPackCacheTTL is the default expiration for cached full-clone pack responses.
	DefaultPackCacheTTL = 5 * time.Minute

	// DefaultPackCacheValkeyMaxEntryBytes caps single responses stored in Valkey (8 MiB).
	DefaultPackCacheValkeyMaxEntryBytes = 8 << 20

	// DefaultPackCacheS3MaxEntryBytes caps single responses spilled to S3 (64 MiB).
	DefaultPackCacheS3MaxEntryBytes = 64 << 20

	maxV2FetchCacheReqBytes      = 512 << 10 // 512 KiB max buffered v2 fetch request body
	maxPktLineLen                = 65520
	defaultPackCacheWriteTimeout = 5 * time.Second
)

// PackResponseCache is a pluggable external cache contract for immutable
// Protocol v2 full-clone git-upload-pack responses.
type PackResponseCache interface {
	Get(ctx context.Context, key string) (data []byte, hit bool, err error)
	Set(ctx context.Context, key string, data []byte, ttl time.Duration) error
}

// NoopPackResponseCache disables external pack response caching.
type NoopPackResponseCache struct{}

// Get always reports a cache miss.
func (NoopPackResponseCache) Get(context.Context, string) ([]byte, bool, error) {
	return nil, false, nil
}

// Set is a no-op.
func (NoopPackResponseCache) Set(context.Context, string, []byte, time.Duration) error {
	return nil
}

// TieredPackCacheConfig configures a two-tier external pack response cache:
// Valkey for hot small/medium packs up to ValkeyMaxEntryBytes and Floci/S3
// spillover for larger packs up to S3MaxEntryBytes.
type TieredPackCacheConfig struct {
	Valkey              PackResponseCache
	S3                  PackResponseCache
	ValkeyMaxEntryBytes int
	S3MaxEntryBytes     int
	Logger              *slog.Logger
	Metrics             *Metrics
}

// TieredPackCache routes small/medium pack responses to Valkey and spills
// larger entries (exceeding ValkeyMaxEntryBytes) to S3, degrading gracefully
// if either external tier is disabled or temporarily unreachable.
type TieredPackCache struct {
	valkey              PackResponseCache
	s3                  PackResponseCache
	valkeyMaxEntryBytes int
	s3MaxEntryBytes     int
	logger              *slog.Logger
	metrics             *Metrics
}

var _ PackResponseCache = (*TieredPackCache)(nil)

// NewTieredPackCache builds a TieredPackCache from cfg.
func NewTieredPackCache(cfg TieredPackCacheConfig) *TieredPackCache {
	valkeyMax := cfg.ValkeyMaxEntryBytes
	if valkeyMax <= 0 && cfg.Valkey != nil {
		valkeyMax = DefaultPackCacheValkeyMaxEntryBytes
	}
	s3Max := cfg.S3MaxEntryBytes
	if s3Max <= 0 && cfg.S3 != nil {
		s3Max = DefaultPackCacheS3MaxEntryBytes
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &TieredPackCache{
		valkey:              cfg.Valkey,
		s3:                  cfg.S3,
		valkeyMaxEntryBytes: valkeyMax,
		s3MaxEntryBytes:     s3Max,
		logger:              logger,
		metrics:             cfg.Metrics,
	}
}

// MaxEntryBytes returns the largest single entry size accepted by any configured tier.
func (c *TieredPackCache) MaxEntryBytes() int {
	if c == nil {
		return 0
	}
	maxBytes := 0
	if c.valkey != nil && c.valkeyMaxEntryBytes > maxBytes {
		maxBytes = c.valkeyMaxEntryBytes
	}
	if c.s3 != nil && c.s3MaxEntryBytes > maxBytes {
		maxBytes = c.s3MaxEntryBytes
	}
	return maxBytes
}

// Get checks Valkey first, then falls back to S3 on miss or Valkey error.
func (c *TieredPackCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	if c.valkey != nil {
		data, hit, err := c.valkey.Get(ctx, key)
		if err != nil {
			c.logger.WarnContext(ctx, "edge: valkey pack cache get failed, falling back", "error", err)
		} else if hit {
			return data, true, nil
		}
	}
	if c.s3 != nil {
		data, hit, err := c.s3.Get(ctx, key)
		if err != nil {
			c.logger.WarnContext(ctx, "edge: s3 pack cache get failed, falling back", "error", err)
			return nil, false, nil
		}
		if hit {
			return data, true, nil
		}
	}
	return nil, false, nil
}

// Set writes entries <= ValkeyMaxEntryBytes to Valkey (when configured) and
// spills larger entries (or all entries when Valkey is absent) up to
// S3MaxEntryBytes to S3.
func (c *TieredPackCache) Set(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if c == nil || len(data) == 0 {
		return nil
	}
	if c.valkey != nil && len(data) <= c.valkeyMaxEntryBytes {
		if err := c.valkey.Set(ctx, key, data, ttl); err != nil {
			c.logger.WarnContext(ctx, "edge: valkey pack cache set failed", "error", err)
			return err
		}
		return nil
	}
	if c.s3 != nil && len(data) <= c.s3MaxEntryBytes {
		if err := c.s3.Set(ctx, key, data, ttl); err != nil {
			c.logger.WarnContext(ctx, "edge: s3 pack cache set failed", "error", err)
			return err
		}
		return nil
	}
	if c.s3 != nil {
		c.metrics.RecordPackCacheWrite(ctx, "s3", "too_large")
	} else if c.valkey != nil {
		c.metrics.RecordPackCacheWrite(ctx, "valkey", "too_large")
	}
	return nil
}

type packCacheFlight struct {
	done chan struct{}
	data []byte
}

// PackCacheCoordinatorConfig configures singleflight coalescing around a PackResponseCache.
type PackCacheCoordinatorConfig struct {
	Backend         PackResponseCache
	TTL             time.Duration
	MaxCaptureBytes int
	WriteTimeout    time.Duration
	Logger          *slog.Logger
	Metrics         *Metrics
}

// PackCacheCoordinator wraps a PackResponseCache with in-process singleflight
// coalescing so concurrent identical full-clone fetches execute only one
// upstream git-upload-pack call.
type PackCacheCoordinator struct {
	backend         PackResponseCache
	ttl             time.Duration
	maxCaptureBytes int
	writeTimeout    time.Duration
	logger          *slog.Logger
	metrics         *Metrics

	mu       sync.Mutex
	inflight map[string]*packCacheFlight
}

// NewPackCacheCoordinator constructs a PackCacheCoordinator. Passing a nil
// backend returns nil so callers treat caching as disabled.
func NewPackCacheCoordinator(cfg PackCacheCoordinatorConfig) *PackCacheCoordinator {
	if cfg.Backend == nil {
		return nil
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = DefaultPackCacheTTL
	}
	maxCapture := cfg.MaxCaptureBytes
	if maxCapture <= 0 {
		if tiered, ok := cfg.Backend.(*TieredPackCache); ok {
			maxCapture = tiered.MaxEntryBytes()
		}
	}
	if maxCapture <= 0 {
		maxCapture = DefaultPackCacheValkeyMaxEntryBytes
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultPackCacheWriteTimeout
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &PackCacheCoordinator{
		backend:         cfg.Backend,
		ttl:             ttl,
		maxCaptureBytes: maxCapture,
		writeTimeout:    writeTimeout,
		logger:          logger,
		metrics:         cfg.Metrics,
		inflight:        make(map[string]*packCacheFlight),
	}
}

// MaxCaptureBytes returns the maximum response size buffered for caching.
func (c *PackCacheCoordinator) MaxCaptureBytes() int {
	if c == nil {
		return 0
	}
	return c.maxCaptureBytes
}

// GetOrJoin checks the external cache, joins an in-flight leader for key if
// present, or registers the caller as leader and returns an idempotent finish
// callback that writes the response to the external cache and wakes followers.
func (c *PackCacheCoordinator) GetOrJoin(ctx context.Context, key string) (data []byte, finish func([]byte), hit bool, err error) {
	if c == nil || c.backend == nil || c.maxCaptureBytes <= 0 {
		return nil, nil, false, nil
	}

	cached, ok, getErr := c.backend.Get(ctx, key)
	if getErr != nil {
		c.logger.WarnContext(ctx, "edge: external pack cache get error, bypassing cache", "error", getErr)
	} else if ok && len(cached) > 0 {
		return cached, nil, true, nil
	}

	c.mu.Lock()
	if flight, exists := c.inflight[key]; exists {
		c.mu.Unlock()
		select {
		case <-flight.done:
			if len(flight.data) > 0 {
				c.metrics.RecordPackCacheLookup(ctx, "singleflight", "hit")
				c.metrics.RecordPackCacheBytes(ctx, "singleflight", "hit_served", int64(len(flight.data)))
				return flight.data, nil, true, nil
			}
			c.metrics.RecordPackCacheLookup(ctx, "singleflight", "miss")
			return nil, nil, false, nil
		case <-ctx.Done():
			c.metrics.RecordPackCacheLookup(ctx, "singleflight", "error")
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
			if len(resp) > 0 && len(resp) <= c.maxCaptureBytes {
				stored = append([]byte(nil), resp...)
			}
			if len(stored) > 0 {
				writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.writeTimeout)
				if setErr := c.backend.Set(writeCtx, key, stored, c.ttl); setErr != nil {
					c.logger.WarnContext(writeCtx, "edge: external pack cache set error", "error", setErr)
				}
				cancel()
			}
			c.mu.Lock()
			if len(stored) > 0 {
				flight.data = stored
			}
			delete(c.inflight, key)
			close(flight.done)
			c.mu.Unlock()
		})
	}
	return nil, finish, false, nil
}

// IsCacheableV2FullCloneFetch returns true when raw is a complete Protocol v2
// `command=fetch` request ending in flush-pkt ("0000") with at least one
// immutable `want <oid>` line, a `done` line, and no `have ` or mutable
// `want-ref ` lines.
func IsCacheableV2FullCloneFetch(raw []byte) bool {
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

func nextPktLine(raw []byte, pos int) (pktLen int, payload []byte, nextPos int, ok bool) {
	if pos+4 > len(raw) {
		return 0, nil, pos, false
	}
	n, err := strconv.ParseInt(string(raw[pos:pos+4]), 16, 32)
	if err != nil || n < 0 || n > maxPktLineLen {
		return 0, nil, pos, false
	}
	if n < 4 {
		return int(n), nil, pos + 4, true
	}
	end := pos + int(n)
	if end > len(raw) {
		return 0, nil, pos, false
	}
	return int(n), raw[pos+4 : end], end, true
}

// UploadPackCacheKey computes a tenant- and repo-scoped content-addressed cache key.
func UploadPackCacheKey(repo tenant.RepoKey, gitProtocol string, requestBytes []byte) string {
	h := sha256.New()
	h.Write([]byte(repo.String()))
	h.Write([]byte{0})
	h.Write([]byte(gitProtocol))
	h.Write([]byte{0})
	h.Write(requestBytes)
	return hex.EncodeToString(h.Sum(nil))
}

func readBoundedRequest(r io.Reader, maxBytes int) (buf []byte, completeEOF bool, err error) {
	if r == nil || maxBytes <= 0 {
		return nil, false, nil
	}
	limited := io.LimitReader(r, int64(maxBytes)+1)
	data, readErr := io.ReadAll(limited)
	if readErr != nil {
		return data, false, readErr
	}
	if len(data) > maxBytes {
		return data, false, nil
	}
	return data, true, nil
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

// ErrPackCacheEntryTooLarge is returned by a single tier when a value exceeds its cap.
var ErrPackCacheEntryTooLarge = errors.New("edge: pack cache entry exceeds max size")
