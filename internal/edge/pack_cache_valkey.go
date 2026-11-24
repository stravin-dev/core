package edge

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultValkeyKeyPrefix    = "stravin:packcache:v1:"
	defaultValkeyDialTimeout  = 2 * time.Second
	defaultValkeyReadTimeout  = 3 * time.Second
	defaultValkeyWriteTimeout = 3 * time.Second
	defaultValkeyPoolSize     = 16
)

// ValkeyPackCacheConfig configures a ValkeyPackCache.
type ValkeyPackCacheConfig struct {
	Addr          string
	KeyPrefix     string
	MaxEntryBytes int
	DialTimeout   time.Duration
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	PoolSize      int
	Metrics       *Metrics
}

// ValkeyPackCache implements PackResponseCache over Valkey's RESP2 protocol
// using a pooled TCP connection without external client dependencies.
type ValkeyPackCache struct {
	addr          string
	keyPrefix     string
	maxEntryBytes int
	dialTimeout   time.Duration
	readTimeout   time.Duration
	writeTimeout  time.Duration
	metrics       *Metrics

	mu     sync.Mutex
	closed bool
	pool   chan net.Conn
}

var _ PackResponseCache = (*ValkeyPackCache)(nil)

// NewValkeyPackCache creates a Valkey-backed PackResponseCache.
func NewValkeyPackCache(cfg ValkeyPackCacheConfig) (*ValkeyPackCache, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil, errors.New("edge: valkey address is required")
	}
	keyPrefix := cfg.KeyPrefix
	if keyPrefix == "" {
		keyPrefix = defaultValkeyKeyPrefix
	}
	maxEntry := cfg.MaxEntryBytes
	if maxEntry <= 0 {
		maxEntry = DefaultPackCacheValkeyMaxEntryBytes
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultValkeyDialTimeout
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = defaultValkeyReadTimeout
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultValkeyWriteTimeout
	}
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultValkeyPoolSize
	}
	return &ValkeyPackCache{
		addr:          addr,
		keyPrefix:     keyPrefix,
		maxEntryBytes: maxEntry,
		dialTimeout:   dialTimeout,
		readTimeout:   readTimeout,
		writeTimeout:  writeTimeout,
		metrics:       cfg.Metrics,
		pool:          make(chan net.Conn, poolSize),
	}, nil
}

// Close closes all idle pooled connections to Valkey.
func (c *ValkeyPackCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.pool)
	c.mu.Unlock()

	var errs []error
	for conn := range c.pool {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Get fetches key from Valkey. Missing keys return (nil, false, nil).
func (c *ValkeyPackCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	start := time.Now()
	defer func() {
		c.metrics.RecordPackCacheOpDuration(ctx, "valkey", "get", time.Since(start))
	}()
	fullKey := c.keyPrefix + key
	for attempt := range 2 {
		conn, fromPool, err := c.acquireConn(ctx)
		if err != nil {
			c.metrics.RecordPackCacheLookup(ctx, "valkey", "error")
			return nil, false, err
		}
		data, hit, err := c.doGet(ctx, conn, fullKey)
		if err == nil {
			c.releaseConn(conn)
			if hit {
				c.metrics.RecordPackCacheLookup(ctx, "valkey", "hit")
				c.metrics.RecordPackCacheBytes(ctx, "valkey", "hit_served", int64(len(data)))
			} else {
				c.metrics.RecordPackCacheLookup(ctx, "valkey", "miss")
			}
			return data, hit, nil
		}
		_ = conn.Close()
		if !fromPool || attempt == 1 || ctx.Err() != nil {
			c.metrics.RecordPackCacheLookup(ctx, "valkey", "error")
			return nil, false, err
		}
	}
	c.metrics.RecordPackCacheLookup(ctx, "valkey", "miss")
	return nil, false, nil
}

// Set stores key in Valkey with EX expiration (seconds). Entries exceeding
// MaxEntryBytes return ErrPackCacheEntryTooLarge without writing.
func (c *ValkeyPackCache) Set(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	if c == nil || len(data) == 0 {
		return nil
	}
	if len(data) > c.maxEntryBytes {
		c.metrics.RecordPackCacheWrite(ctx, "valkey", "too_large")
		return ErrPackCacheEntryTooLarge
	}
	start := time.Now()
	defer func() {
		c.metrics.RecordPackCacheOpDuration(ctx, "valkey", "set", time.Since(start))
	}()
	fullKey := c.keyPrefix + key
	ttlSecs := int64(ttl / time.Second)
	if ttl > 0 && ttlSecs < 1 {
		ttlSecs = 1
	}

	for attempt := range 2 {
		conn, fromPool, err := c.acquireConn(ctx)
		if err != nil {
			c.metrics.RecordPackCacheWrite(ctx, "valkey", "error")
			return err
		}
		err = c.doSet(ctx, conn, fullKey, data, ttlSecs)
		if err == nil {
			c.releaseConn(conn)
			c.metrics.RecordPackCacheWrite(ctx, "valkey", "ok")
			c.metrics.RecordPackCacheBytes(ctx, "valkey", "written", int64(len(data)))
			return nil
		}
		_ = conn.Close()
		if !fromPool || attempt == 1 || ctx.Err() != nil {
			c.metrics.RecordPackCacheWrite(ctx, "valkey", "error")
			return err
		}
	}
	return nil
}

func (c *ValkeyPackCache) acquireConn(ctx context.Context) (net.Conn, bool, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		c.metrics.RecordValkeyPoolDial(ctx, "error", false)
		return nil, false, errors.New("edge: valkey pack cache closed")
	}

	select {
	case conn, ok := <-c.pool:
		if ok && conn != nil {
			c.metrics.RecordValkeyPoolDial(ctx, "ok", true)
			return conn, true, nil
		}
	default:
	}

	dialer := net.Dialer{Timeout: c.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		c.metrics.RecordValkeyPoolDial(ctx, "error", false)
		return nil, false, fmt.Errorf("edge: dial valkey %s: %w", c.addr, err)
	}
	c.metrics.RecordValkeyPoolDial(ctx, "ok", false)
	return conn, false, nil
}

func (c *ValkeyPackCache) releaseConn(conn net.Conn) {
	if conn == nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return
	}
	select {
	case c.pool <- conn:
	default:
		_ = conn.Close()
	}
}

func (c *ValkeyPackCache) applyDeadline(ctx context.Context, conn net.Conn, opTimeout time.Duration) {
	deadline := time.Now().Add(opTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
}

func (c *ValkeyPackCache) doGet(ctx context.Context, conn net.Conn, key string) ([]byte, bool, error) {
	c.applyDeadline(ctx, conn, c.writeTimeout)
	if err := writeRESPCommand(conn, []byte("GET"), []byte(key)); err != nil {
		return nil, false, err
	}

	c.applyDeadline(ctx, conn, c.readTimeout)
	br := bufio.NewReader(conn)
	return readRESPBulkString(br, c.maxEntryBytes)
}

func (c *ValkeyPackCache) doSet(ctx context.Context, conn net.Conn, key string, data []byte, ttlSecs int64) error {
	c.applyDeadline(ctx, conn, c.writeTimeout)
	var err error
	if ttlSecs > 0 {
		err = writeRESPCommand(conn, []byte("SET"), []byte(key), data, []byte("EX"), []byte(strconv.FormatInt(ttlSecs, 10)))
	} else {
		err = writeRESPCommand(conn, []byte("SET"), []byte(key), data)
	}
	if err != nil {
		return err
	}

	c.applyDeadline(ctx, conn, c.readTimeout)
	br := bufio.NewReader(conn)
	return readRESPSimpleOK(br)
}

func writeRESPCommand(w io.Writer, args ...[]byte) error {
	bw := bufio.NewWriter(w)
	if _, err := fmt.Fprintf(bw, "*%d\r\n", len(args)); err != nil {
		return err
	}
	for _, arg := range args {
		if _, err := fmt.Fprintf(bw, "$%d\r\n", len(arg)); err != nil {
			return err
		}
		if _, err := bw.Write(arg); err != nil {
			return err
		}
		if _, err := bw.WriteString("\r\n"); err != nil {
			return err
		}
	}
	return bw.Flush()
}

func readRESPBulkString(br *bufio.Reader, maxBytes int) ([]byte, bool, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, false, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return nil, false, errors.New("edge: empty valkey response")
	}
	switch line[0] {
	case '-':
		return nil, false, fmt.Errorf("edge: valkey error: %s", line[1:])
	case '$':
		n, parseErr := strconv.Atoi(line[1:])
		if parseErr != nil {
			return nil, false, fmt.Errorf("edge: invalid valkey bulk length %q: %w", line, parseErr)
		}
		if n < 0 {
			return nil, false, nil
		}
		if maxBytes > 0 && n > maxBytes {
			return nil, false, ErrPackCacheEntryTooLarge
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, false, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return nil, false, errors.New("edge: malformed valkey bulk string terminator")
		}
		return buf[:n], true, nil
	default:
		return nil, false, fmt.Errorf("edge: unexpected valkey response prefix %q", line[0])
	}
}

func readRESPSimpleOK(br *bufio.Reader) error {
	line, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return errors.New("edge: empty valkey response")
	}
	switch line[0] {
	case '+':
		return nil
	case '-':
		return fmt.Errorf("edge: valkey error: %s", line[1:])
	default:
		return fmt.Errorf("edge: unexpected valkey SET response %q", line)
	}
}
