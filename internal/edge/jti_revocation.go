package edge

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultValkeyRevocationKeyPrefix matches platform-auth.md §6: auth:jti:revoked:{tenant_id}:{jti}.
	DefaultValkeyRevocationKeyPrefix = "auth:jti:revoked:"
	// DefaultValkeyRevocationTimeout enforces sub-50ms hot-path jti checks per platform-auth.md §7.
	DefaultValkeyRevocationTimeout = 50 * time.Millisecond
	defaultValkeyRevocationDial    = 200 * time.Millisecond
	defaultValkeyRevocationPool    = 16
)

// RevocationChecker is the GoF Strategy interface for checking whether a JWT jti has been revoked.
type RevocationChecker interface {
	IsRevoked(ctx context.Context, tenantID tenant.TenantID, jti string) (bool, error)
}

// ValkeyRevocationConfig configures a ValkeyRevocationChecker.
type ValkeyRevocationConfig struct {
	Addr         string
	KeyPrefix    string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
}

// ValkeyRevocationChecker checks the Valkey jti deny-list over RESP2 using a pooled
// TCP connection without external client dependencies.
type ValkeyRevocationChecker struct {
	addr         string
	keyPrefix    string
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration

	mu     sync.Mutex
	closed bool
	pool   chan net.Conn
}

var _ RevocationChecker = (*ValkeyRevocationChecker)(nil)

// NewValkeyRevocationChecker constructs a Valkey-backed jti deny-list checker.
func NewValkeyRevocationChecker(cfg ValkeyRevocationConfig) (*ValkeyRevocationChecker, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil, errors.New("edge: valkey revocation address is required")
	}
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = DefaultValkeyRevocationKeyPrefix
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultValkeyRevocationDial
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = DefaultValkeyRevocationTimeout
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = DefaultValkeyRevocationTimeout
	}
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultValkeyRevocationPool
	}
	return &ValkeyRevocationChecker{
		addr:         addr,
		keyPrefix:    prefix,
		dialTimeout:  dialTimeout,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		pool:         make(chan net.Conn, poolSize),
	}, nil
}

// Close closes all idle pooled connections to Valkey.
func (c *ValkeyRevocationChecker) Close() error {
	if c == nil {
		return nil
	}
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

// IsRevoked executes EXISTS on auth:jti:revoked:{tenant_id}:{jti} in Valkey.
// Any transport or protocol error is returned so the caller can fail closed.
func (c *ValkeyRevocationChecker) IsRevoked(ctx context.Context, tenantID tenant.TenantID, jti string) (bool, error) {
	if c == nil {
		return false, errors.New("edge: nil valkey revocation checker")
	}
	if err := tenantID.Validate(); err != nil {
		return false, err
	}
	if strings.TrimSpace(jti) == "" {
		return false, errors.New("edge: empty jti")
	}

	key := c.keyPrefix + string(tenantID) + ":" + jti
	for attempt := range 2 {
		conn, fromPool, err := c.acquireConn(ctx)
		if err != nil {
			return false, err
		}
		exists, err := c.doExists(ctx, conn, key)
		if err == nil {
			c.releaseConn(conn)
			return exists, nil
		}
		_ = conn.Close()
		if !fromPool || attempt == 1 || ctx.Err() != nil {
			return false, err
		}
	}
	return false, errors.New("edge: valkey revocation lookup failed")
}

func (c *ValkeyRevocationChecker) acquireConn(ctx context.Context) (net.Conn, bool, error) {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, false, errors.New("edge: valkey revocation checker closed")
	}

	select {
	case conn, ok := <-c.pool:
		if ok && conn != nil {
			return conn, true, nil
		}
	default:
	}

	dialer := net.Dialer{Timeout: c.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, false, fmt.Errorf("edge: dial valkey %s: %w", c.addr, err)
	}
	return conn, false, nil
}

func (c *ValkeyRevocationChecker) releaseConn(conn net.Conn) {
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

func (c *ValkeyRevocationChecker) applyDeadline(ctx context.Context, conn net.Conn, opTimeout time.Duration) {
	deadline := time.Now().Add(opTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
}

func (c *ValkeyRevocationChecker) doExists(ctx context.Context, conn net.Conn, key string) (bool, error) {
	c.applyDeadline(ctx, conn, c.writeTimeout)
	if err := writeRESPCommand(conn, []byte("EXISTS"), []byte(key)); err != nil {
		return false, err
	}
	c.applyDeadline(ctx, conn, c.readTimeout)
	br := bufio.NewReader(conn)
	n, err := readRESPInteger(br)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func readRESPInteger(br *bufio.Reader) (int64, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return 0, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if line == "" {
		return 0, errors.New("edge: empty valkey response")
	}
	switch line[0] {
	case ':':
		n, parseErr := strconv.ParseInt(line[1:], 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("edge: invalid valkey integer %q: %w", line, parseErr)
		}
		return n, nil
	case '-':
		return 0, fmt.Errorf("edge: valkey error: %s", line[1:])
	default:
		return 0, fmt.Errorf("edge: unexpected valkey integer response %q", line)
	}
}
