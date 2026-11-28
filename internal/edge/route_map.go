package edge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultMaxOrgDepth is the maximum number of organization segments allowed in a Git
	// path (ADR-0006: depth 0..3 -> up to 4 org segments + 1 repo segment = 5 total segments).
	DefaultMaxOrgDepth = 4
	// DefaultValkeyRouteKeyPrefix is the default Valkey key prefix for full_path -> core ID routes.
	DefaultValkeyRouteKeyPrefix = "route:repo:"
	// DefaultValkeyRouteTimeout bounds hot-path Valkey route lookups.
	DefaultValkeyRouteTimeout = 50 * time.Millisecond
	// DefaultRouteCacheTTL is the default short in-process TTL cache duration when enabled.
	DefaultRouteCacheTTL = 15 * time.Second
	// DefaultRouteAuthStream is the NATS JetStream stream carrying org/repo lifecycle events.
	DefaultRouteAuthStream = "STRAVIN_AUTH"

	defaultValkeyRouteDial       = 200 * time.Millisecond
	defaultValkeyRoutePool       = 16
	defaultRouteMaxValueBytes    = 4096
	defaultRouteMaxCacheEntries  = 10000
	defaultNATSConsumerInitDelay = 5 * time.Second
)

var (
	// ErrInvalidRepoPath indicates a malformed or unsafe Git repository path.
	ErrInvalidRepoPath = errors.New("edge: invalid repository path")
	// ErrRouteNotFound indicates no route-map entry exists for the requested repository path.
	ErrRouteNotFound = errors.New("edge: route not found")
	// ErrRouteResolverUnavailable indicates a route-map lookup failed due to a backend error.
	ErrRouteResolverUnavailable = errors.New("edge: route resolver unavailable")
)

// ParsedRepoPath holds a validated, bounded multi-segment Git repository path.
type ParsedRepoPath struct {
	FullPath    string   // Canonical slash-delimited "{org_path}/{repo_slug}" without leading "/" or ".git".
	OrgSegments []string // 1..maxOrgDepth validated organization slug segments.
	RepoSlug    string   // Validated repository slug (without trailing ".git").
	Segments    []string // All segments: append(OrgSegments, RepoSlug), length 2..(maxOrgDepth+1).
}

// IsLegacyTwoSegment reports whether the path is a flat "{tenant}/{repo}" 2-segment path.
func (p ParsedRepoPath) IsLegacyTwoSegment() bool {
	return len(p.Segments) == 2
}

// LegacyRepoKey converts a 2-segment path directly into a tenant.RepoKey.
func (p ParsedRepoPath) LegacyRepoKey() (tenant.RepoKey, error) {
	if !p.IsLegacyTwoSegment() {
		return tenant.RepoKey{}, ErrRouteNotFound
	}
	return tenant.NewRepoKey(p.OrgSegments[0], p.RepoSlug)
}

// ParseRepoPath validates a bounded multi-segment repository path ("{org1}/.../{repo}[.git]")
// fail-closed before any Valkey or storage lookup.
func ParseRepoPath(raw string, requireGitSuffix bool, maxOrgDepth int) (ParsedRepoPath, error) {
	if maxOrgDepth <= 0 {
		maxOrgDepth = DefaultMaxOrgDepth
	}
	if err := validateRawPathSafety(raw); err != nil {
		return ParsedRepoPath{}, err
	}

	// Trim at most ONE leading slash so leading "//" yields an empty segment and fails closed.
	trimmed := strings.TrimPrefix(raw, "/")
	if trimmed == "" {
		return ParsedRepoPath{}, fmt.Errorf("%w: empty path", ErrInvalidRepoPath)
	}

	parts := strings.Split(trimmed, "/")
	return ParseRepoPathSegments(parts, requireGitSuffix, maxOrgDepth)
}

// ParseRepoPathSegments validates already-split path segments fail-closed.
func ParseRepoPathSegments(parts []string, requireGitSuffix bool, maxOrgDepth int) (ParsedRepoPath, error) {
	if maxOrgDepth <= 0 {
		maxOrgDepth = DefaultMaxOrgDepth
	}
	maxSegments := maxOrgDepth + 1
	if len(parts) < 2 || len(parts) > maxSegments {
		return ParsedRepoPath{}, fmt.Errorf("%w: segment count %d outside [2,%d]", ErrInvalidRepoPath, len(parts), maxSegments)
	}

	orgCount := len(parts) - 1
	orgSegments := make([]string, orgCount)
	for i := range orgCount {
		seg := parts[i]
		if err := validatePathSegment(seg); err != nil {
			return ParsedRepoPath{}, err
		}
		if _, err := tenant.ParseTenantID(seg); err != nil {
			return ParsedRepoPath{}, fmt.Errorf("%w: org segment %q: %w", ErrInvalidRepoPath, seg, err)
		}
		orgSegments[i] = seg
	}

	repoSeg := parts[orgCount]
	if err := validatePathSegment(repoSeg); err != nil {
		return ParsedRepoPath{}, err
	}
	if requireGitSuffix && !strings.HasSuffix(repoSeg, ".git") {
		return ParsedRepoPath{}, fmt.Errorf("%w: repo segment must end with .git", ErrInvalidRepoPath)
	}
	repoSlug := strings.TrimSuffix(repoSeg, ".git")
	if err := validatePathSegment(repoSlug); err != nil {
		return ParsedRepoPath{}, err
	}
	if _, err := tenant.ParseRepoID(repoSlug); err != nil {
		return ParsedRepoPath{}, fmt.Errorf("%w: repo segment %q: %w", ErrInvalidRepoPath, repoSlug, err)
	}

	all := make([]string, len(parts))
	copy(all, orgSegments)
	all[orgCount] = repoSlug

	return ParsedRepoPath{
		FullPath:    strings.Join(all, "/"),
		OrgSegments: orgSegments,
		RepoSlug:    repoSlug,
		Segments:    all,
	}, nil
}

func validateRawPathSafety(raw string) error {
	if raw == "" {
		return fmt.Errorf("%w: empty path", ErrInvalidRepoPath)
	}
	for i := range len(raw) {
		c := raw[i]
		if c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("%w: illegal control or backslash character", ErrInvalidRepoPath)
		}
	}
	lower := strings.ToLower(raw)
	if strings.Contains(lower, "..") ||
		strings.Contains(lower, "%2e") ||
		strings.Contains(lower, "%2f") ||
		strings.Contains(lower, "%5c") ||
		strings.Contains(lower, "%00") {
		return fmt.Errorf("%w: path traversal rejected", ErrInvalidRepoPath)
	}
	return nil
}

func validatePathSegment(seg string) error {
	if seg == "" || seg == "." || seg == ".." || seg == "-" {
		return fmt.Errorf("%w: invalid segment %q", ErrInvalidRepoPath, seg)
	}
	for i := range len(seg) {
		c := seg[i]
		if c < 0x20 || c == 0x7f || c == '\\' {
			return fmt.Errorf("%w: illegal character in segment", ErrInvalidRepoPath)
		}
	}
	return nil
}

// RouteEntry maps a mutable Git path to immutable Stravin Core storage identifiers.
type RouteEntry struct {
	RootTenantID  tenant.TenantID `json:"root_tenant_id"`
	RepoID        tenant.RepoID   `json:"repo_id"`
	CanonicalPath string          `json:"canonical_path,omitempty"`
}

// Validate verifies that both RootTenantID and RepoID are valid Stravin identifiers.
func (e RouteEntry) Validate() error {
	if err := e.RootTenantID.Validate(); err != nil {
		return fmt.Errorf("edge: invalid route root_tenant_id: %w", err)
	}
	if err := e.RepoID.Validate(); err != nil {
		return fmt.Errorf("edge: invalid route repo_id: %w", err)
	}
	return nil
}

// RepoKey returns the immutable tenant.RepoKey addressed in Stravin Core.
func (e RouteEntry) RepoKey() tenant.RepoKey {
	return tenant.RepoKey{
		TenantID: e.RootTenantID,
		RepoID:   e.RepoID,
	}
}

// RouteResolver is the GoF Strategy interface for resolving mutable Git paths
// ("acme/eng/payments/core") to immutable Core (root_tenant_id, repo_id) identifiers.
type RouteResolver interface {
	ResolveRoute(ctx context.Context, fullPath string) (RouteEntry, error)
}

// ResolveRepoPath resolves a validated ParsedRepoPath using resolver.
//   - If resolver returns a valid RouteEntry, its immutable (root_tenant_id, repo_id) is used.
//   - If resolver is nil or returns ErrRouteNotFound:
//   - 2-segment paths fall back cleanly to (seg[0], seg[1]) for backward compatibility.
//   - >2-segment paths fail closed with ErrRouteNotFound.
//   - Any backend/transport error fails closed with ErrRouteResolverUnavailable.
func ResolveRepoPath(ctx context.Context, resolver RouteResolver, parsed ParsedRepoPath) (tenant.RepoKey, error) {
	if resolver == nil {
		if parsed.IsLegacyTwoSegment() {
			return parsed.LegacyRepoKey()
		}
		return tenant.RepoKey{}, ErrRouteNotFound
	}

	entry, err := resolver.ResolveRoute(ctx, parsed.FullPath)
	if err == nil {
		if valErr := entry.Validate(); valErr != nil {
			return tenant.RepoKey{}, fmt.Errorf("%w: %w", ErrRouteResolverUnavailable, valErr)
		}
		return entry.RepoKey(), nil
	}
	if errors.Is(err, ErrRouteNotFound) {
		if parsed.IsLegacyTwoSegment() {
			return parsed.LegacyRepoKey()
		}
		return tenant.RepoKey{}, ErrRouteNotFound
	}
	return tenant.RepoKey{}, fmt.Errorf("%w: %w", ErrRouteResolverUnavailable, err)
}

// ParseRouteEntryValue parses a Valkey route value in either JSON format
// ({"root_tenant_id":"...","repo_id":"..."}) or compact "<root_tenant_id>/<repo_id>" format.
func ParseRouteEntryValue(raw []byte) (RouteEntry, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" {
		return RouteEntry{}, errors.New("edge: empty route entry value")
	}

	var entry RouteEntry
	if strings.HasPrefix(trimmed, "{") {
		if err := json.Unmarshal([]byte(trimmed), &entry); err != nil {
			return RouteEntry{}, fmt.Errorf("edge: decode route json: %w", err)
		}
	} else {
		tenantRaw, repoRaw, ok := strings.Cut(trimmed, "/")
		if !ok || strings.Contains(repoRaw, "/") {
			return RouteEntry{}, fmt.Errorf("edge: invalid compact route entry %q", trimmed)
		}
		entry = RouteEntry{
			RootTenantID: tenant.TenantID(strings.TrimSpace(tenantRaw)),
			RepoID:       tenant.RepoID(strings.TrimSpace(repoRaw)),
		}
	}

	if err := entry.Validate(); err != nil {
		return RouteEntry{}, err
	}
	return entry, nil
}

// ValkeyRouteResolverConfig configures a ValkeyRouteResolver.
type ValkeyRouteResolverConfig struct {
	Addr            string
	KeyPrefix       string
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolSize        int
	CacheTTL        time.Duration // Optional short in-process TTL cache (0 disables local cache).
	MaxCacheEntries int
	Clock           func() time.Time
	Logger          *slog.Logger
}

type cachedRouteEntry struct {
	entry     RouteEntry
	expiresAt time.Time
}

// ValkeyRouteResolver resolves Git repository paths from Valkey over RESP2 with an
// optional short in-process TTL cache and eager invalidation hooks.
type ValkeyRouteResolver struct {
	addr            string
	keyPrefix       string
	dialTimeout     time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	cacheTTL        time.Duration
	maxCacheEntries int
	now             func() time.Time
	logger          *slog.Logger

	mu     sync.Mutex
	closed bool
	pool   chan net.Conn

	cacheMu sync.RWMutex
	cache   map[string]cachedRouteEntry
}

var _ RouteResolver = (*ValkeyRouteResolver)(nil)

// NewValkeyRouteResolver constructs a Valkey-backed RouteResolver.
func NewValkeyRouteResolver(cfg ValkeyRouteResolverConfig) (*ValkeyRouteResolver, error) {
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil, errors.New("edge: valkey route resolver address is required")
	}
	prefix := cfg.KeyPrefix
	if prefix == "" {
		prefix = DefaultValkeyRouteKeyPrefix
	}
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultValkeyRouteDial
	}
	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = DefaultValkeyRouteTimeout
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = DefaultValkeyRouteTimeout
	}
	poolSize := cfg.PoolSize
	if poolSize <= 0 {
		poolSize = defaultValkeyRoutePool
	}
	maxEntries := cfg.MaxCacheEntries
	if maxEntries <= 0 {
		maxEntries = defaultRouteMaxCacheEntries
	}
	nowFn := cfg.Clock
	if nowFn == nil {
		nowFn = time.Now
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	r := &ValkeyRouteResolver{
		addr:            addr,
		keyPrefix:       prefix,
		dialTimeout:     dialTimeout,
		readTimeout:     readTimeout,
		writeTimeout:    writeTimeout,
		cacheTTL:        cfg.CacheTTL,
		maxCacheEntries: maxEntries,
		now:             nowFn,
		logger:          logger,
		pool:            make(chan net.Conn, poolSize),
	}
	if cfg.CacheTTL > 0 {
		r.cache = make(map[string]cachedRouteEntry)
	}
	return r, nil
}

// Close closes all idle pooled connections to Valkey.
func (r *ValkeyRouteResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	close(r.pool)
	r.mu.Unlock()

	var errs []error
	for conn := range r.pool {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// ResolveRoute resolves fullPath ("acme/eng/payments/core") via optional in-process TTL cache
// and Valkey GET "{KeyPrefix}{fullPath}". Missing keys return ErrRouteNotFound.
func (r *ValkeyRouteResolver) ResolveRoute(ctx context.Context, fullPath string) (RouteEntry, error) {
	if r == nil {
		return RouteEntry{}, errors.New("edge: nil valkey route resolver")
	}
	fullPath = strings.TrimPrefix(strings.TrimSpace(fullPath), "/")
	if fullPath == "" {
		return RouteEntry{}, ErrInvalidRepoPath
	}

	if entry, ok := r.getCached(fullPath); ok {
		return entry, nil
	}

	key := r.keyPrefix + fullPath
	for attempt := range 2 {
		conn, fromPool, err := r.acquireConn(ctx)
		if err != nil {
			return RouteEntry{}, err
		}
		raw, hit, err := r.doGet(ctx, conn, key)
		if err == nil {
			r.releaseConn(conn)
			if !hit {
				return RouteEntry{}, ErrRouteNotFound
			}
			entry, parseErr := ParseRouteEntryValue(raw)
			if parseErr != nil {
				return RouteEntry{}, parseErr
			}
			r.putCached(fullPath, entry)
			return entry, nil
		}
		_ = conn.Close()
		if !fromPool || attempt == 1 || ctx.Err() != nil {
			return RouteEntry{}, err
		}
	}
	return RouteEntry{}, errors.New("edge: valkey route lookup failed")
}

// InvalidatePath evicts a single full_path entry from the in-process cache.
func (r *ValkeyRouteResolver) InvalidatePath(fullPath string) {
	if r == nil || r.cacheTTL <= 0 {
		return
	}
	normalized := normalizeRoutePath(fullPath)
	if normalized == "" {
		return
	}
	r.cacheMu.Lock()
	delete(r.cache, normalized)
	r.cacheMu.Unlock()
}

// InvalidateOrgPrefix evicts all cached routes under orgFullPath (matching exact prefix + "/").
func (r *ValkeyRouteResolver) InvalidateOrgPrefix(orgFullPath string) {
	if r == nil || r.cacheTTL <= 0 {
		return
	}
	normalized := normalizeRoutePath(orgFullPath)
	if normalized == "" {
		return
	}
	prefix := normalized + "/"
	r.cacheMu.Lock()
	for k := range r.cache {
		if k == normalized || strings.HasPrefix(k, prefix) {
			delete(r.cache, k)
		}
	}
	r.cacheMu.Unlock()
}

// InvalidateRepoID evicts all cached routes pointing to (rootTenantID, repoID).
func (r *ValkeyRouteResolver) InvalidateRepoID(rootTenantID tenant.TenantID, repoID tenant.RepoID) {
	if r == nil || r.cacheTTL <= 0 || repoID == "" {
		return
	}
	r.cacheMu.Lock()
	for k, v := range r.cache {
		if v.entry.RepoID == repoID && (rootTenantID == "" || v.entry.RootTenantID == rootTenantID) {
			delete(r.cache, k)
		}
	}
	r.cacheMu.Unlock()
}

// InvalidateTenant evicts all cached routes belonging to rootTenantID.
func (r *ValkeyRouteResolver) InvalidateTenant(rootTenantID tenant.TenantID) {
	if r == nil || r.cacheTTL <= 0 || rootTenantID == "" {
		return
	}
	r.cacheMu.Lock()
	for k, v := range r.cache {
		if v.entry.RootTenantID == rootTenantID {
			delete(r.cache, k)
		}
	}
	r.cacheMu.Unlock()
}

// InvalidateAll clears the entire in-process route cache.
func (r *ValkeyRouteResolver) InvalidateAll() {
	if r == nil || r.cacheTTL <= 0 {
		return
	}
	r.cacheMu.Lock()
	clear(r.cache)
	r.cacheMu.Unlock()
}

func (r *ValkeyRouteResolver) getCached(fullPath string) (RouteEntry, bool) {
	if r.cacheTTL <= 0 {
		return RouteEntry{}, false
	}
	now := r.now()
	r.cacheMu.RLock()
	item, ok := r.cache[fullPath]
	r.cacheMu.RUnlock()
	if !ok {
		return RouteEntry{}, false
	}
	if !now.Before(item.expiresAt) {
		r.cacheMu.Lock()
		if cur, exists := r.cache[fullPath]; exists && !now.Before(cur.expiresAt) {
			delete(r.cache, fullPath)
		}
		r.cacheMu.Unlock()
		return RouteEntry{}, false
	}
	return item.entry, true
}

func (r *ValkeyRouteResolver) putCached(fullPath string, entry RouteEntry) {
	if r.cacheTTL <= 0 {
		return
	}
	now := r.now()
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	if len(r.cache) >= r.maxCacheEntries {
		// Evict expired entries first; if still full, clear to bound memory deterministically.
		for k, v := range r.cache {
			if !now.Before(v.expiresAt) {
				delete(r.cache, k)
			}
		}
		if len(r.cache) >= r.maxCacheEntries {
			clear(r.cache)
		}
	}
	r.cache[fullPath] = cachedRouteEntry{
		entry:     entry,
		expiresAt: now.Add(r.cacheTTL),
	}
}

func (r *ValkeyRouteResolver) acquireConn(ctx context.Context) (net.Conn, bool, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return nil, false, errors.New("edge: valkey route resolver closed")
	}

	select {
	case conn, ok := <-r.pool:
		if ok && conn != nil {
			return conn, true, nil
		}
	default:
	}

	dialer := net.Dialer{Timeout: r.dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, false, fmt.Errorf("edge: dial valkey %s: %w", r.addr, err)
	}
	return conn, false, nil
}

func (r *ValkeyRouteResolver) releaseConn(conn net.Conn) {
	if conn == nil {
		return
	}
	_ = conn.SetDeadline(time.Time{})
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		_ = conn.Close()
		return
	}
	select {
	case r.pool <- conn:
	default:
		_ = conn.Close()
	}
}

func (r *ValkeyRouteResolver) applyDeadline(ctx context.Context, conn net.Conn, opTimeout time.Duration) {
	deadline := time.Now().Add(opTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	_ = conn.SetDeadline(deadline)
}

func (r *ValkeyRouteResolver) doGet(ctx context.Context, conn net.Conn, key string) ([]byte, bool, error) {
	r.applyDeadline(ctx, conn, r.writeTimeout)
	if err := writeRESPCommand(conn, []byte("GET"), []byte(key)); err != nil {
		return nil, false, err
	}
	r.applyDeadline(ctx, conn, r.readTimeout)
	br := bufio.NewReader(conn)
	return readRESPBulkString(br, defaultRouteMaxValueBytes)
}

func normalizeRoutePath(p string) string {
	p = strings.TrimSpace(p)
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")
	p = strings.TrimSuffix(p, ".git")
	return p
}

// RouteInvalidatorConfig configures a NATS JetStream listener that eagerly evicts
// ValkeyRouteResolver in-process cache entries on STRAVIN_AUTH org/repo lifecycle events.
type RouteInvalidatorConfig struct {
	NATSURL    string
	StreamName string
	Resolver   *ValkeyRouteResolver
	Logger     *slog.Logger
}

// RouteInvalidator manages a JetStream consumer for route cache invalidation.
type RouteInvalidator struct {
	nc       *nats.Conn
	consume  jetstream.ConsumeContext
	resolver *ValkeyRouteResolver
	logger   *slog.Logger
	closeMu  sync.Mutex
	closed   bool
}

var routeInvalidationSubjects = []string{
	"stravin.auth.*.org.renamed",
	"stravin.auth.*.org.moved",
	"stravin.auth.*.org.deleted",
	"stravin.auth.*.repo.renamed",
	"stravin.auth.*.repo.moved",
	"stravin.auth.*.repo.deleted",
}

// StartRouteInvalidator connects to NATS JetStream and subscribes an ephemeral consumer
// to STRAVIN_AUTH org/repo move, rename, and delete events.
func StartRouteInvalidator(ctx context.Context, cfg RouteInvalidatorConfig) (*RouteInvalidator, error) {
	natsURL := strings.TrimSpace(cfg.NATSURL)
	if natsURL == "" {
		return nil, errors.New("edge: nats url is required for route invalidator")
	}
	if cfg.Resolver == nil {
		return nil, errors.New("edge: valkey route resolver is required for route invalidator")
	}
	streamName := strings.TrimSpace(cfg.StreamName)
	if streamName == "" {
		streamName = DefaultRouteAuthStream
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	nc, err := nats.Connect(natsURL, nats.Name("stravin-edge-route-invalidator"))
	if err != nil {
		return nil, fmt.Errorf("edge: connect nats %s: %w", natsURL, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("edge: init jetstream: %w", err)
	}

	initCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		initCtx, cancel = context.WithTimeout(ctx, defaultNATSConsumerInitDelay)
		defer cancel()
	}

	stream, err := js.Stream(initCtx, streamName)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("edge: lookup jetstream stream %s: %w", streamName, err)
	}

	consumer, err := stream.CreateConsumer(initCtx, jetstream.ConsumerConfig{
		AckPolicy:      jetstream.AckExplicitPolicy,
		DeliverPolicy:  jetstream.DeliverNewPolicy,
		FilterSubjects: routeInvalidationSubjects,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("edge: create route invalidation consumer on %s: %w", streamName, err)
	}

	inv := &RouteInvalidator{
		nc:       nc,
		resolver: cfg.Resolver,
		logger:   logger,
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		inv.HandleEvent(msg.Subject(), msg.Data())
		_ = msg.Ack()
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("edge: start route invalidation consume: %w", err)
	}
	inv.consume = consumeCtx
	return inv, nil
}

// Close stops consuming JetStream events and closes the underlying NATS connection.
func (i *RouteInvalidator) Close() error {
	if i == nil {
		return nil
	}
	i.closeMu.Lock()
	if i.closed {
		i.closeMu.Unlock()
		return nil
	}
	i.closed = true
	consume := i.consume
	nc := i.nc
	i.closeMu.Unlock()

	if consume != nil {
		consume.Stop()
	}
	if nc != nil {
		nc.Close()
	}
	return nil
}

type routeInvalidationEvent struct {
	TenantID      string                 `json:"tenant_id"`
	RootTenantID  string                 `json:"root_tenant_id"`
	RepoID        string                 `json:"repo_id"`
	FullPath      string                 `json:"full_path"`
	OldFullPath   string                 `json:"old_full_path"`
	NewFullPath   string                 `json:"new_full_path"`
	OrgPath       string                 `json:"org_path"`
	OldOrgPath    string                 `json:"old_org_path"`
	RepoPath      string                 `json:"repo_path"`
	OldRepoPath   string                 `json:"old_repo_path"`
	AffectedPaths []string               `json:"affected_paths"`
	Payload       *routeInvalidationBody `json:"payload"`
	Data          *routeInvalidationBody `json:"data"`
}

type routeInvalidationBody struct {
	TenantID      string   `json:"tenant_id"`
	RootTenantID  string   `json:"root_tenant_id"`
	RepoID        string   `json:"repo_id"`
	FullPath      string   `json:"full_path"`
	OldFullPath   string   `json:"old_full_path"`
	NewFullPath   string   `json:"new_full_path"`
	OrgPath       string   `json:"org_path"`
	OldOrgPath    string   `json:"old_org_path"`
	RepoPath      string   `json:"repo_path"`
	OldRepoPath   string   `json:"old_repo_path"`
	AffectedPaths []string `json:"affected_paths"`
}

// HandleEvent processes a STRAVIN_AUTH org/repo lifecycle event and evicts affected
// entries from the resolver's in-process route cache.
func (i *RouteInvalidator) HandleEvent(subject string, data []byte) {
	if i == nil || i.resolver == nil {
		return
	}

	var ev routeInvalidationEvent
	if len(data) > 0 {
		if err := json.Unmarshal(data, &ev); err != nil {
			i.logger.Warn("edge: invalid route invalidation event payload, clearing route cache",
				"subject", subject,
				"error", err,
			)
			i.resolver.InvalidateAll()
			return
		}
	}

	tenantID := firstNonEmpty(ev.RootTenantID, ev.TenantID)
	repoID := ev.RepoID
	paths := collectEventPaths(ev.FullPath, ev.OldFullPath, ev.NewFullPath, ev.OrgPath, ev.OldOrgPath, ev.RepoPath, ev.OldRepoPath, ev.AffectedPaths)

	for _, nested := range []*routeInvalidationBody{ev.Payload, ev.Data} {
		if nested == nil {
			continue
		}
		if tenantID == "" {
			tenantID = firstNonEmpty(nested.RootTenantID, nested.TenantID)
		}
		if repoID == "" {
			repoID = nested.RepoID
		}
		paths = append(paths, collectEventPaths(
			nested.FullPath, nested.OldFullPath, nested.NewFullPath,
			nested.OrgPath, nested.OldOrgPath, nested.RepoPath, nested.OldRepoPath,
			nested.AffectedPaths,
		)...)
	}

	// Fall back to subject tenant segment: stravin.auth.<tenant_id>.<entity>.<action>
	if tenantID == "" {
		subjParts := strings.Split(subject, ".")
		if len(subjParts) >= 5 && subjParts[0] == "stravin" && subjParts[1] == "auth" {
			tenantID = subjParts[2]
		}
	}

	isOrgEvent := strings.Contains(subject, ".org.")
	isRepoEvent := strings.Contains(subject, ".repo.")

	evictedSpecific := false
	for _, p := range paths {
		if isOrgEvent {
			i.resolver.InvalidateOrgPrefix(p)
		} else {
			i.resolver.InvalidatePath(p)
		}
		evictedSpecific = true
	}

	if isRepoEvent && repoID != "" {
		i.resolver.InvalidateRepoID(tenant.TenantID(tenantID), tenant.RepoID(repoID))
		evictedSpecific = true
	}

	if !evictedSpecific {
		if tenantID != "" {
			i.resolver.InvalidateTenant(tenant.TenantID(tenantID))
		} else {
			i.resolver.InvalidateAll()
		}
	}
}

func collectEventPaths(single ...any) []string {
	var out []string
	for _, item := range single {
		switch v := item.(type) {
		case string:
			if norm := normalizeRoutePath(v); norm != "" {
				out = append(out, norm)
			}
		case []string:
			for _, s := range v {
				if norm := normalizeRoutePath(s); norm != "" {
					out = append(out, norm)
				}
			}
		}
	}
	return out
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}
