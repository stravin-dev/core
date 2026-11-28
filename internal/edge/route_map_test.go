package edge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"stravin.dev/core/internal/tenant"
)

type stubRouteResolver struct {
	mu     sync.RWMutex
	routes map[string]RouteEntry
	err    error
}

func (s *stubRouteResolver) ResolveRoute(_ context.Context, fullPath string) (RouteEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.err != nil {
		return RouteEntry{}, s.err
	}
	entry, ok := s.routes[fullPath]
	if !ok {
		return RouteEntry{}, ErrRouteNotFound
	}
	return entry, nil
}

type mockValkeyRouteServer struct {
	ln      net.Listener
	mu      sync.RWMutex
	values  map[string]string
	failErr string
	hits    atomic.Int64
	wg      sync.WaitGroup
}

func startMockValkeyRouteServer(t *testing.T) *mockValkeyRouteServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock valkey route server: %v", err)
	}
	s := &mockValkeyRouteServer{
		ln:     ln,
		values: make(map[string]string),
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go func(c net.Conn) {
				defer s.wg.Done()
				defer func() { _ = c.Close() }()
				s.handleConn(c)
			}(conn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *mockValkeyRouteServer) set(key, val string) {
	s.mu.Lock()
	s.values[key] = val
	s.mu.Unlock()
}

func (s *mockValkeyRouteServer) del(key string) {
	s.mu.Lock()
	delete(s.values, key)
	s.mu.Unlock()
}

func (s *mockValkeyRouteServer) setFailErr(msg string) {
	s.mu.Lock()
	s.failErr = msg
	s.mu.Unlock()
}

func (s *mockValkeyRouteServer) handleConn(conn net.Conn) {
	br := bufio.NewReader(conn)
	for {
		args, err := readTestRESPArray(br)
		if err != nil {
			return
		}
		if len(args) < 2 || !strings.EqualFold(args[0], "GET") {
			_, _ = io.WriteString(conn, "-ERR unsupported command\r\n")
			continue
		}
		s.hits.Add(1)
		s.mu.RLock()
		failErr := s.failErr
		val, ok := s.values[args[1]]
		s.mu.RUnlock()

		if failErr != "" {
			_, _ = fmt.Fprintf(conn, "-ERR %s\r\n", failErr)
			continue
		}
		if !ok {
			_, _ = io.WriteString(conn, "$-1\r\n")
			continue
		}
		_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
	}
}

func TestParseRepoPath_ValidMultiSegment(t *testing.T) {
	cases := []struct {
		raw        string
		requireGit bool
		wantFull   string
		wantOrgs   []string
		wantRepo   string
	}{
		{
			raw:        "/acme/core.git",
			requireGit: true,
			wantFull:   "acme/core",
			wantOrgs:   []string{"acme"},
			wantRepo:   "core",
		},
		{
			raw:        "/acme/eng/repo.git",
			requireGit: true,
			wantFull:   "acme/eng/repo",
			wantOrgs:   []string{"acme", "eng"},
			wantRepo:   "repo",
		},
		{
			raw:        "/acme/eng/payments/repo.git",
			requireGit: true,
			wantFull:   "acme/eng/payments/repo",
			wantOrgs:   []string{"acme", "eng", "payments"},
			wantRepo:   "repo",
		},
		{
			raw:        "/acme/eng/payments/core/repo.git",
			requireGit: true,
			wantFull:   "acme/eng/payments/core/repo",
			wantOrgs:   []string{"acme", "eng", "payments", "core"},
			wantRepo:   "repo",
		},
		{
			raw:        "acme/eng/payments/core/my_repo-1",
			requireGit: false,
			wantFull:   "acme/eng/payments/core/my_repo-1",
			wantOrgs:   []string{"acme", "eng", "payments", "core"},
			wantRepo:   "my_repo-1",
		},
	}

	for _, tc := range cases {
		got, err := ParseRepoPath(tc.raw, tc.requireGit, DefaultMaxOrgDepth)
		if err != nil {
			t.Fatalf("ParseRepoPath(%q) unexpected error: %v", tc.raw, err)
		}
		if got.FullPath != tc.wantFull || got.RepoSlug != tc.wantRepo || len(got.OrgSegments) != len(tc.wantOrgs) {
			t.Fatalf("ParseRepoPath(%q) = %+v, want full=%q orgs=%v repo=%q", tc.raw, got, tc.wantFull, tc.wantOrgs, tc.wantRepo)
		}
		for i := range tc.wantOrgs {
			if got.OrgSegments[i] != tc.wantOrgs[i] {
				t.Fatalf("ParseRepoPath(%q) org[%d] = %q, want %q", tc.raw, i, got.OrgSegments[i], tc.wantOrgs[i])
			}
		}
	}
}

func TestParseRepoPath_FailClosedRejection(t *testing.T) {
	invalid := []struct {
		name       string
		raw        string
		requireGit bool
		maxDepth   int
	}{
		{"empty", "", true, DefaultMaxOrgDepth},
		{"single_segment", "/acme.git", true, DefaultMaxOrgDepth},
		{"missing_git_suffix", "/acme/core", true, DefaultMaxOrgDepth},
		{"dot_git_only", "/acme/.git", true, DefaultMaxOrgDepth},
		{"dot_segment", "/acme/./core.git", true, DefaultMaxOrgDepth},
		{"dotdot_segment", "/acme/../beta/core.git", true, DefaultMaxOrgDepth},
		{"reserved_dash_org", "/acme/-/core.git", true, DefaultMaxOrgDepth},
		{"reserved_dash_repo", "/acme/-.git", true, DefaultMaxOrgDepth},
		{"empty_middle_segment", "/acme//core.git", true, DefaultMaxOrgDepth},
		{"leading_double_slash", "//acme/core.git", true, DefaultMaxOrgDepth},
		{"trailing_slash", "/acme/core.git/", true, DefaultMaxOrgDepth},
		{"backslash", `/acme\eng/core.git`, true, DefaultMaxOrgDepth},
		{"nul_byte", "/acme/core\x00.git", true, DefaultMaxOrgDepth},
		{"control_char_lf", "/acme/eng\n/core.git", true, DefaultMaxOrgDepth},
		{"control_char_del", "/acme/eng\x7f/core.git", true, DefaultMaxOrgDepth},
		{"pct_encoded_dot", "/acme/%2e%2e/core.git", true, DefaultMaxOrgDepth},
		{"pct_encoded_slash", "/acme%2feng/core.git", true, DefaultMaxOrgDepth},
		{"pct_encoded_backslash", "/acme%5ceng/core.git", true, DefaultMaxOrgDepth},
		{"pct_encoded_nul", "/acme/core%00.git", true, DefaultMaxOrgDepth},
		{"exceeds_default_depth_6_segs", "/a/b/c/d/e/repo.git", true, DefaultMaxOrgDepth},
		{"exceeds_custom_depth_2", "/a/b/c/repo.git", true, 2},
		{"invalid_char_space", "/acme/eng team/core.git", true, DefaultMaxOrgDepth},
		{"invalid_leading_underscore", "/acme/_eng/core.git", true, DefaultMaxOrgDepth},
	}

	for _, tc := range invalid {
		t.Run(tc.name, func(t *testing.T) {
			if got, err := ParseRepoPath(tc.raw, tc.requireGit, tc.maxDepth); err == nil {
				t.Fatalf("expected error for %s (%q), got %+v", tc.name, tc.raw, got)
			}
		})
	}
}

func TestResolveRepoPath_FallbackAndFailClosed(t *testing.T) {
	ctx := context.Background()
	twoSeg, err := ParseRepoPath("/acme/core.git", true, DefaultMaxOrgDepth)
	if err != nil {
		t.Fatalf("ParseRepoPath 2-seg: %v", err)
	}
	fourSeg, err := ParseRepoPath("/acme/eng/payments/api.git", true, DefaultMaxOrgDepth)
	if err != nil {
		t.Fatalf("ParseRepoPath 4-seg: %v", err)
	}

	// 1. Nil resolver: 2-segment falls back cleanly, >2-segment fails closed.
	key, err := ResolveRepoPath(ctx, nil, twoSeg)
	if err != nil || key != (tenant.RepoKey{TenantID: "acme", RepoID: "core"}) {
		t.Fatalf("nil resolver 2-seg = (%+v, %v), want (acme/core, nil)", key, err)
	}
	if _, err := ResolveRepoPath(ctx, nil, fourSeg); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("nil resolver 4-seg err = %v, want ErrRouteNotFound", err)
	}

	// 2. Configured resolver: mapped entry resolves to immutable IDs; unmapped 2-seg falls back; unmapped >2-seg fails closed.
	stub := &stubRouteResolver{
		routes: map[string]RouteEntry{
			"acme/eng/payments/api": {
				RootTenantID: "tnt_01JDF8A1",
				RepoID:       "repo_01JDF8B2",
			},
		},
	}
	key, err = ResolveRepoPath(ctx, stub, fourSeg)
	if err != nil || key != (tenant.RepoKey{TenantID: "tnt_01JDF8A1", RepoID: "repo_01JDF8B2"}) {
		t.Fatalf("mapped 4-seg = (%+v, %v), want (tnt_01JDF8A1/repo_01JDF8B2, nil)", key, err)
	}
	key, err = ResolveRepoPath(ctx, stub, twoSeg)
	if err != nil || key != (tenant.RepoKey{TenantID: "acme", RepoID: "core"}) {
		t.Fatalf("unmapped 2-seg fallback = (%+v, %v), want (acme/core, nil)", key, err)
	}

	unmappedThree, _ := ParseRepoPath("/acme/eng/unknown.git", true, DefaultMaxOrgDepth)
	if _, err := ResolveRepoPath(ctx, stub, unmappedThree); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("unmapped 3-seg err = %v, want ErrRouteNotFound", err)
	}

	// 3. Backend transport error fails closed with ErrRouteResolverUnavailable.
	stub.err = errors.New("valkey connection refused")
	if _, err := ResolveRepoPath(ctx, stub, twoSeg); !errors.Is(err, ErrRouteResolverUnavailable) {
		t.Fatalf("backend error 2-seg err = %v, want ErrRouteResolverUnavailable", err)
	}
}

func TestValkeyRouteResolver_FormatsCacheAndTTL(t *testing.T) {
	srv := startMockValkeyRouteServer(t)
	srv.set("route:repo:acme/eng/payments/api", `{"root_tenant_id":"tnt_root","repo_id":"repo_api","canonical_path":"acme/eng/payments/api"}`)
	srv.set("route:repo:acme/eng/payments/core/web", "tnt_root/repo_web")
	srv.set("route:repo:acme/eng/corrupt", "not-valid-entry")

	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	var mu sync.Mutex
	clk := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}

	resolver, err := NewValkeyRouteResolver(ValkeyRouteResolverConfig{
		Addr:     srv.ln.Addr().String(),
		CacheTTL: 10 * time.Second,
		Clock:    clk,
	})
	if err != nil {
		t.Fatalf("NewValkeyRouteResolver: %v", err)
	}
	defer func() { _ = resolver.Close() }()

	ctx := context.Background()

	// JSON format lookup + in-process cache hit.
	e1, err := resolver.ResolveRoute(ctx, "acme/eng/payments/api")
	if err != nil {
		t.Fatalf("ResolveRoute JSON: %v", err)
	}
	if e1.RootTenantID != "tnt_root" || e1.RepoID != "repo_api" || e1.CanonicalPath != "acme/eng/payments/api" {
		t.Fatalf("unexpected JSON entry: %+v", e1)
	}
	if hits := srv.hits.Load(); hits != 1 {
		t.Fatalf("expected 1 valkey hit, got %d", hits)
	}

	// Second lookup served from in-process TTL cache without hitting Valkey.
	e1Cached, err := resolver.ResolveRoute(ctx, "acme/eng/payments/api")
	if err != nil || e1Cached != e1 {
		t.Fatalf("cached ResolveRoute = (%+v, %v), want %+v", e1Cached, err, e1)
	}
	if hits := srv.hits.Load(); hits != 1 {
		t.Fatalf("expected cached lookup to keep valkey hits at 1, got %d", hits)
	}

	// Compact "<root_tenant_id>/<repo_id>" format lookup (5 segments).
	e2, err := resolver.ResolveRoute(ctx, "acme/eng/payments/core/web")
	if err != nil {
		t.Fatalf("ResolveRoute compact: %v", err)
	}
	if e2.RootTenantID != "tnt_root" || e2.RepoID != "repo_web" {
		t.Fatalf("unexpected compact entry: %+v", e2)
	}

	// Missing key returns ErrRouteNotFound.
	if _, err := resolver.ResolveRoute(ctx, "acme/eng/missing"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("missing route err = %v, want ErrRouteNotFound", err)
	}

	// Corrupt value fails closed.
	if _, err := resolver.ResolveRoute(ctx, "acme/eng/corrupt"); err == nil {
		t.Fatal("expected error for corrupt route entry")
	}

	// After TTL expires, Valkey is queried again and reflects updated target.
	srv.set("route:repo:acme/eng/payments/api", `{"root_tenant_id":"tnt_root","repo_id":"repo_api_v2"}`)
	advance(11 * time.Second)
	e1Refreshed, err := resolver.ResolveRoute(ctx, "acme/eng/payments/api")
	if err != nil {
		t.Fatalf("ResolveRoute after TTL: %v", err)
	}
	if e1Refreshed.RepoID != "repo_api_v2" {
		t.Fatalf("expected refreshed repo_id repo_api_v2, got %q", e1Refreshed.RepoID)
	}

	// Valkey error fails closed on uncached lookup.
	srv.setFailErr("LOADING")
	if _, err := resolver.ResolveRoute(ctx, "acme/eng/uncached"); err == nil {
		t.Fatal("expected error when Valkey returns error")
	}
	srv.setFailErr("")
}

func startEmbeddedAuthJetStream(t *testing.T) (string, jetstream.JetStream) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("natsserver.NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      DefaultRouteAuthStream,
		Subjects:  []string{"stravin.auth.>"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		t.Fatalf("CreateStream STRAVIN_AUTH: %v", err)
	}
	return srv.ClientURL(), js
}

func TestRouteInvalidator_NATSJetStreamOrgAndRepoEvents(t *testing.T) {
	natsURL, js := startEmbeddedAuthJetStream(t)
	valkey := startMockValkeyRouteServer(t)

	valkey.set("route:repo:acme/eng/payments/api", `{"root_tenant_id":"tnt_acme","repo_id":"repo_api"}`)
	valkey.set("route:repo:acme/eng/payments/web", `{"root_tenant_id":"tnt_acme","repo_id":"repo_web"}`)
	valkey.set("route:repo:acme/other/docs", `{"root_tenant_id":"tnt_acme","repo_id":"repo_docs"}`)

	resolver, err := NewValkeyRouteResolver(ValkeyRouteResolverConfig{
		Addr:     valkey.ln.Addr().String(),
		CacheTTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewValkeyRouteResolver: %v", err)
	}
	defer func() { _ = resolver.Close() }()

	ctx := context.Background()
	inv, err := StartRouteInvalidator(ctx, RouteInvalidatorConfig{
		NATSURL:  natsURL,
		Resolver: resolver,
	})
	if err != nil {
		t.Fatalf("StartRouteInvalidator: %v", err)
	}
	defer func() { _ = inv.Close() }()

	// Warm in-process cache for all 3 routes.
	for _, p := range []string{"acme/eng/payments/api", "acme/eng/payments/web", "acme/other/docs"} {
		if _, err := resolver.ResolveRoute(ctx, p); err != nil {
			t.Fatalf("warm ResolveRoute(%q): %v", p, err)
		}
	}
	if got := valkey.hits.Load(); got != 3 {
		t.Fatalf("expected 3 initial valkey hits, got %d", got)
	}

	// 1. Sub-org move/rename: delete old Valkey keys and publish stravin.auth.tnt_acme.org.moved.
	valkey.del("route:repo:acme/eng/payments/api")
	valkey.del("route:repo:acme/eng/payments/web")
	evBytes, _ := json.Marshal(map[string]any{
		"tenant_id":     "tnt_acme",
		"old_full_path": "acme/eng/payments",
		"new_full_path": "acme/platform/billing",
	})
	if _, err := js.Publish(ctx, "stravin.auth.tnt_acme.org.moved", evBytes); err != nil {
		t.Fatalf("js.Publish org.moved: %v", err)
	}

	// Wait for invalidation to evict acme/eng/payments/* from in-process cache.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := resolver.ResolveRoute(ctx, "acme/eng/payments/api")
		if errors.Is(err, ErrRouteNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for org.moved cache invalidation (last err=%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := resolver.ResolveRoute(ctx, "acme/eng/payments/web"); !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("expected sibling repo under moved org to be invalidated, got err=%v", err)
	}

	// Unrelated org route must still be served from cache without hitting Valkey.
	hitsBefore := valkey.hits.Load()
	docsEntry, err := resolver.ResolveRoute(ctx, "acme/other/docs")
	if err != nil || docsEntry.RepoID != "repo_docs" {
		t.Fatalf("unrelated org route = (%+v, %v), want repo_docs", docsEntry, err)
	}
	if valkey.hits.Load() != hitsBefore {
		t.Fatalf("expected unrelated org route to stay cached")
	}

	// 2. Repo delete event invalidates by repo_id even if path is omitted.
	valkey.del("route:repo:acme/other/docs")
	repoDelBytes, _ := json.Marshal(map[string]any{
		"tenant_id": "tnt_acme",
		"repo_id":   "repo_docs",
	})
	if _, err := js.Publish(ctx, "stravin.auth.tnt_acme.repo.deleted", repoDelBytes); err != nil {
		t.Fatalf("js.Publish repo.deleted: %v", err)
	}

	deadline = time.Now().Add(3 * time.Second)
	for {
		_, err := resolver.ResolveRoute(ctx, "acme/other/docs")
		if errors.Is(err, ErrRouteNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for repo.deleted cache invalidation (last err=%v)", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
