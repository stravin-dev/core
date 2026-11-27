package edge

import (
	"bufio"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/tenant"
)

type testSigner struct {
	kid string
	alg string
	pub crypto.PublicKey
	key crypto.PrivateKey
}

func newEd25519TestSigner(t *testing.T, kid string) testSigner {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519.GenerateKey: %v", err)
	}
	return testSigner{kid: kid, alg: "EdDSA", pub: pub, key: priv}
}

func newES256TestSigner(t *testing.T, kid string) testSigner {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	return testSigner{kid: kid, alg: "ES256", pub: &priv.PublicKey, key: priv}
}

func newRS256TestSigner(t *testing.T, kid string) testSigner {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return testSigner{kid: kid, alg: "RS256", pub: &priv.PublicKey, key: priv}
}

func (s testSigner) jwkMap() map[string]any {
	switch pub := s.pub.(type) {
	case ed25519.PublicKey:
		return map[string]any{
			"kty": "OKP",
			"crv": "Ed25519",
			"kid": s.kid,
			"use": "sig",
			"alg": "EdDSA",
			"x":   base64.RawURLEncoding.EncodeToString(pub),
		}
	case *ecdsa.PublicKey:
		raw, err := pub.Bytes()
		if err != nil || len(raw) != 65 {
			panic(fmt.Sprintf("ecdsa public key bytes: len=%d err=%v", len(raw), err))
		}
		return map[string]any{
			"kty": "EC",
			"crv": "P-256",
			"kid": s.kid,
			"use": "sig",
			"alg": "ES256",
			"x":   base64.RawURLEncoding.EncodeToString(raw[1:33]),
			"y":   base64.RawURLEncoding.EncodeToString(raw[33:65]),
		}
	case *rsa.PublicKey:
		eBytes := big.NewInt(int64(pub.E)).Bytes()
		return map[string]any{
			"kty": "RSA",
			"kid": s.kid,
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(eBytes),
		}
	default:
		panic(fmt.Sprintf("unsupported key type %T", s.pub))
	}
}

func (s testSigner) pemBytes(t *testing.T) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(s.pub)
	if err != nil {
		t.Fatalf("MarshalPKIXPublicKey: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func marshalJWKS(t *testing.T, signers ...testSigner) []byte {
	t.Helper()
	keys := make([]map[string]any, 0, len(signers))
	for _, s := range signers {
		keys = append(keys, s.jwkMap())
	}
	raw, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatalf("marshal JWKS: %v", err)
	}
	return raw
}

func (s testSigner) signJWT(t *testing.T, headerOverrides map[string]any, claims map[string]any) string {
	t.Helper()
	hdr := map[string]any{
		"alg": s.alg,
		"typ": "JWT",
		"kid": s.kid,
	}
	for k, v := range headerOverrides {
		hdr[k] = v
	}
	hdrJSON, err := json.Marshal(hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(hdrJSON) + "." + base64.RawURLEncoding.EncodeToString(claimsJSON)

	var sig []byte
	switch priv := s.key.(type) {
	case ed25519.PrivateKey:
		sig = ed25519.Sign(priv, []byte(signingInput))
	case *ecdsa.PrivateKey:
		sum := sha256.Sum256([]byte(signingInput))
		r, sv, err := ecdsa.Sign(rand.Reader, priv, sum[:])
		if err != nil {
			t.Fatalf("ecdsa.Sign: %v", err)
		}
		sig = make([]byte, 64)
		r.FillBytes(sig[:32])
		sv.FillBytes(sig[32:])
	case *rsa.PrivateKey:
		sum := sha256.Sum256([]byte(signingInput))
		sig, err = rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, sum[:])
		if err != nil {
			t.Fatalf("rsa.SignPKCS1v15: %v", err)
		}
	default:
		t.Fatalf("unsupported private key %T", s.key)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func baseValidClaims(now time.Time) map[string]any {
	return map[string]any{
		"iss":       "stravin-auth",
		"aud":       "stravin-edge",
		"sub":       "usr_01JDF9A1B2C3D4E5F6G7H8J9K0",
		"jti":       "jti_01JDF9Z1A2B3C4D5E6F7G8H9J0",
		"tenant_id": "acme",
		"repo_id":   "core",
		"scopes":    []string{"git:read"},
		"iat":       now.Unix(),
		"nbf":       now.Unix(),
		"exp":       now.Add(2 * time.Minute).Unix(),
	}
}

type mockValkeyRevocationServer struct {
	ln      net.Listener
	mu      sync.Mutex
	revoked map[string]bool
	failErr string
	hits    atomic.Int64
	wg      sync.WaitGroup
}

func startMockValkeyRevocationServer(t *testing.T) *mockValkeyRevocationServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen mock valkey: %v", err)
	}
	s := &mockValkeyRevocationServer{
		ln:      ln,
		revoked: make(map[string]bool),
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
				s.serveConn(c)
			}(conn)
		}
	}()
	return s
}

func (s *mockValkeyRevocationServer) Addr() string {
	return s.ln.Addr().String()
}

func (s *mockValkeyRevocationServer) SetRevoked(key string, v bool) {
	s.mu.Lock()
	s.revoked[key] = v
	s.mu.Unlock()
}

func (s *mockValkeyRevocationServer) SetFailError(msg string) {
	s.mu.Lock()
	s.failErr = msg
	s.mu.Unlock()
}

func (s *mockValkeyRevocationServer) Close() {
	_ = s.ln.Close()
	s.wg.Wait()
}

func (s *mockValkeyRevocationServer) serveConn(c net.Conn) {
	br := bufio.NewReader(c)
	for {
		args, err := readTestRESPArray(br)
		if err != nil {
			return
		}
		if len(args) < 2 {
			_, _ = c.Write([]byte("-ERR wrong number of arguments\r\n"))
			continue
		}
		cmd := strings.ToUpper(args[0])
		key := args[1]
		s.hits.Add(1)

		s.mu.Lock()
		failMsg := s.failErr
		isRevoked := s.revoked[key]
		s.mu.Unlock()

		if failMsg != "" {
			_, _ = fmt.Fprintf(c, "-ERR %s\r\n", failMsg)
			continue
		}
		switch cmd {
		case "EXISTS":
			if isRevoked {
				_, _ = c.Write([]byte(":1\r\n"))
			} else {
				_, _ = c.Write([]byte(":0\r\n"))
			}
		case "GET":
			if isRevoked {
				_, _ = c.Write([]byte("$1\r\n1\r\n"))
			} else {
				_, _ = c.Write([]byte("$-1\r\n"))
			}
		default:
			_, _ = c.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func readTestRESPArray(br *bufio.Reader) ([]string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for range n {
		lenLine, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lenLine = strings.TrimSuffix(strings.TrimSuffix(lenLine, "\n"), "\r")
		if !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("expected bulk, got %q", lenLine)
		}
		l, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := br.Read(buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

func TestJWKSAuthenticator_AlgorithmsAndPEM(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	edSigner := newEd25519TestSigner(t, "kid-ed")
	ecSigner := newES256TestSigner(t, "kid-ec")
	rsaSigner := newRS256TestSigner(t, "kid-rsa")
	pemSigner := newEd25519TestSigner(t, "kid-pem")

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		StaticJWKS: marshalJWKS(t, edSigner, ecSigner, rsaSigner),
		StaticPEMKeys: map[string][]byte{
			"kid-pem": pemSigner.pemBytes(t),
		},
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	auth, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:      "stravin-auth",
		Audience:    "stravin-edge",
		KeyProvider: ks,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}

	commitOID := "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b"
	for _, s := range []testSigner{edSigner, ecSigner, rsaSigner, pemSigner} {
		t.Run(s.alg+"_"+s.kid, func(t *testing.T) {
			claims := baseValidClaims(now)
			claims["commit_oid"] = commitOID
			claims["scopes"] = []string{"git:read", "git:bundle:read"}
			tok := s.signJWT(t, nil, claims)

			p, err := auth.AuthenticateToken(context.Background(), tok)
			if err != nil {
				t.Fatalf("AuthenticateToken(%s): %v", s.alg, err)
			}
			if p.ActorID != "usr_01JDF9A1B2C3D4E5F6G7H8J9K0" ||
				p.TenantID != "acme" ||
				p.RepoID != "core" ||
				p.JTI != "jti_01JDF9Z1A2B3C4D5E6F7G8H9J0" ||
				p.CommitOID != commitOID ||
				!p.HasScope(ScopeGitRead) ||
				!p.HasScope(ScopeGitBundleRead) {
				t.Fatalf("unexpected principal: %+v", p)
			}
		})
	}
}

func TestJWKSAuthenticator_ValidationRejectsInvalidTokens(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	edSigner := newEd25519TestSigner(t, "kid-ed")
	ecSigner := newES256TestSigner(t, "kid-ec")

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		StaticJWKS: marshalJWKS(t, edSigner, ecSigner),
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	auth, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:      "stravin-auth",
		Audience:    "stravin-edge",
		ClockSkew:   5 * time.Second,
		KeyProvider: ks,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}

	makeToken := func(mut func(hdr, claims map[string]any)) string {
		hdr := map[string]any{}
		claims := baseValidClaims(now)
		mut(hdr, claims)
		return edSigner.signJWT(t, hdr, claims)
	}

	cases := []struct {
		name  string
		token string
	}{
		{
			name:  "malformed jwt",
			token: "not.a.valid.jwt.token",
		},
		{
			name:  "expired beyond skew",
			token: makeToken(func(_, c map[string]any) { c["exp"] = now.Add(-10 * time.Second).Unix() }),
		},
		{
			name:  "not before in future beyond skew",
			token: makeToken(func(_, c map[string]any) { c["nbf"] = now.Add(10 * time.Second).Unix() }),
		},
		{
			name:  "wrong issuer",
			token: makeToken(func(_, c map[string]any) { c["iss"] = "evil-issuer" }),
		},
		{
			name:  "wrong audience string",
			token: makeToken(func(_, c map[string]any) { c["aud"] = "stravin-review" }),
		},
		{
			name:  "wrong audience array",
			token: makeToken(func(_, c map[string]any) { c["aud"] = []string{"stravin-review", "stravin-run"} }),
		},
		{
			name:  "missing sub",
			token: makeToken(func(_, c map[string]any) { delete(c, "sub") }),
		},
		{
			name:  "missing jti",
			token: makeToken(func(_, c map[string]any) { delete(c, "jti") }),
		},
		{
			name:  "missing tenant_id",
			token: makeToken(func(_, c map[string]any) { delete(c, "tenant_id") }),
		},
		{
			name:  "invalid tenant_id",
			token: makeToken(func(_, c map[string]any) { c["tenant_id"] = "../evil" }),
		},
		{
			name:  "invalid commit_oid",
			token: makeToken(func(_, c map[string]any) { c["commit_oid"] = "not-a-hex-oid" }),
		},
		{
			name:  "empty scopes",
			token: makeToken(func(_, c map[string]any) { c["scopes"] = []string{} }),
		},
		{
			name:  "unrecognized scope only",
			token: makeToken(func(_, c map[string]any) { c["scopes"] = []string{"admin:billing"} }),
		},
		{
			name:  "alg none attack",
			token: makeToken(func(h, _ map[string]any) { h["alg"] = "none" }),
		},
		{
			name:  "algorithm confusion header vs jwk",
			token: makeToken(func(h, _ map[string]any) { h["kid"] = "kid-ec"; h["alg"] = "EdDSA" }),
		},
		{
			name: "tampered payload",
			token: func() string {
				tok := edSigner.signJWT(t, nil, baseValidClaims(now))
				parts := strings.Split(tok, ".")
				tamperedClaims := baseValidClaims(now)
				tamperedClaims["tenant_id"] = "beta"
				raw, _ := json.Marshal(tamperedClaims)
				parts[1] = base64.RawURLEncoding.EncodeToString(raw)
				return strings.Join(parts, ".")
			}(),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := auth.AuthenticateToken(context.Background(), tc.token); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("expected ErrUnauthorized for %s, got %v", tc.name, err)
			}
		})
	}

	// Array audience containing expected audience should succeed.
	t.Run("audience array containing expected aud succeeds", func(t *testing.T) {
		tok := makeToken(func(_, c map[string]any) {
			c["aud"] = []string{"stravin-review", "stravin-edge"}
		})
		if _, err := auth.AuthenticateToken(context.Background(), tok); err != nil {
			t.Fatalf("expected array aud to succeed, got %v", err)
		}
	})
}

func TestJWKSAuthenticator_KeyRotationViaHTTPRefresh(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	key1 := newEd25519TestSigner(t, "kid-1")
	key2 := newEd25519TestSigner(t, "kid-2")

	var mu sync.RWMutex
	activeJWKS := marshalJWKS(t, key1)
	var fetchCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetchCount.Add(1)
		mu.RLock()
		body := activeJWKS
		mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		URL:                srv.URL + "/.well-known/jwks.json",
		RefreshInterval:    5 * time.Minute,
		MinRefreshCooldown: 1 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	auth, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:      "stravin-auth",
		Audience:    "stravin-edge",
		KeyProvider: ks,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}

	// Token signed with kid-1 succeeds without extra fetch.
	tok1 := key1.signJWT(t, nil, baseValidClaims(now))
	if _, err := auth.AuthenticateToken(context.Background(), tok1); err != nil {
		t.Fatalf("kid-1 auth failed: %v", err)
	}
	if got := fetchCount.Load(); got != 1 {
		t.Fatalf("expected 1 JWKS fetch, got %d", got)
	}

	// Rotate server JWKS to kid-2.
	mu.Lock()
	activeJWKS = marshalJWKS(t, key1, key2)
	mu.Unlock()
	time.Sleep(5 * time.Millisecond)

	// Token signed with kid-2 triggers on-demand refresh and succeeds.
	tok2 := key2.signJWT(t, nil, baseValidClaims(now))
	if _, err := auth.AuthenticateToken(context.Background(), tok2); err != nil {
		t.Fatalf("kid-2 auth after rotation failed: %v", err)
	}
	if got := fetchCount.Load(); got < 2 {
		t.Fatalf("expected on-demand JWKS refresh on unknown kid, fetches=%d", got)
	}
}

func TestJWKSAuthenticator_ValkeyRevocationAndFailClosed(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	signer := newEd25519TestSigner(t, "kid-1")

	valkey := startMockValkeyRevocationServer(t)
	defer valkey.Close()

	checker, err := NewValkeyRevocationChecker(ValkeyRevocationConfig{
		Addr:         valkey.Addr(),
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		DialTimeout:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewValkeyRevocationChecker: %v", err)
	}
	defer func() { _ = checker.Close() }()

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		StaticJWKS: marshalJWKS(t, signer),
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	auth, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:            "stravin-auth",
		Audience:          "stravin-edge",
		KeyProvider:       ks,
		RevocationChecker: checker,
		RequireRevocation: true,
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}

	tok := signer.signJWT(t, nil, baseValidClaims(now))

	// 1. Not revoked -> succeeds.
	if _, err := auth.AuthenticateToken(context.Background(), tok); err != nil {
		t.Fatalf("expected non-revoked token to succeed, got %v", err)
	}

	// 2. Revoked in Valkey -> immediate rejection (ErrUnauthorized + ErrTokenRevoked).
	valkey.SetRevoked("auth:jti:revoked:acme:jti_01JDF9Z1A2B3C4D5E6F7G8H9J0", true)
	_, err = auth.AuthenticateToken(context.Background(), tok)
	if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, ErrTokenRevoked) {
		t.Fatalf("expected ErrUnauthorized wrapping ErrTokenRevoked, got %v", err)
	}

	// 3. Valkey error -> fail-closed with ErrAuthUnavailable.
	valkey.SetFailError("LOADING Valkey is loading the dataset in memory")
	_, err = auth.AuthenticateToken(context.Background(), tok)
	if !errors.Is(err, ErrAuthUnavailable) {
		t.Fatalf("expected ErrAuthUnavailable on Valkey error, got %v", err)
	}
}

func TestHTTPHandler_JWKSRepoScopesRevocationAndFailClosed(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	signer := newEd25519TestSigner(t, "kid-1")

	valkey := startMockValkeyRevocationServer(t)
	defer valkey.Close()

	checker, err := NewValkeyRevocationChecker(ValkeyRevocationConfig{
		Addr: valkey.Addr(),
	})
	if err != nil {
		t.Fatalf("NewValkeyRevocationChecker: %v", err)
	}
	defer func() { _ = checker.Close() }()

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		StaticJWKS: marshalJWKS(t, signer),
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	auth, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:            "stravin-auth",
		Audience:          "stravin-edge",
		KeyProvider:       ks,
		RevocationChecker: checker,
		RequireRevocation: true,
		Now:               func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}

	env := newTestEdgeEnv(t, LimiterConfig{})
	defer env.close()
	env.handler.auth = auth

	readClaims := baseValidClaims(now)
	readClaims["scopes"] = []string{"git:read"}
	readTok := signer.signJWT(t, nil, readClaims)

	writeClaims := baseValidClaims(now)
	writeClaims["jti"] = "jti_write_1"
	writeClaims["scopes"] = []string{"git:write"}
	writeTok := signer.signJWT(t, nil, writeClaims)

	// 1. git:read allowed on info/refs?service=git-upload-pack -> 200
	req := httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer "+readTok)
	rec := httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("git:read info/refs status = %d, want 200", rec.Code)
	}

	// 2. git:read denied on git-receive-pack -> 403
	req = httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("PACK"))
	req.Header.Set("Authorization", "Bearer "+readTok)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("git:read receive-pack status = %d, want 403", rec.Code)
	}

	// 3. git:read pinned to repo "core" denied on repo "west" -> 403
	req = httptest.NewRequest(http.MethodGet, "/acme/west.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer "+readTok)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("repo-pinned token on other repo status = %d, want 403", rec.Code)
	}

	// 4. git:write allowed on git-receive-pack -> 200
	req = httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("PACK"))
	req.Header.Set("Authorization", "Bearer "+writeTok)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("git:write receive-pack status = %d, want 200", rec.Code)
	}

	// 5. Revoked jti -> 401
	valkey.SetRevoked("auth:jti:revoked:acme:jti_write_1", true)
	req = httptest.NewRequest(http.MethodPost, "/acme/core.git/git-receive-pack", strings.NewReader("PACK"))
	req.Header.Set("Authorization", "Bearer "+writeTok)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token status = %d, want 401", rec.Code)
	}

	// 6. Valkey failure -> 503 fail-closed
	valkey.SetFailError("READONLY")
	req = httptest.NewRequest(http.MethodGet, "/acme/core.git/info/refs?service=git-upload-pack", nil)
	req.Header.Set("Authorization", "Bearer "+readTok)
	rec = httptest.NewRecorder()
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("valkey error status = %d, want 503", rec.Code)
	}
}

func TestAuthorizeRepoOperation_StaticFallbackAndCommitPinning(t *testing.T) {
	repo := tenant.RepoKey{TenantID: "acme", RepoID: "core"}

	// StaticAuthenticator principal (empty RepoID & Scopes) allows both read and write in same tenant.
	staticP := Principal{ActorID: "alice", TenantID: "acme"}
	if err := AuthorizeRepoOperation(staticP, repo, rpc.OpUploadPack); err != nil {
		t.Fatalf("static upload-pack: %v", err)
	}
	if err := AuthorizeRepoOperation(staticP, repo, rpc.OpReceivePack); err != nil {
		t.Fatalf("static receive-pack: %v", err)
	}

	// commit_oid-pinned principal cannot push even if scopes somehow included git:write.
	pinned := Principal{
		ActorID:   "runner",
		TenantID:  "acme",
		RepoID:    "core",
		CommitOID: "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b",
		Scopes:    []string{ScopeGitRead, ScopeGitWrite},
	}
	if err := AuthorizeRepoOperation(pinned, repo, rpc.OpUploadPack); err != nil {
		t.Fatalf("commit-pinned upload-pack should succeed: %v", err)
	}
	if err := AuthorizeRepoOperation(pinned, repo, rpc.OpReceivePack); !errors.Is(err, ErrForbidden) {
		t.Fatalf("commit-pinned receive-pack should return ErrForbidden, got %v", err)
	}
}

func TestJWKSAuthenticator_BackgroundRefreshAndSSHFallback(t *testing.T) {
	now := time.Date(2026, 11, 27, 12, 0, 0, 0, time.UTC)
	key1 := newEd25519TestSigner(t, "kid-bg-1")
	key2 := newEd25519TestSigner(t, "kid-bg-2")

	var mu sync.RWMutex
	activeJWKS := marshalJWKS(t, key1)
	var fetches atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches.Add(1)
		mu.RLock()
		body := activeJWKS
		mu.RUnlock()
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ks, err := NewJWKSKeySet(context.Background(), JWKSKeySetConfig{
		URL:                srv.URL,
		RefreshInterval:    20 * time.Millisecond,
		MinRefreshCooldown: 10 * time.Second, // high cooldown ensures background loop does the rotation
	})
	if err != nil {
		t.Fatalf("NewJWKSKeySet: %v", err)
	}
	defer func() { _ = ks.Close() }()

	mu.Lock()
	activeJWKS = marshalJWKS(t, key1, key2)
	mu.Unlock()

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ks.lookup("kid-bg-2"); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := ks.lookup("kid-bg-2"); !ok {
		t.Fatalf("background refresh did not load rotated key kid-bg-2 (fetches=%d)", fetches.Load())
	}

	// RequireRevocation without RevocationChecker must fail fast.
	if _, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:            "stravin-auth",
		Audience:          "stravin-edge",
		KeyProvider:       ks,
		RequireRevocation: true,
	}); err == nil {
		t.Fatal("expected error when RequireRevocation is true without RevocationChecker")
	}

	// SSHFallback delegation works when configured, and rejects when nil.
	_, pub := generateTestSSHKey(t)
	sshFallback, err := NewStaticAuthenticator(nil, []SSHKeyEntry{
		{Key: pub, Principal: Principal{ActorID: "alice", TenantID: "acme"}},
	})
	if err != nil {
		t.Fatalf("NewStaticAuthenticator: %v", err)
	}

	authWithSSH, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:      "stravin-auth",
		Audience:    "stravin-edge",
		KeyProvider: ks,
		SSHFallback: sshFallback,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator: %v", err)
	}
	p, err := authWithSSH.AuthenticateSSHKey(context.Background(), pub)
	if err != nil || p.TenantID != "acme" || p.ActorID != "alice" {
		t.Fatalf("unexpected SSH fallback principal: %+v err=%v", p, err)
	}

	authNoSSH, err := NewJWKSAuthenticator(JWKSAuthenticatorConfig{
		Issuer:      "stravin-auth",
		Audience:    "stravin-edge",
		KeyProvider: ks,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewJWKSAuthenticator(no ssh): %v", err)
	}
	if _, err := authNoSSH.AuthenticateSSHKey(context.Background(), pub); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized without SSHFallback, got %v", err)
	}
}
