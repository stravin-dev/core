package edge

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultJWKSRefreshInterval = 5 * time.Minute
	defaultJWKSRefreshCooldown = 10 * time.Second
	defaultJWKSHTTPTimeout     = 5 * time.Second
	maxJWKSResponseBytes       = 1 << 20 // 1 MiB
	minRSAModulusBits          = 2048
)

// ErrKeyNotFound indicates that no matching public key exists for the requested kid.
var ErrKeyNotFound = errors.New("edge: jwk key not found")

// JWK holds a parsed public verification key and its optional pinned algorithm.
type JWK struct {
	Kid       string
	Alg       string
	PublicKey crypto.PublicKey
}

// KeyProvider is the GoF Strategy/Proxy interface for resolving JWT verification keys by kid.
type KeyProvider interface {
	ResolveKey(ctx context.Context, kid string) (JWK, error)
}

// JWKSKeySetConfig configures a JWKSKeySet backed by an HTTP JWKS endpoint,
// static JWKS JSON, and/or static PEM public keys.
type JWKSKeySetConfig struct {
	URL                string
	StaticJWKS         []byte
	StaticPEMKeys      map[string][]byte
	RefreshInterval    time.Duration
	MinRefreshCooldown time.Duration
	HTTPTimeout        time.Duration
	HTTPClient         *http.Client
	Logger             *slog.Logger
}

// JWKSKeySet caches parsed JWKS public keys in memory for lock-free hot-path verification
// and refreshes them periodically in the background (or rate-limited on unknown kid).
type JWKSKeySet struct {
	url                string
	staticKeys         map[string]JWK
	refreshInterval    time.Duration
	minRefreshCooldown time.Duration
	client             *http.Client
	logger             *slog.Logger

	keys atomic.Pointer[map[string]JWK]

	refreshMu   sync.Mutex
	lastRefresh time.Time

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ KeyProvider = (*JWKSKeySet)(nil)

// NewJWKSKeySet constructs a JWKSKeySet from cfg, performs an initial fetch when URL is set,
// and starts a background refresh goroutine. Call Close to stop background refreshes.
func NewJWKSKeySet(ctx context.Context, cfg JWKSKeySetConfig) (*JWKSKeySet, error) {
	rawURL := strings.TrimSpace(cfg.URL)
	if rawURL == "" && len(cfg.StaticJWKS) == 0 && len(cfg.StaticPEMKeys) == 0 {
		return nil, errors.New("edge: jwks url, static jwks, or static pem keys required")
	}
	if rawURL != "" {
		parsedURL, err := url.Parse(rawURL)
		if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
			return nil, fmt.Errorf("edge: invalid jwks url %q", rawURL)
		}
	}

	staticKeys := make(map[string]JWK)
	if len(cfg.StaticJWKS) > 0 {
		parsed, err := ParseJWKS(cfg.StaticJWKS)
		if err != nil {
			return nil, err
		}
		for kid, k := range parsed {
			staticKeys[kid] = k
		}
	}
	for kid, pemBytes := range cfg.StaticPEMKeys {
		kid = strings.TrimSpace(kid)
		if kid == "" {
			return nil, errors.New("edge: static pem key requires non-empty kid")
		}
		pub, alg, err := ParsePEMPublicKey(pemBytes)
		if err != nil {
			return nil, fmt.Errorf("edge: static pem key %q: %w", kid, err)
		}
		staticKeys[kid] = JWK{Kid: kid, Alg: alg, PublicKey: pub}
	}

	refreshInterval := cfg.RefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = defaultJWKSRefreshInterval
	}
	cooldown := cfg.MinRefreshCooldown
	if cooldown <= 0 {
		cooldown = defaultJWKSRefreshCooldown
	}
	httpTimeout := cfg.HTTPTimeout
	if httpTimeout <= 0 {
		httpTimeout = defaultJWKSHTTPTimeout
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{
			Timeout: httpTimeout,
			Transport: &http.Transport{
				DialContext:           (&net.Dialer{Timeout: httpTimeout}).DialContext,
				TLSHandshakeTimeout:   httpTimeout,
				ResponseHeaderTimeout: httpTimeout,
				IdleConnTimeout:       60 * time.Second,
			},
		}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	bgCtx, cancel := context.WithCancel(context.Background())
	ks := &JWKSKeySet{
		url:                rawURL,
		staticKeys:         staticKeys,
		refreshInterval:    refreshInterval,
		minRefreshCooldown: cooldown,
		client:             client,
		logger:             logger,
		cancel:             cancel,
	}

	initial := cloneJWKMap(staticKeys)
	ks.keys.Store(&initial)

	if rawURL != "" {
		if err := ks.Refresh(ctx); err != nil {
			cancel()
			return nil, err
		}
		ks.wg.Add(1)
		go ks.refreshLoop(bgCtx)
	}

	return ks, nil
}

// Close stops the background refresh goroutine and waits for it to exit.
func (ks *JWKSKeySet) Close() error {
	if ks == nil {
		return nil
	}
	if ks.cancel != nil {
		ks.cancel()
	}
	ks.wg.Wait()
	return nil
}

// ResolveKey returns the cached public key for kid. On an unknown kid when a JWKS URL is
// configured, it performs a rate-limited on-demand refresh to support seamless key rotation.
func (ks *JWKSKeySet) ResolveKey(ctx context.Context, kid string) (JWK, error) {
	if ks == nil || strings.TrimSpace(kid) == "" {
		return JWK{}, ErrKeyNotFound
	}
	if key, ok := ks.lookup(kid); ok {
		return key, nil
	}
	if ks.url == "" {
		return JWK{}, ErrKeyNotFound
	}
	if err := ks.refreshIfAllowed(ctx); err != nil {
		return JWK{}, fmt.Errorf("%w: refresh failed: %w", ErrKeyNotFound, err)
	}
	if key, ok := ks.lookup(kid); ok {
		return key, nil
	}
	return JWK{}, ErrKeyNotFound
}

// Refresh fetches the remote JWKS document and atomically replaces the cached keyset.
func (ks *JWKSKeySet) Refresh(ctx context.Context) error {
	if ks.url == "" {
		return nil
	}
	ks.refreshMu.Lock()
	defer ks.refreshMu.Unlock()
	return ks.fetchAndStoreLocked(ctx)
}

func (ks *JWKSKeySet) refreshIfAllowed(ctx context.Context) error {
	ks.refreshMu.Lock()
	defer ks.refreshMu.Unlock()
	if time.Since(ks.lastRefresh) < ks.minRefreshCooldown {
		return nil
	}
	return ks.fetchAndStoreLocked(ctx)
}

func (ks *JWKSKeySet) fetchAndStoreLocked(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ks.url, nil) //#nosec G704 -- JWKS URL is validated from operator daemon configuration
	if err != nil {
		return fmt.Errorf("edge: build jwks request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := ks.client.Do(req) //#nosec G704 -- JWKS URL is validated from operator daemon configuration
	if err != nil {
		return fmt.Errorf("edge: fetch jwks %s: %w", ks.url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("edge: jwks endpoint returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxJWKSResponseBytes+1))
	if err != nil {
		return fmt.Errorf("edge: read jwks response: %w", err)
	}
	if len(body) > maxJWKSResponseBytes {
		return errors.New("edge: jwks response exceeds max size")
	}

	remoteKeys, err := ParseJWKS(body)
	if err != nil {
		return err
	}

	merged := cloneJWKMap(ks.staticKeys)
	for kid, k := range remoteKeys {
		merged[kid] = k
	}
	ks.keys.Store(&merged)
	ks.lastRefresh = time.Now()
	return nil
}

func (ks *JWKSKeySet) refreshLoop(ctx context.Context) {
	defer ks.wg.Done()
	ticker := time.NewTicker(ks.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx, cancel := context.WithTimeout(ctx, defaultJWKSHTTPTimeout)
			if err := ks.Refresh(refreshCtx); err != nil && ctx.Err() == nil {
				ks.logger.Warn("jwks background refresh failed; retaining cached keyset", "url", ks.url, "error", err)
			}
			cancel()
		}
	}
}

func (ks *JWKSKeySet) lookup(kid string) (JWK, bool) {
	m := ks.keys.Load()
	if m == nil {
		return JWK{}, false
	}
	k, ok := (*m)[kid]
	return k, ok
}

func cloneJWKMap(src map[string]JWK) map[string]JWK {
	dst := make(map[string]JWK, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

type rawJWKS struct {
	Keys []rawJWK `json:"keys"`
}

type rawJWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// ParseJWKS parses an RFC 7517 JWKS JSON payload containing Ed25519 (OKP),
// P-256 ECDSA (EC), and >=2048-bit RSA signature keys.
func ParseJWKS(data []byte) (map[string]JWK, error) {
	var doc rawJWKS
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("edge: parse jwks json: %w", err)
	}
	if len(doc.Keys) == 0 {
		return nil, errors.New("edge: jwks contains no keys")
	}

	out := make(map[string]JWK, len(doc.Keys))
	for i, rk := range doc.Keys {
		if rk.Use != "" && rk.Use != "sig" {
			continue
		}
		kid := strings.TrimSpace(rk.Kid)
		if kid == "" {
			return nil, fmt.Errorf("edge: jwk[%d] missing kid", i)
		}
		jwk, err := parseRawJWK(rk)
		if err != nil {
			return nil, fmt.Errorf("edge: jwk[%d] (%s): %w", i, kid, err)
		}
		out[kid] = jwk
	}
	if len(out) == 0 {
		return nil, errors.New("edge: jwks contains no usable signing keys")
	}
	return out, nil
}

func parseRawJWK(rk rawJWK) (JWK, error) {
	switch rk.Kty {
	case "OKP":
		if rk.Crv != "Ed25519" {
			return JWK{}, fmt.Errorf("unsupported OKP curve %q", rk.Crv)
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(rk.X)
		if err != nil || len(xBytes) != ed25519.PublicKeySize {
			return JWK{}, errors.New("invalid Ed25519 public key bytes")
		}
		alg := rk.Alg
		if alg == "" {
			alg = "EdDSA"
		}
		if alg != "EdDSA" {
			return JWK{}, fmt.Errorf("incompatible alg %q for Ed25519", alg)
		}
		return JWK{Kid: rk.Kid, Alg: alg, PublicKey: ed25519.PublicKey(xBytes)}, nil

	case "EC":
		if rk.Crv != "P-256" {
			return JWK{}, fmt.Errorf("unsupported EC curve %q", rk.Crv)
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(rk.X)
		if err != nil || len(xBytes) == 0 || len(xBytes) > 32 {
			return JWK{}, errors.New("invalid EC x coordinate")
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(rk.Y)
		if err != nil || len(yBytes) == 0 || len(yBytes) > 32 {
			return JWK{}, errors.New("invalid EC y coordinate")
		}
		var uncompressed [65]byte
		uncompressed[0] = 0x04
		copy(uncompressed[1+(32-len(xBytes)):33], xBytes)
		copy(uncompressed[33+(32-len(yBytes)):65], yBytes)
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), uncompressed[:])
		if err != nil {
			return JWK{}, fmt.Errorf("invalid P-256 public key: %w", err)
		}
		alg := rk.Alg
		if alg == "" {
			alg = "ES256"
		}
		if alg != "ES256" {
			return JWK{}, fmt.Errorf("incompatible alg %q for P-256", alg)
		}
		return JWK{Kid: rk.Kid, Alg: alg, PublicKey: pub}, nil

	case "RSA":
		nBytes, err := base64.RawURLEncoding.DecodeString(rk.N)
		if err != nil || len(nBytes) == 0 {
			return JWK{}, errors.New("invalid RSA modulus")
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(rk.E)
		if err != nil || len(eBytes) == 0 || len(eBytes) > 8 {
			return JWK{}, errors.New("invalid RSA exponent")
		}
		n := new(big.Int).SetBytes(nBytes)
		if n.BitLen() < minRSAModulusBits {
			return JWK{}, fmt.Errorf("RSA key too small (%d bits < %d)", n.BitLen(), minRSAModulusBits)
		}
		eBig := new(big.Int).SetBytes(eBytes)
		if !eBig.IsInt64() {
			return JWK{}, errors.New("RSA exponent overflow")
		}
		e := int(eBig.Int64())
		if e < 3 || e%2 == 0 {
			return JWK{}, errors.New("invalid RSA public exponent")
		}
		alg := rk.Alg
		if alg == "" {
			alg = "RS256"
		}
		if alg != "RS256" {
			return JWK{}, fmt.Errorf("incompatible alg %q for RSA", alg)
		}
		return JWK{Kid: rk.Kid, Alg: alg, PublicKey: &rsa.PublicKey{N: n, E: e}}, nil

	default:
		return JWK{}, fmt.Errorf("unsupported kty %q", rk.Kty)
	}
}

// ParsePEMPublicKey decodes a PEM-encoded PKIX or PKCS#1 public key for Ed25519,
// P-256 ECDSA, or >=2048-bit RSA and returns the key with its canonical JWT alg.
func ParsePEMPublicKey(pemBytes []byte) (crypto.PublicKey, string, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, "", errors.New("edge: invalid pem block")
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		rsaPub, rsaErr := x509.ParsePKCS1PublicKey(block.Bytes)
		if rsaErr != nil {
			return nil, "", fmt.Errorf("edge: parse pem public key: %w", err)
		}
		pub = rsaPub
	}
	switch k := pub.(type) {
	case ed25519.PublicKey:
		return k, "EdDSA", nil
	case *ecdsa.PublicKey:
		if k.Curve != elliptic.P256() {
			return nil, "", errors.New("edge: only P-256 ECDSA keys are supported")
		}
		return k, "ES256", nil
	case *rsa.PublicKey:
		if k.N.BitLen() < minRSAModulusBits {
			return nil, "", fmt.Errorf("edge: RSA key too small (%d bits < %d)", k.N.BitLen(), minRSAModulusBits)
		}
		return k, "RS256", nil
	default:
		return nil, "", fmt.Errorf("edge: unsupported public key type %T", pub)
	}
}
