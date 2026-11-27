package edge

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/tenant"
)

const defaultJWTClockSkew = 30 * time.Second

// JWKSAuthenticatorConfig configures a local off-hot-path JWKSAuthenticator.
type JWKSAuthenticatorConfig struct {
	Issuer            string
	Audience          string
	ClockSkew         time.Duration
	AllowedScopes     []string
	KeyProvider       KeyProvider
	RevocationChecker RevocationChecker
	RequireRevocation bool
	SSHFallback       Authenticator
	Now               func() time.Time
}

// JWKSAuthenticator implements Authenticator by verifying stravin-auth JWTs locally
// against a cached JWKS KeyProvider and checking the Valkey jti deny-list.
type JWKSAuthenticator struct {
	issuer        string
	audience      string
	clockSkew     time.Duration
	allowedScopes map[string]struct{}
	keys          KeyProvider
	revocation    RevocationChecker
	sshFallback   Authenticator
	now           func() time.Time
}

var _ Authenticator = (*JWKSAuthenticator)(nil)

// NewJWKSAuthenticator validates cfg and constructs a JWKSAuthenticator.
func NewJWKSAuthenticator(cfg JWKSAuthenticatorConfig) (*JWKSAuthenticator, error) {
	issuer := strings.TrimSpace(cfg.Issuer)
	if issuer == "" {
		return nil, errors.New("edge: jwks authenticator issuer is required")
	}
	audience := strings.TrimSpace(cfg.Audience)
	if audience == "" {
		return nil, errors.New("edge: jwks authenticator audience is required")
	}
	if cfg.KeyProvider == nil {
		return nil, errors.New("edge: jwks authenticator key provider is required")
	}
	if cfg.RequireRevocation && cfg.RevocationChecker == nil {
		return nil, errors.New("edge: revocation checker is required when RequireRevocation is enabled")
	}

	skew := cfg.ClockSkew
	if skew < 0 {
		skew = 0
	}
	if skew == 0 && cfg.Now == nil {
		skew = defaultJWTClockSkew
	}

	scopes := cfg.AllowedScopes
	if len(scopes) == 0 {
		scopes = []string{ScopeGitRead, ScopeGitWrite, ScopeGitBundleRead}
	}
	allowed := make(map[string]struct{}, len(scopes))
	for _, s := range scopes {
		if trimmed := strings.TrimSpace(s); trimmed != "" {
			allowed[trimmed] = struct{}{}
		}
	}

	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	return &JWKSAuthenticator{
		issuer:        issuer,
		audience:      audience,
		clockSkew:     skew,
		allowedScopes: allowed,
		keys:          cfg.KeyProvider,
		revocation:    cfg.RevocationChecker,
		sshFallback:   cfg.SSHFallback,
		now:           nowFn,
	}, nil
}

type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

type jwtAudience []string

func (a *jwtAudience) UnmarshalJSON(data []byte) error {
	var single string
	if err := json.Unmarshal(data, &single); err == nil {
		*a = []string{single}
		return nil
	}
	var multi []string
	if err := json.Unmarshal(data, &multi); err == nil {
		*a = multi
		return nil
	}
	return errors.New("edge: invalid jwt aud claim")
}

type jwtClaims struct {
	Iss       string      `json:"iss"`
	Sub       string      `json:"sub"`
	Aud       jwtAudience `json:"aud"`
	JTI       string      `json:"jti"`
	TenantID  string      `json:"tenant_id"`
	RepoID    string      `json:"repo_id"`
	CommitOID string      `json:"commit_oid"`
	RefGlobs  []string    `json:"ref_globs"`
	Scopes    []string    `json:"scopes"`
	RevEpoch  int64       `json:"rev_epoch"`
	Exp       *int64      `json:"exp"`
	Nbf       *int64      `json:"nbf"`
}

// AuthenticateToken verifies the compact JWS locally against cached JWKS keys,
// validates all required claims, and checks the Valkey jti deny-list when enabled.
func (a *JWKSAuthenticator) AuthenticateToken(ctx context.Context, token string) (Principal, error) {
	headerB64, payloadB64, sigB64, ok := splitCompactJWS(token)
	if !ok {
		return Principal{}, ErrUnauthorized
	}

	hdrBytes, err := base64.RawURLEncoding.DecodeString(headerB64)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	var hdr jwtHeader
	if err := json.Unmarshal(hdrBytes, &hdr); err != nil {
		return Principal{}, ErrUnauthorized
	}
	if !isAllowedJWTAlg(hdr.Alg) || strings.TrimSpace(hdr.Kid) == "" {
		return Principal{}, ErrUnauthorized
	}
	if hdr.Typ != "" && !strings.EqualFold(hdr.Typ, "JWT") && !strings.EqualFold(hdr.Typ, "at+jwt") {
		return Principal{}, ErrUnauthorized
	}

	jwk, err := a.keys.ResolveKey(ctx, hdr.Kid)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	if jwk.Alg != "" && jwk.Alg != hdr.Alg {
		return Principal{}, ErrUnauthorized
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	signingInput := []byte(headerB64 + "." + payloadB64)
	if !verifyJWTSignature(hdr.Alg, jwk.PublicKey, signingInput, sigBytes) {
		return Principal{}, ErrUnauthorized
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}
	var claims jwtClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return Principal{}, ErrUnauthorized
	}

	principal, err := a.validateClaims(claims)
	if err != nil {
		return Principal{}, ErrUnauthorized
	}

	if a.revocation != nil {
		revoked, err := a.revocation.IsRevoked(ctx, principal.TenantID, principal.JTI)
		if err != nil {
			return Principal{}, fmt.Errorf("%w: %w", ErrAuthUnavailable, err)
		}
		if revoked {
			return Principal{}, fmt.Errorf("%w: %w", ErrUnauthorized, ErrTokenRevoked)
		}
	}

	return principal, nil
}

// AuthenticateSSHKey delegates SSH public key authentication to the optional SSHFallback.
func (a *JWKSAuthenticator) AuthenticateSSHKey(ctx context.Context, key ssh.PublicKey) (Principal, error) {
	if a.sshFallback == nil {
		return Principal{}, ErrUnauthorized
	}
	return a.sshFallback.AuthenticateSSHKey(ctx, key)
}

func (a *JWKSAuthenticator) validateClaims(c jwtClaims) (Principal, error) {
	if subtle.ConstantTimeCompare([]byte(c.Iss), []byte(a.issuer)) != 1 {
		return Principal{}, errors.New("edge: issuer mismatch")
	}
	if !audienceContains(c.Aud, a.audience) {
		return Principal{}, errors.New("edge: audience mismatch")
	}

	now := a.now()
	if c.Exp == nil || *c.Exp <= 0 {
		return Principal{}, errors.New("edge: missing exp")
	}
	if !now.Add(-a.clockSkew).Before(time.Unix(*c.Exp, 0)) {
		return Principal{}, errors.New("edge: token expired")
	}
	if c.Nbf != nil && now.Add(a.clockSkew).Before(time.Unix(*c.Nbf, 0)) {
		return Principal{}, errors.New("edge: token not yet valid")
	}

	sub := strings.TrimSpace(c.Sub)
	if sub == "" {
		return Principal{}, errors.New("edge: missing sub")
	}
	jti := strings.TrimSpace(c.JTI)
	if jti == "" {
		return Principal{}, errors.New("edge: missing jti")
	}

	tenantID, err := tenant.ParseTenantID(strings.TrimSpace(c.TenantID))
	if err != nil {
		return Principal{}, err
	}

	var repoID tenant.RepoID
	if rawRepo := strings.TrimSpace(c.RepoID); rawRepo != "" {
		repoID, err = tenant.ParseRepoID(rawRepo)
		if err != nil {
			return Principal{}, err
		}
	}

	commitOID := strings.TrimSpace(c.CommitOID)
	if commitOID != "" && !isValidGitOID(commitOID) {
		return Principal{}, errors.New("edge: invalid commit_oid")
	}

	if !a.hasRecognizedScope(c.Scopes) {
		return Principal{}, errors.New("edge: missing recognized git scope")
	}

	p := Principal{
		ActorID:   sub,
		TenantID:  tenantID,
		JTI:       jti,
		RepoID:    repoID,
		CommitOID: commitOID,
		RefGlobs:  append([]string(nil), c.RefGlobs...),
		Scopes:    append([]string(nil), c.Scopes...),
		RevEpoch:  c.RevEpoch,
	}
	if err := p.Validate(); err != nil {
		return Principal{}, err
	}
	return p, nil
}

func (a *JWKSAuthenticator) hasRecognizedScope(scopes []string) bool {
	if len(scopes) == 0 {
		return false
	}
	for _, s := range scopes {
		if _, ok := a.allowedScopes[s]; ok {
			return true
		}
	}
	return false
}

func splitCompactJWS(token string) (string, string, string, bool) {
	if token == "" {
		return "", "", "", false
	}
	first := strings.IndexByte(token, '.')
	if first <= 0 {
		return "", "", "", false
	}
	rest := token[first+1:]
	second := strings.IndexByte(rest, '.')
	if second <= 0 || second == len(rest)-1 {
		return "", "", "", false
	}
	sig := rest[second+1:]
	if strings.IndexByte(sig, '.') >= 0 {
		return "", "", "", false
	}
	return token[:first], rest[:second], sig, true
}

func isAllowedJWTAlg(alg string) bool {
	switch alg {
	case "EdDSA", "ES256", "RS256":
		return true
	default:
		return false
	}
}

func verifyJWTSignature(alg string, pub crypto.PublicKey, signingInput, sig []byte) bool {
	switch alg {
	case "EdDSA":
		edPub, ok := pub.(ed25519.PublicKey)
		if !ok || len(sig) != ed25519.SignatureSize {
			return false
		}
		return ed25519.Verify(edPub, signingInput, sig)

	case "ES256":
		ecPub, ok := pub.(*ecdsa.PublicKey)
		if !ok || len(sig) != 64 {
			return false
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		sum := sha256.Sum256(signingInput)
		return ecdsa.Verify(ecPub, sum[:], r, s)

	case "RS256":
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok || rsaPub.N.BitLen() < minRSAModulusBits {
			return false
		}
		sum := sha256.Sum256(signingInput)
		return rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, sum[:], sig) == nil

	default:
		return false
	}
}

func audienceContains(aud []string, expected string) bool {
	matched := 0
	for _, item := range aud {
		if subtle.ConstantTimeCompare([]byte(item), []byte(expected)) == 1 {
			matched = 1
		}
	}
	return matched == 1
}

func isValidGitOID(oid string) bool {
	if len(oid) != 40 && len(oid) != 64 {
		return false
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
