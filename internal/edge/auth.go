package edge

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/crypto/ssh"

	"stravin.dev/core/internal/tenant"
)

var (
	// ErrUnauthorized indicates missing or invalid authentication credentials.
	ErrUnauthorized = errors.New("edge: unauthorized")
	// ErrForbidden indicates an authenticated principal is not allowed to access the requested tenant.
	ErrForbidden = errors.New("edge: forbidden")
)

// Principal represents an authenticated caller bound to a specific tenant.
type Principal struct {
	ActorID  string
	TenantID tenant.TenantID
}

// Validate checks that the principal carries a valid TenantID and non-empty ActorID.
func (p Principal) Validate() error {
	if err := p.TenantID.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(p.ActorID) == "" {
		return errors.New("edge: empty actor id")
	}
	return nil
}

// AuthorizeTenant enforces strict tenant isolation: the authenticated principal's
// TenantID must match the requested repository's TenantID.
func AuthorizeTenant(p Principal, requested tenant.TenantID) error {
	if err := p.TenantID.Validate(); err != nil {
		return ErrForbidden
	}
	if err := requested.Validate(); err != nil {
		return ErrForbidden
	}
	if subtle.ConstantTimeCompare([]byte(p.TenantID), []byte(requested)) != 1 {
		return ErrForbidden
	}
	return nil
}

// Authenticator is the GoF Strategy interface for verifying HTTP tokens and SSH public keys.
type Authenticator interface {
	AuthenticateToken(ctx context.Context, token string) (Principal, error)
	AuthenticateSSHKey(ctx context.Context, key ssh.PublicKey) (Principal, error)
}

// TokenEntry binds a static secret token to a Principal.
type TokenEntry struct {
	Token     string
	Principal Principal
}

// SSHKeyEntry binds an authorized SSH public key to a Principal.
type SSHKeyEntry struct {
	Key       ssh.PublicKey
	Principal Principal
}

type sshKeyRecord struct {
	keyType   string
	marshaled []byte
	principal Principal
}

// StaticAuthenticator implements Authenticator using configured tokens and SSH public keys
// with constant-time credential comparisons.
type StaticAuthenticator struct {
	tokens  []TokenEntry
	sshKeys []sshKeyRecord
}

// NewStaticAuthenticator validates entries and constructs a StaticAuthenticator.
func NewStaticAuthenticator(tokens []TokenEntry, keys []SSHKeyEntry) (*StaticAuthenticator, error) {
	validTokens := make([]TokenEntry, 0, len(tokens))
	for i, entry := range tokens {
		if entry.Token == "" {
			return nil, fmt.Errorf("edge: token entry %d has empty token", i)
		}
		if err := entry.Principal.Validate(); err != nil {
			return nil, fmt.Errorf("edge: token entry %d: %w", i, err)
		}
		validTokens = append(validTokens, entry)
	}

	validKeys := make([]sshKeyRecord, 0, len(keys))
	for i, entry := range keys {
		if entry.Key == nil {
			return nil, fmt.Errorf("edge: ssh key entry %d has nil key", i)
		}
		if err := entry.Principal.Validate(); err != nil {
			return nil, fmt.Errorf("edge: ssh key entry %d: %w", i, err)
		}
		validKeys = append(validKeys, sshKeyRecord{
			keyType:   entry.Key.Type(),
			marshaled: entry.Key.Marshal(),
			principal: entry.Principal,
		})
	}

	return &StaticAuthenticator{
		tokens:  validTokens,
		sshKeys: validKeys,
	}, nil
}

// AuthenticateToken verifies token in constant time against all configured tokens.
func (a *StaticAuthenticator) AuthenticateToken(_ context.Context, token string) (Principal, error) {
	if token == "" || len(a.tokens) == 0 {
		return Principal{}, ErrUnauthorized
	}

	candidate := []byte(token)
	var matched Principal
	found := 0

	for _, entry := range a.tokens {
		if subtle.ConstantTimeCompare(candidate, []byte(entry.Token)) == 1 {
			matched = entry.Principal
			found = 1
		}
	}
	if found == 1 {
		return matched, nil
	}
	return Principal{}, ErrUnauthorized
}

// AuthenticateSSHKey verifies key in constant time against all configured authorized keys.
func (a *StaticAuthenticator) AuthenticateSSHKey(_ context.Context, key ssh.PublicKey) (Principal, error) {
	if key == nil || len(a.sshKeys) == 0 {
		return Principal{}, ErrUnauthorized
	}

	candidateType := key.Type()
	candidateBytes := key.Marshal()
	var matched Principal
	found := 0

	for _, entry := range a.sshKeys {
		if subtle.ConstantTimeCompare(candidateBytes, entry.marshaled) == 1 && candidateType == entry.keyType {
			matched = entry.Principal()
			found = 1
		}
	}
	if found == 1 {
		return matched, nil
	}
	return Principal{}, ErrUnauthorized
}

func (r sshKeyRecord) Principal() Principal {
	return r.principal
}

// ParseTokens parses comma- or newline-delimited "tenant_id:actor_id:token" entries.
// Blank lines and lines starting with '#' are ignored.
func ParseTokens(spec string) ([]TokenEntry, error) {
	var entries []TokenEntry
	scanner := bufio.NewScanner(strings.NewReader(spec))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		for _, item := range strings.Split(line, ",") {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			parts := strings.SplitN(item, ":", 3)
			if len(parts) != 3 {
				return nil, fmt.Errorf("edge: invalid token entry on line %d (expected tenant:actor:token)", lineNum)
			}
			tenantID, err := tenant.ParseTenantID(strings.TrimSpace(parts[0]))
			if err != nil {
				return nil, fmt.Errorf("edge: token line %d: %w", lineNum, err)
			}
			actorID := strings.TrimSpace(parts[1])
			tok := strings.TrimSpace(parts[2])
			entry := TokenEntry{
				Token: tok,
				Principal: Principal{
					ActorID:  actorID,
					TenantID: tenantID,
				},
			}
			if tok == "" {
				return nil, fmt.Errorf("edge: token line %d: empty token", lineNum)
			}
			if err := entry.Principal.Validate(); err != nil {
				return nil, fmt.Errorf("edge: token line %d: %w", lineNum, err)
			}
			entries = append(entries, entry)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}

// LoadTokensFile reads and parses token entries from path.
func LoadTokensFile(path string) ([]TokenEntry, error) {
	data, err := os.ReadFile(path) //#nosec G304 -- path comes from operator daemon configuration
	if err != nil {
		return nil, fmt.Errorf("edge: read tokens file: %w", err)
	}
	return ParseTokens(string(data))
}

// ParseAuthorizedKeys parses OpenSSH authorized_keys lines where the key comment
// carries "tenant_id:actor_id" (or "tenant_id", defaulting actor to "ssh-user").
func ParseAuthorizedKeys(data []byte) ([]SSHKeyEntry, error) {
	var entries []SSHKeyEntry
	rest := bytes.TrimSpace(data)
	for len(rest) > 0 {
		if rest[0] == '#' || rest[0] == '\n' || rest[0] == '\r' {
			idx := bytes.IndexByte(rest, '\n')
			if idx < 0 {
				break
			}
			rest = bytes.TrimSpace(rest[idx+1:])
			continue
		}
		pub, comment, _, next, err := ssh.ParseAuthorizedKey(rest)
		if err != nil {
			return nil, fmt.Errorf("edge: parse authorized key: %w", err)
		}
		principal, err := parseSSHCommentPrincipal(comment)
		if err != nil {
			return nil, err
		}
		entries = append(entries, SSHKeyEntry{
			Key:       pub,
			Principal: principal,
		})
		rest = bytes.TrimSpace(next)
	}
	return entries, nil
}

// LoadAuthorizedKeysFile reads and parses authorized SSH keys from path.
func LoadAuthorizedKeysFile(path string) ([]SSHKeyEntry, error) {
	data, err := os.ReadFile(path) //#nosec G304 -- path comes from operator daemon configuration
	if err != nil {
		return nil, fmt.Errorf("edge: read authorized keys file: %w", err)
	}
	return ParseAuthorizedKeys(data)
}

func parseSSHCommentPrincipal(comment string) (Principal, error) {
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return Principal{}, errors.New("edge: ssh authorized key missing tenant:actor comment")
	}
	parts := strings.SplitN(comment, ":", 2)
	tenantID, err := tenant.ParseTenantID(strings.TrimSpace(parts[0]))
	if err != nil {
		return Principal{}, fmt.Errorf("edge: ssh key comment tenant: %w", err)
	}
	actorID := "ssh-user"
	if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
		actorID = strings.TrimSpace(parts[1])
	}
	return Principal{
		ActorID:  actorID,
		TenantID: tenantID,
	}, nil
}
