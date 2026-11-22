// Package tenant defines mandatory multi-tenant identity types, validation,
// storage key prefixes, and context propagation helpers shared across all
// Stravin Core daemons.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const (
	// MaxTenantIDLength bounds TenantID length for safe DynamoDB/S3/NVMe keys.
	MaxTenantIDLength = 64
	// MaxRepoIDLength bounds RepoID length for safe DynamoDB/S3/NVMe keys.
	MaxRepoIDLength = 128
)

var (
	ErrEmptyTenantID   = errors.New("tenant: empty tenant id")
	ErrInvalidTenantID = errors.New("tenant: invalid tenant id")
	ErrEmptyRepoID     = errors.New("tenant: empty repo id")
	ErrInvalidRepoID   = errors.New("tenant: invalid repo id")
	ErrMissingTenant   = errors.New("tenant: missing tenant id in context")
	ErrMissingRepo     = errors.New("tenant: missing repo id in context")
)

// TenantID uniquely identifies an organization/tenant across storage and RPC boundaries.
type TenantID string

// RepoID uniquely identifies a repository within a tenant.
type RepoID string

// RepoKey pairs a validated TenantID and RepoID.
type RepoKey struct {
	TenantID TenantID
	RepoID   RepoID
}

// ParseTenantID validates and returns a TenantID.
func ParseTenantID(raw string) (TenantID, error) {
	if err := validateIdentifier(raw, MaxTenantIDLength, ErrEmptyTenantID, ErrInvalidTenantID); err != nil {
		return "", err
	}
	return TenantID(raw), nil
}

// Validate checks that t is a valid TenantID.
func (t TenantID) Validate() error {
	return validateIdentifier(string(t), MaxTenantIDLength, ErrEmptyTenantID, ErrInvalidTenantID)
}

// ParseRepoID validates and returns a RepoID.
func ParseRepoID(raw string) (RepoID, error) {
	if err := validateIdentifier(raw, MaxRepoIDLength, ErrEmptyRepoID, ErrInvalidRepoID); err != nil {
		return "", err
	}
	return RepoID(raw), nil
}

// Validate checks that r is a valid RepoID.
func (r RepoID) Validate() error {
	return validateIdentifier(string(r), MaxRepoIDLength, ErrEmptyRepoID, ErrInvalidRepoID)
}

// NewRepoKey validates and constructs a RepoKey.
func NewRepoKey(tenantID, repoID string) (RepoKey, error) {
	t, err := ParseTenantID(tenantID)
	if err != nil {
		return RepoKey{}, err
	}
	r, err := ParseRepoID(repoID)
	if err != nil {
		return RepoKey{}, err
	}
	return RepoKey{TenantID: t, RepoID: r}, nil
}

// Validate verifies both TenantID and RepoID on k.
func (k RepoKey) Validate() error {
	if err := k.TenantID.Validate(); err != nil {
		return err
	}
	return k.RepoID.Validate()
}

// String returns "<tenant_id>/<repo_id>".
func (k RepoKey) String() string {
	return fmt.Sprintf("%s/%s", k.TenantID, k.RepoID)
}

// DynamoPrefix returns the canonical DynamoDB partition-key prefix "T#<tenant>#REPO#<repo>".
func (k RepoKey) DynamoPrefix() string {
	return fmt.Sprintf("T#%s#REPO#%s", k.TenantID, k.RepoID)
}

// S3Prefix returns the canonical S3 object key prefix "<hashPrefix>/<tenant>/<repo>".
func (k RepoKey) S3Prefix(hashPrefix string) string {
	return fmt.Sprintf("%s/%s/%s", hashPrefix, k.TenantID, k.RepoID)
}

func validateIdentifier(raw string, maxLen int, emptyErr, invalidErr error) error {
	if raw == "" {
		return emptyErr
	}
	if len(raw) > maxLen || strings.Contains(raw, "..") {
		return fmt.Errorf("%w: %q", invalidErr, raw)
	}
	if !isAlphaNum(raw[0]) {
		return fmt.Errorf("%w: %q", invalidErr, raw)
	}
	for i := 1; i < len(raw); i++ {
		c := raw[i]
		if !isAlphaNum(c) && c != '.' && c != '-' && c != '_' {
			return fmt.Errorf("%w: %q", invalidErr, raw)
		}
	}
	return nil
}

func isAlphaNum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

type tenantCtxKey struct{}
type repoCtxKey struct{}

// WithTenant validates id and attaches it to ctx.
func WithTenant(ctx context.Context, id TenantID) (context.Context, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, tenantCtxKey{}, id), nil
}

// TenantFromContext extracts TenantID from ctx if present.
func TenantFromContext(ctx context.Context) (TenantID, bool) {
	id, ok := ctx.Value(tenantCtxKey{}).(TenantID)
	return id, ok && id != ""
}

// RequireTenant returns the validated TenantID from ctx or ErrMissingTenant.
func RequireTenant(ctx context.Context) (TenantID, error) {
	id, ok := TenantFromContext(ctx)
	if !ok {
		return "", ErrMissingTenant
	}
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

// WithRepo validates id and attaches it to ctx.
func WithRepo(ctx context.Context, id RepoID) (context.Context, error) {
	if err := id.Validate(); err != nil {
		return nil, err
	}
	return context.WithValue(ctx, repoCtxKey{}, id), nil
}

// RepoFromContext extracts RepoID from ctx if present.
func RepoFromContext(ctx context.Context) (RepoID, bool) {
	id, ok := ctx.Value(repoCtxKey{}).(RepoID)
	return id, ok && id != ""
}

// RequireRepo returns the validated RepoID from ctx or ErrMissingRepo.
func RequireRepo(ctx context.Context) (RepoID, error) {
	id, ok := RepoFromContext(ctx)
	if !ok {
		return "", ErrMissingRepo
	}
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

// WithRepoKey validates key and attaches both TenantID and RepoID to ctx.
func WithRepoKey(ctx context.Context, key RepoKey) (context.Context, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, tenantCtxKey{}, key.TenantID)
	ctx = context.WithValue(ctx, repoCtxKey{}, key.RepoID)
	return ctx, nil
}

// RepoKeyFromContext extracts both TenantID and RepoID from ctx if present.
func RepoKeyFromContext(ctx context.Context) (RepoKey, bool) {
	t, okT := TenantFromContext(ctx)
	r, okR := RepoFromContext(ctx)
	if !okT || !okR {
		return RepoKey{}, false
	}
	return RepoKey{TenantID: t, RepoID: r}, true
}

// RequireRepoKey returns a validated RepoKey from ctx or a missing/invalid error.
func RequireRepoKey(ctx context.Context) (RepoKey, error) {
	t, err := RequireTenant(ctx)
	if err != nil {
		return RepoKey{}, err
	}
	r, err := RequireRepo(ctx)
	if err != nil {
		return RepoKey{}, err
	}
	return RepoKey{TenantID: t, RepoID: r}, nil
}
