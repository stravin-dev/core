package tenant_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func TestParseTenantID(t *testing.T) {
	valid := []string{
		"acme",
		"org-1",
		"org_prod.2",
		"0123456789",
		strings.Repeat("a", tenant.MaxTenantIDLength),
	}
	for _, raw := range valid {
		id, err := tenant.ParseTenantID(raw)
		if err != nil {
			t.Fatalf("ParseTenantID(%q) unexpected error: %v", raw, err)
		}
		if string(id) != raw {
			t.Fatalf("got %q, want %q", id, raw)
		}
	}

	invalid := []struct {
		raw  string
		want error
	}{
		{"", tenant.ErrEmptyTenantID},
		{"   ", tenant.ErrInvalidTenantID},
		{"../escape", tenant.ErrInvalidTenantID},
		{"acme/sub", tenant.ErrInvalidTenantID},
		{`acme\sub`, tenant.ErrInvalidTenantID},
		{"acme#1", tenant.ErrInvalidTenantID},
		{"-leading-dash", tenant.ErrInvalidTenantID},
		{".leading-dot", tenant.ErrInvalidTenantID},
		{"has space", tenant.ErrInvalidTenantID},
		{"null\x00byte", tenant.ErrInvalidTenantID},
		{strings.Repeat("a", tenant.MaxTenantIDLength+1), tenant.ErrInvalidTenantID},
	}
	for _, tc := range invalid {
		_, err := tenant.ParseTenantID(tc.raw)
		if !errors.Is(err, tc.want) {
			t.Errorf("ParseTenantID(%q) err = %v, want %v", tc.raw, err, tc.want)
		}
	}
}

func TestParseRepoID(t *testing.T) {
	valid := []string{
		"core",
		"my-repo_1.git",
		strings.Repeat("r", tenant.MaxRepoIDLength),
	}
	for _, raw := range valid {
		id, err := tenant.ParseRepoID(raw)
		if err != nil {
			t.Fatalf("ParseRepoID(%q) unexpected error: %v", raw, err)
		}
		if string(id) != raw {
			t.Fatalf("got %q, want %q", id, raw)
		}
	}

	invalid := []struct {
		raw  string
		want error
	}{
		{"", tenant.ErrEmptyRepoID},
		{"..", tenant.ErrInvalidRepoID},
		{"a/b", tenant.ErrInvalidRepoID},
		{"a#b", tenant.ErrInvalidRepoID},
		{"repo..name", tenant.ErrInvalidRepoID},
		{strings.Repeat("r", tenant.MaxRepoIDLength+1), tenant.ErrInvalidRepoID},
	}
	for _, tc := range invalid {
		_, err := tenant.ParseRepoID(tc.raw)
		if !errors.Is(err, tc.want) {
			t.Errorf("ParseRepoID(%q) err = %v, want %v", tc.raw, err, tc.want)
		}
	}
}

func TestRepoKeyFormatting(t *testing.T) {
	key, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey unexpected error: %v", err)
	}
	if got, want := key.String(), "acme/core"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if got, want := key.DynamoPrefix(), "T#acme#REPO#core"; got != want {
		t.Errorf("DynamoPrefix() = %q, want %q", got, want)
	}
	if got, want := key.S3Prefix("ab"), "ab/acme/core"; got != want {
		t.Errorf("S3Prefix() = %q, want %q", got, want)
	}
}

func TestContextPropagation(t *testing.T) {
	ctx := context.Background()

	if _, err := tenant.RequireTenant(ctx); !errors.Is(err, tenant.ErrMissingTenant) {
		t.Fatalf("RequireTenant on empty ctx = %v, want ErrMissingTenant", err)
	}
	if _, err := tenant.RequireRepo(ctx); !errors.Is(err, tenant.ErrMissingRepo) {
		t.Fatalf("RequireRepo on empty ctx = %v, want ErrMissingRepo", err)
	}
	if _, err := tenant.RequireRepoKey(ctx); !errors.Is(err, tenant.ErrMissingTenant) {
		t.Fatalf("RequireRepoKey on empty ctx = %v, want ErrMissingTenant", err)
	}

	if _, err := tenant.WithTenant(ctx, tenant.TenantID("bad/id")); !errors.Is(err, tenant.ErrInvalidTenantID) {
		t.Fatalf("WithTenant invalid = %v, want ErrInvalidTenantID", err)
	}

	key, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	ctx, err = tenant.WithRepoKey(ctx, key)
	if err != nil {
		t.Fatalf("WithRepoKey: %v", err)
	}

	gotTenant, err := tenant.RequireTenant(ctx)
	if err != nil || gotTenant != "acme" {
		t.Fatalf("RequireTenant = (%q, %v), want (acme, nil)", gotTenant, err)
	}
	gotRepo, err := tenant.RequireRepo(ctx)
	if err != nil || gotRepo != "core" {
		t.Fatalf("RequireRepo = (%q, %v), want (core, nil)", gotRepo, err)
	}
	gotKey, err := tenant.RequireRepoKey(ctx)
	if err != nil || gotKey != key {
		t.Fatalf("RequireRepoKey = (%+v, %v), want (%+v, nil)", gotKey, err, key)
	}
}
