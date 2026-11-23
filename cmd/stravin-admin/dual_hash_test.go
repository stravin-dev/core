package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"stravin.dev/core/internal/admin"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

func TestRepoCreateObjectFormatFlag(t *testing.T) {
	store := newMemStore()
	factory := func(_ context.Context, _ string) (*admin.Service, error) {
		return admin.NewService(store, store, nil)
	}

	// 1. Default without --object-format persists sha1.
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{
		"repo", "create", "--tenant", "acme", "--repo", "default-sha1",
	}, factory, &stdout, &stderr); err != nil {
		t.Fatalf("repo create default: %v (stderr=%s)", err, stderr.String())
	}
	var res1 admin.RegisterRepoResult
	if err := json.Unmarshal(stdout.Bytes(), &res1); err != nil {
		t.Fatalf("unmarshal stdout: %v", err)
	}
	if res1.ObjectFormat != "sha1" {
		t.Fatalf("default object_format = %q, want sha1", res1.ObjectFormat)
	}

	// 2. Explicit --object-format sha256 persists sha256 in DDB META.
	stdout.Reset()
	stderr.Reset()
	if err := run(context.Background(), []string{
		"repo", "create", "--tenant", "acme", "--repo", "next-sha256", "--object-format", "sha256",
	}, factory, &stdout, &stderr); err != nil {
		t.Fatalf("repo create sha256: %v (stderr=%s)", err, stderr.String())
	}
	var res2 admin.RegisterRepoResult
	if err := json.Unmarshal(stdout.Bytes(), &res2); err != nil {
		t.Fatalf("unmarshal stdout: %v", err)
	}
	if res2.ObjectFormat != "sha256" {
		t.Fatalf("sha256 object_format = %q, want sha256", res2.ObjectFormat)
	}
	rk, _ := tenant.NewRepoKey("acme", "next-sha256")
	if got := store.metas[rk].ObjectFormat; got != ddb.ObjectFormatSHA256 {
		t.Fatalf("stored DDB RepoMeta.ObjectFormat = %q, want sha256", got)
	}

	// 3. Invalid --object-format rejected.
	stdout.Reset()
	stderr.Reset()
	if err := run(context.Background(), []string{
		"repo", "create", "--tenant", "acme", "--repo", "bad-fmt", "--object-format", "blake3",
	}, factory, &stdout, &stderr); err == nil {
		t.Fatalf("expected error for invalid --object-format blake3")
	}
}
