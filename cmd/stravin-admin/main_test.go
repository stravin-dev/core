package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"stravin.dev/core/internal/admin"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type memStore struct {
	metas    map[tenant.RepoKey]ddb.RepoMeta
	hooks    map[tenant.RepoKey][]ddb.HookConfig
	webhooks map[tenant.RepoKey][]ddb.WebhookConfig
	wasms    map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{
		metas:    make(map[tenant.RepoKey]ddb.RepoMeta),
		hooks:    make(map[tenant.RepoKey][]ddb.HookConfig),
		webhooks: make(map[tenant.RepoKey][]ddb.WebhookConfig),
		wasms:    make(map[string][]byte),
	}
}

func (m *memStore) CreateMeta(_ context.Context, meta ddb.RepoMeta) error {
	if _, ok := m.metas[meta.Repo]; ok {
		return ddb.ErrAlreadyExists
	}
	m.metas[meta.Repo] = meta
	return nil
}

func (m *memStore) GetRepoHooks(_ context.Context, repo tenant.RepoKey) ([]ddb.HookConfig, error) {
	h, ok := m.hooks[repo]
	if !ok {
		return nil, ddb.ErrNotFound
	}
	return h, nil
}

func (m *memStore) PutRepoHooks(_ context.Context, repo tenant.RepoKey, hooks []ddb.HookConfig) error {
	m.hooks[repo] = hooks
	return nil
}

func (m *memStore) GetWebhooks(_ context.Context, repo tenant.RepoKey) ([]ddb.WebhookConfig, error) {
	w, ok := m.webhooks[repo]
	if !ok {
		return nil, ddb.ErrNotFound
	}
	return w, nil
}

func (m *memStore) PutWebhooks(_ context.Context, repo tenant.RepoKey, webhooks []ddb.WebhookConfig) error {
	m.webhooks[repo] = webhooks
	return nil
}

func (m *memStore) PutWasmHook(_ context.Context, sha string, data []byte) error {
	m.wasms[sha] = append([]byte(nil), data...)
	return nil
}

func (m *memStore) Encrypt(_ context.Context, _ tenant.TenantID, plaintext []byte) ([]byte, error) {
	return append([]byte("kms:"), plaintext...), nil
}

func TestCLI_RepoHookAndWebhookSubcommands(t *testing.T) {
	store := newMemStore()
	factory := func(_ context.Context, _ string) (*admin.Service, error) {
		return admin.NewService(store, store, store)
	}

	// 1. repo create
	var out, errBuf bytes.Buffer
	if err := run(context.Background(), []string{
		"repo", "create", "--tenant", "local", "--repo", "cli-test", "--ref-shards", "2", "--manifest-shards", "2",
	}, factory, &out, &errBuf); err != nil {
		t.Fatalf("repo create: %v (stderr=%s)", err, errBuf.String())
	}
	var repoRes admin.RegisterRepoResult
	if err := json.Unmarshal(out.Bytes(), &repoRes); err != nil {
		t.Fatalf("unmarshal repo result: %v", err)
	}
	if !repoRes.Created || repoRes.RefShards != 2 {
		t.Fatalf("unexpected repo result: %+v", repoRes)
	}

	// 2. hook put
	wasmFile := filepath.Join(t.TempDir(), "allow.wasm")
	if err := os.WriteFile(wasmFile, []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}, 0o600); err != nil {
		t.Fatalf("write wasm fixture: %v", err)
	}
	out.Reset()
	errBuf.Reset()
	if err := run(context.Background(), []string{
		"hook", "put", "--tenant", "local", "--repo", "cli-test", "--hook-id", "allow-hook", "--wasm", wasmFile,
	}, factory, &out, &errBuf); err != nil {
		t.Fatalf("hook put: %v (stderr=%s)", err, errBuf.String())
	}
	var hookRes admin.ConfigureWasmHookResult
	if err := json.Unmarshal(out.Bytes(), &hookRes); err != nil {
		t.Fatalf("unmarshal hook result: %v", err)
	}
	if hookRes.HookID != "allow-hook" || len(store.wasms) != 1 {
		t.Fatalf("unexpected hook result: %+v", hookRes)
	}

	// 3. webhook put
	out.Reset()
	errBuf.Reset()
	if err := run(context.Background(), []string{
		"webhook", "put", "--tenant", "local", "--repo", "cli-test", "--webhook-id", "wh-1",
		"--url", "http://host.docker.internal:9000/hook", "--secret", "top-secret",
	}, factory, &out, &errBuf); err != nil {
		t.Fatalf("webhook put: %v (stderr=%s)", err, errBuf.String())
	}
	var whRes admin.ConfigureWebhookResult
	if err := json.Unmarshal(out.Bytes(), &whRes); err != nil {
		t.Fatalf("unmarshal webhook result: %v", err)
	}
	if whRes.WebhookID != "wh-1" || !whRes.Enabled {
		t.Fatalf("unexpected webhook result: %+v", whRes)
	}
}
