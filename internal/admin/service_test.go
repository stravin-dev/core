package admin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

type fakeRepoStore struct {
	metas    map[tenant.RepoKey]ddb.RepoMeta
	hooks    map[tenant.RepoKey][]ddb.HookConfig
	webhooks map[tenant.RepoKey][]ddb.WebhookConfig
}

func newFakeRepoStore() *fakeRepoStore {
	return &fakeRepoStore{
		metas:    make(map[tenant.RepoKey]ddb.RepoMeta),
		hooks:    make(map[tenant.RepoKey][]ddb.HookConfig),
		webhooks: make(map[tenant.RepoKey][]ddb.WebhookConfig),
	}
}

func (f *fakeRepoStore) CreateMeta(_ context.Context, meta ddb.RepoMeta) error {
	if _, exists := f.metas[meta.Repo]; exists {
		return ddb.ErrAlreadyExists
	}
	f.metas[meta.Repo] = meta
	return nil
}

func (f *fakeRepoStore) GetRepoHooks(_ context.Context, repo tenant.RepoKey) ([]ddb.HookConfig, error) {
	h, ok := f.hooks[repo]
	if !ok {
		return nil, ddb.ErrNotFound
	}
	return append([]ddb.HookConfig(nil), h...), nil
}

func (f *fakeRepoStore) PutRepoHooks(_ context.Context, repo tenant.RepoKey, hooks []ddb.HookConfig) error {
	f.hooks[repo] = append([]ddb.HookConfig(nil), hooks...)
	return nil
}

func (f *fakeRepoStore) GetWebhooks(_ context.Context, repo tenant.RepoKey) ([]ddb.WebhookConfig, error) {
	w, ok := f.webhooks[repo]
	if !ok {
		return nil, ddb.ErrNotFound
	}
	return append([]ddb.WebhookConfig(nil), w...), nil
}

func (f *fakeRepoStore) PutWebhooks(_ context.Context, repo tenant.RepoKey, webhooks []ddb.WebhookConfig) error {
	f.webhooks[repo] = append([]ddb.WebhookConfig(nil), webhooks...)
	return nil
}

type fakeWasmStore struct {
	blobs map[string][]byte
}

func newFakeWasmStore() *fakeWasmStore {
	return &fakeWasmStore{blobs: make(map[string][]byte)}
}

func (f *fakeWasmStore) PutWasmHook(_ context.Context, sha string, data []byte) error {
	f.blobs[sha] = append([]byte(nil), data...)
	return nil
}

type fakeCipher struct {
	lastTenant tenant.TenantID
}

func (f *fakeCipher) Encrypt(_ context.Context, tenantID tenant.TenantID, plaintext []byte) ([]byte, error) {
	f.lastTenant = tenantID
	return append([]byte("enc:"), plaintext...), nil
}

func TestService_RegisterRepoAndIdempotency(t *testing.T) {
	store := newFakeRepoStore()
	svc, err := NewService(store, nil, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	res, err := svc.RegisterRepo(context.Background(), RegisterRepoInput{
		TenantID: "local",
		RepoID:   "demo",
	})
	if err != nil {
		t.Fatalf("RegisterRepo: %v", err)
	}
	if !res.Created || res.HomeRegion != "us-east-1" || res.FailoverEpoch != 1 || res.RefShards != 1 || res.ManifestShards != 1 {
		t.Fatalf("unexpected RegisterRepoResult: %+v", res)
	}

	// Non-idempotent duplicate fails with ErrAlreadyExists.
	if _, err := svc.RegisterRepo(context.Background(), RegisterRepoInput{
		TenantID: "local",
		RepoID:   "demo",
	}); !errors.Is(err, ddb.ErrAlreadyExists) {
		t.Fatalf("duplicate RegisterRepo = %v, want ErrAlreadyExists", err)
	}

	// Idempotent duplicate succeeds cleanly with Created=false.
	res2, err := svc.RegisterRepo(context.Background(), RegisterRepoInput{
		TenantID:      "local",
		RepoID:        "demo",
		FailoverEpoch: lease.Epoch(1),
		Idempotent:    true,
	})
	if err != nil {
		t.Fatalf("idempotent RegisterRepo: %v", err)
	}
	if res2.Created {
		t.Fatalf("expected Created=false on idempotent re-register")
	}
}

func TestService_ConfigureWasmHookAndAppend(t *testing.T) {
	ddbStore := newFakeRepoStore()
	s3Store := newFakeWasmStore()
	svc, err := NewService(ddbStore, s3Store, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	wasm1 := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	sum1 := sha256.Sum256(wasm1)
	wantSHA1 := hex.EncodeToString(sum1[:])

	res1, err := svc.ConfigureWasmHook(context.Background(), ConfigureWasmHookInput{
		TenantID:  "local",
		RepoID:    "demo",
		HookID:    "hook-1",
		WasmBytes: wasm1,
	})
	if err != nil {
		t.Fatalf("ConfigureWasmHook(1): %v", err)
	}
	if res1.WasmSHA256 != wantSHA1 || res1.S3Key != "hooks/"+wantSHA1+".wasm" {
		t.Fatalf("unexpected hook result: %+v", res1)
	}
	if !bytes.Equal(s3Store.blobs[wantSHA1], wasm1) {
		t.Fatalf("wasm bytes not uploaded to S3")
	}

	wasm2 := append(append([]byte(nil), wasm1...), 0x00)
	if _, err := svc.ConfigureWasmHook(context.Background(), ConfigureWasmHookInput{
		TenantID:  "local",
		RepoID:    "demo",
		HookID:    "hook-2",
		WasmBytes: wasm2,
		Append:    true,
	}); err != nil {
		t.Fatalf("ConfigureWasmHook(2): %v", err)
	}

	rk := tenant.RepoKey{TenantID: "local", RepoID: "demo"}
	if got := len(ddbStore.hooks[rk]); got != 2 {
		t.Fatalf("hook count = %d, want 2", got)
	}
}

func TestService_ConfigureWebhookEncryptsWithTenantAAD(t *testing.T) {
	ddbStore := newFakeRepoStore()
	cipher := &fakeCipher{}
	svc, err := NewService(ddbStore, nil, cipher)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	res, err := svc.ConfigureWebhook(context.Background(), ConfigureWebhookInput{
		TenantID:  "local",
		RepoID:    "demo",
		WebhookID: "wh-1",
		URL:       "http://localhost:9999/hook",
		Secret:    "s3cr3t",
		KMSKeyID:  "alias/stravin-local-dev",
		Enabled:   true,
	})
	if err != nil {
		t.Fatalf("ConfigureWebhook: %v", err)
	}
	if res.WebhookID != "wh-1" || !res.Enabled {
		t.Fatalf("unexpected webhook result: %+v", res)
	}
	if cipher.lastTenant != "local" {
		t.Fatalf("cipher tenant = %q, want local", cipher.lastTenant)
	}

	rk := tenant.RepoKey{TenantID: "local", RepoID: "demo"}
	got := ddbStore.webhooks[rk]
	if len(got) != 1 || !bytes.Equal(got[0].EncryptedSecret, []byte("enc:s3cr3t")) {
		t.Fatalf("unexpected persisted webhooks: %+v", got)
	}
}
