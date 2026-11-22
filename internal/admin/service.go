// Package admin provides the GoF Facade for seeding and administering Stravin
// repositories, Tier 1 Wasm hooks, and Tier 2 KMS-encrypted webhooks.
package admin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

// RepoStore is the narrow DynamoDB seam used by admin operations.
// *ddb.Repository satisfies this interface directly.
type RepoStore interface {
	CreateMeta(ctx context.Context, meta ddb.RepoMeta) error
	GetRepoHooks(ctx context.Context, repo tenant.RepoKey) ([]ddb.HookConfig, error)
	PutRepoHooks(ctx context.Context, repo tenant.RepoKey, hooks []ddb.HookConfig) error
	GetWebhooks(ctx context.Context, repo tenant.RepoKey) ([]ddb.WebhookConfig, error)
	PutWebhooks(ctx context.Context, repo tenant.RepoKey, webhooks []ddb.WebhookConfig) error
}

// WasmStore is the narrow S3 seam for uploading Tier 1 Wasm hook artifacts.
// *s3.Store satisfies this interface directly.
type WasmStore interface {
	PutWasmHook(ctx context.Context, wasmSHA256 string, wasmBytes []byte) error
}

// SecretCipher is the narrow KMS envelope encryption seam for Tier 2 webhook secrets.
// *crypto.KMSEnvelopeCipher satisfies this interface directly.
type SecretCipher interface {
	Encrypt(ctx context.Context, tenantID tenant.TenantID, plaintext []byte) ([]byte, error)
}

// Service coordinates repository registration, Tier 1 Wasm hook deployment,
// and Tier 2 webhook configuration across DynamoDB, S3, and KMS.
type Service struct {
	ddb    RepoStore
	s3     WasmStore
	cipher SecretCipher
}

// NewService constructs an admin Service. s3Store and cipher may be nil when
// only subcommands that do not require them are invoked.
func NewService(ddbStore RepoStore, s3Store WasmStore, cipher SecretCipher) (*Service, error) {
	if ddbStore == nil {
		return nil, errors.New("admin: ddb store is required")
	}
	return &Service{
		ddb:    ddbStore,
		s3:     s3Store,
		cipher: cipher,
	}, nil
}

// RegisterRepoInput configures a new repository META item.
type RegisterRepoInput struct {
	TenantID       string
	RepoID         string
	HomeRegion     string
	FailoverEpoch  lease.Epoch
	RefShards      int
	ManifestShards int
	DefaultBranch  string
	Idempotent     bool
}

// RegisterRepoResult summarizes a registered repository META item.
type RegisterRepoResult struct {
	TenantID       string `json:"tenant_id"`
	RepoID         string `json:"repo_id"`
	HomeRegion     string `json:"home_region"`
	FailoverEpoch  uint64 `json:"failover_epoch"`
	RefShards      int    `json:"ref_shards"`
	ManifestShards int    `json:"manifest_shards"`
	DefaultBranch  string `json:"default_branch"`
	Created        bool   `json:"created"`
}

// RegisterRepo writes a repository META item to DynamoDB.
func (s *Service) RegisterRepo(ctx context.Context, in RegisterRepoInput) (RegisterRepoResult, error) {
	rk, err := tenant.NewRepoKey(in.TenantID, in.RepoID)
	if err != nil {
		return RegisterRepoResult{}, err
	}
	region := strings.TrimSpace(in.HomeRegion)
	if region == "" {
		region = "us-east-1"
	}
	epoch := in.FailoverEpoch
	if epoch == 0 {
		epoch = 1
	}
	refShards := in.RefShards
	if refShards == 0 {
		refShards = 1
	}
	manifestShards := in.ManifestShards
	if manifestShards == 0 {
		manifestShards = 1
	}
	defaultBranch := strings.TrimSpace(in.DefaultBranch)
	if defaultBranch == "" {
		defaultBranch = "refs/heads/main"
	}

	meta := ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     lease.Region(region),
		FailoverEpoch:  epoch,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		WriteState:     lease.ModeActive,
		DefaultBranch:  defaultBranch,
	}

	created := true
	if err := s.ddb.CreateMeta(ctx, meta); err != nil {
		if in.Idempotent && errors.Is(err, ddb.ErrAlreadyExists) {
			created = false
		} else {
			return RegisterRepoResult{}, fmt.Errorf("admin: create repo meta: %w", err)
		}
	}

	return RegisterRepoResult{
		TenantID:       string(rk.TenantID),
		RepoID:         string(rk.RepoID),
		HomeRegion:     string(meta.HomeRegion),
		FailoverEpoch:  uint64(meta.FailoverEpoch),
		RefShards:      meta.RefShards,
		ManifestShards: meta.ManifestShards,
		DefaultBranch:  meta.DefaultBranch,
		Created:        created,
	}, nil
}

// ConfigureWasmHookInput uploads a Tier 1 .wasm module to S3 and binds it in DynamoDB.
type ConfigureWasmHookInput struct {
	TenantID      string
	RepoID        string
	HookID        string
	WasmBytes     []byte
	RefGlob       string
	TimeoutMS     int
	MemoryLimitMB int
	FailMode      ddb.FailMode
	Append        bool
}

// ConfigureWasmHookResult summarizes a deployed Tier 1 Wasm hook.
type ConfigureWasmHookResult struct {
	TenantID   string `json:"tenant_id"`
	RepoID     string `json:"repo_id"`
	HookID     string `json:"hook_id"`
	WasmSHA256 string `json:"wasm_sha256"`
	S3Key      string `json:"s3_key"`
	RefGlob    string `json:"ref_glob"`
	FailMode   string `json:"fail_mode"`
}

// ConfigureWasmHook hashes and uploads WasmBytes to S3 (hooks/{sha256}.wasm)
// and writes the repository HOOKS item in DynamoDB.
func (s *Service) ConfigureWasmHook(ctx context.Context, in ConfigureWasmHookInput) (ConfigureWasmHookResult, error) {
	if s.s3 == nil {
		return ConfigureWasmHookResult{}, errors.New("admin: s3 store is required for wasm hooks")
	}
	if len(in.WasmBytes) == 0 {
		return ConfigureWasmHookResult{}, errors.New("admin: wasm payload is empty")
	}
	rk, err := tenant.NewRepoKey(in.TenantID, in.RepoID)
	if err != nil {
		return ConfigureWasmHookResult{}, err
	}

	sum := sha256.Sum256(in.WasmBytes)
	wasmSHA := hex.EncodeToString(sum[:])

	refGlob := strings.TrimSpace(in.RefGlob)
	if refGlob == "" {
		refGlob = "refs/heads/*"
	}
	timeoutMS := in.TimeoutMS
	if timeoutMS == 0 {
		timeoutMS = 500
	}
	memMB := in.MemoryLimitMB
	if memMB == 0 {
		memMB = 64
	}
	failMode := in.FailMode
	if failMode == "" {
		failMode = ddb.FailModeClosed
	}

	cfg := ddb.HookConfig{
		HookID:        strings.TrimSpace(in.HookID),
		WasmSHA256:    wasmSHA,
		RefGlob:       refGlob,
		TimeoutMS:     timeoutMS,
		MemoryLimitMB: memMB,
		FailMode:      failMode,
	}
	if err := cfg.Validate(); err != nil {
		return ConfigureWasmHookResult{}, err
	}

	if err := s.s3.PutWasmHook(ctx, wasmSHA, in.WasmBytes); err != nil {
		return ConfigureWasmHookResult{}, fmt.Errorf("admin: upload wasm hook: %w", err)
	}

	hooks := []ddb.HookConfig{cfg}
	if in.Append {
		existing, err := s.ddb.GetRepoHooks(ctx, rk)
		if err != nil && !errors.Is(err, ddb.ErrNotFound) {
			return ConfigureWasmHookResult{}, fmt.Errorf("admin: read existing hooks: %w", err)
		}
		hooks = upsertHook(existing, cfg)
	}

	if err := s.ddb.PutRepoHooks(ctx, rk, hooks); err != nil {
		return ConfigureWasmHookResult{}, fmt.Errorf("admin: write repo hooks: %w", err)
	}

	s3Key, err := s3.WasmHookKey(wasmSHA)
	if err != nil {
		return ConfigureWasmHookResult{}, err
	}

	return ConfigureWasmHookResult{
		TenantID:   string(rk.TenantID),
		RepoID:     string(rk.RepoID),
		HookID:     cfg.HookID,
		WasmSHA256: wasmSHA,
		S3Key:      s3Key,
		RefGlob:    cfg.RefGlob,
		FailMode:   string(cfg.FailMode),
	}, nil
}

// ConfigureWebhookInput configures a Tier 2 async webhook with a KMS-encrypted secret.
type ConfigureWebhookInput struct {
	TenantID         string
	RepoID           string
	WebhookID        string
	URL              string
	Secret           string
	KMSKeyID         string
	RefGlob          string
	Enabled          bool
	MaxAttempts      int
	InitialBackoffMS int
	MaxBackoffMS     int
	DLQURL           string
	Append           bool
}

// ConfigureWebhookResult summarizes a persisted Tier 2 webhook configuration.
type ConfigureWebhookResult struct {
	TenantID  string `json:"tenant_id"`
	RepoID    string `json:"repo_id"`
	WebhookID string `json:"webhook_id"`
	URL       string `json:"url"`
	KMSKeyID  string `json:"kms_key_id"`
	RefGlob   string `json:"ref_glob"`
	Enabled   bool   `json:"enabled"`
}

// ConfigureWebhook encrypts Secret via KMS envelope encryption and persists the
// WEBHOOKS item for the repository in DynamoDB.
func (s *Service) ConfigureWebhook(ctx context.Context, in ConfigureWebhookInput) (ConfigureWebhookResult, error) {
	if s.cipher == nil {
		return ConfigureWebhookResult{}, errors.New("admin: kms cipher is required for webhooks")
	}
	rk, err := tenant.NewRepoKey(in.TenantID, in.RepoID)
	if err != nil {
		return ConfigureWebhookResult{}, err
	}
	if in.Secret == "" {
		return ConfigureWebhookResult{}, errors.New("admin: webhook secret is required")
	}

	refGlob := strings.TrimSpace(in.RefGlob)
	if refGlob == "" {
		refGlob = "refs/heads/*"
	}
	maxAttempts := in.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = 3
	}
	initBackoff := in.InitialBackoffMS
	if initBackoff == 0 {
		initBackoff = 100
	}
	maxBackoff := in.MaxBackoffMS
	if maxBackoff == 0 {
		maxBackoff = 2000
	}

	encryptedSecret, err := s.cipher.Encrypt(ctx, rk.TenantID, []byte(in.Secret))
	if err != nil {
		return ConfigureWebhookResult{}, fmt.Errorf("admin: encrypt webhook secret: %w", err)
	}

	cfg := ddb.WebhookConfig{
		WebhookID:       strings.TrimSpace(in.WebhookID),
		URL:             strings.TrimSpace(in.URL),
		EncryptedSecret: encryptedSecret,
		KMSKeyID:        strings.TrimSpace(in.KMSKeyID),
		RefGlob:         refGlob,
		Enabled:         in.Enabled,
		Retry: ddb.WebhookRetryConfig{
			MaxAttempts:      maxAttempts,
			InitialBackoffMS: initBackoff,
			MaxBackoffMS:     maxBackoff,
			DLQURL:           strings.TrimSpace(in.DLQURL),
		},
	}
	if err := cfg.Validate(); err != nil {
		return ConfigureWebhookResult{}, err
	}

	webhooks := []ddb.WebhookConfig{cfg}
	if in.Append {
		existing, err := s.ddb.GetWebhooks(ctx, rk)
		if err != nil && !errors.Is(err, ddb.ErrNotFound) {
			return ConfigureWebhookResult{}, fmt.Errorf("admin: read existing webhooks: %w", err)
		}
		webhooks = upsertWebhook(existing, cfg)
	}

	if err := s.ddb.PutWebhooks(ctx, rk, webhooks); err != nil {
		return ConfigureWebhookResult{}, fmt.Errorf("admin: write webhooks: %w", err)
	}

	return ConfigureWebhookResult{
		TenantID:  string(rk.TenantID),
		RepoID:    string(rk.RepoID),
		WebhookID: cfg.WebhookID,
		URL:       cfg.URL,
		KMSKeyID:  cfg.KMSKeyID,
		RefGlob:   cfg.RefGlob,
		Enabled:   cfg.Enabled,
	}, nil
}

func upsertHook(existing []ddb.HookConfig, next ddb.HookConfig) []ddb.HookConfig {
	out := make([]ddb.HookConfig, 0, len(existing)+1)
	replaced := false
	for _, h := range existing {
		if h.HookID == next.HookID {
			out = append(out, next)
			replaced = true
			continue
		}
		out = append(out, h)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}

func upsertWebhook(existing []ddb.WebhookConfig, next ddb.WebhookConfig) []ddb.WebhookConfig {
	out := make([]ddb.WebhookConfig, 0, len(existing)+1)
	replaced := false
	for _, w := range existing {
		if w.WebhookID == next.WebhookID {
			out = append(out, next)
			replaced = true
			continue
		}
		out = append(out, w)
	}
	if !replaced {
		out = append(out, next)
	}
	return out
}
