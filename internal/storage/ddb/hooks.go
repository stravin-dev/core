package ddb

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/tenant"
)

// FailMode determines whether a Tier 1 Wasm hook blocks pushes on runtime errors/timeouts.
type FailMode string

const (
	FailModeClosed FailMode = "closed"
	FailModeOpen   FailMode = "open"
)

// HookConfig configures a single synchronous Tier 1 Wasm hook in a repo or org pipeline.
type HookConfig struct {
	HookID        string
	WasmSHA256    string
	RefGlob       string
	TimeoutMS     int
	MemoryLimitMB int
	FailMode      FailMode
}

// Validate checks HookConfig fields.
func (h HookConfig) Validate() error {
	if err := validateNonEmpty("hook_id", h.HookID); err != nil {
		return err
	}
	if err := validateNonEmpty("wasm_sha256", h.WasmSHA256); err != nil {
		return err
	}
	if err := validateNonEmpty("ref_glob", h.RefGlob); err != nil {
		return err
	}
	if h.TimeoutMS <= 0 {
		return fmt.Errorf("%w: timeout_ms must be positive", ErrInvalidInput)
	}
	if h.MemoryLimitMB <= 0 {
		return fmt.Errorf("%w: memory_limit_mb must be positive", ErrInvalidInput)
	}
	switch h.FailMode {
	case FailModeClosed, FailModeOpen:
	default:
		return fmt.Errorf("%w: invalid fail_mode %q", ErrInvalidInput, h.FailMode)
	}
	return nil
}

// GetRepoHooks retrieves the ordered Tier 1 Wasm hook list for repo.
func (r *Repository) GetRepoHooks(ctx context.Context, repo tenant.RepoKey) ([]HookConfig, error) {
	key, err := RepoHooksKey(repo)
	if err != nil {
		return nil, err
	}
	return r.getHooksByKey(ctx, key)
}

// PutRepoHooks persists the ordered Tier 1 Wasm hook list for repo.
func (r *Repository) PutRepoHooks(ctx context.Context, repo tenant.RepoKey, hooks []HookConfig) error {
	key, err := RepoHooksKey(repo)
	if err != nil {
		return err
	}
	return r.putHooksByKey(ctx, key, hooks)
}

// GetOrgHooks retrieves the ordered org-level inherited Tier 1 Wasm hook list.
func (r *Repository) GetOrgHooks(ctx context.Context, tenantID tenant.TenantID, orgID string) ([]HookConfig, error) {
	key, err := OrgHooksKey(tenantID, orgID)
	if err != nil {
		return nil, err
	}
	return r.getHooksByKey(ctx, key)
}

// PutOrgHooks persists the ordered org-level inherited Tier 1 Wasm hook list.
func (r *Repository) PutOrgHooks(ctx context.Context, tenantID tenant.TenantID, orgID string, hooks []HookConfig) error {
	key, err := OrgHooksKey(tenantID, orgID)
	if err != nil {
		return err
	}
	return r.putHooksByKey(ctx, key, hooks)
}

// GetEffectiveHooks returns org-inherited hooks (if orgID is non-empty) followed by repo hooks.
// Missing org or repo hook items are treated as empty lists rather than errors.
func (r *Repository) GetEffectiveHooks(ctx context.Context, repo tenant.RepoKey, orgID string) ([]HookConfig, error) {
	if err := repo.Validate(); err != nil {
		return nil, err
	}
	var merged []HookConfig
	if orgID != "" {
		orgHooks, err := r.GetOrgHooks(ctx, repo.TenantID, orgID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		merged = append(merged, orgHooks...)
	}
	repoHooks, err := r.GetRepoHooks(ctx, repo)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, err
	}
	merged = append(merged, repoHooks...)
	return merged, nil
}

func (r *Repository) getHooksByKey(ctx context.Context, key Key) ([]HookConfig, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, ErrNotFound
	}
	return unmarshalHooks(out.Item)
}

func (r *Repository) putHooksByKey(ctx context.Context, key Key, hooks []HookConfig) error {
	list := make([]types.AttributeValue, 0, len(hooks))
	for _, h := range hooks {
		if err := h.Validate(); err != nil {
			return err
		}
		list = append(list, &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"hook_id":         attrS(h.HookID),
				"wasm_sha256":     attrS(h.WasmSHA256),
				"ref_glob":        attrS(h.RefGlob),
				"timeout_ms":      attrNInt(h.TimeoutMS),
				"memory_limit_mb": attrNInt(h.MemoryLimitMB),
				"fail_mode":       attrS(string(h.FailMode)),
			},
		})
	}
	_, err := r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item: map[string]types.AttributeValue{
			AttrPK:  attrS(key.PK),
			AttrSK:  attrS(key.SK),
			"hooks": &types.AttributeValueMemberL{Value: list},
		},
	})
	return err
}

func unmarshalHooks(item map[string]types.AttributeValue) ([]HookConfig, error) {
	av, ok := item["hooks"]
	if !ok {
		return nil, nil
	}
	l, ok := av.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("ddb: hooks attribute is not a list")
	}
	hooks := make([]HookConfig, 0, len(l.Value))
	for _, elem := range l.Value {
		m, ok := elem.(*types.AttributeValueMemberM)
		if !ok {
			return nil, fmt.Errorf("ddb: hook entry is not a map")
		}
		timeoutMS, err := getInt(m.Value, "timeout_ms")
		if err != nil {
			return nil, err
		}
		memMB, err := getInt(m.Value, "memory_limit_mb")
		if err != nil {
			return nil, err
		}
		hooks = append(hooks, HookConfig{
			HookID:        getString(m.Value, "hook_id"),
			WasmSHA256:    getString(m.Value, "wasm_sha256"),
			RefGlob:       getString(m.Value, "ref_glob"),
			TimeoutMS:     timeoutMS,
			MemoryLimitMB: memMB,
			FailMode:      FailMode(getString(m.Value, "fail_mode")),
		})
	}
	return hooks, nil
}
