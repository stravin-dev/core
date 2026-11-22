// Package ddb implements Stravin Core's single-table adaptive key-sharded
// DynamoDB repository, atomic Ref-CAS transactions, Pack Manifest queries,
// compaction/regional leases, and Tier 1/Tier 2 hook/webhook persistence.
package ddb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

// API is the narrow AWS SDK v2 DynamoDB client interface used by Repository.
// *dynamodb.Client satisfies this interface directly.
type API interface {
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	TransactWriteItems(ctx context.Context, params *dynamodb.TransactWriteItemsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

// MetaRepository provides access to repository META records.
type MetaRepository interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (RepoMeta, error)
	CreateMeta(ctx context.Context, meta RepoMeta) error
	UpdateMeta(ctx context.Context, meta RepoMeta, expectedEpoch lease.Epoch) error
}

// RefRepository provides atomic Ref-CAS commits and ref/history queries.
type RefRepository interface {
	CommitPush(ctx context.Context, in CommitPushInput) (CommitPushResult, error)
	GetRef(ctx context.Context, repo tenant.RepoKey, refName string, refShards int) (RefState, error)
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]RefState, error)
	ListRefHistory(ctx context.Context, repo tenant.RepoKey, refName string, refShards int, afterULID string, limit int32) ([]RefHistory, error)
}

// ManifestRepository provides Pack Manifest reads and two-phase compaction swaps.
type ManifestRepository interface {
	PutManifest(ctx context.Context, m PackManifest, manifestShards int) error
	QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]PackManifest, error)
	ListManifestsSince(ctx context.Context, repo tenant.RepoKey, manifestShards int, afterULID string, activeOnly bool) ([]PackManifest, error)
	PublishCompactedPacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []PackManifest) error
	SupersedePacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []PackManifest) error
}

// LeaseRepository manages compaction shard leases and regional write leases.
type LeaseRepository interface {
	lease.Store
	AcquireShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ShardLease, error)
	RenewShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ShardLease, error)
	ReleaseShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string) error
	GetShardLease(ctx context.Context, repo tenant.RepoKey, shard int) (ShardLease, error)
	PutRegionalWriteLease(ctx context.Context, l RegionalWriteLease) error
	AcquireRegionalWriteLease(ctx context.Context, region lease.Region, holderID string, epoch lease.Epoch, mode lease.Mode, ttl time.Duration) (RegionalWriteLease, error)
	GetRegionalWriteLease(ctx context.Context, region lease.Region) (RegionalWriteLease, error)
}

// HookRepository manages Tier 1 Wasm hook configurations with optional org inheritance.
type HookRepository interface {
	GetRepoHooks(ctx context.Context, repo tenant.RepoKey) ([]HookConfig, error)
	PutRepoHooks(ctx context.Context, repo tenant.RepoKey, hooks []HookConfig) error
	GetOrgHooks(ctx context.Context, tenantID tenant.TenantID, orgID string) ([]HookConfig, error)
	PutOrgHooks(ctx context.Context, tenantID tenant.TenantID, orgID string, hooks []HookConfig) error
	GetEffectiveHooks(ctx context.Context, repo tenant.RepoKey, orgID string) ([]HookConfig, error)
}

// WebhookRepository manages Tier 2 asynchronous webhook target configurations.
type WebhookRepository interface {
	GetWebhooks(ctx context.Context, repo tenant.RepoKey) ([]WebhookConfig, error)
	PutWebhooks(ctx context.Context, repo tenant.RepoKey, webhooks []WebhookConfig) error
}

// CheckpointRepository manages DynamoDB Streams shard checkpoints.
type CheckpointRepository interface {
	GetStreamCheckpoint(ctx context.Context, shardID string) (StreamCheckpoint, error)
	PutStreamCheckpoint(ctx context.Context, cp StreamCheckpoint) error
}

// Option configures a Repository.
type Option func(*Repository)

// WithClock overrides the wall-clock source for deterministic timestamps and TTLs.
func WithClock(clock lease.Clock) Option {
	return func(r *Repository) {
		if clock != nil {
			r.clock = clock
		}
	}
}

// WithULIDGenerator overrides ULID generation for deterministic tests.
func WithULIDGenerator(fn func(time.Time) (string, error)) Option {
	return func(r *Repository) {
		if fn != nil {
			r.ulidGen = fn
		}
	}
}

// Repository is the DynamoDB single-table data-access adapter for Stravin Core.
type Repository struct {
	client    API
	tableName string
	clock     lease.Clock
	ulidGen   func(time.Time) (string, error)
}

// NewRepository constructs a Repository backed by an AWS SDK v2 DynamoDB API.
func NewRepository(client API, tableName string, opts ...Option) (*Repository, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil dynamodb client", ErrInvalidInput)
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, fmt.Errorf("%w: empty table name", ErrInvalidInput)
	}
	r := &Repository{
		client:    client,
		tableName: tableName,
		clock:     lease.RealClock{},
		ulidGen:   NewULID,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r, nil
}

// TableName returns the configured DynamoDB table name.
func (r *Repository) TableName() string {
	return r.tableName
}

var (
	_ API                  = (*dynamodb.Client)(nil)
	_ MetaRepository       = (*Repository)(nil)
	_ RefRepository        = (*Repository)(nil)
	_ ManifestRepository   = (*Repository)(nil)
	_ LeaseRepository      = (*Repository)(nil)
	_ lease.Store          = (*Repository)(nil)
	_ HookRepository       = (*Repository)(nil)
	_ WebhookRepository    = (*Repository)(nil)
	_ CheckpointRepository = (*Repository)(nil)
)
