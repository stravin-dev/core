// Package compaction implements Stravin Core's sharded background packfile
// compaction engine (Tier-0 streaming merge and Tier-1 deep repack with .midx
// and commit-graph generation), two-phase DynamoDB manifest swap, fair-share
// tenant scheduling, and delayed garbage collection for COMPACTED and orphaned
// packfiles.
package compaction

import (
	"context"
	"errors"
	"time"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

// Sentinel errors returned by the compaction and GC engine.
var (
	ErrLeaseContention  = errors.New("compaction: shard lease held by another worker")
	ErrLeaseLost        = errors.New("compaction: shard lease ownership lost")
	ErrInvalidConfig    = errors.New("compaction: invalid configuration")
	ErrInvalidTier      = errors.New("compaction: unsupported compaction tier")
	ErrNothingToCompact = errors.New("compaction: insufficient active packs to compact")
)

// Tier identifies the LSM compaction tier.
type Tier string

const (
	// Tier0 performs fast pure-Go streaming object merge without delta recompression.
	Tier0 Tier = "tier0"
	// Tier1 performs deep sandboxed git repack with .midx and commit-graph generation.
	Tier1 Tier = "tier1"
)

// ShardTarget identifies a single repository manifest shard queued for compaction.
type ShardTarget struct {
	Repo           tenant.RepoKey
	Shard          int
	ManifestShards int
	RefShards      int
	OriginRegion   lease.Region
	Tier           Tier
	Epoch          string
	ObjectFormat   ddb.ObjectFormat
}

// JobResult summarizes the outcome of a single shard compaction run.
type JobResult struct {
	Repo           tenant.RepoKey
	Shard          int
	Tier           Tier
	InputPacks     int
	OutputPackID   string
	OutputSize     int64
	BytesReclaimed int64
	AuxMIDXKey     string
	AuxGraphKey    string
	BundleKey      string
	Skipped        bool
}

// GCResult summarizes the outcome of a single repository delayed-GC sweep.
type GCResult struct {
	Repo               tenant.RepoKey
	CompactedReaped    int
	OrphanedReaped     int
	BytesReclaimed     int64
	SkippedActive      int
	SkippedGrace       int
	SkippedReplication int
}

// LeaseStore is the narrow DynamoDB compaction-lease contract.
type LeaseStore interface {
	AcquireShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ddb.ShardLease, error)
	RenewShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ddb.ShardLease, error)
	ReleaseShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string) error
}

// ManifestStore is the narrow DynamoDB Pack Manifest contract.
type ManifestStore interface {
	QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
	ListManifestsSince(ctx context.Context, repo tenant.RepoKey, manifestShards int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
	PublishCompactedPacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []ddb.PackManifest) error
	SupersedePacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []ddb.PackManifest) error
}

// ManifestGCStore tracks GC eligibility timestamps (compacted_at / gc_after)
// and deletes reaped COMPACTED manifest entries.
type ManifestGCStore interface {
	MarkGCEligible(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []ddb.PackManifest, compactedAt, gcAfter time.Time) error
	GetGCEligibility(ctx context.Context, repo tenant.RepoKey, manifestShards int, pack ddb.PackManifest) (gcAfter time.Time, ok bool, err error)
	DeleteManifest(ctx context.Context, repo tenant.RepoKey, manifestShards int, pack ddb.PackManifest) error
}

// ReplacementTracker optionally records and retrieves the replacement compacted
// pack ID on superseded COMPACTED manifest entries so delayed GC can verify
// S3 CRR replication of the exact replacement pack before deletion.
type ReplacementTracker interface {
	MarkGCEligibleWithReplacement(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []ddb.PackManifest, compactedAt, gcAfter time.Time, replacementPackID string) error
	GetReplacementPackID(ctx context.Context, repo tenant.RepoKey, manifestShards int, pack ddb.PackManifest) (packID string, ok bool, err error)
}

// RefLister provides repository refs so Tier-1 commit-graph generation can walk
// reachable commits.
type RefLister interface {
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error)
}

// PackObjectStore is the narrow S3 object-plane contract required by compaction and GC.
type PackObjectStore interface {
	PutPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string, packData, idxData []byte) error
	GetPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
	GetIndex(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
	PutAuxMIDX(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error
	PutAuxGraph(ctx context.Context, repo tenant.RepoKey, hashPrefixOrSHA, manifestEpoch string, data []byte) error
	DeleteObject(ctx context.Context, key string) error
	ListRepoPacks(ctx context.Context, repo tenant.RepoKey) ([]s3.PackObjectInfo, error)
}

// BundlePutter optionally uploads a repository Git bundle to S3 after Tier-1
// compaction so stravin-node can advertise it over Protocol v2 bundle-uri.
type BundlePutter interface {
	PutBundle(ctx context.Context, repo tenant.RepoKey, data []byte) error
}

// StrategyRequest passes inputs to a CompactionStrategy implementation.
type StrategyRequest struct {
	Repo          tenant.RepoKey
	Shard         int
	Epoch         string
	OriginRegion  lease.Region
	HashAlgorithm pack.HashAlgorithm
	Inputs        []ddb.PackManifest
	Refs          []ddb.RefState
}

// StrategyResult reports the S3 artifacts produced by a CompactionStrategy.
type StrategyResult struct {
	OutputPackID string
	OutputSize   int64
	AuxMIDXKey   string
	AuxGraphKey  string
	BundleKey    string
}

// CompactionStrategy is the GoF Strategy interface implemented by Tier-0
// streaming pack merge and Tier-1 deep repack.
type CompactionStrategy interface {
	Tier() Tier
	Compact(ctx context.Context, req StrategyRequest) (StrategyResult, error)
}

var (
	_ LeaseStore      = (*ddb.Repository)(nil)
	_ ManifestStore   = (*ddb.Repository)(nil)
	_ RefLister       = (*ddb.Repository)(nil)
	_ PackObjectStore = (*s3.Store)(nil)
)
