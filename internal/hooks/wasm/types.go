package wasm

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"runtime"
	"time"
)

// Default resource guardrails for Tier 1 synchronous Wasm hooks.
const (
	DefaultModuleCacheSize  = 64
	DefaultTimeoutMS        = 50
	MaxTimeoutMS            = 250
	DefaultMemoryLimitMB    = 16
	MaxMemoryLimitMB        = 64
	DefaultMaxCommitsWalked = 1000
	DefaultMaxBlobBytesRead = 10 << 20 // 10 MiB
	DefaultMaxHostCalls     = 1024
	DefaultDiffTreeLimit    = 1000
	MaxDiffTreeLimit        = 10_000
	wasmPagesPerMB          = 16 // 1 Wasm linear memory page = 64 KiB
)

// Sentinel errors returned by the Tier 1 Wasm hook engine.
var (
	ErrTimeout             = errors.New("hooks/wasm: execution timed out")
	ErrBudgetExceeded      = errors.New("hooks/wasm: host-call budget exceeded")
	ErrMemoryLimitExceeded = errors.New("hooks/wasm: memory limit exceeded")
	ErrChecksumMismatch    = errors.New("hooks/wasm: wasm sha256 mismatch")
	ErrInvalidConfig       = errors.New("hooks/wasm: invalid configuration")
	ErrModuleTrap          = errors.New("hooks/wasm: module trapped")
	ErrMissingArtifact     = errors.New("hooks/wasm: wasm artifact not found")
)

// WasmLoader fetches content-addressed .wasm hook binaries by lowercase SHA-256 hex digest.
// Implementations (e.g., S3) are wired outside this package.
type WasmLoader interface {
	LoadWasm(ctx context.Context, sha256Hex string) ([]byte, error)
}

// HookConfigStore lists ordered Tier 1 Wasm hooks configured for a repository/organization.
// Implementations (e.g., DynamoDB) are wired outside this package.
type HookConfigStore interface {
	ListHooks(ctx context.Context, tenantID, repoID, orgID string) ([]HookConfig, error)
}

// ObjectReader is the narrow read-only Git object inspection contract consumed by HookHostABI.
// It mirrors internal/pack.ObjectReader without coupling internal/hooks/wasm to internal/pack.
type ObjectReader interface {
	ListNewCommits(oldOID, newOID [20]byte, limit uint32) ([]CommitMeta, error)
	DiffTree(oldTreeOID, newTreeOID [20]byte, limit uint32) ([]TreeChange, error)
	ReadBlobChunk(blobOID [20]byte, offset uint64, maxBytes uint32) ([]byte, error)
}

// ContextDiffTreeReader is an optional extension of ObjectReader that accepts a
// context.Context so host-side tree traversal aborts promptly on hook timeout.
type ContextDiffTreeReader interface {
	DiffTreeContext(ctx context.Context, oldTreeOID, newTreeOID [20]byte, limit uint32) ([]TreeChange, error)
}

// FailMode controls whether a hook runtime failure blocks or warns on a push.
type FailMode string

const (
	FailModeClosed FailMode = "closed"
	FailModeOpen   FailMode = "open"
)

// HookConfig describes a single synchronous Tier 1 Wasm hook in a push pipeline.
type HookConfig struct {
	HookID        string
	WasmSHA256    string
	RefGlob       string
	TimeoutMS     int
	MemoryLimitMB int
	FailMode      FailMode
}

// RefUpdate represents a single ref transition in an incoming Git push.
type RefUpdate struct {
	Ref    string
	OldOID [20]byte
	NewOID [20]byte
}

// PushContext describes the incoming Git push evaluated by Tier 1 hooks.
type PushContext struct {
	TenantID string
	RepoID   string
	OrgID    string
	ActorID  string
	Updates  []RefUpdate
}

// Signature represents a Git commit author or committer identity.
type Signature struct {
	Name  string    `json:"name"`
	Email string    `json:"email"`
	When  time.Time `json:"when"`
	Raw   string    `json:"raw,omitempty"`
}

// CommitMeta holds header fields and message parsed from a Git commit object.
type CommitMeta struct {
	OID       [20]byte
	TreeOID   [20]byte
	Parents   [][20]byte
	Author    Signature
	Committer Signature
	Message   string
}

// ChangeKind classifies a file-level difference between two Git trees.
type ChangeKind string

const (
	ChangeAdd    ChangeKind = "add"
	ChangeModify ChangeKind = "modify"
	ChangeDelete ChangeKind = "delete"
)

// TreeChange describes a single changed path between two Git trees.
type TreeChange struct {
	Path    string
	Kind    ChangeKind
	OldMode uint32
	NewMode uint32
	OldOID  [20]byte
	NewOID  [20]byte
	OldSize int64
	NewSize int64
}

// VerdictDecision is the allow/reject decision emitted by a hook.
type VerdictDecision string

const (
	VerdictAllow  VerdictDecision = "ALLOW"
	VerdictReject VerdictDecision = "REJECT"
)

// RefVerdict records an optional per-ref decision from a hook.
type RefVerdict struct {
	Ref      string          `json:"ref"`
	Decision VerdictDecision `json:"decision"`
	Message  string          `json:"message,omitempty"`
}

// HookVerdict is the structured decision emitted via HookHostABI.SetVerdict.
type HookVerdict struct {
	Decision    VerdictDecision `json:"decision"`
	Message     string          `json:"message,omitempty"`
	RefVerdicts []RefVerdict    `json:"ref_verdicts,omitempty"`
}

// HookOutcome captures the result of running a single hook in the pipeline.
type HookOutcome struct {
	HookID      string
	Decision    VerdictDecision
	Message     string
	RefVerdicts []RefVerdict
	Warning     string
	Duration    time.Duration
	CacheHit    bool
}

// PipelineResult is the aggregate outcome of evaluating all matching Tier 1 hooks for a push.
type PipelineResult struct {
	Allowed      bool
	RejectedBy   string
	Message      string
	RefVerdicts  []RefVerdict
	Warnings     []string
	HookOutcomes []HookOutcome
}

// EngineConfig configures resource guardrails, caching, and bulkhead concurrency for Engine.
type EngineConfig struct {
	ModuleCacheSize      int
	MaxConcurrency       int
	DefaultTimeoutMS     int
	MaxTimeoutMS         int
	DefaultMemoryLimitMB int
	MaxMemoryLimitMB     int
	MaxCommitsWalked     uint32
	MaxBlobBytesRead     uint64
	MaxHostCalls         uint32
	Logger               *slog.Logger
}

func (c EngineConfig) withDefaults() EngineConfig {
	if c.ModuleCacheSize <= 0 {
		c.ModuleCacheSize = DefaultModuleCacheSize
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = max(1, runtime.NumCPU()/4)
	}
	if c.DefaultTimeoutMS <= 0 {
		c.DefaultTimeoutMS = DefaultTimeoutMS
	}
	if c.MaxTimeoutMS <= 0 {
		c.MaxTimeoutMS = MaxTimeoutMS
	}
	if c.DefaultTimeoutMS > c.MaxTimeoutMS {
		c.DefaultTimeoutMS = c.MaxTimeoutMS
	}
	if c.DefaultMemoryLimitMB <= 0 {
		c.DefaultMemoryLimitMB = DefaultMemoryLimitMB
	}
	if c.MaxMemoryLimitMB <= 0 {
		c.MaxMemoryLimitMB = MaxMemoryLimitMB
	}
	if c.DefaultMemoryLimitMB > c.MaxMemoryLimitMB {
		c.DefaultMemoryLimitMB = c.MaxMemoryLimitMB
	}
	if c.MaxCommitsWalked == 0 {
		c.MaxCommitsWalked = DefaultMaxCommitsWalked
	}
	if c.MaxBlobBytesRead == 0 {
		c.MaxBlobBytesRead = DefaultMaxBlobBytesRead
	}
	if c.MaxHostCalls == 0 {
		c.MaxHostCalls = DefaultMaxHostCalls
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

func encodeOIDHex(oid [20]byte) string {
	return hex.EncodeToString(oid[:])
}
