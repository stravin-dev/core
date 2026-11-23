package compaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
)

const (
	defaultMinPacksTier0 = 2
	defaultMinPacksTier1 = 1
	defaultMaxBatchSize  = 100
	defaultGCGraceWindow = 24 * time.Hour

	// DefaultMaxTier0PackBytes caps the size of individual packs eligible for
	// Tier-0 streaming merges so large base packs are not repeatedly rewritten.
	DefaultMaxTier0PackBytes int64 = 16 * 1024 * 1024
)

// EngineConfig configures the sharded compaction Engine.
type EngineConfig struct {
	Leases            *LeaseManager
	Manifests         ManifestStore
	ManifestGC        ManifestGCStore
	Refs              RefLister
	Strategies        map[Tier]CompactionStrategy
	MinPacksTier0     int
	MinPacksTier1     int
	MaxTier0PackBytes int64
	MaxBatchSize      int
	GCGraceWindow     time.Duration
	Clock             lease.Clock
	ULIDGen           func(time.Time) (string, error)
	Metrics           *Metrics
	Logger            *slog.Logger
}

// Engine executes sharded compaction jobs: lease acquisition -> S3 artifact
// build/upload -> two-phase DynamoDB manifest swap (Publish -> Supersede).
type Engine struct {
	leases            *LeaseManager
	manifests         ManifestStore
	manifestGC        ManifestGCStore
	refs              RefLister
	strategies        map[Tier]CompactionStrategy
	minPacksTier0     int
	minPacksTier1     int
	maxTier0PackBytes int64
	maxBatchSize      int
	gcGraceWindow     time.Duration
	clock             lease.Clock
	ulidGen           func(time.Time) (string, error)
	metrics           *Metrics
	logger            *slog.Logger
}

// NewEngine constructs a compaction Engine from cfg.
func NewEngine(cfg EngineConfig) (*Engine, error) {
	if cfg.Leases == nil {
		return nil, fmt.Errorf("%w: nil lease manager", ErrInvalidConfig)
	}
	if cfg.Manifests == nil {
		return nil, fmt.Errorf("%w: nil manifest store", ErrInvalidConfig)
	}
	if len(cfg.Strategies) == 0 {
		return nil, fmt.Errorf("%w: at least one compaction strategy required", ErrInvalidConfig)
	}

	minTier0 := cfg.MinPacksTier0
	if minTier0 <= 0 {
		minTier0 = defaultMinPacksTier0
	}
	minTier1 := cfg.MinPacksTier1
	if minTier1 <= 0 {
		minTier1 = defaultMinPacksTier1
	}
	maxTier0Bytes := cfg.MaxTier0PackBytes
	if maxTier0Bytes == 0 {
		maxTier0Bytes = DefaultMaxTier0PackBytes
	}
	maxBatch := cfg.MaxBatchSize
	if maxBatch <= 0 || maxBatch > defaultMaxBatchSize {
		maxBatch = defaultMaxBatchSize
	}
	grace := cfg.GCGraceWindow
	if grace <= 0 {
		grace = defaultGCGraceWindow
	}
	clk := cfg.Clock
	if clk == nil {
		clk = lease.RealClock{}
	}
	ulidGen := cfg.ULIDGen
	if ulidGen == nil {
		ulidGen = ddb.NewULID
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	strats := make(map[Tier]CompactionStrategy, len(cfg.Strategies))
	for tier, s := range cfg.Strategies {
		if s == nil {
			return nil, fmt.Errorf("%w: nil strategy for tier %q", ErrInvalidConfig, tier)
		}
		strats[tier] = s
	}

	return &Engine{
		leases:            cfg.Leases,
		manifests:         cfg.Manifests,
		manifestGC:        cfg.ManifestGC,
		refs:              cfg.Refs,
		strategies:        strats,
		minPacksTier0:     minTier0,
		minPacksTier1:     minTier1,
		maxTier0PackBytes: maxTier0Bytes,
		maxBatchSize:      maxBatch,
		gcGraceWindow:     grace,
		clock:             clk,
		ulidGen:           ulidGen,
		metrics:           cfg.Metrics,
		logger:            logger,
	}, nil
}

// CompactShard claims the shard lease, runs the requested Tier-0 or Tier-1
// strategy, executes the two-phase manifest swap (Publish -> Supersede), and
// releases the shard lease.
func (e *Engine) CompactShard(ctx context.Context, target ShardTarget) (JobResult, error) {
	start := e.clock.Now()
	if target.Tier == "" {
		target.Tier = Tier0
	}
	strategy, ok := e.strategies[target.Tier]
	if !ok {
		return JobResult{}, fmt.Errorf("%w: %q", ErrInvalidTier, target.Tier)
	}
	if err := target.Repo.Validate(); err != nil {
		return JobResult{}, err
	}
	if err := ddb.ValidateManifestShards(target.ManifestShards); err != nil {
		return JobResult{}, err
	}
	hashAlgo, err := pack.HashAlgorithmForFormat(string(target.ObjectFormat))
	if err != nil {
		return JobResult{}, err
	}

	spanCtx, rootSpan := e.metrics.Tracer().Start(ctx, "pack.compact.shard",
		trace.WithAttributes(
			append(
				telemetry.NewSpanAttrs().
					TenantID(string(target.Repo.TenantID)).
					RepoID(string(target.Repo.RepoID)).
					HomeRegion(string(target.OriginRegion)).
					ManifestShard(target.Shard).
					Build(),
				attribute.String("compaction.tier", string(target.Tier)),
			)...,
		),
	)
	defer rootSpan.End()

	held, err := e.leases.Acquire(spanCtx, target.Repo, target.Shard)
	if err != nil {
		resultLabel := "error"
		if errors.Is(err, ErrLeaseContention) {
			resultLabel = "lease_contention"
		}
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, resultLabel, e.clock.Now().Sub(start))
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return JobResult{}, err
	}
	defer func() {
		if relErr := held.Release(context.Background()); relErr != nil {
			e.logger.Warn("failed to release compaction shard lease",
				"tenant_id", string(target.Repo.TenantID),
				"repo_id", string(target.Repo.RepoID),
				"shard", target.Shard,
				"error", relErr,
			)
		}
	}()

	leaseCtx := held.Context()

	activePacks, err := e.manifests.QueryManifestShard(leaseCtx, target.Repo, target.Shard, "", true)
	if err != nil {
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, "error", e.clock.Now().Sub(start))
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return JobResult{}, fmt.Errorf("compaction: query shard %d: %w", target.Shard, err)
	}
	e.metrics.recordUncompactedPacks(spanCtx, target.Shard, len(activePacks))

	if target.Tier == Tier0 && e.maxTier0PackBytes > 0 {
		activePacks = filterTier0EligiblePacks(activePacks, e.maxTier0PackBytes)
	}

	minPacks := e.minPacksTier0
	if target.Tier == Tier1 {
		minPacks = e.minPacksTier1
	}
	if len(activePacks) < minPacks {
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, "skipped", e.clock.Now().Sub(start))
		return JobResult{
			Repo:       target.Repo,
			Shard:      target.Shard,
			Tier:       target.Tier,
			InputPacks: len(activePacks),
			Skipped:    true,
		}, nil
	}

	sort.Slice(activePacks, func(i, j int) bool {
		if activePacks[i].ULID != activePacks[j].ULID {
			return activePacks[i].ULID < activePacks[j].ULID
		}
		return activePacks[i].PackID < activePacks[j].PackID
	})
	if len(activePacks) > e.maxBatchSize {
		activePacks = activePacks[:e.maxBatchSize]
	}

	var refs []ddb.RefState
	if target.Tier == Tier1 && e.refs != nil && target.RefShards > 0 {
		refs, err = e.refs.ListRefs(leaseCtx, target.Repo, target.RefShards)
		if err != nil {
			e.metrics.recordCompactionCycle(spanCtx, target.Tier, "error", e.clock.Now().Sub(start))
			rootSpan.RecordError(err)
			rootSpan.SetStatus(codes.Error, err.Error())
			return JobResult{}, fmt.Errorf("compaction: list refs for tier1: %w", err)
		}
	}

	originRegion := target.OriginRegion
	if originRegion == "" {
		originRegion = activePacks[0].OriginRegion
	}

	stratRes, err := strategy.Compact(leaseCtx, StrategyRequest{
		Repo:          target.Repo,
		Shard:         target.Shard,
		Epoch:         target.Epoch,
		OriginRegion:  originRegion,
		HashAlgorithm: hashAlgo,
		Inputs:        activePacks,
		Refs:          refs,
	})
	if err != nil {
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, "error", e.clock.Now().Sub(start))
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return JobResult{}, err
	}

	if err := leaseCtx.Err(); err != nil {
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, "lease_lost", e.clock.Now().Sub(start))
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return JobResult{}, fmt.Errorf("%w: %w", ErrLeaseLost, err)
	}

	// Phase 1 (Publish): Insert the new consolidated ACTIVE pack entry into MS#{m}.
	now := e.clock.Now().UTC()
	newULID, err := e.ulidGen(now)
	if err != nil {
		return JobResult{}, fmt.Errorf("compaction: generate ulid: %w", err)
	}
	newManifest := ddb.PackManifest{
		Repo:         target.Repo,
		ULID:         newULID,
		PackID:       stratRes.OutputPackID,
		SizeBytes:    stratRes.OutputSize,
		OriginRegion: originRegion,
		Status:       ddb.PackStatusActive,
		CreatedAt:    now,
	}

	if err := e.publishPhase(leaseCtx, target, newManifest); err != nil {
		e.metrics.recordCompactionCycle(spanCtx, target.Tier, "error", e.clock.Now().Sub(start))
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return JobResult{}, err
	}

	// Filter out any input pack whose PackID matches the newly published output
	// (e.g. an idempotent single-pack Tier-1 repack producing identical bytes).
	toSupersede := make([]ddb.PackManifest, 0, len(activePacks))
	var inputBytes int64
	for _, p := range activePacks {
		inputBytes += p.SizeBytes
		if p.PackID == stratRes.OutputPackID && p.ULID == newULID {
			continue
		}
		toSupersede = append(toSupersede, p)
	}

	// Phase 2 (Supersede): Mark old source packs COMPACTED with GC grace window.
	if len(toSupersede) > 0 {
		if err := e.supersedePhase(leaseCtx, target, toSupersede, now, newManifest.PackID); err != nil {
			e.metrics.recordCompactionCycle(spanCtx, target.Tier, "error", e.clock.Now().Sub(start))
			rootSpan.RecordError(err)
			rootSpan.SetStatus(codes.Error, err.Error())
			return JobResult{}, err
		}
	}

	reclaimed := inputBytes - stratRes.OutputSize
	if reclaimed < 0 {
		reclaimed = 0
	}
	e.metrics.recordBytesReclaimed(spanCtx, string(target.Tier), reclaimed)
	e.metrics.recordCompactionCycle(spanCtx, target.Tier, "ok", e.clock.Now().Sub(start))

	return JobResult{
		Repo:           target.Repo,
		Shard:          target.Shard,
		Tier:           target.Tier,
		InputPacks:     len(activePacks),
		OutputPackID:   stratRes.OutputPackID,
		OutputSize:     stratRes.OutputSize,
		BytesReclaimed: reclaimed,
		AuxMIDXKey:     stratRes.AuxMIDXKey,
		AuxGraphKey:    stratRes.AuxGraphKey,
		BundleKey:      stratRes.BundleKey,
	}, nil
}

func (e *Engine) publishPhase(ctx context.Context, target ShardTarget, newManifest ddb.PackManifest) error {
	pubCtx, span := e.metrics.Tracer().Start(ctx, "ddb.manifest_publish",
		trace.WithAttributes(
			append(
				telemetry.NewSpanAttrs().
					TenantID(string(target.Repo.TenantID)).
					RepoID(string(target.Repo.RepoID)).
					ManifestShard(target.Shard).
					Build(),
				telemetry.AttrPackID.String(newManifest.PackID),
			)...,
		),
	)
	defer span.End()

	if err := e.manifests.PublishCompactedPacks(pubCtx, target.Repo, target.ManifestShards, []ddb.PackManifest{newManifest}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("compaction: phase 1 publish manifest: %w", err)
	}
	return nil
}

func (e *Engine) supersedePhase(ctx context.Context, target ShardTarget, oldPacks []ddb.PackManifest, compactedAt time.Time, replacementPackID string) error {
	supCtx, span := e.metrics.Tracer().Start(ctx, "ddb.manifest_supersede",
		trace.WithAttributes(
			append(
				telemetry.NewSpanAttrs().
					TenantID(string(target.Repo.TenantID)).
					RepoID(string(target.Repo.RepoID)).
					ManifestShard(target.Shard).
					Build(),
				attribute.Int("compaction.superseded_count", len(oldPacks)),
			)...,
		),
	)
	defer span.End()

	if err := e.manifests.SupersedePacks(supCtx, target.Repo, target.ManifestShards, oldPacks); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("compaction: phase 2 supersede manifests: %w", err)
	}

	if e.manifestGC != nil {
		gcAfter := compactedAt.Add(e.gcGraceWindow)
		var err error
		if tracker, ok := e.manifestGC.(ReplacementTracker); ok {
			err = tracker.MarkGCEligibleWithReplacement(supCtx, target.Repo, target.ManifestShards, oldPacks, compactedAt, gcAfter, replacementPackID)
		} else {
			err = e.manifestGC.MarkGCEligible(supCtx, target.Repo, target.ManifestShards, oldPacks, compactedAt, gcAfter)
		}
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("compaction: mark gc eligibility: %w", err)
		}
	}
	return nil
}

func filterTier0EligiblePacks(packs []ddb.PackManifest, maxBytes int64) []ddb.PackManifest {
	eligible := make([]ddb.PackManifest, 0, len(packs))
	for _, p := range packs {
		if p.SizeBytes <= maxBytes {
			eligible = append(eligible, p)
		}
	}
	return eligible
}
