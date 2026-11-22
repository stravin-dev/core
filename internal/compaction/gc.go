package compaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

// RecoveryRefPrefix identifies non-destructive recovery refs published by
// stravin-ctrl whose backing packs must never be reaped by delayed GC.
const RecoveryRefPrefix = "refs/stravin-recovery/"

// GCSweeperConfig configures delayed garbage collection for COMPACTED and
// orphaned S3 packfiles.
type GCSweeperConfig struct {
	Manifests         ManifestStore
	ManifestGC        ManifestGCStore
	Objects           PackObjectStore
	Refs              RefLister
	RefShards         int
	ReplicationGate   ReplicationGate
	GraceWindow       time.Duration
	OrphanGraceWindow time.Duration
	Clock             lease.Clock
	Metrics           *Metrics
	Logger            *slog.Logger
}

// GCSweeper reaps COMPACTED packs whose grace window has elapsed and orphaned
// S3 packs (such as packs uploaded before a Tier 1 Wasm hook rejected the push)
// that are not referenced by any ACTIVE manifest entry past the orphan grace window.
type GCSweeper struct {
	manifests         ManifestStore
	manifestGC        ManifestGCStore
	objects           PackObjectStore
	refs              RefLister
	refShards         int
	replicationGate   ReplicationGate
	graceWindow       time.Duration
	orphanGraceWindow time.Duration
	clock             lease.Clock
	metrics           *Metrics
	logger            *slog.Logger
}

// NewGCSweeper constructs a delayed GC sweeper with safe defaults.
func NewGCSweeper(cfg GCSweeperConfig) (*GCSweeper, error) {
	if cfg.Manifests == nil {
		return nil, fmt.Errorf("%w: nil manifest store", ErrInvalidConfig)
	}
	if cfg.Objects == nil {
		return nil, fmt.Errorf("%w: nil object store", ErrInvalidConfig)
	}
	grace := cfg.GraceWindow
	if grace <= 0 {
		grace = defaultGCGraceWindow
	}
	orphanGrace := cfg.OrphanGraceWindow
	if orphanGrace <= 0 {
		orphanGrace = grace
	}
	clk := cfg.Clock
	if clk == nil {
		clk = lease.RealClock{}
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &GCSweeper{
		manifests:         cfg.Manifests,
		manifestGC:        cfg.ManifestGC,
		objects:           cfg.Objects,
		refs:              cfg.Refs,
		refShards:         cfg.RefShards,
		replicationGate:   cfg.ReplicationGate,
		graceWindow:       grace,
		orphanGraceWindow: orphanGrace,
		clock:             clk,
		metrics:           cfg.Metrics,
		logger:            logger,
	}, nil
}

// SweepRepo runs one delayed-GC pass over a single repository:
//  1. Reap COMPACTED manifest entries past their gc_after grace timestamp.
//  2. Reap orphaned S3 .pack/.idx objects unreferenced by any ACTIVE manifest
//     once older than the orphan grace window.
func (g *GCSweeper) SweepRepo(ctx context.Context, repo tenant.RepoKey, manifestShards int) (GCResult, error) {
	if err := repo.Validate(); err != nil {
		return GCResult{}, err
	}
	if err := ddb.ValidateManifestShards(manifestShards); err != nil {
		return GCResult{}, err
	}

	spanCtx, span := g.metrics.Tracer().Start(ctx, "pack.gc.sweep",
		trace.WithAttributes(
			telemetry.NewSpanAttrs().
				TenantID(string(repo.TenantID)).
				RepoID(string(repo.RepoID)).
				Build()...,
		),
	)
	defer span.End()

	allManifests, err := g.manifests.ListManifestsSince(spanCtx, repo, manifestShards, "", false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return GCResult{}, fmt.Errorf("compaction gc: list manifests: %w", err)
	}

	activePackIDs := make(map[string]struct{})
	knownManifestPacks := make(map[string]struct{}, len(allManifests))
	var compacted []ddb.PackManifest

	for _, m := range allManifests {
		knownManifestPacks[m.PackID] = struct{}{}
		switch m.Status {
		case ddb.PackStatusActive:
			activePackIDs[m.PackID] = struct{}{}
		case ddb.PackStatusCompacted:
			compacted = append(compacted, m)
		}
	}

	// Protect any pack referenced by an active recovery ref (refs/stravin-recovery/*)
	// from both COMPACTED and orphaned-pack deletion.
	if g.refs != nil && g.refShards > 0 {
		refs, err := g.refs.ListRefs(spanCtx, repo, g.refShards)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return GCResult{}, fmt.Errorf("compaction gc: list recovery refs: %w", err)
		}
		for _, ref := range refs {
			if strings.HasPrefix(ref.RefName, RecoveryRefPrefix) && ref.PackID != "" {
				activePackIDs[ref.PackID] = struct{}{}
			}
		}
	}

	now := g.clock.Now().UTC()
	res := GCResult{Repo: repo}

	activePackList := make([]string, 0, len(activePackIDs))
	for id := range activePackIDs {
		activePackList = append(activePackList, id)
	}
	sort.Strings(activePackList)

	// Per-sweep cache of replacement packs already confirmed COMPLETED to avoid
	// redundant HeadObject calls when many COMPACTED entries share one replacement.
	verifiedReplacements := make(map[string]bool)

	// Pass 1: Reap COMPACTED packs past grace window.
	for _, entry := range compacted {
		if err := spanCtx.Err(); err != nil {
			return res, err
		}
		if _, isActive := activePackIDs[entry.PackID]; isActive {
			res.SkippedActive++
			continue
		}

		gcAfter, err := g.resolveGCAfter(spanCtx, repo, manifestShards, entry)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return res, fmt.Errorf("compaction gc: resolve gc_after for %s: %w", entry.PackID, err)
		}
		if now.Before(gcAfter) {
			res.SkippedGrace++
			continue
		}

		if g.replicationGate != nil {
			replIDs, err := g.resolveReplacementPackIDs(spanCtx, repo, manifestShards, entry, activePackList)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return res, fmt.Errorf("compaction gc: resolve replacement pack for %s: %w", entry.PackID, err)
			}
			if !g.checkReplicationGate(spanCtx, repo, replIDs, verifiedReplacements) {
				res.SkippedReplication++
				continue
			}
		}

		if err := g.deletePackAndIndex(spanCtx, repo, entry.PackID); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return res, err
		}
		if g.manifestGC != nil {
			if err := g.manifestGC.DeleteManifest(spanCtx, repo, manifestShards, entry); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return res, err
			}
		}

		res.CompactedReaped++
		res.BytesReclaimed += entry.SizeBytes
	}

	// Pass 2: Reap unreferenced orphaned S3 packs (e.g. rejected Tier 1 Wasm hook pushes).
	s3Packs, err := g.objects.ListRepoPacks(spanCtx, repo)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return res, fmt.Errorf("compaction gc: list s3 packs: %w", err)
	}

	for _, obj := range s3Packs {
		if err := spanCtx.Err(); err != nil {
			return res, err
		}
		if _, isActive := activePackIDs[obj.PackSHA256]; isActive {
			res.SkippedActive++
			continue
		}
		if _, tracked := knownManifestPacks[obj.PackSHA256]; tracked {
			// Already evaluated in Pass 1 (either reaped or still inside grace window).
			continue
		}
		if obj.LastModified.IsZero() || now.Before(obj.LastModified.UTC().Add(g.orphanGraceWindow)) {
			res.SkippedGrace++
			continue
		}
		if g.replicationGate != nil && g.replicationGate.IsBreakerOpen() {
			g.metrics.recordGCCRRBreakerOpen(spanCtx, true)
			g.metrics.recordGCCRRBlocked(spanCtx, "breaker_open")
			res.SkippedReplication++
			continue
		}

		if err := g.deletePackAndIndex(spanCtx, repo, obj.PackSHA256); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return res, err
		}
		res.OrphanedReaped++
		res.BytesReclaimed += obj.SizeBytes
	}

	g.metrics.recordGCReaped(spanCtx, "compacted", res.CompactedReaped)
	g.metrics.recordGCReaped(spanCtx, "orphaned", res.OrphanedReaped)
	g.metrics.recordBytesReclaimed(spanCtx, "gc", res.BytesReclaimed)

	span.SetAttributes(
		attribute.Int("gc.compacted_reaped", res.CompactedReaped),
		attribute.Int("gc.orphaned_reaped", res.OrphanedReaped),
		attribute.Int("gc.skipped_replication", res.SkippedReplication),
		attribute.Int64("gc.bytes_reclaimed", res.BytesReclaimed),
	)

	return res, nil
}

func (g *GCSweeper) resolveReplacementPackIDs(
	ctx context.Context,
	repo tenant.RepoKey,
	manifestShards int,
	entry ddb.PackManifest,
	activePackList []string,
) ([]string, error) {
	if tracker, ok := g.manifestGC.(ReplacementTracker); ok {
		packID, found, err := tracker.GetReplacementPackID(ctx, repo, manifestShards, entry)
		if err != nil {
			return nil, err
		}
		if found && packID != "" {
			return []string{packID}, nil
		}
	}
	return activePackList, nil
}

func (g *GCSweeper) checkReplicationGate(
	ctx context.Context,
	repo tenant.RepoKey,
	replacementPackIDs []string,
	verifiedCache map[string]bool,
) bool {
	if len(replacementPackIDs) > 0 && !g.replicationGate.IsBreakerOpen() {
		allCached := true
		for _, id := range replacementPackIDs {
			if !verifiedCache[id] {
				allCached = false
				break
			}
		}
		if allCached {
			return true
		}
	}

	decision := g.replicationGate.AllowDeletion(ctx, repo, replacementPackIDs)
	if decision.Allowed {
		for _, id := range replacementPackIDs {
			verifiedCache[id] = true
		}
		return true
	}
	return false
}

func (g *GCSweeper) resolveGCAfter(ctx context.Context, repo tenant.RepoKey, manifestShards int, entry ddb.PackManifest) (time.Time, error) {
	if g.manifestGC != nil {
		gcAfter, ok, err := g.manifestGC.GetGCEligibility(ctx, repo, manifestShards, entry)
		if err != nil {
			return time.Time{}, err
		}
		if ok && !gcAfter.IsZero() {
			return gcAfter.UTC(), nil
		}
	}
	return entry.CreatedAt.UTC().Add(g.graceWindow), nil
}

func (g *GCSweeper) deletePackAndIndex(ctx context.Context, repo tenant.RepoKey, packSHA256 string) error {
	packKey, err := s3.PackKey(repo, packSHA256)
	if err != nil {
		return fmt.Errorf("compaction gc: pack key %s: %w", packSHA256, err)
	}
	idxKey, err := s3.IndexKey(repo, packSHA256)
	if err != nil {
		return fmt.Errorf("compaction gc: idx key %s: %w", packSHA256, err)
	}

	if err := g.objects.DeleteObject(ctx, packKey); err != nil && !errors.Is(err, s3.ErrNotFound) {
		return fmt.Errorf("compaction gc: delete pack %s: %w", packKey, err)
	}
	if err := g.objects.DeleteObject(ctx, idxKey); err != nil && !errors.Is(err, s3.ErrNotFound) {
		return fmt.Errorf("compaction gc: delete idx %s: %w", idxKey, err)
	}
	return nil
}
