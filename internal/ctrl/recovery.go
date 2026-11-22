package ctrl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
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

const (
	// RecoveryRefPrefix is the non-destructive ref namespace where stravin-ctrl
	// publishes recoverable RPO commits after ungraceful regional failover.
	RecoveryRefPrefix = "refs/stravin-recovery/"
	// DefaultRecoveryConcurrency bounds parallel S3 HeadObject checks per reconcile.
	DefaultRecoveryConcurrency = 8
	recoveryShortOIDLen        = 12
)

// RecoveryDDBStore is the narrow DynamoDB seam required by RecoveryReconciler.
// *ddb.Repository satisfies this interface directly.
type RecoveryDDBStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error)
	ListRefHistory(ctx context.Context, repo tenant.RepoKey, refName string, refShards int, afterULID string, limit int32) ([]ddb.RefHistory, error)
	ListManifestsSince(ctx context.Context, repo tenant.RepoKey, manifestShards int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
	CommitPush(ctx context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error)
}

// RecoveryS3Store is the narrow S3 seam required by RecoveryReconciler.
// *s3.Store satisfies this interface directly.
type RecoveryS3Store interface {
	HeadObject(ctx context.Context, key string) (s3.ObjectMeta, error)
}

// RecoveryConfig configures RecoveryReconciler.
type RecoveryConfig struct {
	DDB           RecoveryDDBStore
	S3            RecoveryS3Store
	Audit         RecoveryAuditStore
	Region        lease.Region
	DefaultWindow time.Duration
	Concurrency   int
	Clock         lease.Clock
	ULIDGen       func(time.Time) (string, error)
	Metrics       *Metrics
	Logger        *slog.Logger
}

// RecoveryRequest triggers orphaned-pack recovery reconciliation for a single repository.
type RecoveryRequest struct {
	Repo           tenant.RepoKey
	LookbackWindow time.Duration
	Concurrency    int
	DryRun         bool
	// TargetRefs optionally names additional source refs (for example refs deleted
	// during failover rewind) whose Ref History should also be inspected.
	TargetRefs []string
}

// PublishedRecoveryRef describes one non-destructive recovery ref published (or
// planned in DryRun mode) by RecoveryReconciler.
type PublishedRecoveryRef struct {
	SourceRef     string       `json:"source_ref"`
	RecoveryRef   string       `json:"recovery_ref"`
	CommitOID     string       `json:"commit_oid"`
	PackID        string       `json:"pack_id"`
	PackSizeBytes int64        `json:"pack_size_bytes"`
	OriginRegion  lease.Region `json:"origin_region"`
	AuditID       string       `json:"audit_id,omitempty"`
}

// RecoveryReport is the structured, auditable output of a recovery reconciliation pass.
type RecoveryReport struct {
	Repo                tenant.RepoKey         `json:"repo"`
	Region              lease.Region           `json:"region"`
	FailoverEpoch       lease.Epoch            `json:"failover_epoch"`
	Status              string                 `json:"status"` // "clean", "published", or "dry_run"
	DryRun              bool                   `json:"dry_run"`
	StartedAt           time.Time              `json:"started_at"`
	CompletedAt         time.Time              `json:"completed_at"`
	ScannedRefs         int                    `json:"scanned_refs"`
	HeadChecks          int                    `json:"head_checks"`
	Published           []PublishedRecoveryRef `json:"published"`
	AlreadyExists       []string               `json:"already_exists"`
	SkippedMissingPacks []string               `json:"skipped_missing_packs"`
}

// RecoveryCommand implements the GoF Command pattern for a single reconciliation run.
type RecoveryCommand interface {
	Execute(ctx context.Context) (RecoveryReport, error)
}

// RecoveryReconciler scans Ref History for commits diverged or rewound relative
// to current Ref State, verifies their backing packfiles exist in S3, and
// publishes non-destructive refs/stravin-recovery/* refs idempotently.
type RecoveryReconciler struct {
	ddb           RecoveryDDBStore
	s3            RecoveryS3Store
	audit         RecoveryAuditStore
	region        lease.Region
	defaultWindow time.Duration
	concurrency   int
	clock         lease.Clock
	ulidGen       func(time.Time) (string, error)
	metrics       *Metrics
	logger        *slog.Logger
}

// NewRecoveryReconciler constructs a validated RecoveryReconciler.
func NewRecoveryReconciler(cfg RecoveryConfig) (*RecoveryReconciler, error) {
	if cfg.DDB == nil {
		return nil, fmt.Errorf("%w: ddb store is required", ErrInvalidConfig)
	}
	if cfg.S3 == nil {
		return nil, fmt.Errorf("%w: s3 store is required", ErrInvalidConfig)
	}
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = DefaultRecoveryConcurrency
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

	return &RecoveryReconciler{
		ddb:           cfg.DDB,
		s3:            cfg.S3,
		audit:         cfg.Audit,
		region:        cfg.Region,
		defaultWindow: cfg.DefaultWindow,
		concurrency:   conc,
		clock:         clk,
		ulidGen:       ulidGen,
		metrics:       cfg.Metrics,
		logger:        logger,
	}, nil
}

// FormatRecoveryRef formats a deterministic, git-safe recovery ref name:
// refs/stravin-recovery/<sanitized-ref>-<short-oid>.
func FormatRecoveryRef(sourceRef, oid string) (string, error) {
	trimmedRef := strings.TrimSpace(sourceRef)
	if trimmedRef == "" {
		return "", fmt.Errorf("%w: empty source ref", ddb.ErrInvalidInput)
	}
	normalizedOID := strings.ToLower(strings.TrimSpace(oid))
	if err := ddb.ValidateOID(normalizedOID); err != nil {
		return "", err
	}
	sanitized := sanitizeRefComponent(strings.TrimPrefix(trimmedRef, "refs/"))
	return fmt.Sprintf("%s%s-%s", RecoveryRefPrefix, sanitized, normalizedOID[:recoveryShortOIDLen]), nil
}

func sanitizeRefComponent(raw string) string {
	var b strings.Builder
	b.Grow(len(raw))
	lastDash := false
	for i := 0; i < len(raw); i++ {
		ch := raw[i]
		isAlphaNum := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')
		isSafePunct := ch == '.' || ch == '_' || ch == '-'
		if isSafePunct && ch == '.' && i+1 < len(raw) && raw[i+1] == '.' {
			// Collapse ".." into a single dash so the component never violates git-check-ref-format.
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
			i++
			continue
		}
		if isAlphaNum || isSafePunct {
			if ch == '-' {
				if lastDash {
					continue
				}
				lastDash = true
			} else {
				lastDash = false
			}
			b.WriteByte(ch)
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	cleaned := strings.Trim(b.String(), "-.")
	if cleaned == "" {
		return "ref"
	}
	return cleaned
}

// NewCommand creates a GoF Command bound to req for execution.
func (r *RecoveryReconciler) NewCommand(req RecoveryRequest) RecoveryCommand {
	return &reconcileRepoCommand{reconciler: r, req: req}
}

// ReconcileRepo scans Ref History and publishes non-destructive recovery refs for
// diverged/rewound commits whose packfiles exist in S3.
func (r *RecoveryReconciler) ReconcileRepo(ctx context.Context, req RecoveryRequest) (RecoveryReport, error) {
	return r.NewCommand(req).Execute(ctx)
}

type reconcileRepoCommand struct {
	reconciler *RecoveryReconciler
	req        RecoveryRequest
}

type recoveryCandidate struct {
	sourceRef    string
	recoveryRef  string
	oid          string
	packID       string
	originRegion lease.Region
}

type packCheckResult struct {
	exists    bool
	sizeBytes int64
}

func (c *reconcileRepoCommand) Execute(ctx context.Context) (RecoveryReport, error) {
	r := c.reconciler
	req := c.req

	if err := req.Repo.Validate(); err != nil {
		return RecoveryReport{}, err
	}
	window := req.LookbackWindow
	if window <= 0 {
		window = r.defaultWindow
	}
	conc := req.Concurrency
	if conc <= 0 {
		conc = r.concurrency
	}

	startedAt := r.clock.Now().UTC()
	meta, err := r.ddb.GetMeta(ctx, req.Repo)
	if err != nil {
		return RecoveryReport{}, fmt.Errorf("ctrl: load repo meta for recovery: %w", err)
	}

	region := r.region
	if region == "" {
		region = meta.HomeRegion
	}

	ctx, span := r.metrics.Tracer().Start(ctx, "ctrl.recovery.reconcile", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(req.Repo.TenantID)).
			RepoID(string(req.Repo.RepoID)).
			HomeRegion(string(meta.HomeRegion)).
			FailoverEpoch(int64(meta.FailoverEpoch)).
			Build()...,
	))
	defer span.End()

	refs, err := r.ddb.ListRefs(ctx, req.Repo, meta.RefShards)
	if err != nil {
		return r.failRecovery(ctx, span, region, fmt.Errorf("ctrl: list refs for recovery: %w", err))
	}

	currentRefs := make(map[string]ddb.RefState, len(refs))
	existingRecoveryRefs := make(map[string]struct{})
	sourceRefSet := make(map[string]struct{}, len(refs)+len(req.TargetRefs))
	for _, ref := range refs {
		if strings.HasPrefix(ref.RefName, RecoveryRefPrefix) {
			existingRecoveryRefs[ref.RefName] = struct{}{}
			continue
		}
		currentRefs[ref.RefName] = ref
		sourceRefSet[ref.RefName] = struct{}{}
	}
	for _, target := range req.TargetRefs {
		trimmed := strings.TrimSpace(target)
		if trimmed == "" || strings.HasPrefix(trimmed, RecoveryRefPrefix) {
			continue
		}
		sourceRefSet[trimmed] = struct{}{}
	}

	sourceRefs := make([]string, 0, len(sourceRefSet))
	for name := range sourceRefSet {
		sourceRefs = append(sourceRefs, name)
	}
	sort.Strings(sourceRefs)

	var cutoff time.Time
	if window > 0 {
		cutoff = startedAt.Add(-window)
	}

	// Collect diverged history entries per source ref.
	var (
		candidates      []recoveryCandidate
		uniquePackIDSet = make(map[string]struct{})
	)
	for _, refName := range sourceRefs {
		history, err := r.ddb.ListRefHistory(ctx, req.Repo, refName, meta.RefShards, "", 0)
		if err != nil {
			return r.failRecovery(ctx, span, region, fmt.Errorf("ctrl: list ref history for %s: %w", refName, err))
		}
		curState, hasCurrent := currentRefs[refName]
		currentOID := ""
		if hasCurrent {
			currentOID = curState.OID
		}

		diverged := findDivergedHistoryEntries(history, currentOID, cutoff)
		for _, entry := range diverged {
			recRef, err := FormatRecoveryRef(refName, entry.OID)
			if err != nil {
				continue
			}
			origin := entry.OriginRegion
			if origin == "" {
				origin = region
			}
			candidates = append(candidates, recoveryCandidate{
				sourceRef:    refName,
				recoveryRef:  recRef,
				oid:          entry.OID,
				packID:       entry.PackID,
				originRegion: origin,
			})
			uniquePackIDSet[entry.PackID] = struct{}{}
		}
	}

	packIDs := make([]string, 0, len(uniquePackIDSet))
	for id := range uniquePackIDSet {
		packIDs = append(packIDs, id)
	}
	sort.Strings(packIDs)

	packResults, headChecks, err := r.verifyPacksParallel(ctx, req.Repo, packIDs, conc)
	if err != nil {
		return r.failRecovery(ctx, span, region, err)
	}

	report := RecoveryReport{
		Repo:                req.Repo,
		Region:              region,
		FailoverEpoch:       meta.FailoverEpoch,
		Status:              "clean",
		DryRun:              req.DryRun,
		StartedAt:           startedAt,
		ScannedRefs:         len(sourceRefs),
		HeadChecks:          headChecks,
		Published:           []PublishedRecoveryRef{},
		AlreadyExists:       []string{},
		SkippedMissingPacks: []string{},
	}

	seenMissingPack := make(map[string]struct{})
	seenRecoveryRef := make(map[string]struct{})
	for _, cand := range candidates {
		if _, dup := seenRecoveryRef[cand.recoveryRef]; dup {
			continue
		}
		seenRecoveryRef[cand.recoveryRef] = struct{}{}

		if _, exists := existingRecoveryRefs[cand.recoveryRef]; exists {
			report.AlreadyExists = append(report.AlreadyExists, cand.recoveryRef)
			continue
		}

		check := packResults[cand.packID]
		if !check.exists {
			if _, dup := seenMissingPack[cand.packID]; !dup {
				seenMissingPack[cand.packID] = struct{}{}
				report.SkippedMissingPacks = append(report.SkippedMissingPacks, cand.packID)
			}
			continue
		}

		pub := PublishedRecoveryRef{
			SourceRef:     cand.sourceRef,
			RecoveryRef:   cand.recoveryRef,
			CommitOID:     cand.oid,
			PackID:        cand.packID,
			PackSizeBytes: check.sizeBytes,
			OriginRegion:  cand.originRegion,
		}

		if req.DryRun {
			report.Published = append(report.Published, pub)
			continue
		}

		created, err := r.publishRecoveryRef(ctx, req.Repo, meta, region, cand, check.sizeBytes)
		if err != nil {
			return r.failRecovery(ctx, span, region, err)
		}
		if !created {
			report.AlreadyExists = append(report.AlreadyExists, cand.recoveryRef)
			continue
		}

		publishedAt := r.clock.Now().UTC()
		if r.audit != nil {
			auditID, err := r.ulidGen(publishedAt)
			if err != nil {
				return r.failRecovery(ctx, span, region, fmt.Errorf("ctrl: generate recovery audit id: %w", err))
			}
			pub.AuditID = auditID
			if err := r.audit.RecordRecoveryAudit(ctx, RecoveryAuditRecord{
				AuditID:       auditID,
				Repo:          req.Repo,
				Region:        region,
				FailoverEpoch: meta.FailoverEpoch,
				SourceRef:     cand.sourceRef,
				RecoveryRef:   cand.recoveryRef,
				CommitOID:     cand.oid,
				PackID:        cand.packID,
				PackSizeBytes: check.sizeBytes,
				OriginRegion:  cand.originRegion,
				PublishedAt:   publishedAt,
			}); err != nil {
				return r.failRecovery(ctx, span, region, err)
			}
		}

		r.metrics.recordRecoveryRefPublished(ctx, region)
		report.Published = append(report.Published, pub)
	}

	sort.Strings(report.AlreadyExists)
	sort.Strings(report.SkippedMissingPacks)

	if len(report.Published) > 0 {
		if req.DryRun {
			report.Status = "dry_run"
		} else {
			report.Status = "published"
		}
	}
	report.CompletedAt = r.clock.Now().UTC()

	span.SetAttributes(
		attribute.Int("recovery.scanned_refs", report.ScannedRefs),
		attribute.Int("recovery.head_checks", report.HeadChecks),
		attribute.Int("recovery.published", len(report.Published)),
		attribute.Int("recovery.already_exists", len(report.AlreadyExists)),
		attribute.Int("recovery.skipped_missing_packs", len(report.SkippedMissingPacks)),
		attribute.String("recovery.status", report.Status),
	)
	r.metrics.recordRecoveryRun(ctx, region, report.Status)

	r.logger.InfoContext(ctx, "ctrl: completed orphaned-pack recovery reconciliation",
		"tenant_id", string(req.Repo.TenantID),
		"repo_id", string(req.Repo.RepoID),
		"region", string(region),
		"status", report.Status,
		"scanned_refs", report.ScannedRefs,
		"published", len(report.Published),
		"already_exists", len(report.AlreadyExists),
		"skipped_missing_packs", len(report.SkippedMissingPacks),
	)

	return report, nil
}

func (r *RecoveryReconciler) failRecovery(ctx context.Context, span trace.Span, region lease.Region, err error) (RecoveryReport, error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	r.metrics.recordRecoveryRun(ctx, region, "error")
	return RecoveryReport{}, err
}

// findDivergedHistoryEntries returns RefHistory entries whose commits are NOT in
// the current ref tip's forward-commit lineage and carry a backing pack ID.
// Only diverged tip entries (not superseded by another diverged child entry) are
// returned, ordered deterministically by newest ULID first.
func findDivergedHistoryEntries(history []ddb.RefHistory, currentOID string, cutoff time.Time) []ddb.RefHistory {
	if len(history) == 0 {
		return nil
	}

	sorted := make([]ddb.RefHistory, len(history))
	copy(sorted, history)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ULID < sorted[j].ULID
	})

	// Build first-seen parent map so rewind records (which re-point to an earlier
	// OID) never falsely link the rewound tip as an ancestor of the rollback target.
	firstParent := make(map[string]string, len(sorted))
	for _, h := range sorted {
		if h.OID == "" || h.OID == ddb.ZeroOID {
			continue
		}
		if _, seen := firstParent[h.OID]; !seen {
			firstParent[h.OID] = h.PrevOID
		}
	}

	// Walk current tip's ancestry via first-seen parent pointers.
	inCurrentLineage := make(map[string]struct{})
	for cur := currentOID; cur != "" && cur != ddb.ZeroOID; {
		if _, visited := inCurrentLineage[cur]; visited {
			break
		}
		inCurrentLineage[cur] = struct{}{}
		parent, ok := firstParent[cur]
		if !ok {
			break
		}
		cur = parent
	}

	// Collect all diverged entries with a backing pack ID.
	divergedByOID := make(map[string]ddb.RefHistory)
	for _, h := range sorted {
		if h.OID == "" || h.OID == ddb.ZeroOID || h.PackID == "" {
			continue
		}
		if !cutoff.IsZero() && !h.UpdatedAt.IsZero() && h.UpdatedAt.Before(cutoff) {
			continue
		}
		if _, ok := inCurrentLineage[h.OID]; ok {
			continue
		}
		divergedByOID[h.OID] = h
	}
	if len(divergedByOID) == 0 {
		return nil
	}

	// Mark diverged OIDs that are parents of another diverged OID so we surface
	// diverged branch tips rather than every intermediate ancestor commit.
	hasDivergedChild := make(map[string]struct{})
	for oid := range divergedByOID {
		if parent, ok := firstParent[oid]; ok && parent != "" && parent != ddb.ZeroOID {
			if _, parentDiverged := divergedByOID[parent]; parentDiverged {
				hasDivergedChild[parent] = struct{}{}
			}
		}
	}

	var tips []ddb.RefHistory
	for oid, h := range divergedByOID {
		if _, superseded := hasDivergedChild[oid]; superseded {
			continue
		}
		tips = append(tips, h)
	}
	sort.Slice(tips, func(i, j int) bool {
		if tips[i].ULID != tips[j].ULID {
			return tips[i].ULID > tips[j].ULID
		}
		return tips[i].OID < tips[j].OID
	})
	return tips
}

func (r *RecoveryReconciler) verifyPacksParallel(ctx context.Context, repo tenant.RepoKey, packIDs []string, concurrency int) (map[string]packCheckResult, int, error) {
	results := make(map[string]packCheckResult, len(packIDs))
	if len(packIDs) == 0 {
		return results, 0, nil
	}
	if concurrency > len(packIDs) {
		concurrency = len(packIDs)
	}

	checkCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan string, len(packIDs))
	for _, id := range packIDs {
		jobs <- id
	}
	close(jobs)

	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		headChecks int
		firstErr   error
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for packID := range jobs {
				if checkCtx.Err() != nil {
					return
				}
				res, checks, err := r.verifyPackExists(checkCtx, repo, packID)
				mu.Lock()
				headChecks += checks
				if err != nil {
					if firstErr == nil {
						firstErr = err
						cancel()
					}
				} else {
					results[packID] = res
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return nil, headChecks, firstErr
	}
	return results, headChecks, nil
}

func (r *RecoveryReconciler) verifyPackExists(ctx context.Context, repo tenant.RepoKey, packID string) (packCheckResult, int, error) {
	packKey, err := s3.PackKey(repo, packID)
	if err != nil {
		return packCheckResult{}, 0, err
	}
	idxKey, err := s3.IndexKey(repo, packID)
	if err != nil {
		return packCheckResult{}, 0, err
	}

	checks := 1
	packMeta, err := r.s3.HeadObject(ctx, packKey)
	if err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			return packCheckResult{exists: false}, checks, nil
		}
		return packCheckResult{}, checks, fmt.Errorf("ctrl: head recovery pack %s: %w", packID, err)
	}

	checks++
	if _, err := r.s3.HeadObject(ctx, idxKey); err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			return packCheckResult{exists: false}, checks, nil
		}
		return packCheckResult{}, checks, fmt.Errorf("ctrl: head recovery idx %s: %w", packID, err)
	}

	return packCheckResult{exists: true, sizeBytes: packMeta.SizeBytes}, checks, nil
}

// publishRecoveryRef atomically creates the recovery ref with attribute_not_exists(oid)
// and ensures an ACTIVE PackManifest entry exists so delayed GC cannot reap the pack.
// Returns (false, nil) if the recovery ref already existed (idempotent conflict).
func (r *RecoveryReconciler) publishRecoveryRef(
	ctx context.Context,
	repo tenant.RepoKey,
	meta ddb.RepoMeta,
	region lease.Region,
	cand recoveryCandidate,
	packSize int64,
) (bool, error) {
	_, err := r.ddb.CommitPush(ctx, ddb.CommitPushInput{
		Repo:           repo,
		RefShards:      meta.RefShards,
		ManifestShards: meta.ManifestShards,
		Epoch:          meta.FailoverEpoch,
		OriginRegion:   region,
		TraceParent:    telemetry.TraceparentFromContext(ctx),
		Updates: []ddb.RefUpdate{
			{
				RefName: cand.recoveryRef,
				OldOID:  ddb.ZeroOID,
				NewOID:  cand.oid,
			},
		},
		Pack: &ddb.PackUpload{
			PackID:       cand.packID,
			SizeBytes:    packSize,
			OriginRegion: cand.originRegion,
		},
	})
	if err != nil {
		if errors.Is(err, ddb.ErrRefConflict) {
			return false, nil
		}
		return false, fmt.Errorf("ctrl: publish recovery ref %s: %w", cand.recoveryRef, err)
	}
	return true, nil
}
