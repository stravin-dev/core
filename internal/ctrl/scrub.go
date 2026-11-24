package ctrl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
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
	// DefaultScrubLookback is the 20-minute lookback window from core/ha-consistency.md
	// (exceeding S3 RTC's 15-minute replication SLA).
	DefaultScrubLookback = 20 * time.Minute
	// DefaultScrubConcurrency bounds parallel S3 HeadObject requests per scrub.
	DefaultScrubConcurrency = 8
)

// ScrubDDBStore is the narrow DynamoDB seam required by Scrubber.
// *ddb.Repository satisfies this interface directly.
type ScrubDDBStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
	ListManifestsSince(ctx context.Context, repo tenant.RepoKey, manifestShards int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error)
	ListRefHistory(ctx context.Context, repo tenant.RepoKey, refName string, refShards int, afterULID string, limit int32) ([]ddb.RefHistory, error)
	CommitPush(ctx context.Context, in ddb.CommitPushInput) (ddb.CommitPushResult, error)
}

// ScrubS3Store is the narrow S3 seam required by Scrubber.
// *s3.Store satisfies this interface directly.
type ScrubS3Store interface {
	HeadObject(ctx context.Context, key string) (s3.ObjectMeta, error)
}

// ScrubConfig configures the deterministic S3 integrity scrubber and ref rewind engine.
type ScrubConfig struct {
	DDB           ScrubDDBStore
	S3            ScrubS3Store
	Audit         AuditStore
	Region        lease.Region
	DefaultWindow time.Duration
	Concurrency   int
	Clock         lease.Clock
	ULIDGen       func(time.Time) (string, error)
	Metrics       *Metrics
	Logger        *slog.Logger
}

// ScrubRequest triggers a single-repository S3 integrity scrub and deterministic ref rewind.
type ScrubRequest struct {
	Repo           tenant.RepoKey
	LookbackWindow time.Duration
	Concurrency    int
	DryRun         bool
}

// RewindAction describes a single deterministic reference rewind or deletion
// performed when its backing packfile is missing from S3.
type RewindAction struct {
	RefName       string   `json:"ref_name"`
	Action        string   `json:"action"` // "rewind" or "delete"
	FromOID       string   `json:"from_oid"`
	ToOID         string   `json:"to_oid"`
	MissingPackID string   `json:"missing_pack_id"`
	TargetPackID  string   `json:"target_pack_id,omitempty"`
	TruncatedOIDs []string `json:"truncated_oids"`
}

// ScrubReport is the structured, auditable output of an S3 integrity scrub cycle.
type ScrubReport struct {
	AuditID        string         `json:"audit_id"`
	Repo           tenant.RepoKey `json:"repo"`
	Region         lease.Region   `json:"region"`
	FailoverEpoch  lease.Epoch    `json:"failover_epoch"`
	Status         string         `json:"status"` // "clean" or "rewound" or "dry_run"
	DryRun         bool           `json:"dry_run"`
	StartedAt      time.Time      `json:"started_at"`
	CompletedAt    time.Time      `json:"completed_at"`
	LookbackWindow time.Duration  `json:"lookback_window_ns"`
	CheckedPacks   int            `json:"checked_packs"`
	HeadChecks     int            `json:"head_checks"`
	MissingPackIDs []string       `json:"missing_pack_ids"`
	RewoundRefs    []RewindAction `json:"rewound_refs"`
	TruncatedOIDs  []string       `json:"truncated_oids"`
}

// ScrubCommand implements the GoF Command pattern for a single scrub/rewind execution.
type ScrubCommand interface {
	Execute(ctx context.Context) (ScrubReport, error)
}

// Scrubber verifies recent Pack Manifest entries against S3 via bounded parallel
// HeadObject checks and deterministically rewinds affected refs using Ref History.
type Scrubber struct {
	ddb           ScrubDDBStore
	s3            ScrubS3Store
	audit         AuditStore
	region        lease.Region
	defaultWindow time.Duration
	concurrency   int
	clock         lease.Clock
	ulidGen       func(time.Time) (string, error)
	metrics       *Metrics
	logger        *slog.Logger
}

// NewScrubber constructs a validated Scrubber.
func NewScrubber(cfg ScrubConfig) (*Scrubber, error) {
	if cfg.DDB == nil {
		return nil, fmt.Errorf("%w: ddb store is required", ErrInvalidConfig)
	}
	if cfg.S3 == nil {
		return nil, fmt.Errorf("%w: s3 store is required", ErrInvalidConfig)
	}
	window := cfg.DefaultWindow
	if window <= 0 {
		window = DefaultScrubLookback
	}
	conc := cfg.Concurrency
	if conc <= 0 {
		conc = DefaultScrubConcurrency
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

	return &Scrubber{
		ddb:           cfg.DDB,
		s3:            cfg.S3,
		audit:         cfg.Audit,
		region:        cfg.Region,
		defaultWindow: window,
		concurrency:   conc,
		clock:         clk,
		ulidGen:       ulidGen,
		metrics:       cfg.Metrics,
		logger:        logger,
	}, nil
}

// NewCommand creates a GoF Command bound to req for execution.
func (s *Scrubber) NewCommand(req ScrubRequest) ScrubCommand {
	return &scrubRepoCommand{scrubber: s, req: req}
}

// ScrubRepo runs a deterministic S3 HeadObject integrity scrub and Ref History rewind.
func (s *Scrubber) ScrubRepo(ctx context.Context, req ScrubRequest) (ScrubReport, error) {
	return s.NewCommand(req).Execute(ctx)
}

type scrubRepoCommand struct {
	scrubber *Scrubber
	req      ScrubRequest
}

func (c *scrubRepoCommand) Execute(ctx context.Context) (ScrubReport, error) {
	s := c.scrubber
	req := c.req

	if err := req.Repo.Validate(); err != nil {
		return ScrubReport{}, err
	}
	window := req.LookbackWindow
	if window <= 0 {
		window = s.defaultWindow
	}
	conc := req.Concurrency
	if conc <= 0 {
		conc = s.concurrency
	}

	startedAt := s.clock.Now().UTC()
	meta, err := s.ddb.GetMeta(ctx, req.Repo)
	if err != nil {
		return ScrubReport{}, fmt.Errorf("ctrl: load repo meta for scrub: %w", err)
	}

	region := s.region
	if region == "" {
		region = meta.HomeRegion
	}

	ctx, span := s.metrics.Tracer().Start(ctx, "ctrl.scrub.region", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(req.Repo.TenantID)).
			RepoID(string(req.Repo.RepoID)).
			HomeRegion(string(meta.HomeRegion)).
			FailoverEpoch(epochToInt64(meta.FailoverEpoch)).
			Build()...,
	))
	defer span.End()

	// 1. Query recent ACTIVE Pack Manifest entries within the lookback window.
	manifests, err := s.ddb.ListManifestsSince(ctx, req.Repo, meta.ManifestShards, "", true)
	if err != nil {
		return s.failScrub(ctx, span, region, startedAt, fmt.Errorf("ctrl: list manifests: %w", err))
	}

	cutoff := startedAt.Add(-window)
	packIDs := collectRecentPackIDs(manifests, cutoff)

	// 2. Bounded parallel HeadObject checks for both .pack and .idx objects.
	packPresence, headChecks, err := s.checkPacksParallel(ctx, req.Repo, region, packIDs, conc)
	if err != nil {
		return s.failScrub(ctx, span, region, startedAt, err)
	}

	var missingPackIDs []string
	for _, id := range packIDs {
		if !packPresence[id] {
			missingPackIDs = append(missingPackIDs, id)
		}
	}
	sort.Strings(missingPackIDs)

	report := ScrubReport{
		Repo:           req.Repo,
		Region:         region,
		FailoverEpoch:  meta.FailoverEpoch,
		Status:         "clean",
		DryRun:         req.DryRun,
		StartedAt:      startedAt,
		LookbackWindow: window,
		CheckedPacks:   len(packIDs),
		HeadChecks:     headChecks,
		MissingPackIDs: missingPackIDs,
		RewoundRefs:    []RewindAction{},
		TruncatedOIDs:  []string{},
	}

	// 3. If any packs are missing, walk Ref History for affected refs and compute rewinds.
	if len(missingPackIDs) > 0 {
		actions, extraHeads, err := s.planRewinds(ctx, req.Repo, region, meta, packPresence)
		if err != nil {
			return s.failScrub(ctx, span, region, startedAt, err)
		}
		report.HeadChecks += extraHeads
		report.RewoundRefs = actions
		report.TruncatedOIDs = collectTruncatedOIDs(actions)

		if len(actions) > 0 {
			if req.DryRun {
				report.Status = "dry_run"
			} else {
				if err := s.applyRewinds(ctx, req.Repo, meta, actions); err != nil {
					return s.failScrub(ctx, span, region, startedAt, err)
				}
				report.Status = "rewound"
				for _, act := range actions {
					s.metrics.recordRewoundRef(ctx, region, act.Action)
				}
			}
		}
	}

	report.CompletedAt = s.clock.Now().UTC()
	auditID, err := s.ulidGen(report.CompletedAt)
	if err != nil {
		return s.failScrub(ctx, span, region, startedAt, fmt.Errorf("ctrl: generate scrub audit id: %w", err))
	}
	report.AuditID = auditID

	if s.audit != nil && !req.DryRun {
		if err := s.audit.RecordScrubAudit(ctx, report); err != nil {
			return s.failScrub(ctx, span, region, startedAt, err)
		}
	}

	span.SetAttributes(
		attribute.Int("scrub.checked_packs", report.CheckedPacks),
		attribute.Int("scrub.head_checks", report.HeadChecks),
		attribute.Int("scrub.missing_packs", len(report.MissingPackIDs)),
		attribute.Int("scrub.rewound_refs", len(report.RewoundRefs)),
		attribute.String("scrub.status", report.Status),
	)
	s.metrics.recordScrubRun(ctx, region, report.Status, report.CompletedAt.Sub(startedAt))

	s.logger.InfoContext(ctx, "ctrl: completed s3 integrity scrub",
		"audit_id", report.AuditID,
		"tenant_id", string(req.Repo.TenantID),
		"repo_id", string(req.Repo.RepoID),
		"region", string(region),
		"status", report.Status,
		"checked_packs", report.CheckedPacks,
		"missing_packs", len(report.MissingPackIDs),
		"rewound_refs", len(report.RewoundRefs),
	)

	return report, nil
}

func (s *Scrubber) failScrub(ctx context.Context, span trace.Span, region lease.Region, startedAt time.Time, err error) (ScrubReport, error) {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	s.metrics.recordScrubRun(ctx, region, "error", s.clock.Now().UTC().Sub(startedAt))
	return ScrubReport{}, err
}

func collectRecentPackIDs(manifests []ddb.PackManifest, cutoff time.Time) []string {
	seen := make(map[string]struct{}, len(manifests))
	var ids []string
	for _, m := range manifests {
		if m.Status != ddb.PackStatusActive {
			continue
		}
		if !m.CreatedAt.IsZero() && m.CreatedAt.Before(cutoff) {
			continue
		}
		if _, ok := seen[m.PackID]; ok {
			continue
		}
		seen[m.PackID] = struct{}{}
		ids = append(ids, m.PackID)
	}
	sort.Strings(ids)
	return ids
}

func (s *Scrubber) checkPacksParallel(ctx context.Context, repo tenant.RepoKey, region lease.Region, packIDs []string, concurrency int) (map[string]bool, int, error) {
	presence := make(map[string]bool, len(packIDs))
	if len(packIDs) == 0 {
		return presence, 0, nil
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
				exists, checks, err := s.verifyPackExists(checkCtx, repo, region, packID)
				mu.Lock()
				headChecks += checks
				if err != nil {
					if firstErr == nil {
						firstErr = err
						cancel()
					}
				} else {
					presence[packID] = exists
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	if firstErr != nil {
		return nil, headChecks, firstErr
	}
	return presence, headChecks, nil
}

// verifyPackExists runs HeadObject for both .pack and .idx. A pack is intact
// only if both objects exist in S3. Non-NotFound errors are propagated so
// transient S3 failures never cause spurious ref rewinds.
func (s *Scrubber) verifyPackExists(ctx context.Context, repo tenant.RepoKey, region lease.Region, packID string) (bool, int, error) {
	packKey, err := s3.PackKey(repo, packID)
	if err != nil {
		return false, 0, err
	}
	idxKey, err := s3.IndexKey(repo, packID)
	if err != nil {
		return false, 0, err
	}

	checks := 1
	if _, err := s.s3.HeadObject(ctx, packKey); err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			s.metrics.recordHeadCheck(ctx, region, "missing")
			return false, checks, nil
		}
		s.metrics.recordHeadCheck(ctx, region, "error")
		return false, checks, fmt.Errorf("ctrl: head pack %s: %w", packID, err)
	}
	s.metrics.recordHeadCheck(ctx, region, "ok")

	checks++
	if _, err := s.s3.HeadObject(ctx, idxKey); err != nil {
		if errors.Is(err, s3.ErrNotFound) {
			s.metrics.recordHeadCheck(ctx, region, "missing")
			return false, checks, nil
		}
		s.metrics.recordHeadCheck(ctx, region, "error")
		return false, checks, fmt.Errorf("ctrl: head idx %s: %w", packID, err)
	}
	s.metrics.recordHeadCheck(ctx, region, "ok")
	return true, checks, nil
}

func (s *Scrubber) planRewinds(
	ctx context.Context,
	repo tenant.RepoKey,
	region lease.Region,
	meta ddb.RepoMeta,
	packPresence map[string]bool,
) ([]RewindAction, int, error) {
	refs, err := s.ddb.ListRefs(ctx, repo, meta.RefShards)
	if err != nil {
		return nil, 0, fmt.Errorf("ctrl: list refs for rewind: %w", err)
	}

	var (
		actions    []RewindAction
		extraHeads int
	)

	for _, ref := range refs {
		if ref.PackID == "" {
			continue
		}
		intact, known := packPresence[ref.PackID]
		if !known || intact {
			continue
		}

		history, err := s.ddb.ListRefHistory(ctx, repo, ref.RefName, meta.RefShards, "", 0)
		if err != nil {
			return nil, extraHeads, fmt.Errorf("ctrl: list ref history for %s: %w", ref.RefName, err)
		}

		action, checks, err := s.selectDeterministicRewind(ctx, repo, region, ref, history, packPresence)
		extraHeads += checks
		if err != nil {
			return nil, extraHeads, err
		}
		actions = append(actions, action)
	}

	return actions, extraHeads, nil
}

// selectDeterministicRewind walks history from newest to oldest and picks the
// most recent entry whose backing pack exists in S3. If no prior intact pack
// exists (or a prior delete entry is encountered first), the ref is deleted.
func (s *Scrubber) selectDeterministicRewind(
	ctx context.Context,
	repo tenant.RepoKey,
	region lease.Region,
	ref ddb.RefState,
	history []ddb.RefHistory,
	packPresence map[string]bool,
) (RewindAction, int, error) {
	// Ensure deterministic ascending ULID order before walking in reverse.
	sorted := make([]ddb.RefHistory, len(history))
	copy(sorted, history)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ULID < sorted[j].ULID
	})

	var (
		truncated    []string
		seenTrunc    = make(map[string]struct{})
		extraHeads   int
		targetOID    = ddb.ZeroOID
		targetPackID string
	)

	addTruncated := func(oid string) {
		if oid == "" || oid == ddb.ZeroOID {
			return
		}
		if _, dup := seenTrunc[oid]; dup {
			return
		}
		seenTrunc[oid] = struct{}{}
		truncated = append(truncated, oid)
	}

	// Always include the current dangling tip OID if not already captured by history.
	foundTarget := false
	for i := len(sorted) - 1; i >= 0; i-- {
		h := sorted[i]
		if h.OID == ddb.ZeroOID {
			// Reached a historical deletion before finding an intact commit: delete ref.
			break
		}
		if h.PackID == "" {
			// Metadata-only update (e.g. fast-forward or prior rewind) with no new pack.
			// Keep walking unless this entry is already older than the missing tip.
			addTruncated(h.OID)
			continue
		}

		exists, known := packPresence[h.PackID]
		if !known {
			var checks int
			var err error
			exists, checks, err = s.verifyPackExists(ctx, repo, region, h.PackID)
			extraHeads += checks
			if err != nil {
				return RewindAction{}, extraHeads, err
			}
			packPresence[h.PackID] = exists
		}

		if exists {
			targetOID = h.OID
			targetPackID = h.PackID
			foundTarget = true
			break
		}
		addTruncated(h.OID)
	}

	if !foundTarget || targetOID != ref.OID {
		addTruncated(ref.OID)
	}

	actionKind := "delete"
	if foundTarget {
		actionKind = "rewind"
	}

	return RewindAction{
		RefName:       ref.RefName,
		Action:        actionKind,
		FromOID:       ref.OID,
		ToOID:         targetOID,
		MissingPackID: ref.PackID,
		TargetPackID:  targetPackID,
		TruncatedOIDs: truncated,
	}, extraHeads, nil
}

func (s *Scrubber) applyRewinds(ctx context.Context, repo tenant.RepoKey, meta ddb.RepoMeta, actions []RewindAction) error {
	for start := 0; start < len(actions); start += ddb.MaxAtomicPushRefs {
		end := start + ddb.MaxAtomicPushRefs
		if end > len(actions) {
			end = len(actions)
		}
		batch := actions[start:end]
		updates := make([]ddb.RefUpdate, 0, len(batch))
		for _, act := range batch {
			updates = append(updates, ddb.RefUpdate{
				RefName: act.RefName,
				OldOID:  act.FromOID,
				NewOID:  act.ToOID,
			})
		}

		_, err := s.ddb.CommitPush(ctx, ddb.CommitPushInput{
			Repo:           repo,
			RefShards:      meta.RefShards,
			ManifestShards: meta.ManifestShards,
			Epoch:          meta.FailoverEpoch,
			OriginRegion:   meta.HomeRegion,
			TraceParent:    telemetry.TraceparentFromContext(ctx),
			Updates:        updates,
			Pack:           nil,
		})
		if err != nil {
			return fmt.Errorf("ctrl: apply ref rewind batch: %w", err)
		}
	}
	return nil
}

func collectTruncatedOIDs(actions []RewindAction) []string {
	seen := make(map[string]struct{})
	var all []string
	for _, act := range actions {
		for _, oid := range act.TruncatedOIDs {
			if _, dup := seen[oid]; dup {
				continue
			}
			seen[oid] = struct{}{}
			all = append(all, oid)
		}
	}
	sort.Strings(all)
	return all
}
