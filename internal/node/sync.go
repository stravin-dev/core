package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const (
	defaultMaxConcurrentPackFetches = 8
	defaultPruneReconcileInterval   = 30 * time.Second
)

// MetadataStore is the narrow DynamoDB read seam used by Materializer.
// *ddb.Repository satisfies this interface directly.
type MetadataStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error)
	QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
}

// PackFetcher is the narrow S3 read seam used by Materializer.
// *s3.Store satisfies this interface directly.
type PackFetcher interface {
	GetPack(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
	GetIndex(ctx context.Context, repo tenant.RepoKey, packSHA256 string) ([]byte, error)
}

// MaterializerConfig configures the incremental DynamoDB + S3 -> NVMe cache materializer.
type MaterializerConfig struct {
	Meta                     MetadataStore
	Packs                    PackFetcher
	Cache                    *cache.Cache
	GitExecutor              gitexec.GitExecutor
	Telemetry                *telemetry.Provider
	Metrics                  *Metrics
	Logger                   *slog.Logger
	MaxConcurrentPackFetches int
	PruneReconcileInterval   time.Duration
}

// SyncResult holds the authoritative DynamoDB state and pinned NVMe bare-repo handle.
// Callers MUST invoke Handle.Release() when done reading from the bare repo.
type SyncResult struct {
	Meta         ddb.RepoMeta
	Refs         []ddb.RefState
	PacksFetched int
	PacksCached  int
	Handle       *cache.RepoHandle
}

// Materializer implements the GoF Facade / Template Method pipeline for
// synchronizing DynamoDB metadata and S3 packfiles into the warm NVMe cache.
type Materializer struct {
	meta                   MetadataStore
	packs                  PackFetcher
	cache                  *cache.Cache
	gitExec                gitexec.GitExecutor
	refExec                gitexec.BareRepoExecutor
	tracer                 trace.Tracer
	metrics                *Metrics
	logger                 *slog.Logger
	maxConcurrency         int
	pruneReconcileInterval time.Duration

	reconcileMu   sync.Mutex
	lastReconcile map[tenant.RepoKey]time.Time
}

// NewMaterializer constructs a Materializer with validated dependencies.
func NewMaterializer(cfg MaterializerConfig) (*Materializer, error) {
	if cfg.Meta == nil {
		return nil, errors.New("node: metadata store is required")
	}
	if cfg.Packs == nil {
		return nil, errors.New("node: pack fetcher is required")
	}
	if cfg.Cache == nil {
		return nil, errors.New("node: cache is required")
	}
	tp := cfg.Telemetry
	if tp == nil {
		tp = telemetry.NewNoopProvider()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	gitExec := cfg.GitExecutor
	if gitExec == nil {
		gitExec = gitexec.NewSubprocessExecutor(gitexec.Config{Logger: logger})
	}
	refExec, ok := gitExec.(gitexec.BareRepoExecutor)
	if !ok {
		refExec = gitexec.NewSubprocessExecutor(gitexec.Config{Logger: logger})
	}
	maxConc := cfg.MaxConcurrentPackFetches
	if maxConc <= 0 {
		maxConc = defaultMaxConcurrentPackFetches
	}
	reconcileInterval := cfg.PruneReconcileInterval
	if reconcileInterval <= 0 {
		reconcileInterval = defaultPruneReconcileInterval
	}
	return &Materializer{
		meta:                   cfg.Meta,
		packs:                  cfg.Packs,
		cache:                  cfg.Cache,
		gitExec:                gitExec,
		refExec:                refExec,
		tracer:                 tp.Tracer("stravin-node"),
		metrics:                cfg.Metrics,
		logger:                 logger,
		maxConcurrency:         maxConc,
		pruneReconcileInterval: reconcileInterval,
		lastReconcile:          make(map[tenant.RepoKey]time.Time),
	}, nil
}

// LoadAuthoritativeRefs reads META and all ref shards (RS#{s}) strictly from
// DynamoDB without touching local disk or S3.
func (m *Materializer) LoadAuthoritativeRefs(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, []ddb.RefState, error) {
	if err := repo.Validate(); err != nil {
		return ddb.RepoMeta{}, nil, err
	}

	meta, err := m.meta.GetMeta(ctx, repo)
	if err != nil {
		return ddb.RepoMeta{}, nil, fmt.Errorf("node: get meta %s: %w", repo, err)
	}

	refCtx, span := m.tracer.Start(ctx, "ddb.query_refs", trace.WithAttributes(
		telemetry.NewSpanAttrs().
			TenantID(string(repo.TenantID)).
			RepoID(string(repo.RepoID)).
			HomeRegion(string(meta.HomeRegion)).
			FailoverEpoch(int64(meta.FailoverEpoch)).
			Build()...,
	))
	refs, err := m.meta.ListRefs(refCtx, repo, meta.RefShards)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return ddb.RepoMeta{}, nil, fmt.Errorf("node: list refs %s: %w", repo, err)
	}
	span.End()

	return meta, refs, nil
}

// SyncAndPin runs the full read-path synchronization pipeline:
//  1. Load authoritative META + RS#{s} refs from DynamoDB.
//  2. Under singleflight cache.Materialize (returning a pinned RepoHandle), query
//     manifest shards MS#{m} incrementally using per-shard last_seen_ulid watermarks.
//  3. Concurrently fetch any missing ACTIVE .pack/.idx objects from S3 and install
//     them into the warm NVMe cache.
//  4. Advance manifest watermarks only after all missing packs are installed.
//  5. Materialize disposable local bare-repo refs strictly from DynamoDB.
func (m *Materializer) SyncAndPin(ctx context.Context, repo tenant.RepoKey) (SyncResult, error) {
	meta, refs, err := m.LoadAuthoritativeRefs(ctx, repo)
	if err != nil {
		return SyncResult{}, err
	}

	var fetchedCount, cachedCount int
	handle, err := m.cache.Materialize(ctx, repo, func(syncCtx context.Context, repoPath string) error {
		fetched, cached, pruned, syncErr := m.syncManifestPacks(syncCtx, repo, meta)
		if syncErr != nil {
			return syncErr
		}
		fetchedCount = fetched
		cachedCount = cached

		if err := m.ensureReftableBareRepo(syncCtx, repo, repoPath, meta); err != nil {
			return fmt.Errorf("node: configure reftable bare repo %s: %w", repo, err)
		}
		refsChanged, err := m.materializeReftableRefs(syncCtx, repo, repoPath, refs, meta.DefaultBranch)
		if err != nil {
			return fmt.Errorf("node: materialize reftable refs %s: %w", repo, err)
		}
		m.refreshAuxIndexes(syncCtx, repo, repoPath, fetched > 0 || pruned > 0 || refsChanged, len(refs) > 0)
		return nil
	})
	if err != nil {
		return SyncResult{}, err
	}

	if m.metrics != nil {
		m.metrics.RecordCacheBytes(ctx, m.cache.Stats().TotalBytes)
	}

	return SyncResult{
		Meta:         meta,
		Refs:         refs,
		PacksFetched: fetchedCount,
		PacksCached:  cachedCount,
		Handle:       handle,
	}, nil
}

func (m *Materializer) syncManifestPacks(ctx context.Context, repo tenant.RepoKey, meta ddb.RepoMeta) (fetched, cached, pruned int, err error) {
	maxULIDByShard := make(map[int]string, meta.ManifestShards)
	missingSet := make(map[string]struct{})
	var missingOrder []string

	for shard := 0; shard < meta.ManifestShards; shard++ {
		afterULID := m.cache.Watermark(repo, shard)

		qCtx, qSpan := m.tracer.Start(ctx, "ddb.query_manifest", trace.WithAttributes(
			telemetry.NewSpanAttrs().
				TenantID(string(repo.TenantID)).
				RepoID(string(repo.RepoID)).
				ManifestShard(shard).
				Build()...,
		))
		entries, qErr := m.meta.QueryManifestShard(qCtx, repo, shard, afterULID, false)
		if qErr != nil {
			qSpan.RecordError(qErr)
			qSpan.SetStatus(codes.Error, qErr.Error())
			qSpan.End()
			return 0, 0, 0, fmt.Errorf("node: query manifest shard %d for %s: %w", shard, repo, qErr)
		}
		qSpan.End()

		for _, pm := range entries {
			if pm.ULID > maxULIDByShard[shard] {
				maxULIDByShard[shard] = pm.ULID
			}
			if pm.Status != ddb.PackStatusActive {
				continue
			}

			_, lookupSpan := m.tracer.Start(ctx, "node.cache.lookup", trace.WithAttributes(
				telemetry.AttrTenantID.String(string(repo.TenantID)),
				telemetry.AttrRepoID.String(string(repo.RepoID)),
				telemetry.AttrPackID.String(pm.PackID),
			))
			hit := m.cache.HasPack(repo, pm.PackID)
			lookupSpan.End()

			if m.metrics != nil {
				m.metrics.RecordCacheLookup(ctx, hit)
			}
			if hit {
				cached++
				continue
			}
			if _, seen := missingSet[pm.PackID]; !seen {
				missingSet[pm.PackID] = struct{}{}
				missingOrder = append(missingOrder, pm.PackID)
			}
		}
	}

	if len(missingOrder) > 0 {
		if err := m.fetchAndInstallPacks(ctx, repo, missingOrder); err != nil {
			return 0, cached, 0, err
		}
		fetched = len(missingOrder)
	}

	// Only advance watermarks after every missing pack has been durably installed.
	for shard, maxULID := range maxULIDByShard {
		if maxULID != "" {
			m.cache.AdvanceWatermark(repo, shard, maxULID)
		}
	}

	if m.shouldReconcilePrune(repo, fetched > 0) {
		pruned = m.pruneSupersededPacks(ctx, repo, meta)
	}

	return fetched, cached, pruned, nil
}

func (m *Materializer) shouldReconcilePrune(repo tenant.RepoKey, fetchedNewPacks bool) bool {
	if fetchedNewPacks {
		return true
	}
	m.reconcileMu.Lock()
	defer m.reconcileMu.Unlock()
	last, ok := m.lastReconcile[repo]
	return !ok || time.Since(last) >= m.pruneReconcileInterval
}

func (m *Materializer) markReconciled(repo tenant.RepoKey) {
	m.reconcileMu.Lock()
	m.lastReconcile[repo] = time.Now()
	m.reconcileMu.Unlock()
}

func (m *Materializer) pruneSupersededPacks(ctx context.Context, repo tenant.RepoKey, meta ddb.RepoMeta) int {
	cachedIDs := m.cache.CachedPackIDs(repo)
	if len(cachedIDs) == 0 {
		m.markReconciled(repo)
		return 0
	}

	activeSet := make(map[string]struct{}, len(cachedIDs))
	for shard := 0; shard < meta.ManifestShards; shard++ {
		activeEntries, err := m.meta.QueryManifestShard(ctx, repo, shard, "", true)
		if err != nil {
			return 0
		}
		for _, pm := range activeEntries {
			if pm.Status == ddb.PackStatusActive {
				activeSet[pm.PackID] = struct{}{}
			}
		}
	}

	var stale []string
	for _, id := range cachedIDs {
		if _, ok := activeSet[id]; !ok {
			stale = append(stale, id)
		}
	}
	if len(stale) > 0 {
		_ = m.cache.PrunePacks(repo, stale)
	}
	m.markReconciled(repo)
	return len(stale)
}

func (m *Materializer) fetchAndInstallPacks(ctx context.Context, repo tenant.RepoKey, packIDs []string) error {
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, m.maxConcurrency)
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancel()
	}

	for _, packID := range packIDs {
		packID := packID
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-fetchCtx.Done():
				recordErr(fetchCtx.Err())
				return
			}

			if err := m.fetchAndInstallOnePack(fetchCtx, repo, packID); err != nil {
				recordErr(err)
			}
		}()
	}

	wg.Wait()
	return firstErr
}

func (m *Materializer) fetchAndInstallOnePack(ctx context.Context, repo tenant.RepoKey, packID string) error {
	spanCtx, span := m.tracer.Start(ctx, "s3.get_pack", trace.WithAttributes(
		telemetry.AttrTenantID.String(string(repo.TenantID)),
		telemetry.AttrRepoID.String(string(repo.RepoID)),
		telemetry.AttrPackID.String(packID),
	))
	defer span.End()

	packData, idxData, err := m.fetchPackPair(spanCtx, repo, packID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if m.metrics != nil {
			m.metrics.RecordS3PackFetch(spanCtx, "error")
		}
		return fmt.Errorf("node: fetch pack %s for %s: %w", packID, repo, err)
	}

	if err := m.cache.InstallPack(repo, packID, packData, idxData); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("node: install pack %s for %s: %w", packID, repo, err)
	}

	if m.metrics != nil {
		m.metrics.RecordS3PackFetch(spanCtx, "ok")
	}
	return nil
}

func (m *Materializer) fetchPackPair(ctx context.Context, repo tenant.RepoKey, packID string) (packData, idxData []byte, err error) {
	pairCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		wg      sync.WaitGroup
		packErr error
		idxErr  error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		packData, packErr = m.packs.GetPack(pairCtx, repo, packID)
		if packErr != nil {
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		idxData, idxErr = m.packs.GetIndex(pairCtx, repo, packID)
		if idxErr != nil {
			cancel()
		}
	}()
	wg.Wait()

	if packErr != nil {
		return nil, nil, packErr
	}
	if idxErr != nil {
		return nil, nil, idxErr
	}
	return packData, idxData, nil
}

// ensureReftableBareRepo ensures the disposable NVMe bare repository is
// initialized with --ref-format=reftable and the repository's object format
// (sha1 or sha256), while preserving any already-installed objects/pack files.
func (m *Materializer) ensureReftableBareRepo(ctx context.Context, repo tenant.RepoKey, repoPath string, meta ddb.RepoMeta) error {
	objFormat := meta.EffectiveObjectFormat()
	if isReftableBareRepoWithFormat(repoPath, objFormat) {
		return nil
	}

	// Remove disposable legacy files-backend metadata so `git init --bare --ref-format=reftable`
	// initializes cleanly without touching objects/ or .stravin-watermarks.json.
	for _, rel := range []string{"HEAD", "config", "refs", "packed-refs", "reftable"} {
		_ = os.RemoveAll(filepath.Join(repoPath, rel))
	}

	defaultBranch := strings.TrimSpace(meta.DefaultBranch)
	if defaultBranch == "" {
		defaultBranch = defaultBranchRef
	}

	return m.refExec.ExecInitBare(ctx, gitexec.InitBareRequest{
		Repo:          repo,
		RepoDir:       repoPath,
		ObjectFormat:  string(objFormat),
		RefFormat:     "reftable",
		DefaultBranch: defaultBranch,
	})
}

func isReftableBareRepoWithFormat(repoPath string, format ddb.ObjectFormat) bool {
	if _, err := os.Stat(filepath.Join(repoPath, "reftable", "tables.list")); err != nil {
		return false
	}
	cfgBytes, err := os.ReadFile(filepath.Join(repoPath, "config")) //nolint:gosec // path is derived from cache RepoPath
	if err != nil {
		return false
	}
	cfg := string(cfgBytes)
	if !strings.Contains(cfg, "refstorage = reftable") {
		return false
	}
	wantSHA256 := ddb.NormalizeObjectFormat(format) == ddb.ObjectFormatSHA256
	hasSHA256 := strings.Contains(cfg, "objectformat = sha256")
	return wantSHA256 == hasSHA256
}

// materializeReftableRefs synchronizes the bare repo's reftable refs and HEAD
// strictly from DynamoDB's authoritative RefState list using git plumbing
// (for-each-ref, update-ref --stdin, symbolic-ref) without touching .git/refs/*.
func (m *Materializer) materializeReftableRefs(ctx context.Context, repo tenant.RepoKey, repoPath string, refs []ddb.RefState, defaultBranch string) (bool, error) {
	if strings.TrimSpace(defaultBranch) == "" {
		defaultBranch = defaultBranchRef
	}

	desired := make(map[string]string, len(refs))
	for _, r := range refs {
		if !isSafeRefName(r.RefName) || r.OID == "" {
			continue
		}
		desired[r.RefName] = r.OID
	}

	current, err := m.refExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{
		Repo:    repo,
		RepoDir: repoPath,
	})
	if err != nil {
		return false, err
	}

	var staleNames []string
	for refName := range current {
		if _, keep := desired[refName]; !keep {
			staleNames = append(staleNames, refName)
		}
	}
	sort.Strings(staleNames)

	var updateNames []string
	for refName, oid := range desired {
		if current[refName] != oid {
			updateNames = append(updateNames, refName)
		}
	}
	sort.Strings(updateNames)

	updates := make([]gitexec.RefUpdateSpec, 0, len(staleNames)+len(updateNames))
	for _, refName := range staleNames {
		updates = append(updates, gitexec.RefUpdateSpec{RefName: refName, Delete: true})
	}
	for _, refName := range updateNames {
		updates = append(updates, gitexec.RefUpdateSpec{RefName: refName, NewOID: desired[refName]})
	}

	if len(updates) > 0 {
		if err := m.refExec.ExecUpdateRef(ctx, gitexec.UpdateRefRequest{
			Repo:    repo,
			RepoDir: repoPath,
			Updates: updates,
		}); err != nil {
			return false, err
		}
	}

	if err := m.refExec.ExecSymbolicRef(ctx, gitexec.SymbolicRefRequest{
		Repo:    repo,
		RepoDir: repoPath,
		Name:    "HEAD",
		Target:  defaultBranch,
	}); err != nil {
		return false, err
	}

	return len(updates) > 0, nil
}

// refreshAuxIndexes maintains multi-pack-index (+ reachability .bitmap) and
// commit-graph on the warm NVMe bare repository when packs or refs change.
// Failures on empty/zero-commit repositories are logged and skipped gracefully
// because auxiliary indexes are pure read-path accelerators.
func (m *Materializer) refreshAuxIndexes(ctx context.Context, repo tenant.RepoKey, repoPath string, changed, hasRefs bool) {
	packs, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
	if err != nil || len(packs) == 0 {
		return
	}

	_, midxStatErr := os.Stat(filepath.Join(repoPath, "objects", "pack", "multi-pack-index"))
	_, cgStatErr := os.Stat(filepath.Join(repoPath, "objects", "info", "commit-graph"))
	if !changed && midxStatErr == nil && (!hasRefs || cgStatErr == nil) {
		return
	}

	if err := m.gitExec.ExecRepack(ctx, gitexec.RepackRequest{
		Repo:        repo,
		RepoDir:     repoPath,
		WriteMIDX:   true,
		WriteBitmap: hasRefs,
	}); err != nil {
		if hasRefs {
			// Gracefully fall back to MIDX without bitmap for zero-commit repos (e.g. blob/tag-only).
			if fallbackErr := m.gitExec.ExecRepack(ctx, gitexec.RepackRequest{
				Repo:      repo,
				RepoDir:   repoPath,
				WriteMIDX: true,
			}); fallbackErr != nil {
				m.logger.DebugContext(ctx, "node: skipped multi-pack-index refresh",
					"repo", repo.String(), "error", fallbackErr)
			}
		} else {
			m.logger.DebugContext(ctx, "node: skipped multi-pack-index refresh",
				"repo", repo.String(), "error", err)
		}
	}

	if hasRefs {
		if err := m.gitExec.ExecRepack(ctx, gitexec.RepackRequest{
			Repo:             repo,
			RepoDir:          repoPath,
			WriteCommitGraph: true,
		}); err != nil {
			m.logger.DebugContext(ctx, "node: skipped commit-graph refresh",
				"repo", repo.String(), "error", err)
		}
	}
}

// isSafeRefName enforces git-check-ref-format rules and requires a refs/ prefix
// with at least one non-empty path component after refs/.
func isSafeRefName(refName string) bool {
	rest, ok := strings.CutPrefix(refName, "refs/")
	if !ok || rest == "" {
		return false
	}
	if strings.HasSuffix(refName, ".") || strings.Contains(refName, "..") || strings.Contains(refName, "@{") {
		return false
	}
	for i := 0; i < len(refName); i++ {
		b := refName[i]
		if b < 0x20 || b == 0x7f {
			return false
		}
		switch b {
		case ' ', '~', '^', ':', '?', '*', '[', '\\':
			return false
		}
	}
	for _, part := range strings.Split(rest, "/") {
		if part == "" || part == "@" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}
