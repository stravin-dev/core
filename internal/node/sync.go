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
	defaultAuxIndexRefreshTimeout   = 30 * time.Second
	defaultAuxIndexRefreshInterval  = 2 * time.Second
	maxImmediateAuxRetries          = 2
)

// MetadataStore is the narrow DynamoDB read seam used by Materializer.
// *ddb.Repository satisfies this interface directly.
type MetadataStore interface {
	GetMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error)
	ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]ddb.RefState, error)
	QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]ddb.PackManifest, error)
}

// RefGetter is an optional narrow DynamoDB single-ref read seam used by
// receive-pack update/delete pushes to avoid full ListRefs scans.
// *ddb.Repository satisfies this interface directly.
type RefGetter interface {
	GetRef(ctx context.Context, repo tenant.RepoKey, refName string, refShards int) (ddb.RefState, error)
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
	// AuxIndexRefreshInterval sets the minimum cooldown between background
	// MIDX/bitmap/commit-graph refreshes per repository. Zero defaults to 2s;
	// negative values disable the cooldown (useful for deterministic tests).
	AuxIndexRefreshInterval time.Duration
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

type repoSyncSnapshot struct {
	meta ddb.RepoMeta
	refs []ddb.RefState
}

type reftableSyncState struct {
	repoPath   string
	head       string
	refs       map[string]string
	tablesList string
}

type auxRepoState struct {
	running   bool
	dirty     bool
	changed   bool
	hasRefs   bool
	immediate bool
	repoPath  string
	lastRun   time.Time
	doneCh    chan struct{}
	wakeCh    chan struct{}
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
	auxRefreshInterval     time.Duration
	refFlights             *refLoadFlightGroup

	reconcileMu   sync.Mutex
	lastReconcile map[tenant.RepoKey]time.Time

	lastSyncMu sync.Mutex
	lastSync   map[tenant.RepoKey]repoSyncSnapshot

	refStateMu sync.Mutex
	refState   map[tenant.RepoKey]reftableSyncState

	auxMu     sync.Mutex
	auxClosed bool
	auxStates map[tenant.RepoKey]*auxRepoState
	auxCtx    context.Context
	auxCancel context.CancelFunc
	auxWG     sync.WaitGroup
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
	auxInterval := cfg.AuxIndexRefreshInterval
	switch {
	case auxInterval == 0:
		auxInterval = defaultAuxIndexRefreshInterval
	case auxInterval < 0:
		auxInterval = 0
	}
	auxCtx, auxCancel := context.WithCancel(context.Background())
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
		auxRefreshInterval:     auxInterval,
		refFlights:             newRefLoadFlightGroup(),
		lastReconcile:          make(map[tenant.RepoKey]time.Time),
		lastSync:               make(map[tenant.RepoKey]repoSyncSnapshot),
		refState:               make(map[tenant.RepoKey]reftableSyncState),
		auxStates:              make(map[tenant.RepoKey]*auxRepoState),
		auxCtx:                 auxCtx,
		auxCancel:              auxCancel,
	}, nil
}

// Close cancels pending background auxiliary index refreshes and waits for all
// in-flight refresh goroutines to exit. It is safe to call multiple times.
func (m *Materializer) Close() {
	m.auxMu.Lock()
	if !m.auxClosed {
		m.auxClosed = true
		m.auxCancel()
	}
	m.auxMu.Unlock()
	m.auxWG.Wait()
}

// WaitAuxIndexes blocks until all currently scheduled or running background
// auxiliary index refreshes complete, or ctx is canceled.
func (m *Materializer) WaitAuxIndexes(ctx context.Context) error {
	for {
		m.auxMu.Lock()
		var waitCh chan struct{}
		for _, st := range m.auxStates {
			if st.running && st.doneCh != nil {
				waitCh = st.doneCh
				break
			}
		}
		m.auxMu.Unlock()
		if waitCh == nil {
			return nil
		}
		select {
		case <-waitCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// LoadRepoMeta reads authoritative repository META strictly from DynamoDB
// without querying ref shards, local disk, or S3.
func (m *Materializer) LoadRepoMeta(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, error) {
	if err := repo.Validate(); err != nil {
		return ddb.RepoMeta{}, err
	}

	meta, err := m.meta.GetMeta(ctx, repo)
	if err != nil {
		return ddb.RepoMeta{}, fmt.Errorf("node: get meta %s: %w", repo, err)
	}
	return meta, nil
}

// LoadAuthoritativeRefs reads META and all ref shards (RS#{s}) strictly from
// DynamoDB without touching local disk or S3. Concurrent callers for the same
// repository coalesce via linearizable generation-batched singleflight.
func (m *Materializer) LoadAuthoritativeRefs(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, []ddb.RefState, error) {
	if err := repo.Validate(); err != nil {
		return ddb.RepoMeta{}, nil, err
	}
	return m.refFlights.Do(ctx, repo, func(flightCtx context.Context) (ddb.RepoMeta, []ddb.RefState, error) {
		return m.loadAuthoritativeRefsUncached(flightCtx, repo)
	})
}

func (m *Materializer) loadAuthoritativeRefsUncached(ctx context.Context, repo tenant.RepoKey) (ddb.RepoMeta, []ddb.RefState, error) {
	meta, err := m.LoadRepoMeta(ctx, repo)
	if err != nil {
		return ddb.RepoMeta{}, nil, err
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

// LoadAuthoritativeRefsForCommands loads META and the authoritative ref state
// needed to validate cmds. When no command creates a new ref and the metadata
// store implements RefGetter, it fetches only the specific refs in cmds via
// GetRef (avoiding a full ListRefs scan). The returned bool reports whether
// the full repository ref set was loaded (for ref/directory collision checks).
func (m *Materializer) LoadAuthoritativeRefsForCommands(
	ctx context.Context,
	repo tenant.RepoKey,
	cmds []ddb.RefUpdate,
) (ddb.RepoMeta, []ddb.RefState, bool, error) {
	anyCreate := false
	for _, c := range cmds {
		if c.IsCreate() {
			anyCreate = true
			break
		}
	}

	getter, hasGetRef := m.meta.(RefGetter)
	if anyCreate || !hasGetRef {
		meta, refs, err := m.LoadAuthoritativeRefs(ctx, repo)
		return meta, refs, true, err
	}

	meta, err := m.LoadRepoMeta(ctx, repo)
	if err != nil {
		return ddb.RepoMeta{}, nil, false, err
	}
	if len(cmds) == 0 {
		return meta, nil, false, nil
	}

	if len(cmds) == 1 {
		rs, getErr := getter.GetRef(ctx, repo, cmds[0].RefName, meta.RefShards)
		if getErr != nil {
			if errors.Is(getErr, ddb.ErrNotFound) {
				return meta, nil, false, nil
			}
			return ddb.RepoMeta{}, nil, false, fmt.Errorf("node: get ref %s for %s: %w", cmds[0].RefName, repo, getErr)
		}
		return meta, []ddb.RefState{rs}, false, nil
	}

	qCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make([]ddb.RefState, len(cmds))
	found := make([]bool, len(cmds))
	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	for i, c := range cmds {
		i, refName := i, c.RefName
		wg.Add(1)
		go func() {
			defer wg.Done()
			rs, getErr := getter.GetRef(qCtx, repo, refName, meta.RefShards)
			if getErr != nil {
				if errors.Is(getErr, ddb.ErrNotFound) {
					return
				}
				errMu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("node: get ref %s for %s: %w", refName, repo, getErr)
				}
				errMu.Unlock()
				cancel()
				return
			}
			results[i] = rs
			found[i] = true
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return ddb.RepoMeta{}, nil, false, firstErr
	}

	refs := make([]ddb.RefState, 0, len(cmds))
	for i, ok := range found {
		if ok {
			refs = append(refs, results[i])
		}
	}
	return meta, refs, false, nil
}

// SyncAndPin runs the full read-path synchronization pipeline under linearizable
// singleflight cache.Materialize (returning a pinned RepoHandle):
//  1. Load authoritative META + RS#{s} refs from DynamoDB inside the serialized
//     flight so a prior snapshot can never overwrite newer refs.
//  2. Query manifest shards MS#{m} incrementally using per-shard last_seen_ulid watermarks.
//  3. Concurrently fetch any missing ACTIVE .pack/.idx objects from S3 and install
//     them into the warm NVMe cache.
//  4. Advance manifest watermarks only after all missing packs are installed.
//  5. Materialize disposable local bare-repo refs strictly from DynamoDB.
func (m *Materializer) SyncAndPin(ctx context.Context, repo tenant.RepoKey) (SyncResult, error) {
	var (
		meta         ddb.RepoMeta
		refs         []ddb.RefState
		fetchedCount int
		cachedCount  int
		ranSync      bool
	)

	handle, err := m.cache.Materialize(ctx, repo, func(syncCtx context.Context, repoPath string) error {
		loadedMeta, loadedRefs, loadErr := m.LoadAuthoritativeRefs(syncCtx, repo)
		if loadErr != nil {
			return loadErr
		}

		fetched, cached, pruned, syncErr := m.syncManifestPacks(syncCtx, repo, loadedMeta)
		if syncErr != nil {
			return syncErr
		}

		reinitialized, err := m.ensureReftableBareRepo(syncCtx, repo, repoPath, loadedMeta)
		if err != nil {
			return fmt.Errorf("node: configure reftable bare repo %s: %w", repo, err)
		}
		refsChanged, err := m.materializeReftableRefs(syncCtx, repo, repoPath, loadedRefs, loadedMeta.DefaultBranch, reinitialized)
		if err != nil {
			return fmt.Errorf("node: materialize reftable refs %s: %w", repo, err)
		}
		m.scheduleAuxIndexRefresh(repo, repoPath, fetched > 0 || pruned > 0 || refsChanged, len(loadedRefs) > 0)

		m.recordLastSync(repo, loadedMeta, loadedRefs)
		meta = loadedMeta
		refs = loadedRefs
		fetchedCount = fetched
		cachedCount = cached
		ranSync = true
		return nil
	})
	if err != nil {
		return SyncResult{}, err
	}

	if !ranSync {
		meta, refs = m.loadLastSync(repo)
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

func (m *Materializer) recordLastSync(repo tenant.RepoKey, meta ddb.RepoMeta, refs []ddb.RefState) {
	cp := make([]ddb.RefState, len(refs))
	copy(cp, refs)
	m.lastSyncMu.Lock()
	m.lastSync[repo] = repoSyncSnapshot{meta: meta, refs: cp}
	m.lastSyncMu.Unlock()
}

func (m *Materializer) loadLastSync(repo tenant.RepoKey) (ddb.RepoMeta, []ddb.RefState) {
	m.lastSyncMu.Lock()
	snap := m.lastSync[repo]
	m.lastSyncMu.Unlock()
	cp := make([]ddb.RefState, len(snap.refs))
	copy(cp, snap.refs)
	return snap.meta, cp
}

func (m *Materializer) queryManifestShardWithTrace(ctx context.Context, repo tenant.RepoKey, shard int) ([]ddb.PackManifest, error) {
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
		return nil, fmt.Errorf("node: query manifest shard %d for %s: %w", shard, repo, qErr)
	}
	qSpan.End()
	return entries, nil
}

func (m *Materializer) syncManifestPacks(ctx context.Context, repo tenant.RepoKey, meta ddb.RepoMeta) (fetched, cached, pruned int, err error) {
	shardEntries := make([][]ddb.PackManifest, meta.ManifestShards)
	if meta.ManifestShards <= 1 {
		for shard := 0; shard < meta.ManifestShards; shard++ {
			entries, qErr := m.queryManifestShardWithTrace(ctx, repo, shard)
			if qErr != nil {
				return 0, 0, 0, qErr
			}
			shardEntries[shard] = entries
		}
	} else {
		qCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		var (
			wg       sync.WaitGroup
			errMu    sync.Mutex
			firstErr error
		)
		for shard := 0; shard < meta.ManifestShards; shard++ {
			shard := shard
			wg.Add(1)
			go func() {
				defer wg.Done()
				entries, qErr := m.queryManifestShardWithTrace(qCtx, repo, shard)
				if qErr != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = qErr
					}
					errMu.Unlock()
					cancel()
					return
				}
				shardEntries[shard] = entries
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return 0, 0, 0, firstErr
		}
	}

	maxULIDByShard := make(map[int]string, meta.ManifestShards)
	missingSet := make(map[string]struct{})
	var missingOrder []string

	for shard := 0; shard < meta.ManifestShards; shard++ {
		for _, pm := range shardEntries[shard] {
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
func (m *Materializer) ensureReftableBareRepo(ctx context.Context, repo tenant.RepoKey, repoPath string, meta ddb.RepoMeta) (bool, error) {
	objFormat := meta.EffectiveObjectFormat()
	if isReftableBareRepoWithFormat(repoPath, objFormat) {
		return false, nil
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

	if err := m.refExec.ExecInitBare(ctx, gitexec.InitBareRequest{
		Repo:          repo,
		RepoDir:       repoPath,
		ObjectFormat:  string(objFormat),
		RefFormat:     "reftable",
		DefaultBranch: defaultBranch,
	}); err != nil {
		m.deleteRefState(repo)
		return false, err
	}
	return true, nil
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

func readReftableTablesList(repoPath string) string {
	b, err := os.ReadFile(filepath.Join(repoPath, "reftable", "tables.list")) //nolint:gosec // path is derived from cache RepoPath
	if err != nil {
		return ""
	}
	return string(b)
}

func (m *Materializer) deleteRefState(repo tenant.RepoKey) {
	m.refStateMu.Lock()
	delete(m.refState, repo)
	m.refStateMu.Unlock()
}

// materializeReftableRefs synchronizes the bare repo's reftable refs and HEAD
// strictly from DynamoDB's authoritative RefState list using git plumbing
// (for-each-ref, update-ref --stdin, symbolic-ref) without touching .git/refs/*.
func (m *Materializer) materializeReftableRefs(ctx context.Context, repo tenant.RepoKey, repoPath string, refs []ddb.RefState, defaultBranch string, reinitialized bool) (bool, error) {
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

	var (
		current          map[string]string
		materializedHead string
		haveState        bool
	)
	if !reinitialized {
		curTables := readReftableTablesList(repoPath)
		m.refStateMu.Lock()
		if st, ok := m.refState[repo]; ok && st.repoPath == repoPath && st.tablesList == curTables {
			current = st.refs
			materializedHead = st.head
			haveState = true
		}
		m.refStateMu.Unlock()
	}

	if !haveState {
		var err error
		current, err = m.refExec.ExecForEachRef(ctx, gitexec.ForEachRefRequest{
			Repo:    repo,
			RepoDir: repoPath,
		})
		if err != nil {
			m.deleteRefState(repo)
			return false, err
		}
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
			m.deleteRefState(repo)
			return false, err
		}
	}

	if !haveState || materializedHead != defaultBranch {
		if err := m.refExec.ExecSymbolicRef(ctx, gitexec.SymbolicRefRequest{
			Repo:    repo,
			RepoDir: repoPath,
			Name:    "HEAD",
			Target:  defaultBranch,
		}); err != nil {
			m.deleteRefState(repo)
			return false, err
		}
	}

	newTables := readReftableTablesList(repoPath)
	m.refStateMu.Lock()
	m.refState[repo] = reftableSyncState{
		repoPath:   repoPath,
		head:       defaultBranch,
		refs:       desired,
		tablesList: newTables,
	}
	m.refStateMu.Unlock()

	return len(updates) > 0, nil
}

// scheduleAuxIndexRefresh enqueues a coalesced, debounced background refresh of
// multi-pack-index (+ .bitmap) and commit-graph for repo without blocking
// SyncAndPin. When multi-pack-index is absent on disk, the refresh runs
// immediately without waiting for the cooldown interval.
func (m *Materializer) scheduleAuxIndexRefresh(repo tenant.RepoKey, repoPath string, changed, hasRefs bool) {
	_, midxStatErr := os.Stat(filepath.Join(repoPath, "objects", "pack", "multi-pack-index"))
	_, cgStatErr := os.Stat(filepath.Join(repoPath, "objects", "info", "commit-graph"))
	if !changed && midxStatErr == nil && (!hasRefs || cgStatErr == nil) {
		return
	}
	midxMissing := midxStatErr != nil

	m.auxMu.Lock()
	if m.auxClosed {
		m.auxMu.Unlock()
		return
	}
	st, ok := m.auxStates[repo]
	if !ok {
		st = &auxRepoState{wakeCh: make(chan struct{}, 1)}
		m.auxStates[repo] = st
	}
	st.repoPath = repoPath
	st.hasRefs = hasRefs
	st.changed = st.changed || changed
	st.immediate = st.immediate || midxMissing
	st.dirty = true
	if st.running {
		if midxMissing {
			select {
			case st.wakeCh <- struct{}{}:
			default:
			}
		}
		m.auxMu.Unlock()
		return
	}
	st.running = true
	st.doneCh = make(chan struct{})
	m.auxWG.Add(1)
	m.auxMu.Unlock()

	go m.runAuxIndexLoop(repo, st)
}

func (m *Materializer) runAuxIndexLoop(repo tenant.RepoKey, st *auxRepoState) {
	defer m.auxWG.Done()
	immediateRetries := 0
	for {
		m.auxMu.Lock()
		if m.auxClosed || !st.dirty {
			st.running = false
			st.dirty = false
			close(st.doneCh)
			st.doneCh = nil
			m.auxMu.Unlock()
			return
		}
		var wait time.Duration
		if !st.immediate && !st.lastRun.IsZero() && m.auxRefreshInterval > 0 {
			if elapsed := time.Since(st.lastRun); elapsed < m.auxRefreshInterval {
				wait = m.auxRefreshInterval - elapsed
			}
		}
		if wait <= 0 {
			repoPath := st.repoPath
			changed := st.changed
			hasRefs := st.hasRefs
			st.dirty = false
			st.changed = false
			st.immediate = false
			m.auxMu.Unlock()

			needsRetry := m.runPinnedAuxIndexRefresh(repo, repoPath, changed, hasRefs)

			m.auxMu.Lock()
			st.lastRun = time.Now()
			if needsRetry && !m.auxClosed {
				st.dirty = true
				st.changed = true
				if immediateRetries < maxImmediateAuxRetries {
					immediateRetries++
					st.immediate = true
				}
			} else {
				immediateRetries = 0
			}
			m.auxMu.Unlock()
			continue
		}
		wakeCh := st.wakeCh
		m.auxMu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-m.auxCtx.Done():
			timer.Stop()
			m.auxMu.Lock()
			st.running = false
			st.dirty = false
			close(st.doneCh)
			st.doneCh = nil
			m.auxMu.Unlock()
			return
		case <-wakeCh:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (m *Materializer) runPinnedAuxIndexRefresh(repo tenant.RepoKey, fallbackPath string, changed, hasRefs bool) bool {
	handle, err := m.cache.Pin(repo)
	if err != nil {
		return false
	}
	defer handle.Release()
	repoPath := handle.Path()
	if repoPath == "" {
		repoPath = fallbackPath
	}
	return m.executeAuxIndexRefresh(m.auxCtx, repo, repoPath, changed, hasRefs)
}

// refreshAuxIndexes maintains multi-pack-index (+ reachability .bitmap) and
// commit-graph on the warm NVMe bare repository when packs or refs change.
// It detaches from caller cancellation with a bounded timeout so client
// disconnects cannot abort writes mid-flight, removes stale lockfiles before
// and on failure, and surfaces unexpected failures at Warn level + OTel counter.
func (m *Materializer) refreshAuxIndexes(ctx context.Context, repo tenant.RepoKey, repoPath string, changed, hasRefs bool) {
	m.executeAuxIndexRefresh(context.WithoutCancel(ctx), repo, repoPath, changed, hasRefs)
}

func (m *Materializer) executeAuxIndexRefresh(ctx context.Context, repo tenant.RepoKey, repoPath string, changed, hasRefs bool) (needsMIDXRetry bool) {
	activeIdxNames, midxGen := m.cache.ActivePackIdxNames(repo)
	hasActivePacks := len(activeIdxNames) > 0
	if !hasActivePacks {
		if len(m.cache.CachedPackIDs(repo)) > 0 {
			// All cached packs are pending prune; skip MIDX until new packs arrive.
			return false
		}
		packs, err := filepath.Glob(filepath.Join(repoPath, "objects", "pack", "*.pack"))
		if err != nil || len(packs) == 0 {
			return false
		}
	}

	_, midxStatErr := os.Stat(filepath.Join(repoPath, "objects", "pack", "multi-pack-index"))
	_, cgStatErr := os.Stat(filepath.Join(repoPath, "objects", "info", "commit-graph"))
	needMIDX := changed || midxStatErr != nil
	needCG := hasRefs && (changed || cgStatErr != nil)
	if !needMIDX && !needCG {
		return false
	}

	auxCtx, cancel := context.WithTimeout(ctx, defaultAuxIndexRefreshTimeout)
	defer cancel()

	midxLockPath := filepath.Join(repoPath, "objects", "pack", "multi-pack-index.lock")
	cgLockPath := filepath.Join(repoPath, "objects", "info", "commit-graph.lock")

	if needMIDX {
		m.removeStaleAuxLock(auxCtx, midxLockPath)
		midxBuilt := true
		if err := m.gitExec.ExecRepack(auxCtx, gitexec.RepackRequest{
			Repo:        repo,
			RepoDir:     repoPath,
			WriteMIDX:   true,
			WriteBitmap: hasRefs,
			StdinPacks:  activeIdxNames,
		}); err != nil {
			m.removeStaleAuxLock(auxCtx, midxLockPath)
			if hasRefs {
				// Gracefully fall back to MIDX without bitmap for zero-commit repos (e.g. blob/tag-only).
				if fallbackErr := m.gitExec.ExecRepack(auxCtx, gitexec.RepackRequest{
					Repo:       repo,
					RepoDir:    repoPath,
					WriteMIDX:  true,
					StdinPacks: activeIdxNames,
				}); fallbackErr != nil {
					midxBuilt = false
					m.removeStaleAuxLock(auxCtx, midxLockPath)
					m.logger.WarnContext(auxCtx, "node: failed to refresh multi-pack-index",
						"repo", repo.String(), "error", fallbackErr)
					if m.metrics != nil {
						m.metrics.RecordAuxIndexRefreshFailure(auxCtx, "midx")
					}
				}
			} else {
				midxBuilt = false
				m.logger.WarnContext(auxCtx, "node: failed to refresh multi-pack-index",
					"repo", repo.String(), "error", err)
				if m.metrics != nil {
					m.metrics.RecordAuxIndexRefreshFailure(auxCtx, "midx")
				}
			}
		}
		if midxBuilt && hasActivePacks {
			if !m.cache.MarkMIDXBuilt(repo, midxGen) {
				needsMIDXRetry = true
			}
		}
	}

	if needCG {
		m.removeStaleAuxLock(auxCtx, cgLockPath)
		if err := m.gitExec.ExecRepack(auxCtx, gitexec.RepackRequest{
			Repo:             repo,
			RepoDir:          repoPath,
			WriteCommitGraph: true,
		}); err != nil {
			m.removeStaleAuxLock(auxCtx, cgLockPath)
			m.logger.WarnContext(auxCtx, "node: failed to refresh commit-graph",
				"repo", repo.String(), "error", err)
			if m.metrics != nil {
				m.metrics.RecordAuxIndexRefreshFailure(auxCtx, "commit_graph")
			}
		}
	}
	return needsMIDXRetry
}

func (m *Materializer) removeStaleAuxLock(ctx context.Context, path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.logger.WarnContext(ctx, "node: failed to remove stale aux index lock",
			"path", path, "error", err)
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
