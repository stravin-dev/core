package node

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

const defaultMaxConcurrentPackFetches = 8

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
	Telemetry                *telemetry.Provider
	Metrics                  *Metrics
	MaxConcurrentPackFetches int
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
	meta           MetadataStore
	packs          PackFetcher
	cache          *cache.Cache
	tracer         trace.Tracer
	metrics        *Metrics
	maxConcurrency int
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
	maxConc := cfg.MaxConcurrentPackFetches
	if maxConc <= 0 {
		maxConc = defaultMaxConcurrentPackFetches
	}
	return &Materializer{
		meta:           cfg.Meta,
		packs:          cfg.Packs,
		cache:          cfg.Cache,
		tracer:         tp.Tracer("stravin-node"),
		metrics:        cfg.Metrics,
		maxConcurrency: maxConc,
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
		fetched, cached, syncErr := m.syncManifestPacks(syncCtx, repo, meta)
		if syncErr != nil {
			return syncErr
		}
		fetchedCount = fetched
		cachedCount = cached

		if err := materializeDisposableRefs(repoPath, refs, meta.DefaultBranch); err != nil {
			return fmt.Errorf("node: materialize disposable refs %s: %w", repo, err)
		}
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

func (m *Materializer) syncManifestPacks(ctx context.Context, repo tenant.RepoKey, meta ddb.RepoMeta) (fetched, cached int, err error) {
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
			return 0, 0, fmt.Errorf("node: query manifest shard %d for %s: %w", shard, repo, qErr)
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
			return 0, cached, err
		}
		fetched = len(missingOrder)
	}

	// Only advance watermarks after every missing pack has been durably installed.
	for shard, maxULID := range maxULIDByShard {
		if maxULID != "" {
			m.cache.AdvanceWatermark(repo, shard, maxULID)
		}
	}

	return fetched, cached, nil
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

// materializeDisposableRefs overwrites the bare repo's local refs/ and HEAD
// strictly from DynamoDB's authoritative RefState list, deleting any stale
// local ref files that are no longer present in DynamoDB.
func materializeDisposableRefs(repoPath string, refs []ddb.RefState, defaultBranch string) error {
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

	refsDir := filepath.Join(repoPath, "refs")
	if err := os.MkdirAll(filepath.Join(refsDir, "heads"), 0o750); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(refsDir, "tags"), 0o750); err != nil {
		return err
	}

	// Remove any stale ref files on disk not present in DynamoDB using os.Root
	// to prevent symlink TOCTOU traversal outside repoPath.
	root, err := os.OpenRoot(repoPath)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()

	var staleRelPaths []string
	if err := filepath.WalkDir(refsDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}
		rel, relErr := filepath.Rel(repoPath, path)
		if relErr != nil {
			return relErr
		}
		refName := filepath.ToSlash(rel)
		if _, keep := desired[refName]; !keep {
			staleRelPaths = append(staleRelPaths, rel)
		}
		return nil
	}); err != nil {
		return err
	}
	for _, rel := range staleRelPaths {
		_ = root.Remove(rel)
	}

	// Also remove packed-refs if present so git never reads stale packed refs.
	_ = os.Remove(filepath.Join(repoPath, "packed-refs"))

	// Write each authoritative ref from DynamoDB.
	for refName, oid := range desired {
		fullPath := filepath.Join(repoPath, filepath.FromSlash(refName))
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o750); err != nil {
			return err
		}
		if err := writeFileAtomicLocal(fullPath, []byte(oid+"\n")); err != nil {
			return err
		}
	}

	// Update HEAD symref to match META.DefaultBranch.
	return writeFileAtomicLocal(filepath.Join(repoPath, "HEAD"), []byte("ref: "+defaultBranch+"\n"))
}

func isSafeRefName(refName string) bool {
	if !strings.HasPrefix(refName, "refs/") {
		return false
	}
	if strings.Contains(refName, "..") || strings.ContainsAny(refName, "\\\x00\r\n") {
		return false
	}
	return true
}

func writeFileAtomicLocal(targetPath string, data []byte) error {
	dir := filepath.Dir(targetPath)
	base := filepath.Base(targetPath)
	tmp, err := os.CreateTemp(dir, ".tmp-"+base+"-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, targetPath); err != nil {
		return err
	}
	cleanup = false
	return nil
}
