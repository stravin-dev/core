package cache

import (
	"fmt"
	"os"
	"path/filepath"

	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/tenant"
)

// Compile-time assertion that RepoBaseResolver satisfies pack.BaseResolver.
var _ pack.BaseResolver = (*RepoBaseResolver)(nil)

// RepoBaseResolver is a GoF Adapter exposing a repository's warm NVMe .pack/.idx
// files as a pack.BaseResolver for thin-pack completion and object fallback reads.
type RepoBaseResolver struct {
	cache  *Cache
	repo   tenant.RepoKey
	limits pack.Limits
}

// ResolveBase searches the repository's materialized .idx/.pack files for oid
// while holding a short reader lease so eviction cannot unlink packs mid-read.
func (r *RepoBaseResolver) ResolveBase(oid pack.OID) (pack.ObjectType, []byte, error) {
	handle, err := r.cache.Pin(r.repo)
	if err != nil {
		return 0, nil, fmt.Errorf("%s: %w", oid, pack.ErrObjectNotFound)
	}
	defer handle.Release()

	packIDs := r.cache.CachedPackIDs(r.repo)
	packDir := filepath.Join(handle.Path(), "objects", "pack")
	algo := oid.Algorithm()

	for _, packID := range packIDs {
		idx, err := r.loadIdx(packDir, packID, algo)
		if err != nil {
			continue
		}
		if _, _, ok := idx.Lookup(oid); !ok {
			continue
		}

		obj, err := r.readObjectFromPack(packDir, packID, oid)
		if err != nil {
			return 0, nil, err
		}
		return obj.Type, obj.Data, nil
	}

	return 0, nil, fmt.Errorf("%s: %w", oid, pack.ErrObjectNotFound)
}

func (r *RepoBaseResolver) loadIdx(packDir, packID string, algo pack.HashAlgorithm) (*pack.Idx, error) {
	if cached, ok := r.cache.getCachedIdx(r.repo, packID); ok && cached.Algorithm().Name() == algo.Name() {
		return cached, nil
	}

	idxPath := filepath.Join(packDir, packFileBase(packID)+".idx")
	data, err := os.ReadFile(idxPath) //nolint:gosec // path is derived from validated RepoKey and packID
	if err != nil {
		return nil, err
	}
	idx, err := pack.ParseIdxWithAlgorithm(data, algo)
	if err != nil {
		return nil, err
	}
	r.cache.putCachedIdx(r.repo, packID, idx)
	return idx, nil
}

func (r *RepoBaseResolver) readObjectFromPack(packDir, packID string, oid pack.OID) (*pack.Object, error) {
	packPath := filepath.Join(packDir, packFileBase(packID)+".pack")
	f, err := os.Open(packPath) //nolint:gosec // path is derived from validated RepoKey and packID
	if err != nil {
		return nil, fmt.Errorf("open pack %s: %w", packID, err)
	}
	defer func() {
		_ = f.Close()
	}()

	parsed, err := pack.Parse(f, pack.ParseOptions{
		Limits:        r.limits,
		HashAlgorithm: oid.Algorithm(),
	})
	if err != nil {
		return nil, fmt.Errorf("parse pack %s: %w", packID, err)
	}
	obj, ok := parsed.Objects[oid]
	if !ok {
		return nil, fmt.Errorf("%s in pack %s: %w", oid, packID, pack.ErrObjectNotFound)
	}
	return obj, nil
}
