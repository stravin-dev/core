package node

import (
	"context"
	"errors"

	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

// WasmBlobStore is the narrow S3 seam for fetching content-addressed Wasm hook binaries.
// *s3.Store satisfies this interface directly.
type WasmBlobStore interface {
	GetWasmHook(ctx context.Context, wasmSHA256 string) ([]byte, error)
}

// HookPolicyStore is the narrow DynamoDB seam for loading effective Tier 1 Wasm hooks.
// *ddb.Repository satisfies this interface directly.
type HookPolicyStore interface {
	GetEffectiveHooks(ctx context.Context, repo tenant.RepoKey, orgID string) ([]ddb.HookConfig, error)
}

// s3WasmLoader adapts WasmBlobStore to wasm.WasmLoader (GoF Adapter).
type s3WasmLoader struct {
	store WasmBlobStore
}

var _ wasm.WasmLoader = (*s3WasmLoader)(nil)

// NewS3WasmLoader wraps an S3 store as a wasm.WasmLoader.
func NewS3WasmLoader(store WasmBlobStore) wasm.WasmLoader {
	return &s3WasmLoader{store: store}
}

func (l *s3WasmLoader) LoadWasm(ctx context.Context, sha256Hex string) ([]byte, error) {
	if l.store == nil {
		return nil, wasm.ErrMissingArtifact
	}
	return l.store.GetWasmHook(ctx, sha256Hex)
}

// ddbHookConfigStore adapts HookPolicyStore to wasm.HookConfigStore (GoF Adapter).
type ddbHookConfigStore struct {
	store HookPolicyStore
}

var _ wasm.HookConfigStore = (*ddbHookConfigStore)(nil)

// NewDDBHookConfigStore wraps a DynamoDB repository as a wasm.HookConfigStore.
func NewDDBHookConfigStore(store HookPolicyStore) wasm.HookConfigStore {
	return &ddbHookConfigStore{store: store}
}

func (s *ddbHookConfigStore) ListHooks(ctx context.Context, tenantID, repoID, orgID string) ([]wasm.HookConfig, error) {
	if s.store == nil {
		return nil, nil
	}
	rk, err := tenant.NewRepoKey(tenantID, repoID)
	if err != nil {
		return nil, err
	}
	ddbHooks, err := s.store.GetEffectiveHooks(ctx, rk, orgID)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]wasm.HookConfig, 0, len(ddbHooks))
	for _, h := range ddbHooks {
		out = append(out, wasm.HookConfig{
			HookID:        h.HookID,
			WasmSHA256:    h.WasmSHA256,
			RefGlob:       h.RefGlob,
			TimeoutMS:     h.TimeoutMS,
			MemoryLimitMB: h.MemoryLimitMB,
			FailMode:      wasm.FailMode(h.FailMode),
		})
	}
	return out, nil
}

// packObjectReaderAdapter adapts pack.ObjectReader to wasm.ObjectReader (GoF Adapter).
type packObjectReaderAdapter struct {
	inner pack.ObjectReader
}

var (
	_ wasm.ObjectReader          = (*packObjectReaderAdapter)(nil)
	_ wasm.ContextDiffTreeReader = (*packObjectReaderAdapter)(nil)
	_ wasm.DualHashObjectReader  = (*packObjectReaderAdapter)(nil)
)

type packContextDiffTreeReader interface {
	DiffTreeContext(ctx context.Context, oldTreeOID, newTreeOID pack.OID, limit uint32) ([]pack.TreeChange, error)
}

// NewPackObjectReaderAdapter wraps a pack.ObjectReader for HookHostABI consumption.
func NewPackObjectReaderAdapter(inner pack.ObjectReader) wasm.ObjectReader {
	if inner == nil {
		return nil
	}
	return &packObjectReaderAdapter{inner: inner}
}

func (a *packObjectReaderAdapter) ListNewCommits(oldOID, newOID [20]byte, limit uint32) ([]wasm.CommitMeta, error) {
	commits, err := a.inner.ListNewCommits(pack.OIDFromSHA1(oldOID), pack.OIDFromSHA1(newOID), limit)
	if err != nil {
		return nil, err
	}
	return convertCommits(commits), nil
}

func (a *packObjectReaderAdapter) ListNewCommitsByHex(oldOIDHex, newOIDHex string, limit uint32) ([]wasm.CommitMeta, error) {
	oldOID, err := pack.ParseOID(oldOIDHex)
	if err != nil {
		return nil, err
	}
	newOID, err := pack.ParseOID(newOIDHex)
	if err != nil {
		return nil, err
	}
	commits, err := a.inner.ListNewCommits(oldOID, newOID, limit)
	if err != nil {
		return nil, err
	}
	return convertCommits(commits), nil
}

func (a *packObjectReaderAdapter) DiffTree(oldTreeOID, newTreeOID [20]byte, limit uint32) ([]wasm.TreeChange, error) {
	changes, err := a.inner.DiffTree(pack.OIDFromSHA1(oldTreeOID), pack.OIDFromSHA1(newTreeOID), limit)
	if err != nil {
		return nil, err
	}
	return convertTreeChanges(changes), nil
}

func (a *packObjectReaderAdapter) DiffTreeContext(ctx context.Context, oldTreeOID, newTreeOID [20]byte, limit uint32) ([]wasm.TreeChange, error) {
	return a.diffTreeOIDs(ctx, pack.OIDFromSHA1(oldTreeOID), pack.OIDFromSHA1(newTreeOID), limit)
}

func (a *packObjectReaderAdapter) DiffTreeByHex(ctx context.Context, oldTreeOIDHex, newTreeOIDHex string, limit uint32) ([]wasm.TreeChange, error) {
	oldOID, err := pack.ParseOID(oldTreeOIDHex)
	if err != nil {
		return nil, err
	}
	newOID, err := pack.ParseOID(newTreeOIDHex)
	if err != nil {
		return nil, err
	}
	return a.diffTreeOIDs(ctx, oldOID, newOID, limit)
}

func (a *packObjectReaderAdapter) diffTreeOIDs(ctx context.Context, oldTreeOID, newTreeOID pack.OID, limit uint32) ([]wasm.TreeChange, error) {
	var (
		changes []pack.TreeChange
		err     error
	)
	if ctxReader, ok := a.inner.(packContextDiffTreeReader); ok {
		changes, err = ctxReader.DiffTreeContext(ctx, oldTreeOID, newTreeOID, limit)
	} else {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		changes, err = a.inner.DiffTree(oldTreeOID, newTreeOID, limit)
	}
	if err != nil {
		return nil, err
	}
	return convertTreeChanges(changes), nil
}

func convertCommits(commits []pack.CommitMeta) []wasm.CommitMeta {
	out := make([]wasm.CommitMeta, 0, len(commits))
	for _, c := range commits {
		parents := make([][20]byte, 0, len(c.Parents))
		parentHexes := make([]string, 0, len(c.Parents))
		for _, p := range c.Parents {
			if raw, ok := p.SHA1Bytes(); ok {
				parents = append(parents, raw)
			}
			parentHexes = append(parentHexes, p.String())
		}
		oidRaw, _ := c.OID.SHA1Bytes()
		treeRaw, _ := c.TreeOID.SHA1Bytes()
		out = append(out, wasm.CommitMeta{
			OID:         oidRaw,
			TreeOID:     treeRaw,
			Parents:     parents,
			OIDHex:      c.OID.String(),
			TreeOIDHex:  c.TreeOID.String(),
			ParentHexes: parentHexes,
			Author: wasm.Signature{
				Name:  c.Author.Name,
				Email: c.Author.Email,
				When:  c.Author.When,
				Raw:   c.Author.Raw,
			},
			Committer: wasm.Signature{
				Name:  c.Committer.Name,
				Email: c.Committer.Email,
				When:  c.Committer.When,
				Raw:   c.Committer.Raw,
			},
			Message: c.Message,
		})
	}
	return out
}

func convertTreeChanges(changes []pack.TreeChange) []wasm.TreeChange {
	out := make([]wasm.TreeChange, 0, len(changes))
	for _, ch := range changes {
		oldRaw, _ := ch.OldOID.SHA1Bytes()
		newRaw, _ := ch.NewOID.SHA1Bytes()
		out = append(out, wasm.TreeChange{
			Path:      ch.Path,
			Kind:      wasm.ChangeKind(ch.Kind),
			OldMode:   ch.OldMode,
			NewMode:   ch.NewMode,
			OldOID:    oldRaw,
			NewOID:    newRaw,
			OldOIDHex: ch.OldOID.String(),
			NewOIDHex: ch.NewOID.String(),
			OldSize:   ch.OldSize,
			NewSize:   ch.NewSize,
		})
	}
	return out
}

func (a *packObjectReaderAdapter) ReadBlobChunk(blobOID [20]byte, offset uint64, maxBytes uint32) ([]byte, error) {
	return a.inner.ReadBlobChunk(pack.OIDFromSHA1(blobOID), offset, maxBytes)
}

func (a *packObjectReaderAdapter) ReadBlobChunkByHex(blobOIDHex string, offset uint64, maxBytes uint32) ([]byte, error) {
	oid, err := pack.ParseOID(blobOIDHex)
	if err != nil {
		return nil, err
	}
	return a.inner.ReadBlobChunk(oid, offset, maxBytes)
}
