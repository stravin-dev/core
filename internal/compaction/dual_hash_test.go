package compaction

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

type memPackObjectStore struct {
	packs   map[string][]byte
	idxs    map[string][]byte
	midxs   map[string][]byte
	graphs  map[string][]byte
	bundles map[string][]byte
}

func newMemPackObjectStore() *memPackObjectStore {
	return &memPackObjectStore{
		packs:   make(map[string][]byte),
		idxs:    make(map[string][]byte),
		midxs:   make(map[string][]byte),
		graphs:  make(map[string][]byte),
		bundles: make(map[string][]byte),
	}
}

func (m *memPackObjectStore) PutPack(_ context.Context, _ tenant.RepoKey, packSHA256 string, packData, idxData []byte) error {
	m.packs[packSHA256] = append([]byte(nil), packData...)
	m.idxs[packSHA256] = append([]byte(nil), idxData...)
	return nil
}

func (m *memPackObjectStore) GetPack(_ context.Context, _ tenant.RepoKey, packSHA256 string) ([]byte, error) {
	b, ok := m.packs[packSHA256]
	if !ok {
		return nil, fmt.Errorf("pack %s not found", packSHA256)
	}
	return b, nil
}

func (m *memPackObjectStore) GetIndex(_ context.Context, _ tenant.RepoKey, packSHA256 string) ([]byte, error) {
	b, ok := m.idxs[packSHA256]
	if !ok {
		return nil, fmt.Errorf("idx %s not found", packSHA256)
	}
	return b, nil
}

func (m *memPackObjectStore) PutAuxMIDX(_ context.Context, _ tenant.RepoKey, _, epoch string, data []byte) error {
	m.midxs[epoch] = append([]byte(nil), data...)
	return nil
}

func (m *memPackObjectStore) PutAuxGraph(_ context.Context, _ tenant.RepoKey, _, epoch string, data []byte) error {
	m.graphs[epoch] = append([]byte(nil), data...)
	return nil
}

func (m *memPackObjectStore) PutBundle(_ context.Context, repo tenant.RepoKey, data []byte) error {
	m.bundles[repo.String()] = append([]byte(nil), data...)
	return nil
}

func (m *memPackObjectStore) DeleteObject(_ context.Context, _ string) error { return nil }
func (m *memPackObjectStore) ListRepoPacks(_ context.Context, _ tenant.RepoKey) ([]s3.PackObjectInfo, error) {
	return nil, nil
}

func buildSHA256CommitTreeBlob(t *testing.T, filename, content, msg string, parent *pack.OID) ([]pack.Object, pack.OID) {
	t.Helper()
	blobData := []byte(content)
	blobOID := pack.ComputeOIDWith(pack.SHA256, pack.ObjBlob, blobData)

	var treeBuf bytes.Buffer
	fmt.Fprintf(&treeBuf, "100644 %s\x00", filename)
	treeBuf.Write(blobOID.Bytes())
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOIDWith(pack.SHA256, pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\n", treeOID.String())
	if parent != nil {
		fmt.Fprintf(&commitBuf, "parent %s\n", parent.String())
	}
	commitBuf.WriteString("author Stravin Test <test@stravin.dev> 1700000000 +0000\n")
	commitBuf.WriteString("committer Stravin Test <test@stravin.dev> 1700000000 +0000\n\n")
	commitBuf.WriteString(msg)
	commitData := commitBuf.Bytes()
	commitOID := pack.ComputeOIDWith(pack.SHA256, pack.ObjCommit, commitData)

	return []pack.Object{
		{OID: blobOID, Type: pack.ObjBlob, Data: blobData},
		{OID: treeOID, Type: pack.ObjTree, Data: treeData},
		{OID: commitOID, Type: pack.ObjCommit, Data: commitData},
	}, commitOID
}

func buildSHA256PackAndIdx(t *testing.T, objs ...pack.Object) ([]byte, []byte, string) {
	t.Helper()
	objMap := make(map[pack.OID]*pack.Object, len(objs))
	for i := range objs {
		o := objs[i]
		objMap[o.OID] = &o
	}
	packBytes, err := buildConsolidatedPack(objMap, pack.SHA256)
	if err != nil {
		t.Fatalf("buildConsolidatedPack(sha256): %v", err)
	}
	parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{HashAlgorithm: pack.SHA256})
	if err != nil {
		t.Fatalf("pack.Parse(sha256): %v", err)
	}
	idxBytes, err := pack.BuildIdx(parsed)
	if err != nil {
		t.Fatalf("pack.BuildIdx(sha256): %v", err)
	}
	return packBytes, idxBytes, sha256Hex(packBytes)
}

func TestTier0AndTier1CompactionSHA256(t *testing.T) {
	repo, err := tenant.NewRepoKey("acme", "sha256-repo")
	if err != nil {
		t.Fatal(err)
	}
	store := newMemPackObjectStore()

	objs1, c1OID := buildSHA256CommitTreeBlob(t, "a.txt", "hello sha256 v1\n", "commit 1\n", nil)
	objs2, c2OID := buildSHA256CommitTreeBlob(t, "b.txt", "hello sha256 v2\n", "commit 2\n", &c1OID)

	p1, i1, id1 := buildSHA256PackAndIdx(t, objs1...)
	p2, i2, id2 := buildSHA256PackAndIdx(t, objs2...)
	_ = store.PutPack(context.Background(), repo, id1, p1, i1)
	_ = store.PutPack(context.Background(), repo, id2, p2, i2)

	inputs := []ddb.PackManifest{
		{Repo: repo, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA1", PackID: id1, SizeBytes: int64(len(p1)), Status: ddb.PackStatusActive},
		{Repo: repo, ULID: "01ARZ3NDEKTSV4RRFFQ69G5FA2", PackID: id2, SizeBytes: int64(len(p2)), Status: ddb.PackStatusActive},
	}

	t.Run("Tier0_SHA256", func(t *testing.T) {
		tier0, err := NewTier0Strategy(store, pack.DefaultLimits())
		if err != nil {
			t.Fatal(err)
		}
		res, err := tier0.Compact(context.Background(), StrategyRequest{
			Repo:          repo,
			Shard:         0,
			OriginRegion:  lease.Region("us-east-1"),
			HashAlgorithm: pack.SHA256,
			Inputs:        inputs,
		})
		if err != nil {
			t.Fatalf("Tier0.Compact(sha256): %v", err)
		}

		mergedPack, err := store.GetPack(context.Background(), repo, res.OutputPackID)
		if err != nil {
			t.Fatalf("GetPack(merged): %v", err)
		}
		mergedIdx, err := store.GetIndex(context.Background(), repo, res.OutputPackID)
		if err != nil {
			t.Fatalf("GetIndex(merged): %v", err)
		}

		parsed, err := pack.Parse(bytes.NewReader(mergedPack), pack.ParseOptions{HashAlgorithm: pack.SHA256})
		if err != nil {
			t.Fatalf("pack.Parse(merged sha256): %v", err)
		}
		if len(parsed.Objects) != 6 {
			t.Fatalf("merged sha256 pack has %d objects, want 6", len(parsed.Objects))
		}
		idx, err := pack.ParseIdxWithAlgorithm(mergedIdx, pack.SHA256)
		if err != nil {
			t.Fatalf("pack.ParseIdxWithAlgorithm(merged sha256): %v", err)
		}
		if _, _, ok := idx.Lookup(c2OID); !ok {
			t.Fatalf("merged sha256 idx missing commit %s", c2OID)
		}
	})

	t.Run("Tier1_SHA256", func(t *testing.T) {
		if _, err := exec.LookPath("git"); err != nil {
			t.Skip("git binary not found")
		}
		gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{
			DefaultTimeout: 30 * time.Second,
		})
		tier1, err := NewTier1Strategy(store, gitExec, Tier1Config{
			WorkDir: t.TempDir(),
			Timeout: 30 * time.Second,
			Limits:  pack.DefaultLimits(),
		})
		if err != nil {
			t.Fatal(err)
		}
		res, err := tier1.Compact(context.Background(), StrategyRequest{
			Repo:          repo,
			Shard:         0,
			Epoch:         "epoch-sha256-1",
			OriginRegion:  lease.Region("us-east-1"),
			HashAlgorithm: pack.SHA256,
			Inputs:        inputs,
			Refs: []ddb.RefState{
				{RefName: "refs/heads/main", OID: c2OID.String()},
			},
		})
		if err != nil {
			t.Fatalf("Tier1.Compact(sha256): %v", err)
		}

		repackedBytes, err := store.GetPack(context.Background(), repo, res.OutputPackID)
		if err != nil {
			t.Fatalf("GetPack(repacked): %v", err)
		}
		repackedIdx, err := store.GetIndex(context.Background(), repo, res.OutputPackID)
		if err != nil {
			t.Fatalf("GetIndex(repacked): %v", err)
		}
		parsed, err := pack.Parse(bytes.NewReader(repackedBytes), pack.ParseOptions{HashAlgorithm: pack.SHA256})
		if err != nil {
			t.Fatalf("pack.Parse(repacked sha256): %v", err)
		}
		if len(parsed.Objects) != 6 {
			t.Fatalf("repacked sha256 pack has %d objects, want 6", len(parsed.Objects))
		}
		if _, err := pack.ParseIdxWithAlgorithm(repackedIdx, pack.SHA256); err != nil {
			t.Fatalf("pack.ParseIdxWithAlgorithm(repacked sha256): %v", err)
		}
		if len(store.midxs["epoch-sha256-1"]) == 0 {
			t.Fatalf("expected non-empty SHA-256 multi-pack-index")
		}
	})
}
