package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func buildTestGitPack(t *testing.T, objs ...pack.Object) ([]byte, []byte, string) {
	t.Helper()
	objMap := make(map[pack.OID]*pack.Object, len(objs))
	for i := range objs {
		o := objs[i]
		o.OID = pack.ComputeOID(o.Type, o.Data)
		objMap[o.OID] = &o
	}
	packBytes, err := buildConsolidatedPack(objMap)
	if err != nil {
		t.Fatalf("buildConsolidatedPack: %v", err)
	}
	parsed, err := pack.Parse(bytes.NewReader(packBytes), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("pack.Parse: %v", err)
	}
	idxBytes, err := pack.BuildIdx(parsed)
	if err != nil {
		t.Fatalf("pack.BuildIdx: %v", err)
	}
	return packBytes, idxBytes, sha256Hex(packBytes)
}

func buildCommitTreeBlob(t *testing.T, filename, content, msg string, parent *pack.OID) ([]pack.Object, pack.OID) {
	t.Helper()
	blobData := []byte(content)
	blobOID := pack.ComputeOID(pack.ObjBlob, blobData)

	var treeBuf bytes.Buffer
	fmt.Fprintf(&treeBuf, "100644 %s\x00", filename)
	treeBuf.Write(blobOID[:])
	treeData := treeBuf.Bytes()
	treeOID := pack.ComputeOID(pack.ObjTree, treeData)

	var commitBuf bytes.Buffer
	fmt.Fprintf(&commitBuf, "tree %s\n", treeOID.String())
	if parent != nil {
		fmt.Fprintf(&commitBuf, "parent %s\n", parent.String())
	}
	commitBuf.WriteString("author Stravin Test <test@stravin.dev> 1700000000 +0000\n")
	commitBuf.WriteString("committer Stravin Test <test@stravin.dev> 1700000000 +0000\n\n")
	commitBuf.WriteString(msg)
	commitBuf.WriteByte('\n')
	commitData := commitBuf.Bytes()
	commitOID := pack.ComputeOID(pack.ObjCommit, commitData)

	return []pack.Object{
		{OID: blobOID, Type: pack.ObjBlob, Data: blobData},
		{OID: treeOID, Type: pack.ObjTree, Data: treeData},
		{OID: commitOID, Type: pack.ObjCommit, Data: commitData},
	}, commitOID
}

func TestEngine_Tier0TwoPhaseSwapOrderingAndTelemetry(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 13, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-t0", "repo-t0")
	const manifestShards = 1

	leaseStore := newFakeLeaseStore()
	manifests := newFakeManifestStore(manifestShards)
	objects := newFakePackObjectStore(clk)

	var eventLog []string
	objects.onPutPack = func(packSHA string) {
		eventLog = append(eventLog, "s3_put:"+packSHA[:8])
	}

	objs1, commit1 := buildCommitTreeBlob(t, "a.txt", "hello world\n", "first commit", nil)
	pack1, idx1, sha1 := buildTestGitPack(t, objs1...)

	objs2, commit2 := buildCommitTreeBlob(t, "b.txt", "second file\n", "second commit", &commit1)
	pack2, idx2, sha2 := buildTestGitPack(t, objs2...)

	ctx := context.Background()
	if err := objects.PutPack(ctx, rk, sha1, pack1, idx1); err != nil {
		t.Fatalf("PutPack(1): %v", err)
	}
	if err := objects.PutPack(ctx, rk, sha2, pack2, idx2); err != nil {
		t.Fatalf("PutPack(2): %v", err)
	}
	eventLog = nil // reset after fixture setup

	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000001", PackID: sha1,
		SizeBytes: int64(len(pack1)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})
	manifests.put(ddb.PackManifest{
		Repo: rk, ULID: "01J00000000000000000000002", PackID: sha2,
		SizeBytes: int64(len(pack2)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now(),
	})

	tel, err := telemetry.NewInMemoryProvider("stravin-pack")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	leaseMgr, err := NewLeaseManager(leaseStore, LeaseManagerConfig{
		OwnerID: "pack-worker-t0",
		TTL:     5 * time.Second,
		Metrics: metrics,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}
	tier0, err := NewTier0Strategy(objects, pack.DefaultLimits())
	if err != nil {
		t.Fatalf("NewTier0Strategy: %v", err)
	}

	engine, err := NewEngine(EngineConfig{
		Leases:        leaseMgr,
		Manifests:     manifests,
		ManifestGC:    manifests,
		GCGraceWindow: 10 * time.Millisecond,
		Clock:         clk,
		Metrics:       metrics,
		Strategies: map[Tier]CompactionStrategy{
			Tier0: tier0,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	res, err := engine.CompactShard(ctx, ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: manifestShards,
		OriginRegion:   "us-east-1",
		Tier:           Tier0,
	})
	if err != nil {
		t.Fatalf("CompactShard(Tier0): %v", err)
	}
	if res.Skipped || res.InputPacks != 2 || res.OutputPackID == "" {
		t.Fatalf("unexpected JobResult: %+v", res)
	}

	// Verify S3 upload happened before Phase 1 Publish -> Phase 2 Supersede -> MarkGCEligible.
	if len(eventLog) != 1 || !strings.HasPrefix(eventLog[0], "s3_put:") {
		t.Fatalf("expected 1 S3 PutPack event, got %v", eventLog)
	}
	wantDDBOrder := []string{"publish", "supersede", "mark_gc"}
	if len(manifests.events) != len(wantDDBOrder) {
		t.Fatalf("ddb events = %v, want %v", manifests.events, wantDDBOrder)
	}
	for i := range wantDDBOrder {
		if manifests.events[i] != wantDDBOrder[i] {
			t.Fatalf("ddb events = %v, want %v", manifests.events, wantDDBOrder)
		}
	}

	// Verify merged pack + idx in S3 contain both commits.
	mergedPack, err := objects.GetPack(ctx, rk, res.OutputPackID)
	if err != nil {
		t.Fatalf("GetPack(merged): %v", err)
	}
	mergedIdx, err := objects.GetIndex(ctx, rk, res.OutputPackID)
	if err != nil {
		t.Fatalf("GetIndex(merged): %v", err)
	}
	parsed, err := pack.Parse(bytes.NewReader(mergedPack), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("pack.Parse(merged): %v", err)
	}
	idx, err := pack.ParseIdx(mergedIdx)
	if err != nil {
		t.Fatalf("pack.ParseIdx(merged): %v", err)
	}
	if idx.Count() != 6 {
		t.Fatalf("merged idx count = %d, want 6 objects", idx.Count())
	}
	if _, ok := parsed.Objects[commit1]; !ok {
		t.Fatalf("commit1 %s missing from merged pack", commit1)
	}
	if _, ok := parsed.Objects[commit2]; !ok {
		t.Fatalf("commit2 %s missing from merged pack", commit2)
	}

	// Verify only the new pack is ACTIVE and both old packs are COMPACTED.
	active, err := manifests.QueryManifestShard(ctx, rk, 0, "", true)
	if err != nil {
		t.Fatalf("QueryManifestShard(active): %v", err)
	}
	if len(active) != 1 || active[0].PackID != res.OutputPackID {
		t.Fatalf("expected only merged pack active, got %+v", active)
	}

	// Verify required OTel spans were recorded.
	gotSpans := make(map[string]bool)
	for _, sp := range tel.SpanExporter.GetSpans() {
		gotSpans[sp.Name] = true
	}
	for _, wantSpan := range []string{"pack.compact.shard", "ddb.lease_acquire", "ddb.manifest_publish", "ddb.manifest_supersede"} {
		if !gotSpans[wantSpan] {
			t.Fatalf("missing expected OTel span %q (recorded: %v)", wantSpan, gotSpans)
		}
	}
}

func TestEngine_TwoPhaseFailureNeverDropsActivePacks(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 13, 30, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-fail", "repo-fail")
	const manifestShards = 1

	objs1, _ := buildCommitTreeBlob(t, "x.txt", "data-1\n", "c1", nil)
	pack1, idx1, sha1 := buildTestGitPack(t, objs1...)
	objs2, _ := buildCommitTreeBlob(t, "y.txt", "data-2\n", "c2", nil)
	pack2, idx2, sha2 := buildTestGitPack(t, objs2...)

	setup := func(failPub, failSup error) (*Engine, *fakeManifestStore) {
		leaseStore := newFakeLeaseStore()
		manifests := newFakeManifestStore(manifestShards)
		manifests.failPublish = failPub
		manifests.failSupersede = failSup
		objects := newFakePackObjectStore(clk)

		ctx := context.Background()
		_ = objects.PutPack(ctx, rk, sha1, pack1, idx1)
		_ = objects.PutPack(ctx, rk, sha2, pack2, idx2)
		manifests.put(ddb.PackManifest{Repo: rk, ULID: "01J00000000000000000000001", PackID: sha1, SizeBytes: int64(len(pack1)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now()})
		manifests.put(ddb.PackManifest{Repo: rk, ULID: "01J00000000000000000000002", PackID: sha2, SizeBytes: int64(len(pack2)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now()})

		leaseMgr, _ := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w1", TTL: 5 * time.Second})
		tier0, _ := NewTier0Strategy(objects, pack.DefaultLimits())
		eng, _ := NewEngine(EngineConfig{
			Leases:     leaseMgr,
			Manifests:  manifests,
			Clock:      clk,
			Strategies: map[Tier]CompactionStrategy{Tier0: tier0},
		})
		return eng, manifests
	}

	// Case 1: Phase 1 Publish fails -> Phase 2 Supersede MUST NOT run; old packs stay ACTIVE.
	eng1, m1 := setup(errors.New("ddb publish outage"), nil)
	if _, err := eng1.CompactShard(context.Background(), ShardTarget{Repo: rk, Shard: 0, ManifestShards: manifestShards, Tier: Tier0}); err == nil {
		t.Fatal("expected error when Phase 1 publish fails")
	}
	if len(m1.events) != 1 || m1.events[0] != "publish" {
		t.Fatalf("events = %v, want only [publish]", m1.events)
	}
	active1, _ := m1.QueryManifestShard(context.Background(), rk, 0, "", true)
	if len(active1) != 2 {
		t.Fatalf("expected 2 original packs to remain ACTIVE after Phase 1 failure, got %d", len(active1))
	}

	// Case 2: Phase 2 Supersede fails -> New pack is already ACTIVE so repo has zero missing packs.
	eng2, m2 := setup(nil, errors.New("ddb supersede outage"))
	if _, err := eng2.CompactShard(context.Background(), ShardTarget{Repo: rk, Shard: 0, ManifestShards: manifestShards, Tier: Tier0}); err == nil {
		t.Fatal("expected error when Phase 2 supersede fails")
	}
	active2, _ := m2.QueryManifestShard(context.Background(), rk, 0, "", true)
	if len(active2) != 3 {
		t.Fatalf("expected new + old packs ACTIVE (3 total) after Phase 2 failure, got %d", len(active2))
	}
}

type fakeRefLister struct {
	refs []ddb.RefState
}

func (f fakeRefLister) ListRefs(_ context.Context, _ tenant.RepoKey, _ int) ([]ddb.RefState, error) {
	return f.refs, nil
}

func TestEngine_Tier1DeepRepackMIDXAndCommitGraph(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 14, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-t1", "repo-t1")
	const manifestShards = 1

	leaseStore := newFakeLeaseStore()
	manifests := newFakeManifestStore(manifestShards)
	objects := newFakePackObjectStore(clk)

	objs1, commit1 := buildCommitTreeBlob(t, "file1.txt", strings.Repeat("hello-stravin-", 100), "init", nil)
	pack1, idx1, sha1 := buildTestGitPack(t, objs1...)

	objs2, commit2 := buildCommitTreeBlob(t, "file2.txt", strings.Repeat("hello-stravin-", 100)+"v2", "second", &commit1)
	pack2, idx2, sha2 := buildTestGitPack(t, objs2...)

	ctx := context.Background()
	if err := objects.PutPack(ctx, rk, sha1, pack1, idx1); err != nil {
		t.Fatalf("PutPack(1): %v", err)
	}
	if err := objects.PutPack(ctx, rk, sha2, pack2, idx2); err != nil {
		t.Fatalf("PutPack(2): %v", err)
	}

	manifests.put(ddb.PackManifest{Repo: rk, ULID: "01J00000000000000000000001", PackID: sha1, SizeBytes: int64(len(pack1)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now()})
	manifests.put(ddb.PackManifest{Repo: rk, ULID: "01J00000000000000000000002", PackID: sha2, SizeBytes: int64(len(pack2)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive, CreatedAt: clk.Now()})

	leaseMgr, err := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "pack-worker-t1", TTL: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{DefaultTimeout: 30 * time.Second})
	tier1, err := NewTier1Strategy(objects, gitExec, Tier1Config{
		WorkDir: t.TempDir(),
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewTier1Strategy: %v", err)
	}

	engine, err := NewEngine(EngineConfig{
		Leases:     leaseMgr,
		Manifests:  manifests,
		ManifestGC: manifests,
		Refs: fakeRefLister{refs: []ddb.RefState{
			{Repo: rk, RefName: "refs/heads/main", OID: commit2.String()},
		}},
		Clock: clk,
		Strategies: map[Tier]CompactionStrategy{
			Tier1: tier1,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	res, err := engine.CompactShard(ctx, ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: manifestShards,
		RefShards:      1,
		OriginRegion:   "us-east-1",
		Tier:           Tier1,
		Epoch:          "epoch-1",
	})
	if err != nil {
		t.Fatalf("CompactShard(Tier1): %v", err)
	}
	if res.Skipped || res.OutputPackID == "" {
		t.Fatalf("unexpected Tier1 JobResult: %+v", res)
	}
	if res.AuxMIDXKey == "" || res.AuxGraphKey == "" {
		t.Fatalf("expected both AuxMIDXKey and AuxGraphKey, got midx=%q graph=%q", res.AuxMIDXKey, res.AuxGraphKey)
	}

	// Verify MIDX magic ("MIDX") and commit-graph magic ("CGPH") in S3.
	midxKey, _ := s3.AuxMIDXKey(rk, res.OutputPackID, "epoch-1")
	graphKey, _ := s3.AuxGraphKey(rk, res.OutputPackID, "epoch-1")
	if !bytes.HasPrefix(objects.objects[midxKey].data, []byte("MIDX")) {
		t.Fatalf("unexpected midx header: %q", objects.objects[midxKey].data[:min(8, len(objects.objects[midxKey].data))])
	}
	if !bytes.HasPrefix(objects.objects[graphKey].data, []byte("CGPH")) {
		t.Fatalf("unexpected commit-graph header: %q", objects.objects[graphKey].data[:min(8, len(objects.objects[graphKey].data))])
	}
}

func TestWorker_RunAndGracefulShutdown(t *testing.T) {
	clk := newStaticClock(time.Date(2026, 11, 21, 15, 0, 0, 0, time.UTC))
	rk := mustTestRepoKey(t, "tenant-w", "repo-w")
	leaseStore := newFakeLeaseStore()
	manifests := newFakeManifestStore(1)
	objects := newFakePackObjectStore(clk)

	leaseMgr, _ := NewLeaseManager(leaseStore, LeaseManagerConfig{OwnerID: "w-run", TTL: 5 * time.Second})
	tier0, _ := NewTier0Strategy(objects, pack.DefaultLimits())
	engine, _ := NewEngine(EngineConfig{
		Leases:     leaseMgr,
		Manifests:  manifests,
		Clock:      clk,
		Strategies: map[Tier]CompactionStrategy{Tier0: tier0},
	})
	sweeper, _ := NewGCSweeper(GCSweeperConfig{
		Manifests:   manifests,
		ManifestGC:  manifests,
		Objects:     objects,
		GraceWindow: 10 * time.Millisecond,
		Clock:       clk,
	})

	worker, err := NewWorker(WorkerConfig{
		Engine:       engine,
		Sweeper:      sweeper,
		Queue:        NewFairQueue(),
		PollInterval: 10 * time.Millisecond,
		GCInterval:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	worker.Queue().Enqueue(ShardTarget{Repo: rk, Shard: 0, ManifestShards: 1, Tier: Tier0})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if err := worker.Run(ctx); err != nil {
		t.Fatalf("worker.Run: %v", err)
	}
	if worker.Queue().Len() != 0 {
		t.Fatalf("expected queue to be drained, got Len=%d", worker.Queue().Len())
	}
}
