//go:build integration

package compaction

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/pack"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func newFlociDeps(t *testing.T) (*ddb.Repository, *DDBManifestGCAdapter, *s3.Store) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci compaction integration test: AWS_ENDPOINT_URL not set")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	table := os.Getenv("STRAVIN_DDB_TABLE")
	if table == "" {
		table = "stravin-core"
	}
	bucket := os.Getenv("STRAVIN_S3_BUCKET")
	if bucket == "" {
		bucket = "git-objects-us-east-1"
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("ddb.NewRepository: %v", err)
	}
	gcAdapter, err := NewDDBManifestGCAdapter(ddbClient, table)
	if err != nil {
		t.Fatalf("NewDDBManifestGCAdapter: %v", err)
	}

	s3Client := s3.NewAWSClient(cfg, endpoint, true)
	s3Store, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore: %v", err)
	}
	return ddbRepo, gcAdapter, s3Store
}

func TestFlociIntegration_CompactionTier0Tier1AndDelayedGC(t *testing.T) {
	ddbRepo, gcAdapter, s3Store := newFlociDeps(t)
	ctx := context.Background()

	nonce := fmt.Sprintf("pack-it-%d", time.Now().UnixNano())
	rk, err := tenant.NewRepoKey("local", nonce)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	const manifestShards = 1

	// 1. Verify DynamoDB shard lease acquisition + contention + release against Floci.
	mgr1, err := NewLeaseManager(ddbRepo, LeaseManagerConfig{OwnerID: "floci-w1-" + nonce, TTL: 15 * time.Second})
	if err != nil {
		t.Fatalf("NewLeaseManager(1): %v", err)
	}
	mgr2, err := NewLeaseManager(ddbRepo, LeaseManagerConfig{OwnerID: "floci-w2-" + nonce, TTL: 15 * time.Second})
	if err != nil {
		t.Fatalf("NewLeaseManager(2): %v", err)
	}
	held, err := mgr1.Acquire(ctx, rk, 0)
	if err != nil {
		t.Fatalf("mgr1.Acquire: %v", err)
	}
	if _, err := mgr2.Acquire(ctx, rk, 0); !errors.Is(err, ErrLeaseContention) {
		t.Fatalf("competing Acquire = %v, want ErrLeaseContention", err)
	}
	if err := held.Release(ctx); err != nil {
		t.Fatalf("held.Release: %v", err)
	}

	// 2. Seed two push packs in Floci S3 + DDB manifest + one orphaned S3 pack (simulated rejected Wasm push).
	objs1, commit1 := buildCommitTreeBlob(t, "README.md", "stravin core\n", "initial commit", nil)
	pack1, idx1, sha1 := buildTestGitPack(t, objs1...)

	objs2, commit2 := buildCommitTreeBlob(t, "main.go", "package main\n", "add main.go", &commit1)
	pack2, idx2, sha2 := buildTestGitPack(t, objs2...)

	objsOrphan, _ := buildCommitTreeBlob(t, "secret.txt", "rejected-by-wasm\n", "rejected", nil)
	packOrphan, idxOrphan, shaOrphan := buildTestGitPack(t, objsOrphan...)

	if err := s3Store.PutPack(ctx, rk, sha1, pack1, idx1); err != nil {
		t.Fatalf("PutPack(1): %v", err)
	}
	if err := s3Store.PutPack(ctx, rk, sha2, pack2, idx2); err != nil {
		t.Fatalf("PutPack(2): %v", err)
	}
	if err := s3Store.PutPack(ctx, rk, shaOrphan, packOrphan, idxOrphan); err != nil {
		t.Fatalf("PutPack(orphan): %v", err)
	}

	u1, _ := ddb.NewULID(time.Now().UTC())
	time.Sleep(2 * time.Millisecond)
	u2, _ := ddb.NewULID(time.Now().UTC())
	if err := ddbRepo.PutManifest(ctx, ddb.PackManifest{
		Repo: rk, ULID: u1, PackID: sha1, SizeBytes: int64(len(pack1)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive,
	}, manifestShards); err != nil {
		t.Fatalf("PutManifest(1): %v", err)
	}
	if err := ddbRepo.PutManifest(ctx, ddb.PackManifest{
		Repo: rk, ULID: u2, PackID: sha2, SizeBytes: int64(len(pack2)), OriginRegion: "us-east-1", Status: ddb.PackStatusActive,
	}, manifestShards); err != nil {
		t.Fatalf("PutManifest(2): %v", err)
	}

	// 3. Run Tier-0 streaming pack merge + two-phase manifest swap.
	tier0, err := NewTier0Strategy(s3Store, pack.DefaultLimits())
	if err != nil {
		t.Fatalf("NewTier0Strategy: %v", err)
	}
	gitExec := gitexec.NewSubprocessExecutor(gitexec.Config{DefaultTimeout: 30 * time.Second})
	tier1, err := NewTier1Strategy(s3Store, gitExec, Tier1Config{
		WorkDir: t.TempDir(),
		Timeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewTier1Strategy: %v", err)
	}

	engine, err := NewEngine(EngineConfig{
		Leases:        mgr1,
		Manifests:     ddbRepo,
		ManifestGC:    gcAdapter,
		GCGraceWindow: 10 * time.Millisecond,
		Strategies: map[Tier]CompactionStrategy{
			Tier0: tier0,
			Tier1: tier1,
		},
	})
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	res0, err := engine.CompactShard(ctx, ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: manifestShards,
		OriginRegion:   "us-east-1",
		Tier:           Tier0,
	})
	if err != nil {
		t.Fatalf("CompactShard(Tier0): %v", err)
	}
	if res0.Skipped || res0.InputPacks != 2 || res0.OutputPackID == "" {
		t.Fatalf("unexpected Tier0 result: %+v", res0)
	}

	// 4. Run Tier-1 deep repack generating .midx + commit-graph on Floci S3.
	time.Sleep(2 * time.Millisecond)
	res1, err := engine.CompactShard(ctx, ShardTarget{
		Repo:           rk,
		Shard:          0,
		ManifestShards: manifestShards,
		OriginRegion:   "us-east-1",
		Tier:           Tier1,
		Epoch:          "floci-epoch-1",
	})
	if err != nil {
		t.Fatalf("CompactShard(Tier1): %v", err)
	}
	if res1.AuxMIDXKey == "" || res1.AuxGraphKey == "" {
		t.Fatalf("expected Tier1 aux artifacts, got midx=%q graph=%q", res1.AuxMIDXKey, res1.AuxGraphKey)
	}
	midxBytes, err := s3Store.GetAuxMIDX(ctx, rk, res1.OutputPackID, "floci-epoch-1")
	if err != nil || !bytes.HasPrefix(midxBytes, []byte("MIDX")) {
		t.Fatalf("GetAuxMIDX = len %d err=%v", len(midxBytes), err)
	}
	graphBytes, err := s3Store.GetAuxGraph(ctx, rk, res1.OutputPackID, "floci-epoch-1")
	if err != nil || !bytes.HasPrefix(graphBytes, []byte("CGPH")) {
		t.Fatalf("GetAuxGraph = len %d err=%v", len(graphBytes), err)
	}

	// 5. Wait for grace window and run delayed GC sweeper to reap COMPACTED + orphaned packs.
	// S3 LastModified has 1-second resolution in Floci, so wait 1.1s for the orphan grace window.
	time.Sleep(1100 * time.Millisecond)

	sweeper, err := NewGCSweeper(GCSweeperConfig{
		Manifests:         ddbRepo,
		ManifestGC:        gcAdapter,
		Objects:           s3Store,
		GraceWindow:       10 * time.Millisecond,
		OrphanGraceWindow: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewGCSweeper: %v", err)
	}

	gcRes, err := sweeper.SweepRepo(ctx, rk, manifestShards)
	if err != nil {
		t.Fatalf("SweepRepo: %v", err)
	}
	if gcRes.CompactedReaped < 2 || gcRes.OrphanedReaped != 1 {
		t.Fatalf("unexpected Floci GCResult: %+v", gcRes)
	}

	// Orphaned pack must be gone from S3.
	if _, err := s3Store.GetPack(ctx, rk, shaOrphan); !errors.Is(err, s3.ErrNotFound) {
		t.Fatalf("expected orphaned pack deleted, got err=%v", err)
	}
	// Active Tier-1 pack must still exist and be readable.
	activePack, err := s3Store.GetPack(ctx, rk, res1.OutputPackID)
	if err != nil || len(activePack) == 0 {
		t.Fatalf("expected active Tier-1 pack to remain in S3, got len=%d err=%v", len(activePack), err)
	}

	// Verify commit2 is present in the final active pack.
	parsedFinal, err := pack.Parse(bytes.NewReader(activePack), pack.ParseOptions{})
	if err != nil {
		t.Fatalf("pack.Parse(final): %v", err)
	}
	if _, ok := parsedFinal.Objects[commit2]; !ok {
		t.Fatalf("commit2 %s missing from final compacted pack (keys=%d)", strings.ToLower(commit2.String()), len(parsedFinal.Objects))
	}
}
