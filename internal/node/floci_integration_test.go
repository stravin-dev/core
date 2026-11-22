//go:build integration

package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"google.golang.org/grpc/metadata"

	"stravin.dev/core/internal/cache"
	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/gitexec"
	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/rpc"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func TestFlociIntegration_NodeUploadPackReadPath(t *testing.T) {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	region := envOrDefault("AWS_REGION", "us-east-1")
	table := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	bucket := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-us-east-1")
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	if os.Getenv(s3.EnvForcePathStyle) == "" {
		t.Setenv(s3.EnvForcePathStyle, "true")
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("ddb.NewRepository: %v", err)
	}
	s3Client := s3.NewAWSClient(awsCfg, endpoint, s3.ForcePathStyleFromEnv())
	s3Store, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore: %v", err)
	}

	nonce := fmt.Sprintf("node-it-%d", time.Now().UnixNano())
	rk, err := tenant.NewRepoKey("local", nonce)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	commitOID, packBytes, idxBytes := buildSampleGitPack(t)
	packSum := sha256.Sum256(packBytes)
	packID := hex.EncodeToString(packSum[:])

	// 1. Seed META, S3 pack+idx, and Ref-CAS + Manifest in Floci.
	if err := ddbRepo.CreateMeta(ctx, ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     lease.Region(region),
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	}); err != nil {
		t.Fatalf("CreateMeta: %v", err)
	}
	if err := s3Store.PutPack(ctx, rk, packID, packBytes, idxBytes); err != nil {
		t.Fatalf("PutPack: %v", err)
	}
	if _, err := ddbRepo.CommitPush(ctx, ddb.CommitPushInput{
		Repo:           rk,
		RefShards:      2,
		ManifestShards: 2,
		Epoch:          1,
		OriginRegion:   lease.Region(region),
		Updates: []ddb.RefUpdate{
			{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commitOID},
		},
		Pack: &ddb.PackUpload{
			PackID:       packID,
			SizeBytes:    int64(len(packBytes)),
			OriginRegion: lease.Region(region),
		},
	}); err != nil {
		t.Fatalf("CommitPush: %v", err)
	}

	// 2. Wire real Materializer + Server against Floci and cold NVMe cache.
	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  ddbRepo,
		Packs: s3Store,
		Cache: nvmeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer: mat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
		Region:       region,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	client := startBufconnNode(t, srv)

	// 3. Verify advertise-refs strictly from Floci DynamoDB.
	advCtx := metadata.AppendToOutgoingContext(ctx, HeaderAdvertiseRefs, "true")
	advStream, err := client.OpenStream(advCtx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream advertise: %v", err)
	}
	var advBuf bytes.Buffer
	for {
		f, err := advStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("advertise Recv: %v", err)
		}
		advBuf.Write(f.Data)
		if f.EOF {
			break
		}
	}
	if !strings.Contains(advBuf.String(), commitOID+" refs/heads/main\n") {
		t.Fatalf("Floci advertise-refs missing commit %s: %q", commitOID, advBuf.String())
	}

	// 4. Verify stateless-rpc git-upload-pack pulls pack from Floci S3 and streams PACK.
	upCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	upStream, err := client.OpenStream(upCtx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream upload-pack: %v", err)
	}
	var reqPayload bytes.Buffer
	reqPayload.Write(EncodePktLine([]byte(fmt.Sprintf("want %s\n", commitOID))))
	reqPayload.Write(FlushPkt)
	reqPayload.Write(EncodePktLine([]byte("done\n")))
	if err := upStream.Send(&rpc.Frame{Data: reqPayload.Bytes(), EOF: true}); err != nil {
		t.Fatalf("upload-pack Send: %v", err)
	}

	var resp bytes.Buffer
	for {
		f, err := upStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("upload-pack Recv: %v", err)
		}
		resp.Write(f.Data)
		if f.EOF {
			if f.ExitCode != 0 {
				t.Fatalf("upload-pack exit=%d stderr=%q", f.ExitCode, f.ErrMessage)
			}
			break
		}
	}
	if !bytes.Contains(resp.Bytes(), []byte("PACK")) {
		t.Fatalf("expected PACK stream from Floci-backed stravin-node, got %d bytes", resp.Len())
	}
	if !nvmeCache.HasPack(rk, packID) {
		t.Fatalf("expected pack %s cached in NVMe after upload-pack", packID)
	}
}

func TestFlociIntegration_NodeReceivePackPushAndFetchRoundTrip(t *testing.T) {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	region := envOrDefault("AWS_REGION", "us-east-1")
	table := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	bucket := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-us-east-1")
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	if os.Getenv(s3.EnvForcePathStyle) == "" {
		t.Setenv(s3.EnvForcePathStyle, "true")
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("ddb.NewRepository: %v", err)
	}
	s3Client := s3.NewAWSClient(awsCfg, endpoint, s3.ForcePathStyleFromEnv())
	s3Store, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore: %v", err)
	}

	nonce := fmt.Sprintf("node-write-it-%d", time.Now().UnixNano())
	rk, err := tenant.NewRepoKey("local", nonce)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	// Use a unique per-test lease region so parallel integration packages never collide on REGION#us-east-1#LEASE.
	leaseRegion := lease.Region(fmt.Sprintf("%s-node-%d", region, time.Now().UnixNano()))

	if err := ddbRepo.PutRegionalWriteLease(ctx, ddb.RegionalWriteLease{
		Region:    leaseRegion,
		Epoch:     1,
		Mode:      lease.ModeActive,
		HolderID:  "floci-it-ctrl",
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	}); err != nil {
		t.Fatalf("PutRegionalWriteLease: %v", err)
	}

	if err := ddbRepo.CreateMeta(ctx, ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     leaseRegion,
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	}); err != nil {
		t.Fatalf("CreateMeta: %v", err)
	}

	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:  ddbRepo,
		Region: leaseRegion,
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(ctx); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	writeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New write: %v", err)
	}
	writeMat, err := NewMaterializer(MaterializerConfig{
		Meta:  ddbRepo,
		Packs: s3Store,
		Cache: writeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer write: %v", err)
	}

	hookEngine := wasm.NewEngine(
		NewS3WasmLoader(s3Store),
		NewDDBHookConfigStore(ddbRepo),
		wasm.EngineConfig{},
	)
	t.Cleanup(func() { _ = hookEngine.Close(context.Background()) })

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer:   writeMat,
		Cache:          writeCache,
		Packs:          s3Store,
		Committer:      ddbRepo,
		LeaseRefresher: refresher,
		HookEngine:     hookEngine,
		Region:         leaseRegion,
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}

	writeSrv, err := NewServer(ServerConfig{
		Materializer:   writeMat,
		GitExecutor:    gitexec.NewSubprocessExecutor(gitexec.Config{}),
		WriteHandler:   wh,
		LeaseRefresher: refresher,
		Region:         string(leaseRegion),
	})
	if err != nil {
		t.Fatalf("NewServer write: %v", err)
	}
	client := startBufconnNode(t, writeSrv)

	// 1. Initial push via git-receive-pack.
	baseBlob := "floci push round-trip base\n"
	commit1, blob1, pack1 := buildSyntheticCommitPack(t, baseBlob, "floci initial commit\n", nil)
	traceParent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	wire1 := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commit1.String()}},
		"report-status side-band-64k atomic",
		pack1,
	)
	resp1, exit1, err := sendReceivePack(t, ctx, client, rk, traceParent, wire1)
	if err != nil || exit1 != 0 {
		t.Fatalf("initial push failed: err=%v exit=%d", err, exit1)
	}
	b1, _ := splitSideBandResponse(t, resp1)
	if !strings.Contains(string(b1), "ok refs/heads/main\n") {
		t.Fatalf("initial push report-status missing ok: %q", string(b1))
	}

	// Verify Ref History persisted with traceparent.
	hist, err := ddbRepo.ListRefHistory(ctx, rk, "refs/heads/main", 2, "", 10)
	if err != nil || len(hist) != 1 || hist[0].TraceParent != traceParent {
		t.Fatalf("unexpected ref history: hist=%+v err=%v", hist, err)
	}

	// 2. Incremental thin-pack push.
	commit2, thinPack2 := buildSyntheticThinPack(t, []byte(baseBlob), blob1, commit1)
	wire2 := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: commit1.String(), NewOID: commit2.String()}},
		"report-status side-band-64k",
		thinPack2,
	)
	resp2, exit2, err := sendReceivePack(t, ctx, client, rk, traceParent, wire2)
	if err != nil || exit2 != 0 {
		t.Fatalf("thin push failed: err=%v exit=%d", err, exit2)
	}
	b2, _ := splitSideBandResponse(t, resp2)
	if !strings.Contains(string(b2), "ok refs/heads/main\n") {
		t.Fatalf("thin push report-status missing ok: %q", string(b2))
	}

	// 3. Cold-cache fetch via git-upload-pack verifies pushed objects round-trip through Floci S3+DDB.
	readCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New read: %v", err)
	}
	readMat, err := NewMaterializer(MaterializerConfig{
		Meta:  ddbRepo,
		Packs: s3Store,
		Cache: readCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer read: %v", err)
	}
	readSrv, err := NewServer(ServerConfig{
		Materializer: readMat,
		GitExecutor:  gitexec.NewSubprocessExecutor(gitexec.Config{}),
		Region:       region,
	})
	if err != nil {
		t.Fatalf("NewServer read: %v", err)
	}
	readClient := startBufconnNode(t, readSrv)

	upCtx := metadata.AppendToOutgoingContext(ctx, HeaderStatelessRPC, "true")
	upStream, err := readClient.OpenStream(upCtx, rpc.RequestMeta{
		TenantID:  rk.TenantID,
		RepoID:    rk.RepoID,
		Operation: rpc.OpUploadPack,
	})
	if err != nil {
		t.Fatalf("OpenStream upload-pack: %v", err)
	}
	var reqPayload bytes.Buffer
	reqPayload.Write(EncodePktLine([]byte(fmt.Sprintf("want %s\n", commit2.String()))))
	reqPayload.Write(FlushPkt)
	reqPayload.Write(EncodePktLine([]byte("done\n")))
	if err := upStream.Send(&rpc.Frame{Data: reqPayload.Bytes(), EOF: true}); err != nil {
		t.Fatalf("upload-pack Send: %v", err)
	}
	var fetchResp bytes.Buffer
	for {
		f, err := upStream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("upload-pack Recv: %v", err)
		}
		fetchResp.Write(f.Data)
		if f.EOF {
			if f.ExitCode != 0 {
				t.Fatalf("upload-pack exit=%d stderr=%q", f.ExitCode, f.ErrMessage)
			}
			break
		}
	}
	if !bytes.Contains(fetchResp.Bytes(), []byte("PACK")) {
		t.Fatalf("expected PACK stream from cold-cache fetch after push, got %d bytes", fetchResp.Len())
	}

	// 4. Pure ref deletion removes ref in DynamoDB.
	wireDel := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: commit2.String(), NewOID: ddb.ZeroOID}},
		"report-status delete-refs",
		nil,
	)
	respDel, exitDel, err := sendReceivePack(t, ctx, client, rk, traceParent, wireDel)
	if err != nil || exitDel != 0 {
		t.Fatalf("ref delete failed: err=%v exit=%d", err, exitDel)
	}
	if !strings.Contains(string(respDel), "ok refs/heads/main\n") {
		t.Fatalf("ref delete report-status missing ok: %q", string(respDel))
	}
	refsAfter, err := ddbRepo.ListRefs(ctx, rk, 2)
	if err != nil || len(refsAfter) != 0 {
		t.Fatalf("expected 0 refs after deletion, got refs=%+v err=%v", refsAfter, err)
	}
}

func TestFlociIntegration_NodeReceivePackKMSEnvelopeEncryption(t *testing.T) {
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	region := envOrDefault("AWS_REGION", "us-east-1")
	table := envOrDefault("STRAVIN_DDB_TABLE", "stravin-core")
	bucket := envOrDefault("STRAVIN_S3_BUCKET", "git-objects-us-east-1")
	kmsKeyID := envOrDefault("STRAVIN_KMS_KEY_ID", "alias/stravin-local-dev")
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	if os.Getenv(s3.EnvForcePathStyle) == "" {
		t.Setenv(s3.EnvForcePathStyle, "true")
	}

	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(awsCfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	ddbRepo, err := ddb.NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("ddb.NewRepository: %v", err)
	}

	s3Client := s3.NewAWSClient(awsCfg, endpoint, s3.ForcePathStyleFromEnv())
	kmsClient := crypto.NewKMSClient(awsCfg, endpoint)
	encStore, err := buildS3Store(s3Client, bucket, kmsClient, kmsKeyID)
	if err != nil {
		t.Fatalf("buildS3Store: %v", err)
	}
	rawStore, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore(raw): %v", err)
	}

	nonce := fmt.Sprintf("node-kms-it-%d", time.Now().UnixNano())
	rk, err := tenant.NewRepoKey("local", nonce)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}
	leaseRegion := lease.Region(fmt.Sprintf("%s-kms-%d", region, time.Now().UnixNano()))

	if err := ddbRepo.PutRegionalWriteLease(ctx, ddb.RegionalWriteLease{
		Region:    leaseRegion,
		Epoch:     1,
		Mode:      lease.ModeActive,
		HolderID:  "floci-kms-it-ctrl",
		ExpiresAt: time.Now().UTC().Add(5 * time.Minute),
	}); err != nil {
		t.Fatalf("PutRegionalWriteLease: %v", err)
	}
	if err := ddbRepo.CreateMeta(ctx, ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     leaseRegion,
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	}); err != nil {
		t.Fatalf("CreateMeta: %v", err)
	}

	refresher, err := NewLeaseRefresher(LeaseRefresherConfig{
		Store:  ddbRepo,
		Region: leaseRegion,
	})
	if err != nil {
		t.Fatalf("NewLeaseRefresher: %v", err)
	}
	if err := refresher.RefreshOnce(ctx); err != nil {
		t.Fatalf("RefreshOnce: %v", err)
	}

	nvmeCache, err := cache.New(cache.Config{Root: t.TempDir()})
	if err != nil {
		t.Fatalf("cache.New: %v", err)
	}
	mat, err := NewMaterializer(MaterializerConfig{
		Meta:  ddbRepo,
		Packs: encStore,
		Cache: nvmeCache,
	})
	if err != nil {
		t.Fatalf("NewMaterializer: %v", err)
	}
	hookEngine := wasm.NewEngine(
		NewS3WasmLoader(encStore),
		NewDDBHookConfigStore(ddbRepo),
		wasm.EngineConfig{},
	)
	t.Cleanup(func() { _ = hookEngine.Close(context.Background()) })

	wh, err := NewReceivePackHandler(ReceivePackConfig{
		Materializer:   mat,
		Cache:          nvmeCache,
		Packs:          encStore,
		Committer:      ddbRepo,
		LeaseRefresher: refresher,
		HookEngine:     hookEngine,
		Region:         leaseRegion,
	})
	if err != nil {
		t.Fatalf("NewReceivePackHandler: %v", err)
	}
	srv, err := NewServer(ServerConfig{
		Materializer:   mat,
		GitExecutor:    gitexec.NewSubprocessExecutor(gitexec.Config{}),
		WriteHandler:   wh,
		LeaseRefresher: refresher,
		Region:         string(leaseRegion),
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	client := startBufconnNode(t, srv)

	commit1, _, pack1 := buildSyntheticCommitPack(t, "kms-encrypted-blob-content\n", "kms push commit\n", nil)
	wire1 := buildPushWirePayload(
		[]ddb.RefUpdate{{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: commit1.String()}},
		"report-status side-band-64k atomic",
		pack1,
	)
	resp, exitCode, err := sendReceivePack(t, ctx, client, rk, "", wire1)
	if err != nil || exitCode != 0 {
		t.Fatalf("push failed: err=%v exit=%d", err, exitCode)
	}
	b1, _ := splitSideBandResponse(t, resp)
	if !strings.Contains(string(b1), "ok refs/heads/main\n") {
		t.Fatalf("push report-status missing ok: %q", string(b1))
	}

	packs, err := rawStore.ListRepoPacks(ctx, rk)
	if err != nil || len(packs) != 1 {
		t.Fatalf("ListRepoPacks = %+v, err=%v (want 1 pack)", packs, err)
	}
	packInfo := packs[0]
	idxKey, err := s3.IndexKey(rk, packInfo.PackSHA256)
	if err != nil {
		t.Fatalf("IndexKey: %v", err)
	}
	t.Cleanup(func() {
		_ = rawStore.DeleteObject(context.Background(), packInfo.Key)
		_ = rawStore.DeleteObject(context.Background(), idxKey)
	})

	// Raw S3 object must be KMS envelope-encrypted, never plaintext PACK.
	rawPack, err := rawStore.GetObject(ctx, packInfo.Key)
	if err != nil {
		t.Fatalf("raw GetObject(%s): %v", packInfo.Key, err)
	}
	if bytes.HasPrefix(rawPack, []byte("PACK")) {
		t.Fatal("expected KMS envelope-encrypted pack in Floci S3, got plaintext PACK header")
	}
	if len(rawPack) < 4 || rawPack[0] != 1 {
		t.Fatalf("expected envelope version 1 header in Floci S3, got len=%d prefix=%x", len(rawPack), rawPack[:min(len(rawPack), 4)])
	}

	// Encrypted s3.Store.GetPack must decrypt cleanly back to a valid Git PACK.
	decPack, err := encStore.GetPack(ctx, rk, packInfo.PackSHA256)
	if err != nil {
		t.Fatalf("encStore.GetPack: %v", err)
	}
	if !bytes.HasPrefix(decPack, []byte("PACK")) {
		t.Fatalf("decrypted pack missing PACK header: prefix=%q", decPack[:min(len(decPack), 4)])
	}
}
