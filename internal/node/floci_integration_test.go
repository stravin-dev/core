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
	"stravin.dev/core/internal/gitexec"
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
