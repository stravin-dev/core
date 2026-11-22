//go:build integration

package ctrl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/storage/s3"
	"stravin.dev/core/internal/tenant"
)

func newFlociCtrlDeps(t *testing.T) (*dynamodb.Client, string, *ddb.Repository, *DDBAuditStore, *s3.Store) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci ctrl integration test: AWS_ENDPOINT_URL not set")
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
	auditStore, err := NewDDBAuditStore(ddbClient, table)
	if err != nil {
		t.Fatalf("NewDDBAuditStore: %v", err)
	}
	s3Client := s3.NewAWSClient(cfg, endpoint, true)
	s3Store, err := s3.NewStore(s3Client, bucket)
	if err != nil {
		t.Fatalf("s3.NewStore: %v", err)
	}
	return ddbClient, table, ddbRepo, auditStore, s3Store
}

func TestFlociIntegration_LeaseFencingAndFaultInjectedScrub(t *testing.T) {
	ddbClient, table, ddbRepo, auditStore, s3Store := newFlociCtrlDeps(t)
	ctx := context.Background()

	nonce := fmt.Sprintf("ctrl-it-%d", time.Now().UnixNano())
	testRegion := lease.Region("us-ctrl-" + nonce)
	rk, err := tenant.NewRepoKey("local", nonce)
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	// 1. Seed Repo META at epoch 1.
	if err := ddbRepo.CreateMeta(ctx, ddb.RepoMeta{
		Repo:           rk,
		HomeRegion:     testRegion,
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     lease.ModeActive,
	}); err != nil {
		t.Fatalf("CreateMeta: %v", err)
	}

	mgr, err := NewLeaseManager(LeaseManagerConfig{
		Leases:            ddbRepo,
		Meta:              ddbRepo,
		Region:            testRegion,
		HolderID:          "floci-ctrl-" + nonce,
		LeaseTTL:          15 * time.Second,
		HeartbeatInterval: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewLeaseManager: %v", err)
	}

	// Acquire initial regional write lease and verify via lease.StoreVerifier.
	regLease, err := mgr.RenewOnce(ctx)
	if err != nil {
		t.Fatalf("RenewOnce: %v", err)
	}
	verifier := lease.NewStoreVerifier(ddbRepo, nil, 0)
	verified, err := verifier.VerifyWriteLease(ctx, rk, testRegion)
	if err != nil {
		t.Fatalf("VerifyWriteLease: %v", err)
	}
	if verified.Epoch != regLease.Epoch {
		t.Fatalf("verified epoch = %d, want %d", verified.Epoch, regLease.Epoch)
	}

	// Advance epoch via FenceRegion and verify stale epoch 1 is fenced.
	fenceRes, err := mgr.FenceRegion(ctx, FenceRequest{
		TargetMode:   lease.ModeActive,
		AdvanceEpoch: true,
		Repo:         &rk,
	})
	if err != nil {
		t.Fatalf("FenceRegion: %v", err)
	}
	if fenceRes.CurrentEpoch != 2 {
		t.Fatalf("fenced epoch = %d, want 2", fenceRes.CurrentEpoch)
	}
	if err := lease.VerifyEpochFence(fenceRes.CurrentEpoch, 1); !errors.Is(err, lease.ErrStaleEpoch) {
		t.Fatalf("expected stale epoch 1 rejected, got %v", err)
	}

	// 2. Upload two packs to Floci S3 and commit refs via DynamoDB TransactWriteItems.
	pack1 := strings.Repeat("1", 64)
	pack2 := strings.Repeat("2", 64)
	oid1 := strings.Repeat("a", 40)
	oid2 := strings.Repeat("b", 40)
	oid3 := strings.Repeat("c", 40)

	if err := s3Store.PutPack(ctx, rk, pack1, []byte("PACK-1-DATA"), []byte("IDX-1-DATA")); err != nil {
		t.Fatalf("PutPack(1): %v", err)
	}
	if err := s3Store.PutPack(ctx, rk, pack2, []byte("PACK-2-DATA"), []byte("IDX-2-DATA")); err != nil {
		t.Fatalf("PutPack(2): %v", err)
	}

	if _, err := ddbRepo.CommitPush(ctx, ddb.CommitPushInput{
		Repo:           rk,
		RefShards:      2,
		ManifestShards: 2,
		Epoch:          fenceRes.CurrentEpoch,
		OriginRegion:   testRegion,
		Updates: []ddb.RefUpdate{
			{RefName: "refs/heads/main", OldOID: ddb.ZeroOID, NewOID: oid1},
			{RefName: "refs/heads/stable", OldOID: ddb.ZeroOID, NewOID: oid1},
		},
		Pack: &ddb.PackUpload{PackID: pack1, SizeBytes: 11, OriginRegion: testRegion},
	}); err != nil {
		t.Fatalf("CommitPush(1): %v", err)
	}

	time.Sleep(5 * time.Millisecond)

	if _, err := ddbRepo.CommitPush(ctx, ddb.CommitPushInput{
		Repo:           rk,
		RefShards:      2,
		ManifestShards: 2,
		Epoch:          fenceRes.CurrentEpoch,
		OriginRegion:   testRegion,
		Updates: []ddb.RefUpdate{
			{RefName: "refs/heads/main", OldOID: oid1, NewOID: oid2},
			{RefName: "refs/heads/ephemeral", OldOID: ddb.ZeroOID, NewOID: oid3},
		},
		Pack: &ddb.PackUpload{PackID: pack2, SizeBytes: 11, OriginRegion: testRegion},
	}); err != nil {
		t.Fatalf("CommitPush(2): %v", err)
	}

	// 3. Fault injection: delete pack2.pack from Floci S3!
	pack2Key, err := s3.PackKey(rk, pack2)
	if err != nil {
		t.Fatalf("PackKey: %v", err)
	}
	if err := s3Store.DeleteObject(ctx, pack2Key); err != nil {
		t.Fatalf("DeleteObject(pack2): %v", err)
	}

	// 4. Trigger POST /v1/scrub through HTTP admin API.
	scrubber, err := NewScrubber(ScrubConfig{
		DDB:    ddbRepo,
		S3:     s3Store,
		Audit:  auditStore,
		Region: testRegion,
	})
	if err != nil {
		t.Fatalf("NewScrubber: %v", err)
	}
	const flociAdminToken = "floci-ctrl-admin-token"
	handler, err := NewHandler(HandlerConfig{
		Leases:     mgr,
		Scrubber:   scrubber,
		AdminToken: flociAdminToken,
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	srv := httptest.NewServer(handler)
	defer srv.Close()

	reqPayload, _ := json.Marshal(map[string]any{
		"tenant_id":        string(rk.TenantID),
		"repo_id":          string(rk.RepoID),
		"lookback_seconds": 1200,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/scrub", bytes.NewReader(reqPayload))
	if err != nil {
		t.Fatalf("NewRequest POST /v1/scrub: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+flociAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/scrub: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /v1/scrub status = %d, want 200", resp.StatusCode)
	}

	var report ScrubReport
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatalf("decode ScrubReport: %v", err)
	}
	if report.Status != "rewound" {
		t.Fatalf("report.Status = %q, want rewound", report.Status)
	}
	if len(report.MissingPackIDs) != 1 || report.MissingPackIDs[0] != pack2 {
		t.Fatalf("missing_pack_ids = %v, want [%s]", report.MissingPackIDs, pack2)
	}
	if len(report.RewoundRefs) != 2 {
		t.Fatalf("rewound_refs = %+v, want 2", report.RewoundRefs)
	}

	// 5. Verify DDB refs: main rewound to oid1, ephemeral deleted, stable untouched.
	mainRef, err := ddbRepo.GetRef(ctx, rk, "refs/heads/main", 2)
	if err != nil || mainRef.OID != oid1 {
		t.Fatalf("main ref = %+v err=%v, want OID %s", mainRef, err, oid1)
	}
	stableRef, err := ddbRepo.GetRef(ctx, rk, "refs/heads/stable", 2)
	if err != nil || stableRef.OID != oid1 {
		t.Fatalf("stable ref = %+v err=%v, want OID %s", stableRef, err, oid1)
	}
	if _, err := ddbRepo.GetRef(ctx, rk, "refs/heads/ephemeral", 2); !errors.Is(err, ddb.ErrNotFound) {
		t.Fatalf("expected refs/heads/ephemeral deleted, got err=%v", err)
	}

	// 6. Verify immutable audit item exists in Floci DynamoDB.
	auditKey, err := ScrubAuditKey(report)
	if err != nil {
		t.Fatalf("ScrubAuditKey: %v", err)
	}
	auditOut, err := ddbClient.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(table),
		Key:            auditKey.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil || len(auditOut.Item) == 0 {
		t.Fatalf("expected audit item in DDB, got len=%d err=%v", len(auditOut.Item), err)
	}
}
