package ddb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

func newFlociClients(t *testing.T) (*dynamodb.Client, *dynamodbstreams.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		t.Skip("skipping Floci integration test: AWS_ENDPOINT_URL not set")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}
	table := os.Getenv("STRAVIN_DDB_TABLE")
	if table == "" {
		table = "stravin-core"
	}

	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(region))
	if err != nil {
		t.Fatalf("LoadDefaultConfig: %v", err)
	}

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	streamsClient := dynamodbstreams.NewFromConfig(cfg, func(o *dynamodbstreams.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
	return ddbClient, streamsClient, table
}

func TestFlociIntegration_FullSchemaRefCASAndStreams(t *testing.T) {
	ddbClient, streamsClient, table := newFlociClients(t)
	ctx := context.Background()

	repo, err := NewRepository(ddbClient, table)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	nonce := fmt.Sprintf("it-%d", time.Now().UnixNano())
	rk := mustRepoKey(t, "local", nonce)
	const refShards = 4
	const manifestShards = 2

	// 1. META create, duplicate reject, read, and epoch-fenced update.
	meta := RepoMeta{
		Repo:           rk,
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		WriteState:     lease.ModeActive,
		DefaultBranch:  "refs/heads/main",
	}
	if err := repo.CreateMeta(ctx, meta); err != nil {
		t.Fatalf("CreateMeta: %v", err)
	}
	if err := repo.CreateMeta(ctx, meta); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate CreateMeta = %v, want ErrAlreadyExists", err)
	}
	gotMeta, err := repo.GetMeta(ctx, rk)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if gotMeta.FailoverEpoch != 1 || gotMeta.RefShards != refShards || gotMeta.DefaultBranch != "refs/heads/main" {
		t.Fatalf("unexpected meta: %+v", gotMeta)
	}
	meta.FailoverEpoch = 2
	if err := repo.UpdateMeta(ctx, meta, 1); err != nil {
		t.Fatalf("UpdateMeta: %v", err)
	}
	if err := repo.UpdateMeta(ctx, meta, 1); !errors.Is(err, ErrEpochConflict) {
		t.Fatalf("stale UpdateMeta = %v, want ErrEpochConflict", err)
	}

	// 2. CommitPush create branch + pack manifest + W3C traceparent.
	traceParent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	res1, err := repo.CommitPush(ctx, CommitPushInput{
		Repo:           rk,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		Epoch:          2,
		OriginRegion:   "us-east-1",
		TraceParent:    traceParent,
		Updates: []RefUpdate{
			{RefName: "refs/heads/main", OldOID: ZeroOID, NewOID: oidA},
		},
		Pack: &PackUpload{
			PackID:    "pack-1-" + nonce,
			SizeBytes: 1024,
		},
	})
	if err != nil {
		t.Fatalf("CommitPush #1: %v", err)
	}

	gotRef, err := repo.GetRef(ctx, rk, "refs/heads/main", refShards)
	if err != nil {
		t.Fatalf("GetRef: %v", err)
	}
	if gotRef.OID != oidA || gotRef.PackID != "pack-1-"+nonce {
		t.Fatalf("unexpected ref state: %+v", gotRef)
	}

	// 3. Stale Ref-CAS push must fail with ErrRefConflict / ErrRefCASConflict.
	_, err = repo.CommitPush(ctx, CommitPushInput{
		Repo:           rk,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		Epoch:          2,
		OriginRegion:   "us-east-1",
		Updates: []RefUpdate{
			{RefName: "refs/heads/main", OldOID: oidC, NewOID: oidB},
		},
		Pack: &PackUpload{
			PackID:    "pack-conflict-" + nonce,
			SizeBytes: 512,
		},
	})
	if !errors.Is(err, ErrRefConflict) || !errors.Is(err, ErrRefCASConflict) {
		t.Fatalf("stale CommitPush err = %v, want ErrRefConflict", err)
	}
	var rce *RefConflictError
	if !errors.As(err, &rce) || len(rce.ConflictingRefs) != 1 || rce.ConflictingRefs[0] != "refs/heads/main" {
		t.Fatalf("unexpected RefConflictError: %#v", rce)
	}

	// 4. Valid update push + verify >45 ref guard.
	time.Sleep(2 * time.Millisecond)
	res2, err := repo.CommitPush(ctx, CommitPushInput{
		Repo:           rk,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		Epoch:          2,
		OriginRegion:   "us-east-1",
		Updates: []RefUpdate{
			{RefName: "refs/heads/main", OldOID: oidA, NewOID: oidB},
			{RefName: "refs/heads/temp", OldOID: ZeroOID, NewOID: oidB},
		},
		Pack: &PackUpload{
			PackID:    "pack-2-" + nonce,
			SizeBytes: 2048,
		},
	})
	if err != nil {
		t.Fatalf("CommitPush #2: %v", err)
	}

	refs, err := repo.ListRefs(ctx, rk, refShards)
	if err != nil || len(refs) != 2 {
		t.Fatalf("ListRefs = %+v, err=%v", refs, err)
	}
	histAll, err := repo.ListRefHistory(ctx, rk, "refs/heads/main", refShards, "", 0)
	if err != nil || len(histAll) != 2 {
		t.Fatalf("ListRefHistory all = %+v, err=%v", histAll, err)
	}
	histAfter, err := repo.ListRefHistory(ctx, rk, "refs/heads/main", refShards, res1.ULID, 0)
	if err != nil || len(histAfter) != 1 || histAfter[0].ULID != res2.ULID {
		t.Fatalf("ListRefHistory after res1 = %+v, err=%v", histAfter, err)
	}

	// 5. Ref deletion push (0000... OID, no pack required).
	time.Sleep(2 * time.Millisecond)
	if _, err := repo.CommitPush(ctx, CommitPushInput{
		Repo:         rk,
		RefShards:    refShards,
		Epoch:        2,
		OriginRegion: "us-east-1",
		Updates: []RefUpdate{
			{RefName: "refs/heads/temp", OldOID: oidB, NewOID: ZeroOID},
		},
		Pack: nil,
	}); err != nil {
		t.Fatalf("CommitPush delete: %v", err)
	}
	if _, err := repo.GetRef(ctx, rk, "refs/heads/temp", refShards); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted ref GetRef = %v, want ErrNotFound", err)
	}

	// 6. Manifest watermark query + 2-phase compaction swap.
	since1, err := repo.ListManifestsSince(ctx, rk, manifestShards, res1.ULID, true)
	if err != nil {
		t.Fatalf("ListManifestsSince: %v", err)
	}
	if len(since1) != 1 || since1[0].PackID != "pack-2-"+nonce {
		t.Fatalf("unexpected manifests since res1: %+v", since1)
	}

	time.Sleep(2 * time.Millisecond)
	compactedULID, err := NewULID(time.Now().UTC())
	if err != nil {
		t.Fatalf("NewULID: %v", err)
	}
	compactedPack := PackManifest{
		Repo:         rk,
		ULID:         compactedULID,
		PackID:       "pack-compacted-" + nonce,
		SizeBytes:    2500,
		OriginRegion: "us-east-1",
		Status:       PackStatusActive,
	}
	if err := repo.PublishCompactedPacks(ctx, rk, manifestShards, []PackManifest{compactedPack}); err != nil {
		t.Fatalf("PublishCompactedPacks: %v", err)
	}
	oldPacks := []PackManifest{
		{Repo: rk, ULID: res1.ULID, PackID: "pack-1-" + nonce},
		{Repo: rk, ULID: res2.ULID, PackID: "pack-2-" + nonce},
	}
	if err := repo.SupersedePacks(ctx, rk, manifestShards, oldPacks); err != nil {
		t.Fatalf("SupersedePacks: %v", err)
	}
	activePacks, err := repo.ListManifestsSince(ctx, rk, manifestShards, "", true)
	if err != nil {
		t.Fatalf("ListManifestsSince activeOnly: %v", err)
	}
	if len(activePacks) != 1 || activePacks[0].PackID != compactedPack.PackID {
		t.Fatalf("expected only compacted pack active, got %+v", activePacks)
	}

	// 7. Compaction shard leases + regional write lease + lease.StoreVerifier.
	if _, err := repo.AcquireShardLease(ctx, rk, 0, "pack-worker-1", 10*time.Second); err != nil {
		t.Fatalf("AcquireShardLease: %v", err)
	}
	if _, err := repo.AcquireShardLease(ctx, rk, 0, "pack-worker-2", 10*time.Second); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("competing AcquireShardLease = %v, want ErrLeaseHeld", err)
	}
	if _, err := repo.RenewShardLease(ctx, rk, 0, "pack-worker-1", 20*time.Second); err != nil {
		t.Fatalf("RenewShardLease: %v", err)
	}
	if err := repo.ReleaseShardLease(ctx, rk, 0, "pack-worker-1"); err != nil {
		t.Fatalf("ReleaseShardLease: %v", err)
	}

	if _, err := repo.AcquireRegionalWriteLease(ctx, "us-east-1", "ctrl-"+nonce, 2, lease.ModeActive, 30*time.Second); err != nil {
		t.Fatalf("AcquireRegionalWriteLease: %v", err)
	}
	verifier := lease.NewStoreVerifier(repo, lease.RealClock{}, 1*time.Second)
	vl, err := verifier.VerifyWriteLease(ctx, rk, "us-east-1")
	if err != nil {
		t.Fatalf("VerifyWriteLease: %v", err)
	}
	if vl.Epoch != 2 || vl.HomeRegion != "us-east-1" {
		t.Fatalf("unexpected verified lease: %+v", vl)
	}

	// 8. HOOKS (org + repo), WEBHOOKS, and StreamCheckpoint.
	orgID := "org-" + nonce
	if err := repo.PutOrgHooks(ctx, tenant.TenantID("local"), orgID, []HookConfig{
		{HookID: "h-org", WasmSHA256: "abc123", RefGlob: "refs/*", TimeoutMS: 50, MemoryLimitMB: 32, FailMode: FailModeClosed},
	}); err != nil {
		t.Fatalf("PutOrgHooks: %v", err)
	}
	if err := repo.PutRepoHooks(ctx, rk, []HookConfig{
		{HookID: "h-repo", WasmSHA256: "def456", RefGlob: "refs/heads/main", TimeoutMS: 40, MemoryLimitMB: 16, FailMode: FailModeOpen},
	}); err != nil {
		t.Fatalf("PutRepoHooks: %v", err)
	}
	effHooks, err := repo.GetEffectiveHooks(ctx, rk, orgID)
	if err != nil || len(effHooks) != 2 || effHooks[0].HookID != "h-org" || effHooks[1].HookID != "h-repo" {
		t.Fatalf("GetEffectiveHooks = %+v, err=%v", effHooks, err)
	}

	secretBlob := []byte("kms-ciphertext-bytes")
	if err := repo.PutWebhooks(ctx, rk, []WebhookConfig{{
		WebhookID: "wh-1", URL: "https://example.invalid/hook", EncryptedSecret: secretBlob,
		KMSKeyID: "alias/stravin-local-dev", RefGlob: "refs/heads/*", Enabled: true,
		Retry: WebhookRetryConfig{MaxAttempts: 3, InitialBackoffMS: 100, MaxBackoffMS: 1000, DLQURL: "sqs://dlq"},
	}}); err != nil {
		t.Fatalf("PutWebhooks: %v", err)
	}
	gotWH, err := repo.GetWebhooks(ctx, rk)
	if err != nil || len(gotWH) != 1 || !bytes.Equal(gotWH[0].EncryptedSecret, secretBlob) {
		t.Fatalf("GetWebhooks = %+v, err=%v", gotWH, err)
	}

	cpShard := "shard-" + nonce
	if err := repo.PutStreamCheckpoint(ctx, StreamCheckpoint{ShardID: cpShard, SequenceNumber: "1001", ConsumerID: "hook-1"}); err != nil {
		t.Fatalf("PutStreamCheckpoint: %v", err)
	}
	gotCP, err := repo.GetStreamCheckpoint(ctx, cpShard)
	if err != nil || gotCP.SequenceNumber != "1001" {
		t.Fatalf("GetStreamCheckpoint = %+v, err=%v", gotCP, err)
	}

	// 9. Verify DynamoDB Streams NEW_IMAGE emission from CommitPush TransactWriteItems.
	rhKey1, err := RefHistoryKey(rk, "refs/heads/main", res1.ULID, refShards)
	if err != nil {
		t.Fatalf("RefHistoryKey: %v", err)
	}
	verifyStreamNewImage(t, ctx, ddbClient, streamsClient, table, rhKey1, oidA, traceParent)
}

func verifyStreamNewImage(
	t *testing.T,
	ctx context.Context,
	ddbClient *dynamodb.Client,
	streamsClient *dynamodbstreams.Client,
	table string,
	wantKey Key,
	wantOID string,
	wantTraceParent string,
) {
	t.Helper()
	descTbl, err := ddbClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		t.Fatalf("DescribeTable: %v", err)
	}
	streamARN := aws.ToString(descTbl.Table.LatestStreamArn)
	if streamARN == "" {
		t.Fatalf("table %q has no LatestStreamArn", table)
	}

	descStream, err := streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(streamARN),
	})
	if err != nil {
		t.Fatalf("DescribeStream: %v", err)
	}
	if len(descStream.StreamDescription.Shards) == 0 {
		t.Fatalf("DescribeStream returned 0 shards")
	}

	for _, shard := range descStream.StreamDescription.Shards {
		iterOut, err := streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         aws.String(streamARN),
			ShardId:           shard.ShardId,
			ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
		})
		if err != nil || iterOut.ShardIterator == nil {
			continue
		}
		iter := iterOut.ShardIterator
		for page := 0; page < 10 && iter != nil; page++ {
			recs, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: iter,
			})
			if err != nil {
				t.Fatalf("GetRecords: %v", err)
			}
			for _, rec := range recs.Records {
				if rec.Dynamodb == nil || len(rec.Dynamodb.NewImage) == 0 {
					continue
				}
				img := rec.Dynamodb.NewImage
				if streamStr(img, AttrPK) == wantKey.PK && streamStr(img, AttrSK) == wantKey.SK {
					if streamStr(img, "oid") != wantOID {
						t.Fatalf("stream NEW_IMAGE oid=%q, want %q", streamStr(img, "oid"), wantOID)
					}
					if streamStr(img, "traceparent") != wantTraceParent {
						t.Fatalf("stream NEW_IMAGE traceparent=%q, want %q", streamStr(img, "traceparent"), wantTraceParent)
					}
					if streamNum(img, "ttl") == "" {
						t.Fatalf("stream NEW_IMAGE missing ttl attribute")
					}
					return
				}
			}
			if len(recs.Records) == 0 {
				break
			}
			iter = recs.NextShardIterator
		}
	}
	t.Fatalf("did not find DynamoDB Streams NEW_IMAGE record for PK=%s SK=%s", wantKey.PK, wantKey.SK)
}

func streamStr(img map[string]streamtypes.AttributeValue, k string) string {
	av, ok := img[k]
	if !ok {
		return ""
	}
	s, ok := av.(*streamtypes.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return s.Value
}

func streamNum(img map[string]streamtypes.AttributeValue, k string) string {
	av, ok := img[k]
	if !ok {
		return ""
	}
	n, ok := av.(*streamtypes.AttributeValueMemberN)
	if !ok {
		return ""
	}
	return n.Value
}
