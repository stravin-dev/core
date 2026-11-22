package ddb

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type stubAPI struct {
	getItemFn      func(ctx context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error)
	putItemFn      func(ctx context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	updateItemFn   func(ctx context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error)
	deleteItemFn   func(ctx context.Context, in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
	queryFn        func(ctx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error)
	transactWriteF func(ctx context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error)
}

func (s *stubAPI) GetItem(ctx context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return s.getItemFn(ctx, in)
}
func (s *stubAPI) PutItem(ctx context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return s.putItemFn(ctx, in)
}
func (s *stubAPI) UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return s.updateItemFn(ctx, in)
}
func (s *stubAPI) DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return s.deleteItemFn(ctx, in)
}
func (s *stubAPI) Query(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return s.queryFn(ctx, in)
}
func (s *stubAPI) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	return s.transactWriteF(ctx, in)
}

func TestRepository_GetLeaseIntegratesWithStoreVerifier(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	now := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	clk := &fixedClock{now: now}

	metaKey, _ := MetaKey(repo)
	regKey, _ := RegionalWriteLeaseKey(lease.Region("us-east-1"))

	api := &stubAPI{
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			pk := getString(in.Key, AttrPK)
			sk := getString(in.Key, AttrSK)
			if pk == metaKey.PK && sk == metaKey.SK {
				return &dynamodb.GetItemOutput{
					Item: marshalRepoMeta(metaKey, RepoMeta{
						Repo:           repo,
						HomeRegion:     "us-east-1",
						FailoverEpoch:  3,
						RefShards:      4,
						ManifestShards: 2,
						WriteState:     lease.ModeActive,
					}),
				}, nil
			}
			if pk == regKey.PK && sk == regKey.SK {
				return &dynamodb.GetItemOutput{
					Item: marshalRegionalWriteLease(regKey, RegionalWriteLease{
						Region:    "us-east-1",
						Epoch:     3,
						Mode:      lease.ModeActive,
						HolderID:  "ctrl-1",
						ExpiresAt: now.Add(15 * time.Second),
					}),
				}, nil
			}
			return &dynamodb.GetItemOutput{}, nil
		},
	}

	r, err := NewRepository(api, "stravin-core", WithClock(clk))
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	verifier := lease.NewStoreVerifier(r, clk, 2*time.Second)
	got, err := verifier.VerifyWriteLease(context.Background(), repo, lease.Region("us-east-1"))
	if err != nil {
		t.Fatalf("VerifyWriteLease: %v", err)
	}
	if got.Epoch != 3 || got.HolderID != "ctrl-1" || got.HomeRegion != "us-east-1" {
		t.Fatalf("unexpected lease: %+v", got)
	}

	// Wrong region must be rejected by StoreVerifier.
	if _, err := verifier.VerifyWriteLease(context.Background(), repo, lease.Region("us-west-2")); !errors.Is(err, lease.ErrWrongRegion) {
		t.Fatalf("expected ErrWrongRegion, got %v", err)
	}
}

func TestRepository_GetEffectiveHooksMergesOrgAndRepo(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	orgKey, _ := OrgHooksKey(repo.TenantID, "eng")
	repoKey, _ := RepoHooksKey(repo)

	stored := map[string]map[string]types.AttributeValue{}
	api := &stubAPI{
		putItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			k := getString(in.Item, AttrPK) + "|" + getString(in.Item, AttrSK)
			stored[k] = in.Item
			return &dynamodb.PutItemOutput{}, nil
		},
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			k := getString(in.Key, AttrPK) + "|" + getString(in.Key, AttrSK)
			return &dynamodb.GetItemOutput{Item: stored[k]}, nil
		},
	}

	r, _ := NewRepository(api, "stravin-core")
	ctx := context.Background()

	if err := r.PutOrgHooks(ctx, repo.TenantID, "eng", []HookConfig{
		{HookID: "org-secret-scan", WasmSHA256: "aaa", RefGlob: "refs/*", TimeoutMS: 50, MemoryLimitMB: 32, FailMode: FailModeClosed},
	}); err != nil {
		t.Fatalf("PutOrgHooks: %v", err)
	}
	if err := r.PutRepoHooks(ctx, repo, []HookConfig{
		{HookID: "repo-commit-lint", WasmSHA256: "bbb", RefGlob: "refs/heads/main", TimeoutMS: 30, MemoryLimitMB: 16, FailMode: FailModeOpen},
	}); err != nil {
		t.Fatalf("PutRepoHooks: %v", err)
	}

	if _, ok := stored[orgKey.PK+"|"+orgKey.SK]; !ok {
		t.Fatalf("missing org hooks key %+v", orgKey)
	}
	if _, ok := stored[repoKey.PK+"|"+repoKey.SK]; !ok {
		t.Fatalf("missing repo hooks key %+v", repoKey)
	}

	eff, err := r.GetEffectiveHooks(ctx, repo, "eng")
	if err != nil {
		t.Fatalf("GetEffectiveHooks: %v", err)
	}
	if len(eff) != 2 || eff[0].HookID != "org-secret-scan" || eff[1].HookID != "repo-commit-lint" {
		t.Fatalf("unexpected effective hooks: %+v", eff)
	}
}

func TestRepository_WebhooksAndStreamCheckpointRoundTrip(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	stored := map[string]map[string]types.AttributeValue{}
	api := &stubAPI{
		putItemFn: func(_ context.Context, in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			k := getString(in.Item, AttrPK) + "|" + getString(in.Item, AttrSK)
			stored[k] = in.Item
			return &dynamodb.PutItemOutput{}, nil
		},
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			k := getString(in.Key, AttrPK) + "|" + getString(in.Key, AttrSK)
			return &dynamodb.GetItemOutput{Item: stored[k]}, nil
		},
	}

	r, _ := NewRepository(api, "stravin-core")
	ctx := context.Background()

	secret := []byte{0xde, 0xad, 0xbe, 0xef}
	want := []WebhookConfig{{
		WebhookID:       "wh-1",
		URL:             "https://ci.example.com/hook",
		EncryptedSecret: secret,
		KMSKeyID:        "alias/stravin-local-dev",
		RefGlob:         "refs/heads/*",
		Enabled:         true,
		Retry: WebhookRetryConfig{
			MaxAttempts:      5,
			InitialBackoffMS: 250,
			MaxBackoffMS:     10000,
			DLQURL:           "https://sqs.us-east-1.amazonaws.com/123/dlq",
		},
	}}
	if err := r.PutWebhooks(ctx, repo, want); err != nil {
		t.Fatalf("PutWebhooks: %v", err)
	}
	got, err := r.GetWebhooks(ctx, repo)
	if err != nil {
		t.Fatalf("GetWebhooks: %v", err)
	}
	if len(got) != 1 || got[0].WebhookID != "wh-1" || !bytes.Equal(got[0].EncryptedSecret, secret) || got[0].Retry.MaxAttempts != 5 {
		t.Fatalf("unexpected webhooks: %+v", got)
	}

	cp := StreamCheckpoint{
		ShardID:        "shardId-000000000001",
		SequenceNumber: "000000000000000000042",
		ConsumerID:     "stravin-hook-1",
	}
	if err := r.PutStreamCheckpoint(ctx, cp); err != nil {
		t.Fatalf("PutStreamCheckpoint: %v", err)
	}
	gotCP, err := r.GetStreamCheckpoint(ctx, cp.ShardID)
	if err != nil {
		t.Fatalf("GetStreamCheckpoint: %v", err)
	}
	if gotCP.SequenceNumber != cp.SequenceNumber || gotCP.ConsumerID != cp.ConsumerID {
		t.Fatalf("unexpected checkpoint: %+v", gotCP)
	}
}

func TestRepository_ManifestWatermarkQueryParams(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	var captured *dynamodb.QueryInput
	api := &stubAPI{
		queryFn: func(_ context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			captured = in
			return &dynamodb.QueryOutput{}, nil
		},
	}

	r, _ := NewRepository(api, "stravin-core")
	_, err := r.QueryManifestShard(context.Background(), repo, 0, "01ARZ3NDEKTSV4RRFFQ69G5FAV", true)
	if err != nil {
		t.Fatalf("QueryManifestShard: %v", err)
	}
	if aws.ToString(captured.KeyConditionExpression) != "PK = :pk AND SK > :watermark" {
		t.Fatalf("unexpected KeyConditionExpression: %s", aws.ToString(captured.KeyConditionExpression))
	}
	if getString(captured.ExpressionAttributeValues, ":watermark") != "PACK#01ARZ3NDEKTSV4RRFFQ69G5FAV~" {
		t.Fatalf("unexpected watermark: %s", getString(captured.ExpressionAttributeValues, ":watermark"))
	}
	if aws.ToString(captured.FilterExpression) != "#status = :active" {
		t.Fatalf("unexpected FilterExpression: %s", aws.ToString(captured.FilterExpression))
	}
}

func TestRepository_ConditionalErrorsMapped(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	api := &stubAPI{
		putItemFn: func(_ context.Context, _ *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("exists")}
		},
		updateItemFn: func(_ context.Context, _ *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("stale")}
		},
		deleteItemFn: func(_ context.Context, _ *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
			return nil, &types.ConditionalCheckFailedException{Message: aws.String("not owner")}
		},
	}
	r, _ := NewRepository(api, "stravin-core")
	ctx := context.Background()

	meta := RepoMeta{
		Repo: repo, HomeRegion: "us-east-1", FailoverEpoch: 1,
		RefShards: 2, ManifestShards: 2, WriteState: lease.ModeActive,
	}
	if err := r.CreateMeta(ctx, meta); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("CreateMeta = %v, want ErrAlreadyExists", err)
	}
	if err := r.UpdateMeta(ctx, meta, 1); !errors.Is(err, ErrEpochConflict) {
		t.Fatalf("UpdateMeta = %v, want ErrEpochConflict", err)
	}
	if _, err := r.AcquireShardLease(ctx, repo, 0, "worker-a", 10*time.Second); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("AcquireShardLease = %v, want ErrLeaseHeld", err)
	}
	if _, err := r.RenewShardLease(ctx, repo, 0, "worker-a", 10*time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("RenewShardLease = %v, want ErrLeaseLost", err)
	}
	if err := r.ReleaseShardLease(ctx, repo, 0, "worker-a"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("ReleaseShardLease = %v, want ErrLeaseLost", err)
	}
}
