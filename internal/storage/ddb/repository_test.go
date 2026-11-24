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

func TestRepository_ListRefsParallelShardsDeterministicSort(t *testing.T) {
	repo := mustRepoKey(t, "acme", "parallel-refs")
	const refShards = 4
	nowStr := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC).Format(time.RFC3339Nano)

	pkToShard := make(map[string]int, refShards)
	for s := 0; s < refShards; s++ {
		pk, err := RefStateShardPK(repo, s)
		if err != nil {
			t.Fatalf("RefStateShardPK: %v", err)
		}
		pkToShard[pk] = s
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Barrier proves all 4 shards are queried concurrently; sequential queries will time out.
	arrivedCh := make(chan struct{}, refShards)
	allArrived := make(chan struct{})
	go func() {
		for i := 0; i < refShards; i++ {
			select {
			case <-arrivedCh:
			case <-ctx.Done():
				return
			}
		}
		close(allArrived)
	}()

	api := &stubAPI{
		queryFn: func(qCtx context.Context, in *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			pk := getString(in.ExpressionAttributeValues, ":pk")
			shard, ok := pkToShard[pk]
			if !ok {
				return nil, errors.New("unexpected pk " + pk)
			}
			arrivedCh <- struct{}{}
			select {
			case <-allArrived:
			case <-qCtx.Done():
				return nil, qCtx.Err()
			}
			// Return refs in reverse shard order so sorting is exercised.
			refName := "refs/heads/branch-" + string(rune('d'-shard))
			return &dynamodb.QueryOutput{
				Items: []map[string]types.AttributeValue{
					{
						AttrPK:          attrS(pk),
						AttrSK:          attrS("REF#" + refName),
						"ref_name":      attrS(refName),
						"oid":           attrS(oidA),
						"prev_oid":      attrS(ZeroOID),
						"pack_id":       attrS("pack-1"),
						"updated_at":    attrS(nowStr),
						"epoch":         attrNUint64(1),
						"origin_region": attrS("us-east-1"),
					},
				},
			}, nil
		},
	}

	r, err := NewRepository(api, "stravin-core")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	refs, err := r.ListRefs(ctx, repo, refShards)
	if err != nil {
		t.Fatalf("ListRefs: %v", err)
	}
	want := []string{"refs/heads/branch-a", "refs/heads/branch-b", "refs/heads/branch-c", "refs/heads/branch-d"}
	if len(refs) != len(want) {
		t.Fatalf("len(refs) = %d, want %d", len(refs), len(want))
	}
	for i, w := range want {
		if refs[i].RefName != w {
			t.Fatalf("refs[%d].RefName = %q, want %q", i, refs[i].RefName, w)
		}
	}
}

func TestRepository_MetaVersionsRoundTripAndConsistentRead(t *testing.T) {
	repo := mustRepoKey(t, "acme", "meta-ver")
	metaKey, err := MetaKey(repo)
	if err != nil {
		t.Fatalf("MetaKey: %v", err)
	}

	// Legacy META without ref_version / manifest_version defaults to 0.
	legacyItem := map[string]types.AttributeValue{
		AttrPK:            attrS(metaKey.PK),
		AttrSK:            attrS(metaKey.SK),
		"home_region":     attrS("us-east-1"),
		"failover_epoch":  attrNUint64(1),
		"ref_shards":      attrNInt(2),
		"manifest_shards": attrNInt(2),
		"write_state":     attrS(string(lease.ModeActive)),
	}
	legacy, err := unmarshalRepoMeta(repo, legacyItem)
	if err != nil {
		t.Fatalf("unmarshalRepoMeta(legacy): %v", err)
	}
	if legacy.RefVersion != 0 || legacy.ManifestVersion != 0 {
		t.Fatalf("legacy versions = (%d, %d), want (0, 0)", legacy.RefVersion, legacy.ManifestVersion)
	}

	// Versioned META round-trips through GetMeta with ConsistentRead=true.
	wantMeta := RepoMeta{
		Repo:            repo,
		HomeRegion:      "us-east-1",
		FailoverEpoch:   2,
		RefShards:       4,
		ManifestShards:  2,
		WriteState:      lease.ModeActive,
		DefaultBranch:   "refs/heads/main",
		RefVersion:      11,
		ManifestVersion: 5,
	}
	var gotConsistentRead bool
	api := &stubAPI{
		getItemFn: func(_ context.Context, in *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
			gotConsistentRead = aws.ToBool(in.ConsistentRead)
			return &dynamodb.GetItemOutput{Item: marshalRepoMeta(metaKey, wantMeta)}, nil
		},
	}
	r, err := NewRepository(api, "stravin-core")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	got, err := r.GetMeta(context.Background(), repo)
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if !gotConsistentRead {
		t.Fatalf("GetMeta ConsistentRead = false, want true")
	}
	if got.RefVersion != 11 || got.ManifestVersion != 5 {
		t.Fatalf("got versions = (%d, %d), want (11, 5)", got.RefVersion, got.ManifestVersion)
	}
}

func TestRepository_CompactionBumpsManifestVersion(t *testing.T) {
	repo := mustRepoKey(t, "acme", "compact-ver")
	metaKey, err := MetaKey(repo)
	if err != nil {
		t.Fatalf("MetaKey: %v", err)
	}

	makePacks := func(n int) []PackManifest {
		out := make([]PackManifest, n)
		for i := 0; i < n; i++ {
			out[i] = PackManifest{
				Repo:         repo,
				ULID:         "01ARZ3NDEKTSV4RRFFQ69G5FAV",
				PackID:       "pack-" + string(rune('a'+(i%26))) + "-" + time.Duration(i).String(),
				SizeBytes:    128,
				OriginRegion: "us-east-1",
				Status:       PackStatusActive,
			}
		}
		return out
	}

	t.Run("PublishCompactedPacks_in_tx_when_le_99", func(t *testing.T) {
		var txItems []types.TransactWriteItem
		updateCalls := 0
		api := &stubAPI{
			transactWriteF: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
				txItems = in.TransactItems
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
			updateItemFn: func(_ context.Context, _ *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				updateCalls++
				return &dynamodb.UpdateItemOutput{}, nil
			},
		}
		r, _ := NewRepository(api, "stravin-core")
		if err := r.PublishCompactedPacks(context.Background(), repo, 2, makePacks(2)); err != nil {
			t.Fatalf("PublishCompactedPacks: %v", err)
		}
		if len(txItems) != 3 || updateCalls != 0 {
			t.Fatalf("len(txItems)=%d updateCalls=%d, want 3 and 0", len(txItems), updateCalls)
		}
		upd := txItems[2].Update
		if upd == nil || getString(upd.Key, AttrPK) != metaKey.PK || getString(upd.Key, AttrSK) != metaKey.SK {
			t.Fatalf("expected META Update at txItems[2], got %+v", txItems[2])
		}
		if aws.ToString(upd.UpdateExpression) != "ADD manifest_version :one" {
			t.Fatalf("UpdateExpression = %q, want ADD manifest_version :one", aws.ToString(upd.UpdateExpression))
		}
	})

	t.Run("PublishCompactedPacks_update_after_when_100", func(t *testing.T) {
		var txLen int
		var updIn *dynamodb.UpdateItemInput
		api := &stubAPI{
			transactWriteF: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
				txLen = len(in.TransactItems)
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
			updateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				updIn = in
				return &dynamodb.UpdateItemOutput{}, nil
			},
		}
		r, _ := NewRepository(api, "stravin-core")
		if err := r.PublishCompactedPacks(context.Background(), repo, 2, makePacks(100)); err != nil {
			t.Fatalf("PublishCompactedPacks(100): %v", err)
		}
		if txLen != 100 || updIn == nil {
			t.Fatalf("txLen=%d updIn=%v, want 100 and non-nil", txLen, updIn)
		}
		if aws.ToString(updIn.UpdateExpression) != "ADD manifest_version :one" {
			t.Fatalf("UpdateExpression = %q, want ADD manifest_version :one", aws.ToString(updIn.UpdateExpression))
		}
	})

	t.Run("SupersedePacks_in_tx_when_le_99", func(t *testing.T) {
		var txItems []types.TransactWriteItem
		updateCalls := 0
		api := &stubAPI{
			transactWriteF: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
				txItems = in.TransactItems
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
			updateItemFn: func(_ context.Context, _ *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				updateCalls++
				return &dynamodb.UpdateItemOutput{}, nil
			},
		}
		r, _ := NewRepository(api, "stravin-core")
		if err := r.SupersedePacks(context.Background(), repo, 2, makePacks(3)); err != nil {
			t.Fatalf("SupersedePacks: %v", err)
		}
		if len(txItems) != 4 || updateCalls != 0 {
			t.Fatalf("len(txItems)=%d updateCalls=%d, want 4 and 0", len(txItems), updateCalls)
		}
		upd := txItems[3].Update
		if upd == nil || getString(upd.Key, AttrSK) != "META" || aws.ToString(upd.UpdateExpression) != "ADD manifest_version :one" {
			t.Fatalf("unexpected META Update at txItems[3]: %+v", txItems[3])
		}
	})

	t.Run("SupersedePacks_update_after_when_100", func(t *testing.T) {
		var txLen int
		var updIn *dynamodb.UpdateItemInput
		api := &stubAPI{
			transactWriteF: func(_ context.Context, in *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
				txLen = len(in.TransactItems)
				return &dynamodb.TransactWriteItemsOutput{}, nil
			},
			updateItemFn: func(_ context.Context, in *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
				updIn = in
				return &dynamodb.UpdateItemOutput{}, nil
			},
		}
		r, _ := NewRepository(api, "stravin-core")
		if err := r.SupersedePacks(context.Background(), repo, 2, makePacks(100)); err != nil {
			t.Fatalf("SupersedePacks(100): %v", err)
		}
		if txLen != 100 || updIn == nil {
			t.Fatalf("txLen=%d updIn=%v, want 100 and non-nil", txLen, updIn)
		}
		if aws.ToString(updIn.UpdateExpression) != "ADD manifest_version :one" {
			t.Fatalf("UpdateExpression = %q, want ADD manifest_version :one", aws.ToString(updIn.UpdateExpression))
		}
	})
}
