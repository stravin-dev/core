package ddb

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
)

const (
	oidA = "1111111111111111111111111111111111111111"
	oidB = "2222222222222222222222222222222222222222"
	oidC = "3333333333333333333333333333333333333333"
)

func TestBuildPushTransaction_CreateUpdateDeleteAndMetaVersionBump(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	now := time.Date(2026, 11, 21, 18, 0, 0, 0, time.UTC)
	ulid := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	traceParent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	items, refIdx, err := BuildPushTransaction(CommitPushInput{
		Repo:           repo,
		RefShards:      8,
		ManifestShards: 4,
		Epoch:          2,
		OriginRegion:   lease.Region("us-east-1"),
		TraceParent:    traceParent,
		Updates: []RefUpdate{
			{RefName: "refs/heads/new-branch", OldOID: ZeroOID, NewOID: oidA},
			{RefName: "refs/heads/main", OldOID: oidA, NewOID: oidB},
			{RefName: "refs/heads/old-branch", OldOID: oidC, NewOID: ZeroOID},
		},
		Pack: &PackUpload{
			PackID:    "pack-sha256-abc",
			SizeBytes: 4096,
		},
	}, ulid, now, "stravin-core")
	if err != nil {
		t.Fatalf("BuildPushTransaction: %v", err)
	}

	// 3 refs * 2 (RS + RH) + 1 MS + 1 META = 8 items.
	if len(items) != 8 {
		t.Fatalf("got %d tx items, want 8", len(items))
	}

	// Verify refIndex maps the 3 RS item positions (0, 2, 4).
	if refIdx[0] != "refs/heads/new-branch" || refIdx[2] != "refs/heads/main" || refIdx[4] != "refs/heads/old-branch" {
		t.Fatalf("unexpected refIdx: %+v", refIdx)
	}

	// Item 0: Create -> Put with attribute_not_exists(oid).
	if items[0].Put == nil || aws.ToString(items[0].Put.ConditionExpression) != "attribute_not_exists(oid)" {
		t.Fatalf("expected Put with attribute_not_exists(oid) for create, got %+v", items[0])
	}
	// Item 2: Update -> Update with oid = :old_oid.
	if items[2].Update == nil || aws.ToString(items[2].Update.ConditionExpression) != "oid = :old_oid" {
		t.Fatalf("expected Update with oid = :old_oid for update, got %+v", items[2])
	}
	// Item 4: Delete -> Delete with oid = :old_oid.
	if items[4].Delete == nil || aws.ToString(items[4].Delete.ConditionExpression) != "oid = :old_oid" {
		t.Fatalf("expected Delete with oid = :old_oid for delete, got %+v", items[4])
	}

	// Verify RH items have 7d TTL and traceparent.
	expectedTTL := now.Add(RefHistoryTTL).Unix()
	for _, rhPos := range []int{1, 3, 5} {
		put := items[rhPos].Put
		if put == nil {
			t.Fatalf("item %d expected Put for RH", rhPos)
		}
		gotTTL, err := getInt64(put.Item, "ttl")
		if err != nil || gotTTL != expectedTTL {
			t.Fatalf("RH item %d ttl=%d err=%v, want %d", rhPos, gotTTL, err, expectedTTL)
		}
		if gotTP := getString(put.Item, "traceparent"); gotTP != traceParent {
			t.Fatalf("RH item %d traceparent=%q, want %q", rhPos, gotTP, traceParent)
		}
	}

	// Verify MS item is ACTIVE.
	msPut := items[6].Put
	if msPut == nil || getString(msPut.Item, "status") != string(PackStatusActive) {
		t.Fatalf("expected ACTIVE manifest Put at index 6, got %+v", items[6])
	}

	// Verify META item atomically bumps ref_version and manifest_version.
	metaKey, err := MetaKey(repo)
	if err != nil {
		t.Fatalf("MetaKey: %v", err)
	}
	metaUpd := items[7].Update
	if metaUpd == nil {
		t.Fatalf("expected META Update at index 7, got %+v", items[7])
	}
	if getString(metaUpd.Key, AttrPK) != metaKey.PK || getString(metaUpd.Key, AttrSK) != metaKey.SK {
		t.Fatalf("META update key = (%q, %q), want (%q, %q)", getString(metaUpd.Key, AttrPK), getString(metaUpd.Key, AttrSK), metaKey.PK, metaKey.SK)
	}
	if got := aws.ToString(metaUpd.UpdateExpression); got != "ADD ref_version :one, manifest_version :one" {
		t.Fatalf("META UpdateExpression = %q, want %q", got, "ADD ref_version :one, manifest_version :one")
	}
	if one, err := getUint64(metaUpd.ExpressionAttributeValues, ":one"); err != nil || one != 1 {
		t.Fatalf("META :one = %d (err=%v), want 1", one, err)
	}
}

func TestBuildPushTransaction_DeleteOnlyNoPack(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	items, _, err := BuildPushTransaction(CommitPushInput{
		Repo:         repo,
		RefShards:    4,
		Epoch:        1,
		OriginRegion: lease.Region("us-east-1"),
		Updates: []RefUpdate{
			{RefName: "refs/heads/stale", OldOID: oidA, NewOID: ZeroOID},
		},
		Pack: nil,
	}, "01ARZ3NDEKTSV4RRFFQ69G5FAV", time.Now().UTC(), "stravin-core")
	if err != nil {
		t.Fatalf("BuildPushTransaction delete-only: %v", err)
	}
	// RS delete + RH put + META ref_version bump, no MS item.
	if len(items) != 3 {
		t.Fatalf("got %d tx items, want 3", len(items))
	}
	if items[0].Delete == nil || items[1].Put == nil || items[2].Update == nil {
		t.Fatalf("expected RS Delete + RH Put + META Update, got %+v", items)
	}
	if got := aws.ToString(items[2].Update.UpdateExpression); got != "ADD ref_version :one" {
		t.Fatalf("META UpdateExpression = %q, want %q", got, "ADD ref_version :one")
	}
}

func TestBuildPushTransaction_ExceedsMaxAtomicRefs(t *testing.T) {
	repo := mustRepoKey(t, "acme", "core")
	updates := make([]RefUpdate, MaxAtomicPushRefs+1)
	for i := range updates {
		updates[i] = RefUpdate{
			RefName: fmt.Sprintf("refs/heads/b-%02d", i),
			OldOID:  ZeroOID,
			NewOID:  oidA,
		}
	}

	_, _, err := BuildPushTransaction(CommitPushInput{
		Repo:         repo,
		RefShards:    4,
		Epoch:        1,
		OriginRegion: lease.Region("us-east-1"),
		Updates:      updates,
	}, "01ARZ3NDEKTSV4RRFFQ69G5FAV", time.Now().UTC(), "stravin-core")
	if !errors.Is(err, ErrTooManyRefs) {
		t.Fatalf("got err=%v, want ErrTooManyRefs", err)
	}
}

func TestClassifyTransactionError(t *testing.T) {
	refIndex := map[int]string{
		0: "refs/heads/main",
		2: "refs/heads/dev",
	}

	t.Run("ConditionalCheckFailed maps to RefConflictError", func(t *testing.T) {
		tce := &types.TransactionCanceledException{
			Message: aws.String("Transaction cancelled"),
			CancellationReasons: []types.CancellationReason{
				{Code: aws.String("None")},
				{Code: aws.String("None")},
				{Code: aws.String("ConditionalCheckFailed"), Message: aws.String("condition failed")},
				{Code: aws.String("None")},
			},
		}
		err := classifyTransactionError(tce, refIndex)
		if !errors.Is(err, ErrRefConflict) || !errors.Is(err, ErrRefCASConflict) {
			t.Fatalf("expected ErrRefConflict/ErrRefCASConflict, got %v", err)
		}
		var rce *RefConflictError
		if !errors.As(err, &rce) {
			t.Fatalf("expected *RefConflictError, got %T", err)
		}
		if len(rce.ConflictingRefs) != 1 || rce.ConflictingRefs[0] != "refs/heads/dev" {
			t.Fatalf("unexpected conflicting refs: %+v", rce.ConflictingRefs)
		}
	})

	t.Run("TransactionConflict maps to ErrTransactionConflict", func(t *testing.T) {
		tce := &types.TransactionCanceledException{
			Message: aws.String("Transaction cancelled"),
			CancellationReasons: []types.CancellationReason{
				{Code: aws.String("TransactionConflict")},
			},
		}
		err := classifyTransactionError(tce, refIndex)
		if !errors.Is(err, ErrTransactionConflict) {
			t.Fatalf("expected ErrTransactionConflict, got %v", err)
		}
	})
}
