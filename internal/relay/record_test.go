package relay

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func TestParseStreamRecord_RefUpdated(t *testing.T) {
	ts := time.Date(2026, 11, 27, 15, 0, 0, 123456000, time.UTC)
	rec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("100001"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":            &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#monorepo#RH#0"},
				"SK":            &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01JDF8Z7Q2K9M4N6P8R0T2V4W6"},
				"tenant_id":     &streamtypes.AttributeValueMemberS{Value: "acme"},
				"repo_id":       &streamtypes.AttributeValueMemberS{Value: "monorepo"},
				"ref_name":      &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
				"ulid":          &streamtypes.AttributeValueMemberS{Value: "01JDF8Z7Q2K9M4N6P8R0T2V4W6"},
				"oid":           &streamtypes.AttributeValueMemberS{Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				"prev_oid":      &streamtypes.AttributeValueMemberS{Value: "0000000000000000000000000000000000000000"},
				"pack_id":       &streamtypes.AttributeValueMemberS{Value: "pack-123"},
				"traceparent":   &streamtypes.AttributeValueMemberS{Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
				"updated_at":    &streamtypes.AttributeValueMemberS{Value: ts.Format(time.RFC3339Nano)},
				"epoch":         &streamtypes.AttributeValueMemberN{Value: "3"},
				"origin_region": &streamtypes.AttributeValueMemberS{Value: "us-east-1"},
			},
		},
	}

	ev, ok, err := ParseStreamRecord(rec)
	if err != nil {
		t.Fatalf("ParseStreamRecord failed: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for Ref History INSERT")
	}
	if ev.Kind != EventKindRefUpdated {
		t.Fatalf("Kind = %q, want %q", ev.Kind, EventKindRefUpdated)
	}
	if ev.Repo.TenantID != "acme" || ev.Repo.RepoID != "monorepo" {
		t.Fatalf("Repo = %+v, want acme/monorepo", ev.Repo)
	}
	if ev.RefName != "refs/heads/main" || ev.ULID != "01JDF8Z7Q2K9M4N6P8R0T2V4W6" {
		t.Fatalf("RefName=%q ULID=%q", ev.RefName, ev.ULID)
	}
	if ev.OID != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" || ev.PackID != "pack-123" {
		t.Fatalf("OID=%q PackID=%q", ev.OID, ev.PackID)
	}
	if ev.TraceParent != "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" {
		t.Fatalf("TraceParent=%q", ev.TraceParent)
	}
	if ev.Epoch != 3 || ev.OriginRegion != "us-east-1" || !ev.OccurredAt.Equal(ts) {
		t.Fatalf("Epoch=%d OriginRegion=%q OccurredAt=%v", ev.Epoch, ev.OriginRegion, ev.OccurredAt)
	}
}

func TestParseStreamRecord_RepoCreated(t *testing.T) {
	rec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("200001"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":              &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#service-a"},
				"SK":              &streamtypes.AttributeValueMemberS{Value: "META"},
				"tenant_id":       &streamtypes.AttributeValueMemberS{Value: "acme"},
				"repo_id":         &streamtypes.AttributeValueMemberS{Value: "service-a"},
				"home_region":     &streamtypes.AttributeValueMemberS{Value: "us-east-1"},
				"failover_epoch":  &streamtypes.AttributeValueMemberN{Value: "1"},
				"ref_shards":      &streamtypes.AttributeValueMemberN{Value: "4"},
				"manifest_shards": &streamtypes.AttributeValueMemberN{Value: "2"},
				"write_state":     &streamtypes.AttributeValueMemberS{Value: "ACTIVE"},
				"default_branch":  &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
				"object_format":   &streamtypes.AttributeValueMemberS{Value: "sha256"},
			},
		},
	}

	ev, ok, err := ParseStreamRecord(rec)
	if err != nil {
		t.Fatalf("ParseStreamRecord failed: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true for META INSERT")
	}
	if ev.Kind != EventKindRepoCreated {
		t.Fatalf("Kind = %q, want %q", ev.Kind, EventKindRepoCreated)
	}
	if ev.Repo.TenantID != "acme" || ev.Repo.RepoID != "service-a" {
		t.Fatalf("Repo = %+v", ev.Repo)
	}
	if ev.HomeRegion != "us-east-1" || ev.FailoverEpoch != 1 || ev.RefShards != 4 || ev.ManifestShards != 2 {
		t.Fatalf("unexpected meta fields: %+v", ev)
	}
	if ev.DefaultBranch != "refs/heads/main" || ev.ObjectFormat != "sha256" || ev.WriteState != "ACTIVE" {
		t.Fatalf("unexpected meta strings: %+v", ev)
	}
}

func TestParseStreamRecord_RepoDeleted_RemoveAndTombstone(t *testing.T) {
	t.Run("REMOVE on META", func(t *testing.T) {
		rec := streamtypes.Record{
			EventName: streamtypes.OperationTypeRemove,
			Dynamodb: &streamtypes.StreamRecord{
				SequenceNumber: aws.String("300001"),
				Keys: map[string]streamtypes.AttributeValue{
					"PK": &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#old-repo"},
					"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
				},
				OldImage: map[string]streamtypes.AttributeValue{
					"PK":             &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#old-repo"},
					"SK":             &streamtypes.AttributeValueMemberS{Value: "META"},
					"home_region":    &streamtypes.AttributeValueMemberS{Value: "us-west-2"},
					"failover_epoch": &streamtypes.AttributeValueMemberN{Value: "2"},
					"default_branch": &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
					"object_format":  &streamtypes.AttributeValueMemberS{Value: "sha1"},
				},
			},
		}
		ev, ok, err := ParseStreamRecord(rec)
		if err != nil || !ok {
			t.Fatalf("ParseStreamRecord = (%+v, %v, %v), want ok=true", ev, ok, err)
		}
		if ev.Kind != EventKindRepoDeleted {
			t.Fatalf("Kind = %q, want %q", ev.Kind, EventKindRepoDeleted)
		}
		if ev.Repo.TenantID != "acme" || ev.Repo.RepoID != "old-repo" || ev.FailoverEpoch != 2 {
			t.Fatalf("unexpected event: %+v", ev)
		}
	})

	t.Run("MODIFY tombstone on META", func(t *testing.T) {
		rec := streamtypes.Record{
			EventName: streamtypes.OperationTypeModify,
			Dynamodb: &streamtypes.StreamRecord{
				SequenceNumber: aws.String("300002"),
				OldImage: map[string]streamtypes.AttributeValue{
					"PK":      &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#tomb-repo"},
					"SK":      &streamtypes.AttributeValueMemberS{Value: "META"},
					"deleted": &streamtypes.AttributeValueMemberBOOL{Value: false},
				},
				NewImage: map[string]streamtypes.AttributeValue{
					"PK":             &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#tomb-repo"},
					"SK":             &streamtypes.AttributeValueMemberS{Value: "META"},
					"deleted":        &streamtypes.AttributeValueMemberBOOL{Value: true},
					"failover_epoch": &streamtypes.AttributeValueMemberN{Value: "4"},
				},
			},
		}
		ev, ok, err := ParseStreamRecord(rec)
		if err != nil || !ok {
			t.Fatalf("ParseStreamRecord = (%+v, %v, %v), want ok=true", ev, ok, err)
		}
		if ev.Kind != EventKindRepoDeleted || ev.Repo.RepoID != "tomb-repo" || ev.FailoverEpoch != 4 {
			t.Fatalf("unexpected event: %+v", ev)
		}
	})
}

func TestParseStreamRecord_SkipsIrrelevantAndRejectsMalformed(t *testing.T) {
	// Non-matching RS# record is skipped cleanly.
	rsRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			NewImage: map[string]streamtypes.AttributeValue{
				"PK": &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#demo#RS#0"},
				"SK": &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main"},
			},
		},
	}
	if _, ok, err := ParseStreamRecord(rsRec); err != nil || ok {
		t.Fatalf("expected RS# record to be skipped, got ok=%v err=%v", ok, err)
	}

	// Normal META MODIFY (e.g. ref_version increment on push) is skipped cleanly.
	metaMod := streamtypes.Record{
		EventName: streamtypes.OperationTypeModify,
		Dynamodb: &streamtypes.StreamRecord{
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":          &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#demo"},
				"SK":          &streamtypes.AttributeValueMemberS{Value: "META"},
				"ref_version": &streamtypes.AttributeValueMemberN{Value: "5"},
			},
		},
	}
	if _, ok, err := ParseStreamRecord(metaMod); err != nil || ok {
		t.Fatalf("expected non-tombstone META MODIFY to be skipped, got ok=%v err=%v", ok, err)
	}

	// Malformed RH# record returns ErrMalformedRecord.
	badRH := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			NewImage: map[string]streamtypes.AttributeValue{
				"PK": &streamtypes.AttributeValueMemberS{Value: "BAD#RH#0"},
				"SK": &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J"},
			},
		},
	}
	if _, _, err := ParseStreamRecord(badRH); !errors.Is(err, ErrMalformedRecord) {
		t.Fatalf("expected ErrMalformedRecord, got %v", err)
	}
}
