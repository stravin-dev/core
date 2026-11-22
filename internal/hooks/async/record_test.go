package async

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

func TestParseRefHistoryRecord(t *testing.T) {
	now := time.Date(2026, 11, 21, 19, 30, 0, 0, time.UTC)
	oid := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	prevOID := "0000000000000000000000000000000000000000"
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	t.Run("valid Ref History INSERT with full attributes", func(t *testing.T) {
		rec := streamtypes.Record{
			EventName: streamtypes.OperationTypeInsert,
			Dynamodb: &streamtypes.StreamRecord{
				SequenceNumber: aws.String("100000000000000000001"),
				NewImage: map[string]streamtypes.AttributeValue{
					"PK":            &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#monorepo#RH#3"},
					"SK":            &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J00000000000000000000001"},
					"tenant_id":     &streamtypes.AttributeValueMemberS{Value: "acme"},
					"repo_id":       &streamtypes.AttributeValueMemberS{Value: "monorepo"},
					"ref_name":      &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
					"ulid":          &streamtypes.AttributeValueMemberS{Value: "01J00000000000000000000001"},
					"oid":           &streamtypes.AttributeValueMemberS{Value: oid},
					"prev_oid":      &streamtypes.AttributeValueMemberS{Value: prevOID},
					"pack_id":       &streamtypes.AttributeValueMemberS{Value: "pack-abc"},
					"traceparent":   &streamtypes.AttributeValueMemberS{Value: traceparent},
					"updated_at":    &streamtypes.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
					"epoch":         &streamtypes.AttributeValueMemberN{Value: "4"},
					"origin_region": &streamtypes.AttributeValueMemberS{Value: "us-east-1"},
				},
			},
		}

		ev, ok, err := ParseRefHistoryRecord(rec)
		if err != nil {
			t.Fatalf("ParseRefHistoryRecord unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected record to match Ref History INSERT")
		}
		if ev.SequenceNumber != "100000000000000000001" {
			t.Errorf("SequenceNumber = %q, want 100000000000000000001", ev.SequenceNumber)
		}
		if string(ev.Repo.TenantID) != "acme" || string(ev.Repo.RepoID) != "monorepo" {
			t.Errorf("Repo = %+v, want acme/monorepo", ev.Repo)
		}
		if ev.RefName != "refs/heads/main" {
			t.Errorf("RefName = %q, want refs/heads/main", ev.RefName)
		}
		if ev.ULID != "01J00000000000000000000001" {
			t.Errorf("ULID = %q", ev.ULID)
		}
		if ev.OID != oid || ev.PrevOID != prevOID || ev.PackID != "pack-abc" {
			t.Errorf("unexpected git fields: %+v", ev)
		}
		if ev.TraceParent != traceparent {
			t.Errorf("TraceParent = %q, want %q", ev.TraceParent, traceparent)
		}
		if !ev.UpdatedAt.Equal(now) {
			t.Errorf("UpdatedAt = %v, want %v", ev.UpdatedAt, now)
		}
		if ev.Epoch != 4 || ev.OriginRegion != "us-east-1" {
			t.Errorf("Epoch=%d OriginRegion=%q", ev.Epoch, ev.OriginRegion)
		}
	})

	t.Run("fallback to parsing PK and SK when optional attributes omitted", func(t *testing.T) {
		rec := streamtypes.Record{
			EventName: streamtypes.OperationTypeInsert,
			Dynamodb: &streamtypes.StreamRecord{
				SequenceNumber: aws.String("42"),
				NewImage: map[string]streamtypes.AttributeValue{
					"PK":  &streamtypes.AttributeValueMemberS{Value: "T#tenant1#REPO#repo2#RH#0"},
					"SK":  &streamtypes.AttributeValueMemberS{Value: "REF#refs/tags/v1.0#01JXYZ"},
					"oid": &streamtypes.AttributeValueMemberS{Value: oid},
				},
			},
		}

		ev, ok, err := ParseRefHistoryRecord(rec)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatal("expected match")
		}
		if string(ev.Repo.TenantID) != "tenant1" || string(ev.Repo.RepoID) != "repo2" {
			t.Errorf("Repo = %+v, want tenant1/repo2", ev.Repo)
		}
		if ev.RefName != "refs/tags/v1.0" || ev.ULID != "01JXYZ" {
			t.Errorf("RefName=%q ULID=%q", ev.RefName, ev.ULID)
		}
	})

	t.Run("ignores non-INSERT events", func(t *testing.T) {
		for _, op := range []streamtypes.OperationType{streamtypes.OperationTypeModify, streamtypes.OperationTypeRemove} {
			rec := streamtypes.Record{
				EventName: op,
				Dynamodb: &streamtypes.StreamRecord{
					NewImage: map[string]streamtypes.AttributeValue{
						"PK": &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#mono#RH#0"},
						"SK": &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J"},
					},
				},
			}
			_, ok, err := ParseRefHistoryRecord(rec)
			if err != nil || ok {
				t.Errorf("op=%s: got ok=%v err=%v, want false, nil", op, ok, err)
			}
		}
	})

	t.Run("ignores non-RefHistory items", func(t *testing.T) {
		cases := []struct {
			pk string
			sk string
		}{
			{"T#acme#REPO#mono#RS#0", "REF#refs/heads/main"},
			{"T#acme#REPO#mono#MS#0", "PACK#01J#p1"},
			{"T#acme#REPO#mono", "META"},
			{"STREAMS#CHECKPOINT", "SHARD#shard-1"},
			{"T#acme#REPO#mono#RH#0", "OTHER#123"},
		}
		for _, tc := range cases {
			rec := streamtypes.Record{
				EventName: streamtypes.OperationTypeInsert,
				Dynamodb: &streamtypes.StreamRecord{
					NewImage: map[string]streamtypes.AttributeValue{
						"PK": &streamtypes.AttributeValueMemberS{Value: tc.pk},
						"SK": &streamtypes.AttributeValueMemberS{Value: tc.sk},
					},
				},
			}
			_, ok, err := ParseRefHistoryRecord(rec)
			if err != nil || ok {
				t.Errorf("pk=%s sk=%s: got ok=%v err=%v, want false, nil", tc.pk, tc.sk, ok, err)
			}
		}
	})

	t.Run("returns error on malformed Ref History PK", func(t *testing.T) {
		rec := streamtypes.Record{
			EventName: streamtypes.OperationTypeInsert,
			Dynamodb: &streamtypes.StreamRecord{
				NewImage: map[string]streamtypes.AttributeValue{
					"PK": &streamtypes.AttributeValueMemberS{Value: "BAD#RH#0"},
					"SK": &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J"},
				},
			},
		}
		_, ok, err := ParseRefHistoryRecord(rec)
		if err == nil || ok {
			t.Errorf("expected error for malformed PK, got ok=%v err=%v", ok, err)
		}
	})
}
