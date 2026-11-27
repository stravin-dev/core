package relay

import (
	"testing"
	"time"

	"stravin.dev/core/internal/tenant"
)

func TestBuildEnvelope_AllEventKindsAndDeterministicMsgID(t *testing.T) {
	now := time.Date(2026, 11, 27, 16, 0, 0, 0, time.UTC)
	tp := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	t.Run("ref.updated", func(t *testing.T) {
		ev := OutboxEvent{
			Kind:           EventKindRefUpdated,
			SequenceNumber: "1001",
			Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "monorepo"},
			RefName:        "refs/heads/main",
			ULID:           "01JDF8Z7Q2K9M4N6P8R0T2V4W6",
			OID:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			PrevOID:        "0000000000000000000000000000000000000000",
			PackID:         "pack-1",
			Epoch:          2,
			OriginRegion:   "us-east-1",
			OccurredAt:     now,
		}
		subj1, env1, err := BuildEnvelope(ev, tp, now)
		if err != nil {
			t.Fatalf("BuildEnvelope failed: %v", err)
		}
		subj2, env2, err := BuildEnvelope(ev, tp, now.Add(time.Minute))
		if err != nil {
			t.Fatalf("second BuildEnvelope failed: %v", err)
		}
		if subj1 != "stravin.core.acme.monorepo.ref.updated" || subj1 != subj2 {
			t.Fatalf("subject = %q", subj1)
		}
		wantKey := "acme:monorepo:refs/heads/main:01JDF8Z7Q2K9M4N6P8R0T2V4W6"
		if env1.IdempotencyKey != wantKey || env2.IdempotencyKey != wantKey {
			t.Fatalf("IdempotencyKey = %q / %q, want %q", env1.IdempotencyKey, env2.IdempotencyKey, wantKey)
		}
		if env1.EventID != "evt_01JDF8Z7Q2K9M4N6P8R0T2V4W6" {
			t.Fatalf("EventID = %q", env1.EventID)
		}
		if env1.EventType != EventTypeRefUpdated || env1.SchemaVersion != SchemaVersion {
			t.Fatalf("EventType=%q SchemaVersion=%q", env1.EventType, env1.SchemaVersion)
		}
		if env1.TraceParent != tp {
			t.Fatalf("TraceParent = %q, want %q", env1.TraceParent, tp)
		}
	})

	t.Run("repo.created", func(t *testing.T) {
		ev := OutboxEvent{
			Kind:           EventKindRepoCreated,
			SequenceNumber: "2001",
			Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "service-a"},
			HomeRegion:     "us-east-1",
			FailoverEpoch:  1,
			RefShards:      2,
			ManifestShards: 2,
			WriteState:     "ACTIVE",
			DefaultBranch:  "refs/heads/main",
			ObjectFormat:   "sha1",
		}
		subj, env, err := BuildEnvelope(ev, tp, now)
		if err != nil {
			t.Fatalf("BuildEnvelope failed: %v", err)
		}
		if subj != "stravin.core.acme.service-a.repo.created" {
			t.Fatalf("subject = %q", subj)
		}
		if env.EventType != EventTypeRepoCreated {
			t.Fatalf("EventType = %q", env.EventType)
		}
		if env.IdempotencyKey != "acme:service-a:repo.created:seq:2001" {
			t.Fatalf("IdempotencyKey = %q", env.IdempotencyKey)
		}
	})

	t.Run("repo.deleted", func(t *testing.T) {
		ev := OutboxEvent{
			Kind:           EventKindRepoDeleted,
			SequenceNumber: "3001",
			Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "old-repo"},
			HomeRegion:     "us-west-2",
			FailoverEpoch:  3,
			DefaultBranch:  "refs/heads/main",
			ObjectFormat:   "sha256",
		}
		subj, env, err := BuildEnvelope(ev, tp, now)
		if err != nil {
			t.Fatalf("BuildEnvelope failed: %v", err)
		}
		if subj != "stravin.core.acme.old-repo.repo.deleted" {
			t.Fatalf("subject = %q", subj)
		}
		if env.EventType != EventTypeRepoDeleted {
			t.Fatalf("EventType = %q", env.EventType)
		}
		if env.IdempotencyKey != "acme:old-repo:repo.deleted:seq:3001" {
			t.Fatalf("IdempotencyKey = %q", env.IdempotencyKey)
		}
	})
}
