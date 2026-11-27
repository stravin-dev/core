package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

func startEmbeddedJetStream(t *testing.T) (string, jetstream.JetStream) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("natsserver.NewServer: %v", err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS server did not become ready")
	}
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       "STRAVIN_CORE",
		Subjects:   []string{"stravin.core.>"},
		Storage:    jetstream.FileStorage,
		Retention:  jetstream.LimitsPolicy,
		Duplicates: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("CreateStream STRAVIN_CORE: %v", err)
	}

	return srv.ClientURL(), js
}

func TestJetStreamRelay_AllEventsAndDuplicateRedeliveryDeduplication(t *testing.T) {
	natsURL, js := startEmbeddedJetStream(t)

	tel, err := telemetry.NewInMemoryProvider("stravin-relay-js-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	pub, err := NewJetStreamPublisher(JetStreamConfig{
		URL:            natsURL,
		PublishTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewJetStreamPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	disp, err := NewRelayDispatcher(DispatcherConfig{
		Publisher: pub,
		Telemetry: tel,
	})
	if err != nil {
		t.Fatalf("NewRelayDispatcher: %v", err)
	}

	ctx := context.Background()
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	repoCreated := OutboxEvent{
		Kind:           EventKindRepoCreated,
		SequenceNumber: "1001",
		Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "monorepo"},
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		RefShards:      2,
		ManifestShards: 2,
		WriteState:     "ACTIVE",
		DefaultBranch:  "refs/heads/main",
		ObjectFormat:   "sha1",
	}
	refUpdated := OutboxEvent{
		Kind:           EventKindRefUpdated,
		SequenceNumber: "1002",
		Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "monorepo"},
		RefName:        "refs/heads/main",
		ULID:           "01JDF8Z7Q2K9M4N6P8R0T2V4W6",
		OID:            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		PrevOID:        "0000000000000000000000000000000000000000",
		PackID:         "pack-1002",
		TraceParent:    traceParent,
		Epoch:          1,
		OriginRegion:   "us-east-1",
	}
	repoDeleted := OutboxEvent{
		Kind:           EventKindRepoDeleted,
		SequenceNumber: "1003",
		Repo:           tenant.RepoKey{TenantID: "acme", RepoID: "monorepo"},
		HomeRegion:     "us-east-1",
		FailoverEpoch:  1,
		DefaultBranch:  "refs/heads/main",
		ObjectFormat:   "sha1",
	}

	// Publish all 3 event types, then redeliver refUpdated twice to simulate DynamoDB Streams redelivery.
	for _, ev := range []OutboxEvent{repoCreated, refUpdated, refUpdated, refUpdated, repoDeleted} {
		if err := disp.OnEvent(ctx, ev); err != nil {
			t.Fatalf("OnEvent(%s): %v", ev.Kind, err)
		}
	}

	stream, err := js.Stream(ctx, "STRAVIN_CORE")
	if err != nil {
		t.Fatalf("js.Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info: %v", err)
	}
	// Duplicate ref.updated redeliveries MUST be deduplicated by JetStream Nats-Msg-Id.
	if info.State.Msgs != 3 {
		t.Fatalf("STRAVIN_CORE stream msgs = %d, want 3 (duplicates must be deduplicated)", info.State.Msgs)
	}

	wantSubjects := []string{
		"stravin.core.acme.monorepo.repo.created",
		"stravin.core.acme.monorepo.ref.updated",
		"stravin.core.acme.monorepo.repo.deleted",
	}
	for seq, wantSubj := range wantSubjects {
		raw, err := stream.GetMsg(ctx, uint64(seq+1))
		if err != nil {
			t.Fatalf("GetMsg(%d): %v", seq+1, err)
		}
		if raw.Subject != wantSubj {
			t.Fatalf("seq %d subject = %q, want %q", seq+1, raw.Subject, wantSubj)
		}
		if raw.Header.Get(HeaderNatsMsgID) == "" {
			t.Fatalf("seq %d missing %s header", seq+1, HeaderNatsMsgID)
		}
		if raw.Header.Get(HeaderTraceparent) == "" {
			t.Fatalf("seq %d missing %s header", seq+1, HeaderTraceparent)
		}
		if seq == 1 && raw.Header.Get(HeaderTraceparent) != traceParent {
			t.Fatalf("ref.updated traceparent = %q, want %q", raw.Header.Get(HeaderTraceparent), traceParent)
		}

		var env Envelope
		if err := json.Unmarshal(raw.Data, &env); err != nil {
			t.Fatalf("seq %d unmarshal envelope: %v", seq+1, err)
		}
		if env.IdempotencyKey != raw.Header.Get(HeaderNatsMsgID) {
			t.Fatalf("seq %d idempotency_key %q != Nats-Msg-Id %q", seq+1, env.IdempotencyKey, raw.Header.Get(HeaderNatsMsgID))
		}
		if env.TenantID != "acme" || env.RepoID != "monorepo" || env.SchemaVersion != SchemaVersion {
			t.Fatalf("seq %d unexpected envelope metadata: %+v", seq+1, env)
		}
		if env.EventType != fmt.Sprintf("core.%s", []string{"repo.created", "ref.updated", "repo.deleted"}[seq]) {
			t.Fatalf("seq %d event_type = %q", seq+1, env.EventType)
		}
	}
}
