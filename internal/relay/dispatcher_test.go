package relay

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type spyPublisher struct {
	mu       sync.Mutex
	messages []Message
	acks     []PublishAck
	errs     []error
	calls    int
}

func (s *spyPublisher) Publish(_ context.Context, msg Message) (PublishAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := s.calls
	s.calls++
	s.messages = append(s.messages, msg)
	if idx < len(s.errs) && s.errs[idx] != nil {
		return PublishAck{}, s.errs[idx]
	}
	if idx < len(s.acks) {
		return s.acks[idx], nil
	}
	return PublishAck{Stream: "STRAVIN_CORE", Sequence: uint64(idx + 1)}, nil
}

func (s *spyPublisher) Close() error { return nil }

func TestRelayDispatcher_PublishesEnvelopeWithHeadersAndTraceLink(t *testing.T) {
	tel, err := telemetry.NewInMemoryProvider("stravin-relay-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	pub := &spyPublisher{}
	disp, err := NewRelayDispatcher(DispatcherConfig{
		Publisher: pub,
		Telemetry: tel,
		Sleep:     func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRelayDispatcher: %v", err)
	}

	const wantTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ev := OutboxEvent{
		Kind:           EventKindRefUpdated,
		SequenceNumber: "42",
		Repo:           tenant.RepoKey{TenantID: "local", RepoID: "demo"},
		RefName:        "refs/heads/main",
		ULID:           "01JDF8Z7Q2K9M4N6P8R0T2V4W6",
		OID:            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PrevOID:        "0000000000000000000000000000000000000000",
		PackID:         "pack-42",
		TraceParent:    wantTP,
		Epoch:          1,
		OriginRegion:   "us-east-1",
	}

	if err := disp.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent failed: %v", err)
	}

	if len(pub.messages) != 1 {
		t.Fatalf("published %d messages, want 1", len(pub.messages))
	}
	msg := pub.messages[0]
	if msg.Subject != "stravin.core.local.demo.ref.updated" {
		t.Fatalf("subject = %q", msg.Subject)
	}
	wantMsgID := "local:demo:refs/heads/main:01JDF8Z7Q2K9M4N6P8R0T2V4W6"
	if msg.MsgID != wantMsgID || msg.Headers[HeaderNatsMsgID] != wantMsgID {
		t.Fatalf("MsgID = %q / header=%q, want %q", msg.MsgID, msg.Headers[HeaderNatsMsgID], wantMsgID)
	}
	if msg.Headers[HeaderTraceparent] != wantTP {
		t.Fatalf("traceparent header = %q, want %q", msg.Headers[HeaderTraceparent], wantTP)
	}
	if msg.Headers[HeaderEventType] != EventTypeRefUpdated {
		t.Fatalf("event type header = %q", msg.Headers[HeaderEventType])
	}
	if msg.Headers[HeaderSchemaVersion] != SchemaVersion {
		t.Fatalf("schema version header = %q", msg.Headers[HeaderSchemaVersion])
	}

	var env Envelope
	if err := json.Unmarshal(msg.Payload, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.IdempotencyKey != wantMsgID || env.TraceParent != wantTP {
		t.Fatalf("envelope = %+v", env)
	}

	spans := tel.SpanExporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	if len(spans[0].Links) != 1 {
		t.Fatalf("expected 1 span link from traceparent, got %d", len(spans[0].Links))
	}
}

func TestRelayDispatcher_RetriesTransientPublishErrors(t *testing.T) {
	pub := &spyPublisher{
		errs: []error{errors.New("transient nats timeout"), nil},
	}
	disp, err := NewRelayDispatcher(DispatcherConfig{
		Publisher:   pub,
		MaxAttempts: 3,
		Sleep:       func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatalf("NewRelayDispatcher: %v", err)
	}

	ev := OutboxEvent{
		Kind:           EventKindRepoCreated,
		SequenceNumber: "100",
		Repo:           tenant.RepoKey{TenantID: "local", RepoID: "demo"},
	}
	if err := disp.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if pub.calls != 2 {
		t.Fatalf("publish calls = %d, want 2", pub.calls)
	}
}

func TestRelayDispatcher_SynthesizesTraceparentWhenEventMissing(t *testing.T) {
	tel, err := telemetry.NewInMemoryProvider("stravin-relay-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	pub := &spyPublisher{}
	disp, err := NewRelayDispatcher(DispatcherConfig{
		Publisher: pub,
		Telemetry: tel,
	})
	if err != nil {
		t.Fatalf("NewRelayDispatcher: %v", err)
	}

	ev := OutboxEvent{
		Kind:           EventKindRepoDeleted,
		SequenceNumber: "555",
		Repo:           tenant.RepoKey{TenantID: "local", RepoID: "gone"},
	}
	if err := disp.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent failed: %v", err)
	}
	tp := pub.messages[0].Headers[HeaderTraceparent]
	if tp == "" {
		t.Fatal("expected non-empty W3C traceparent header synthesized from relay span")
	}
}
