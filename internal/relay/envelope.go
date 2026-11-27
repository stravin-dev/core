package relay

import (
	"errors"
	"fmt"
	"time"
)

const (
	// SchemaVersion is the Standard Async Event Envelope schema version per ADR-0001.
	SchemaVersion = "1.0"

	// Canonical NATS headers attached to every published STRAVIN_CORE message.
	HeaderNatsMsgID     = "Nats-Msg-Id"
	HeaderEventType     = "Stravin-Event-Type"
	HeaderSchemaVersion = "Stravin-Schema-Version"
	HeaderTraceparent   = "traceparent"
	HeaderTracestate    = "tracestate"
	HeaderBaggage       = "baggage"

	// Canonical event_type values for STRAVIN_CORE envelopes.
	EventTypeRefUpdated  = "core.ref.updated"
	EventTypeRepoCreated = "core.repo.created"
	EventTypeRepoDeleted = "core.repo.deleted"
)

// Envelope is the Standard Async Event Envelope published to NATS JetStream
// across Stravin products per architecture/docs/interfaces/index.md.
type Envelope struct {
	EventID        string `json:"event_id"`
	EventType      string `json:"event_type"`
	SchemaVersion  string `json:"schema_version"`
	OccurredAt     string `json:"occurred_at"`
	TenantID       string `json:"tenant_id"`
	RepoID         string `json:"repo_id"`
	TraceParent    string `json:"traceparent,omitempty"`
	IdempotencyKey string `json:"idempotency_key"`
	Data           any    `json:"data"`
}

// RefUpdatedData is the payload for core.ref.updated envelopes.
type RefUpdatedData struct {
	RefName      string `json:"ref_name"`
	OID          string `json:"oid"`
	PrevOID      string `json:"prev_oid"`
	PackID       string `json:"pack_id,omitempty"`
	ULID         string `json:"ulid,omitempty"`
	Epoch        uint64 `json:"epoch,omitempty"`
	OriginRegion string `json:"origin_region,omitempty"`
}

// RepoCreatedData is the payload for core.repo.created envelopes.
type RepoCreatedData struct {
	HomeRegion     string `json:"home_region,omitempty"`
	FailoverEpoch  uint64 `json:"failover_epoch,omitempty"`
	RefShards      int    `json:"ref_shards,omitempty"`
	ManifestShards int    `json:"manifest_shards,omitempty"`
	WriteState     string `json:"write_state,omitempty"`
	DefaultBranch  string `json:"default_branch,omitempty"`
	ObjectFormat   string `json:"object_format,omitempty"`
}

// RepoDeletedData is the payload for core.repo.deleted envelopes.
type RepoDeletedData struct {
	HomeRegion    string `json:"home_region,omitempty"`
	FailoverEpoch uint64 `json:"failover_epoch,omitempty"`
	DefaultBranch string `json:"default_branch,omitempty"`
	ObjectFormat  string `json:"object_format,omitempty"`
	DeletedAt     string `json:"deleted_at"`
}

// BuildEnvelope constructs the canonical NATS subject and Standard Async Event
// Envelope with deterministic idempotency_key and event_id (GoF Factory Method).
func BuildEnvelope(ev OutboxEvent, traceparent string, now time.Time) (string, Envelope, error) {
	if err := ev.Repo.Validate(); err != nil {
		return "", Envelope{}, fmt.Errorf("relay: invalid repo key: %w", err)
	}

	ts := ev.OccurredAt
	if ts.IsZero() {
		ts = now.UTC()
	} else {
		ts = ts.UTC()
	}

	tenantID := string(ev.Repo.TenantID)
	repoID := string(ev.Repo.RepoID)

	var (
		subject   string
		eventType string
		idemKey   string
		data      any
	)

	switch ev.Kind {
	case EventKindRefUpdated:
		if ev.RefName == "" {
			return "", Envelope{}, errors.New("relay: ref.updated missing ref_name")
		}
		subject = fmt.Sprintf("stravin.core.%s.%s.ref.updated", tenantID, repoID)
		eventType = EventTypeRefUpdated
		idemKey = deterministicRefIdempotencyKey(tenantID, repoID, ev)
		data = RefUpdatedData{
			RefName:      ev.RefName,
			OID:          ev.OID,
			PrevOID:      ev.PrevOID,
			PackID:       ev.PackID,
			ULID:         ev.ULID,
			Epoch:        ev.Epoch,
			OriginRegion: ev.OriginRegion,
		}

	case EventKindRepoCreated:
		subject = fmt.Sprintf("stravin.core.%s.%s.repo.created", tenantID, repoID)
		eventType = EventTypeRepoCreated
		idemKey = deterministicRepoLifecycleIdempotencyKey(tenantID, repoID, string(EventKindRepoCreated), ev)
		data = RepoCreatedData{
			HomeRegion:     ev.HomeRegion,
			FailoverEpoch:  ev.FailoverEpoch,
			RefShards:      ev.RefShards,
			ManifestShards: ev.ManifestShards,
			WriteState:     ev.WriteState,
			DefaultBranch:  ev.DefaultBranch,
			ObjectFormat:   ev.ObjectFormat,
		}

	case EventKindRepoDeleted:
		subject = fmt.Sprintf("stravin.core.%s.%s.repo.deleted", tenantID, repoID)
		eventType = EventTypeRepoDeleted
		idemKey = deterministicRepoLifecycleIdempotencyKey(tenantID, repoID, string(EventKindRepoDeleted), ev)
		data = RepoDeletedData{
			HomeRegion:    ev.HomeRegion,
			FailoverEpoch: ev.FailoverEpoch,
			DefaultBranch: ev.DefaultBranch,
			ObjectFormat:  ev.ObjectFormat,
			DeletedAt:     ts.Format(time.RFC3339Nano),
		}

	default:
		return "", Envelope{}, fmt.Errorf("relay: unsupported event kind %q", ev.Kind)
	}

	env := Envelope{
		EventID:        deterministicEventID(ev),
		EventType:      eventType,
		SchemaVersion:  SchemaVersion,
		OccurredAt:     ts.Format(time.RFC3339Nano),
		TenantID:       tenantID,
		RepoID:         repoID,
		TraceParent:    traceparent,
		IdempotencyKey: idemKey,
		Data:           data,
	}
	return subject, env, nil
}

func deterministicRefIdempotencyKey(tenantID, repoID string, ev OutboxEvent) string {
	switch {
	case ev.ULID != "":
		return fmt.Sprintf("%s:%s:%s:%s", tenantID, repoID, ev.RefName, ev.ULID)
	case ev.SequenceNumber != "":
		return fmt.Sprintf("%s:%s:%s:seq:%s", tenantID, repoID, ev.RefName, ev.SequenceNumber)
	default:
		return fmt.Sprintf("%s:%s:%s:%s", tenantID, repoID, ev.RefName, ev.OID)
	}
}

func deterministicRepoLifecycleIdempotencyKey(tenantID, repoID, kind string, ev OutboxEvent) string {
	switch {
	case ev.ULID != "":
		return fmt.Sprintf("%s:%s:%s:%s", tenantID, repoID, kind, ev.ULID)
	case ev.SequenceNumber != "":
		return fmt.Sprintf("%s:%s:%s:seq:%s", tenantID, repoID, kind, ev.SequenceNumber)
	default:
		return fmt.Sprintf("%s:%s:%s:%d", tenantID, repoID, kind, ev.FailoverEpoch)
	}
}

func deterministicEventID(ev OutboxEvent) string {
	switch {
	case ev.ULID != "":
		return "evt_" + ev.ULID
	case ev.SequenceNumber != "":
		return "evt_seq_" + ev.SequenceNumber
	default:
		return fmt.Sprintf("evt_%s_%s_%s", ev.Kind, ev.Repo.TenantID, ev.Repo.RepoID)
	}
}
