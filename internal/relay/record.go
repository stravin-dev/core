// Package relay implements Stravin Core's transactional DynamoDB Streams to
// NATS JetStream outbox relay (stravin-relay). It emits Standard Async Event
// Envelopes to STRAVIN_CORE with deterministic Nats-Msg-Id deduplication and
// W3C traceparent propagation, with zero Wasm execution and zero outbound HTTP
// webhook calls.
package relay

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

// ErrMalformedRecord is returned when a matching DynamoDB Streams record fails validation.
var ErrMalformedRecord = errors.New("relay: malformed stream record")

// EventKind identifies the Stravin Core domain event parsed from a DynamoDB Streams record.
type EventKind string

const (
	EventKindRefUpdated  EventKind = "ref.updated"
	EventKindRepoCreated EventKind = "repo.created"
	EventKindRepoDeleted EventKind = "repo.deleted"
)

// OutboxEvent is a normalized domain event decoded from a DynamoDB Streams record.
type OutboxEvent struct {
	Kind           EventKind
	SequenceNumber string
	Repo           tenant.RepoKey
	ULID           string
	TraceParent    string
	OccurredAt     time.Time

	// Ref-updated fields (RH# items).
	RefName      string
	OID          string
	PrevOID      string
	PackID       string
	Epoch        uint64
	OriginRegion string

	// Repository lifecycle fields (META items).
	HomeRegion     string
	FailoverEpoch  uint64
	RefShards      int
	ManifestShards int
	WriteState     string
	DefaultBranch  string
	ObjectFormat   string
}

// ParseStreamRecord decodes a DynamoDB Streams record into an OutboxEvent for
// ref.updated (RH# INSERT), repo.created (META INSERT), or repo.deleted
// (META REMOVE or META tombstone MODIFY). Non-matching records return (zero, false, nil).
func ParseStreamRecord(rec streamtypes.Record) (OutboxEvent, bool, error) {
	if rec.Dynamodb == nil {
		return OutboxEvent{}, false, nil
	}

	switch rec.EventName {
	case streamtypes.OperationTypeInsert:
		if len(rec.Dynamodb.NewImage) == 0 {
			return OutboxEvent{}, false, nil
		}
		pk := streamAttrString(rec.Dynamodb.NewImage, ddb.AttrPK)
		sk := streamAttrString(rec.Dynamodb.NewImage, ddb.AttrSK)
		if strings.Contains(pk, "#RH#") && strings.HasPrefix(sk, "REF#") {
			return parseRefHistoryInsert(rec, pk, sk)
		}
		if sk == "META" && isRepoMetaPK(pk) {
			return parseRepoMetaEvent(rec, EventKindRepoCreated, rec.Dynamodb.NewImage, pk)
		}
		return OutboxEvent{}, false, nil

	case streamtypes.OperationTypeRemove:
		img := rec.Dynamodb.OldImage
		pk := firstNonEmpty(streamAttrString(img, ddb.AttrPK), streamAttrString(rec.Dynamodb.Keys, ddb.AttrPK))
		sk := firstNonEmpty(streamAttrString(img, ddb.AttrSK), streamAttrString(rec.Dynamodb.Keys, ddb.AttrSK))
		if sk == "META" && isRepoMetaPK(pk) {
			return parseRepoMetaEvent(rec, EventKindRepoDeleted, img, pk)
		}
		return OutboxEvent{}, false, nil

	case streamtypes.OperationTypeModify:
		if len(rec.Dynamodb.NewImage) == 0 {
			return OutboxEvent{}, false, nil
		}
		pk := streamAttrString(rec.Dynamodb.NewImage, ddb.AttrPK)
		sk := streamAttrString(rec.Dynamodb.NewImage, ddb.AttrSK)
		if sk == "META" && isRepoMetaPK(pk) {
			wasDeleted := streamAttrBool(rec.Dynamodb.OldImage, "deleted")
			isDeleted := streamAttrBool(rec.Dynamodb.NewImage, "deleted")
			if isDeleted && !wasDeleted {
				return parseRepoMetaEvent(rec, EventKindRepoDeleted, rec.Dynamodb.NewImage, pk)
			}
		}
		return OutboxEvent{}, false, nil

	default:
		return OutboxEvent{}, false, nil
	}
}

func parseRefHistoryInsert(rec streamtypes.Record, pk, sk string) (OutboxEvent, bool, error) {
	img := rec.Dynamodb.NewImage

	tenantID := streamAttrString(img, "tenant_id")
	repoID := streamAttrString(img, "repo_id")
	if tenantID == "" || repoID == "" {
		parsedTenant, parsedRepo, err := parseRepoFromRefHistoryPK(pk)
		if err != nil {
			return OutboxEvent{}, false, err
		}
		if tenantID == "" {
			tenantID = parsedTenant
		}
		if repoID == "" {
			repoID = parsedRepo
		}
	}

	repoKey := tenant.RepoKey{
		TenantID: tenant.TenantID(tenantID),
		RepoID:   tenant.RepoID(repoID),
	}
	if err := repoKey.Validate(); err != nil {
		return OutboxEvent{}, false, fmt.Errorf("%w: %w", ErrMalformedRecord, err)
	}

	refName := streamAttrString(img, "ref_name")
	ulid := streamAttrString(img, "ulid")
	if refName == "" || ulid == "" {
		skBody := strings.TrimPrefix(sk, "REF#")
		if idx := strings.LastIndex(skBody, "#"); idx > 0 {
			if refName == "" {
				refName = skBody[:idx]
			}
			if ulid == "" {
				ulid = skBody[idx+1:]
			}
		} else if refName == "" {
			refName = skBody
		}
	}
	if strings.TrimSpace(refName) == "" {
		return OutboxEvent{}, false, fmt.Errorf("%w: missing ref_name", ErrMalformedRecord)
	}

	var occurredAt time.Time
	if raw := streamAttrString(img, "updated_at"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return OutboxEvent{}, false, fmt.Errorf("%w: invalid updated_at %q: %w", ErrMalformedRecord, raw, err)
		}
		occurredAt = parsed
	}

	var epoch uint64
	if raw := streamAttrNumber(img, "epoch"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return OutboxEvent{}, false, fmt.Errorf("%w: invalid epoch %q: %w", ErrMalformedRecord, raw, err)
		}
		epoch = parsed
	}

	return OutboxEvent{
		Kind:           EventKindRefUpdated,
		SequenceNumber: aws.ToString(rec.Dynamodb.SequenceNumber),
		Repo:           repoKey,
		RefName:        refName,
		ULID:           ulid,
		OID:            streamAttrString(img, "oid"),
		PrevOID:        streamAttrString(img, "prev_oid"),
		PackID:         streamAttrString(img, "pack_id"),
		TraceParent:    streamAttrString(img, "traceparent"),
		OccurredAt:     occurredAt,
		Epoch:          epoch,
		OriginRegion:   streamAttrString(img, "origin_region"),
	}, true, nil
}

func parseRepoMetaEvent(
	rec streamtypes.Record,
	kind EventKind,
	img map[string]streamtypes.AttributeValue,
	pk string,
) (OutboxEvent, bool, error) {
	tenantID := streamAttrString(img, "tenant_id")
	repoID := streamAttrString(img, "repo_id")
	if tenantID == "" || repoID == "" {
		parsedTenant, parsedRepo, err := parseRepoFromMetaPK(pk)
		if err != nil {
			return OutboxEvent{}, false, err
		}
		if tenantID == "" {
			tenantID = parsedTenant
		}
		if repoID == "" {
			repoID = parsedRepo
		}
	}

	repoKey := tenant.RepoKey{
		TenantID: tenant.TenantID(tenantID),
		RepoID:   tenant.RepoID(repoID),
	}
	if err := repoKey.Validate(); err != nil {
		return OutboxEvent{}, false, fmt.Errorf("%w: %w", ErrMalformedRecord, err)
	}

	var failoverEpoch uint64
	if raw := streamAttrNumber(img, "failover_epoch"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return OutboxEvent{}, false, fmt.Errorf("%w: invalid failover_epoch %q: %w", ErrMalformedRecord, raw, err)
		}
		failoverEpoch = parsed
	}

	var refShards int
	if raw := streamAttrNumber(img, "ref_shards"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return OutboxEvent{}, false, fmt.Errorf("%w: invalid ref_shards %q: %w", ErrMalformedRecord, raw, err)
		}
		refShards = parsed
	}

	var manifestShards int
	if raw := streamAttrNumber(img, "manifest_shards"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return OutboxEvent{}, false, fmt.Errorf("%w: invalid manifest_shards %q: %w", ErrMalformedRecord, raw, err)
		}
		manifestShards = parsed
	}

	var occurredAt time.Time
	for _, tsKey := range []string{"deleted_at", "updated_at", "created_at"} {
		if raw := streamAttrString(img, tsKey); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return OutboxEvent{}, false, fmt.Errorf("%w: invalid %s %q: %w", ErrMalformedRecord, tsKey, raw, err)
			}
			occurredAt = parsed
			break
		}
	}

	objFormat := string(ddb.NormalizeObjectFormat(ddb.ObjectFormat(streamAttrString(img, "object_format"))))

	return OutboxEvent{
		Kind:           kind,
		SequenceNumber: aws.ToString(rec.Dynamodb.SequenceNumber),
		Repo:           repoKey,
		ULID:           streamAttrString(img, "ulid"),
		TraceParent:    streamAttrString(img, "traceparent"),
		OccurredAt:     occurredAt,
		HomeRegion:     streamAttrString(img, "home_region"),
		FailoverEpoch:  failoverEpoch,
		RefShards:      refShards,
		ManifestShards: manifestShards,
		WriteState:     streamAttrString(img, "write_state"),
		DefaultBranch:  streamAttrString(img, "default_branch"),
		ObjectFormat:   objFormat,
	}, true, nil
}

func isRepoMetaPK(pk string) bool {
	if !strings.HasPrefix(pk, "T#") {
		return false
	}
	rest := strings.TrimPrefix(pk, "T#")
	tID, rID, ok := strings.Cut(rest, "#REPO#")
	if !ok || tID == "" || rID == "" {
		return false
	}
	// Exclude sharded/sub-entity keys like #RH#, #RS#, #MS#, #LEASE, #DLQ.
	return !strings.Contains(rID, "#")
}

func parseRepoFromMetaPK(pk string) (string, string, error) {
	if !strings.HasPrefix(pk, "T#") {
		return "", "", fmt.Errorf("%w: META PK %q does not start with T#", ErrMalformedRecord, pk)
	}
	rest := strings.TrimPrefix(pk, "T#")
	tenantID, repoID, ok := strings.Cut(rest, "#REPO#")
	if !ok || tenantID == "" || repoID == "" || strings.Contains(repoID, "#") {
		return "", "", fmt.Errorf("%w: invalid META PK %q", ErrMalformedRecord, pk)
	}
	return tenantID, repoID, nil
}

func parseRepoFromRefHistoryPK(pk string) (string, string, error) {
	rhIdx := strings.Index(pk, "#RH#")
	if rhIdx <= 0 {
		return "", "", fmt.Errorf("%w: missing #RH# segment in PK %q", ErrMalformedRecord, pk)
	}
	prefix := pk[:rhIdx]
	if !strings.HasPrefix(prefix, "T#") {
		return "", "", fmt.Errorf("%w: PK %q does not start with T#", ErrMalformedRecord, pk)
	}
	rest := strings.TrimPrefix(prefix, "T#")
	tenantID, repoID, ok := strings.Cut(rest, "#REPO#")
	if !ok || tenantID == "" || repoID == "" {
		return "", "", fmt.Errorf("%w: invalid tenant/repo prefix in PK %q", ErrMalformedRecord, pk)
	}
	return tenantID, repoID, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func streamAttrString(img map[string]streamtypes.AttributeValue, key string) string {
	av, ok := img[key]
	if !ok {
		return ""
	}
	s, ok := av.(*streamtypes.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return s.Value
}

func streamAttrNumber(img map[string]streamtypes.AttributeValue, key string) string {
	av, ok := img[key]
	if !ok {
		return ""
	}
	n, ok := av.(*streamtypes.AttributeValueMemberN)
	if !ok {
		return ""
	}
	return n.Value
}

func streamAttrBool(img map[string]streamtypes.AttributeValue, key string) bool {
	av, ok := img[key]
	if !ok {
		return false
	}
	b, ok := av.(*streamtypes.AttributeValueMemberBOOL)
	if !ok {
		return false
	}
	return b.Value
}
