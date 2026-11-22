// Package async implements Stravin Core's Tier 2 asynchronous hook engine:
// a DynamoDB Streams Ref History outbox consumer, W3C traceparent span linker,
// KMS-decrypted HMAC-SHA256 webhook dispatcher, exponential backoff retry
// middleware, and dead-letter queue (DLQ) persistence.
package async

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

var ErrMalformedRecord = errors.New("async: malformed ref history stream record")

// RefHistoryEvent is a parsed DynamoDB Streams INSERT record for a Ref History outbox item.
type RefHistoryEvent struct {
	SequenceNumber string
	Repo           tenant.RepoKey
	RefName        string
	ULID           string
	OID            string
	PrevOID        string
	PackID         string
	TraceParent    string
	UpdatedAt      time.Time
	Epoch          uint64
	OriginRegion   string
}

// ParseRefHistoryRecord filters a DynamoDB Streams record for INSERT events on
// Ref History items (PK containing "#RH#", SK prefixed with "REF#") and decodes
// its NEW_IMAGE into a RefHistoryEvent.
//
// Returns (zero, false, nil) for non-matching records so callers can skip them cleanly.
func ParseRefHistoryRecord(rec streamtypes.Record) (RefHistoryEvent, bool, error) {
	if rec.EventName != streamtypes.OperationTypeInsert {
		return RefHistoryEvent{}, false, nil
	}
	if rec.Dynamodb == nil || len(rec.Dynamodb.NewImage) == 0 {
		return RefHistoryEvent{}, false, nil
	}

	img := rec.Dynamodb.NewImage
	pk := streamAttrString(img, ddb.AttrPK)
	sk := streamAttrString(img, ddb.AttrSK)
	if !strings.Contains(pk, "#RH#") || !strings.HasPrefix(sk, "REF#") {
		return RefHistoryEvent{}, false, nil
	}

	tenantID := streamAttrString(img, "tenant_id")
	repoID := streamAttrString(img, "repo_id")
	if tenantID == "" || repoID == "" {
		parsedTenant, parsedRepo, err := parseRepoFromRefHistoryPK(pk)
		if err != nil {
			return RefHistoryEvent{}, false, err
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
		return RefHistoryEvent{}, false, fmt.Errorf("%w: %w", ErrMalformedRecord, err)
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
		return RefHistoryEvent{}, false, fmt.Errorf("%w: missing ref_name", ErrMalformedRecord)
	}

	var updatedAt time.Time
	if raw := streamAttrString(img, "updated_at"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return RefHistoryEvent{}, false, fmt.Errorf("%w: invalid updated_at %q: %w", ErrMalformedRecord, raw, err)
		}
		updatedAt = parsed
	}

	var epoch uint64
	if raw := streamAttrNumber(img, "epoch"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return RefHistoryEvent{}, false, fmt.Errorf("%w: invalid epoch %q: %w", ErrMalformedRecord, raw, err)
		}
		epoch = parsed
	}

	return RefHistoryEvent{
		SequenceNumber: aws.ToString(rec.Dynamodb.SequenceNumber),
		Repo:           repoKey,
		RefName:        refName,
		ULID:           ulid,
		OID:            streamAttrString(img, "oid"),
		PrevOID:        streamAttrString(img, "prev_oid"),
		PackID:         streamAttrString(img, "pack_id"),
		TraceParent:    streamAttrString(img, "traceparent"),
		UpdatedAt:      updatedAt,
		Epoch:          epoch,
		OriginRegion:   streamAttrString(img, "origin_region"),
	}, true, nil
}

// parseRepoFromRefHistoryPK extracts tenant_id and repo_id from "T#{tenant}#REPO#{repo}#RH#{shard}".
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
