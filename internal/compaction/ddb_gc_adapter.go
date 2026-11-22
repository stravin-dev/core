package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

// DDBManifestGCAdapter implements ManifestGCStore directly against the Stravin
// DynamoDB single table without modifying internal/storage/ddb.
type DDBManifestGCAdapter struct {
	client    ddb.API
	tableName string
}

// NewDDBManifestGCAdapter constructs a DDBManifestGCAdapter for tableName.
func NewDDBManifestGCAdapter(client ddb.API, tableName string) (*DDBManifestGCAdapter, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil dynamodb client", ErrInvalidConfig)
	}
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, fmt.Errorf("%w: empty dynamodb table name", ErrInvalidConfig)
	}
	return &DDBManifestGCAdapter{
		client:    client,
		tableName: tableName,
	}, nil
}

// MarkGCEligible stamps compacted_at and gc_after on COMPACTED manifest entries.
func (a *DDBManifestGCAdapter) MarkGCEligible(
	ctx context.Context,
	repo tenant.RepoKey,
	manifestShards int,
	packs []ddb.PackManifest,
	compactedAt, gcAfter time.Time,
) error {
	for _, p := range packs {
		key, err := ddb.ManifestKey(repo, p.ULID, p.PackID, manifestShards)
		if err != nil {
			return err
		}
		_, err = a.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName:           aws.String(a.tableName),
			Key:                 key.AttributeMap(),
			UpdateExpression:    aws.String("SET compacted_at = :compacted_at, gc_after = :gc_after"),
			ConditionExpression: aws.String("#status = :compacted"),
			ExpressionAttributeNames: map[string]string{
				"#status": "status",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":compacted_at": &types.AttributeValueMemberS{Value: compactedAt.UTC().Format(time.RFC3339Nano)},
				":gc_after":     &types.AttributeValueMemberS{Value: gcAfter.UTC().Format(time.RFC3339Nano)},
				":compacted":    &types.AttributeValueMemberS{Value: string(ddb.PackStatusCompacted)},
			},
		})
		if err != nil {
			return fmt.Errorf("compaction: mark gc_after for %s: %w", p.PackID, err)
		}
	}
	return nil
}

// GetGCEligibility reads the gc_after timestamp stamped on a COMPACTED manifest entry.
func (a *DDBManifestGCAdapter) GetGCEligibility(
	ctx context.Context,
	repo tenant.RepoKey,
	manifestShards int,
	pack ddb.PackManifest,
) (time.Time, bool, error) {
	key, err := ddb.ManifestKey(repo, pack.ULID, pack.PackID, manifestShards)
	if err != nil {
		return time.Time{}, false, err
	}
	out, err := a.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(a.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return time.Time{}, false, err
	}
	if len(out.Item) == 0 {
		return time.Time{}, false, nil
	}
	av, ok := out.Item["gc_after"]
	if !ok {
		return time.Time{}, false, nil
	}
	s, ok := av.(*types.AttributeValueMemberS)
	if !ok || s.Value == "" {
		return time.Time{}, false, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, s.Value)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("compaction: parse gc_after %q: %w", s.Value, err)
	}
	return ts, true, nil
}

// DeleteManifest deletes a reaped COMPACTED manifest item from DynamoDB.
func (a *DDBManifestGCAdapter) DeleteManifest(
	ctx context.Context,
	repo tenant.RepoKey,
	manifestShards int,
	pack ddb.PackManifest,
) error {
	key, err := ddb.ManifestKey(repo, pack.ULID, pack.PackID, manifestShards)
	if err != nil {
		return err
	}
	_, err = a.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(a.tableName),
		Key:                 key.AttributeMap(),
		ConditionExpression: aws.String("#status = :compacted"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":compacted": &types.AttributeValueMemberS{Value: string(ddb.PackStatusCompacted)},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return nil
		}
		return fmt.Errorf("compaction: delete manifest %s: %w", pack.PackID, err)
	}
	return nil
}

var _ ManifestGCStore = (*DDBManifestGCAdapter)(nil)
