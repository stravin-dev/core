package ctrl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/storage/ddb"
)

// AuditStore persists immutable ScrubReport audit records.
type AuditStore interface {
	RecordScrubAudit(ctx context.Context, report ScrubReport) error
}

// DDBAuditStore writes immutable scrub audit records to the Stravin single-table
// DynamoDB store under PK=T#{tenant}#REPO#{repo}#AUDIT, SK=SCRUB#{audit_id}.
type DDBAuditStore struct {
	client    ddb.API
	tableName string
}

var _ AuditStore = (*DDBAuditStore)(nil)

// NewDDBAuditStore constructs a DynamoDB-backed immutable AuditStore.
func NewDDBAuditStore(client ddb.API, tableName string) (*DDBAuditStore, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: nil dynamodb client", ddb.ErrInvalidInput)
	}
	tableName = strings.TrimSpace(tableName)
	if tableName == "" {
		return nil, fmt.Errorf("%w: empty table name", ddb.ErrInvalidInput)
	}
	return &DDBAuditStore{
		client:    client,
		tableName: tableName,
	}, nil
}

// ScrubAuditKey returns PK=T#{tenant}#REPO#{repo}#AUDIT, SK=SCRUB#{audit_id}.
func ScrubAuditKey(report ScrubReport) (ddb.Key, error) {
	if err := report.Repo.Validate(); err != nil {
		return ddb.Key{}, err
	}
	if strings.TrimSpace(report.AuditID) == "" {
		return ddb.Key{}, fmt.Errorf("%w: empty audit_id", ddb.ErrInvalidInput)
	}
	return ddb.Key{
		PK: fmt.Sprintf("%s#AUDIT", report.Repo.DynamoPrefix()),
		SK: fmt.Sprintf("SCRUB#%s", report.AuditID),
	}, nil
}

// RecordScrubAudit persists report as an immutable DynamoDB item.
func (s *DDBAuditStore) RecordScrubAudit(ctx context.Context, report ScrubReport) error {
	key, err := ScrubAuditKey(report)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("ctrl: marshal scrub audit report: %w", err)
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]types.AttributeValue{
			ddb.AttrPK:       &types.AttributeValueMemberS{Value: key.PK},
			ddb.AttrSK:       &types.AttributeValueMemberS{Value: key.SK},
			"audit_id":       &types.AttributeValueMemberS{Value: report.AuditID},
			"tenant_id":      &types.AttributeValueMemberS{Value: string(report.Repo.TenantID)},
			"repo_id":        &types.AttributeValueMemberS{Value: string(report.Repo.RepoID)},
			"region":         &types.AttributeValueMemberS{Value: string(report.Region)},
			"failover_epoch": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", report.FailoverEpoch)},
			"status":         &types.AttributeValueMemberS{Value: report.Status},
			"completed_at":   &types.AttributeValueMemberS{Value: report.CompletedAt.UTC().Format(time.RFC3339Nano)},
			"report_json":    &types.AttributeValueMemberS{Value: string(payload)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		return fmt.Errorf("ctrl: put scrub audit %s: %w", report.AuditID, err)
	}
	return nil
}
