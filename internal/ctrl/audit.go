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

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/tenant"
)

// AuditStore persists immutable ScrubReport audit records.
type AuditStore interface {
	RecordScrubAudit(ctx context.Context, report ScrubReport) error
}

// PromotionAuditRecord captures an immutable failover promotion guard decision.
type PromotionAuditRecord struct {
	AuditID      string            `json:"audit_id"`
	Region       lease.Region      `json:"region"`
	HomeRegion   lease.Region      `json:"home_region,omitempty"`
	Repo         *tenant.RepoKey   `json:"repo,omitempty"`
	CurrentEpoch lease.Epoch       `json:"current_epoch"`
	TargetEpoch  lease.Epoch       `json:"target_epoch"`
	TargetMode   lease.Mode        `json:"target_mode,omitempty"`
	Decision     PromotionDecision `json:"decision"`
	Reason       string            `json:"reason,omitempty"`
	EvaluatedAt  time.Time         `json:"evaluated_at"`
}

// PromotionAuditStore persists immutable failover promotion guard audit records.
type PromotionAuditStore interface {
	RecordPromotionAudit(ctx context.Context, record PromotionAuditRecord) error
}

// RecoveryAuditRecord captures an immutable audit entry for a published
// orphaned-pack recovery ref (refs/stravin-recovery/*).
type RecoveryAuditRecord struct {
	AuditID       string         `json:"audit_id"`
	Repo          tenant.RepoKey `json:"repo"`
	Region        lease.Region   `json:"region"`
	FailoverEpoch lease.Epoch    `json:"failover_epoch"`
	SourceRef     string         `json:"source_ref"`
	RecoveryRef   string         `json:"recovery_ref"`
	CommitOID     string         `json:"commit_oid"`
	PackID        string         `json:"pack_id"`
	PackSizeBytes int64          `json:"pack_size_bytes"`
	OriginRegion  lease.Region   `json:"origin_region"`
	PublishedAt   time.Time      `json:"published_at"`
}

// RecoveryAuditStore persists immutable recovery branch publication audit records.
type RecoveryAuditStore interface {
	RecordRecoveryAudit(ctx context.Context, record RecoveryAuditRecord) error
}

// DDBAuditStore writes immutable scrub, promotion, and recovery audit records to
// the Stravin single-table DynamoDB store.
type DDBAuditStore struct {
	client    ddb.API
	tableName string
}

var (
	_ AuditStore          = (*DDBAuditStore)(nil)
	_ PromotionAuditStore = (*DDBAuditStore)(nil)
	_ RecoveryAuditStore  = (*DDBAuditStore)(nil)
)

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

// PromotionAuditKey returns PK=T#{tenant}#REPO#{repo}#AUDIT (when Repo is set)
// or PK=REGION#{region}#AUDIT, with SK=PROMOTION#{audit_id}.
func PromotionAuditKey(record PromotionAuditRecord) (ddb.Key, error) {
	if strings.TrimSpace(record.AuditID) == "" {
		return ddb.Key{}, fmt.Errorf("%w: empty audit_id", ddb.ErrInvalidInput)
	}
	if record.Repo != nil {
		if err := record.Repo.Validate(); err != nil {
			return ddb.Key{}, err
		}
		return ddb.Key{
			PK: fmt.Sprintf("%s#AUDIT", record.Repo.DynamoPrefix()),
			SK: fmt.Sprintf("PROMOTION#%s", record.AuditID),
		}, nil
	}
	region := strings.TrimSpace(string(record.Region))
	if region == "" {
		return ddb.Key{}, lease.ErrEmptyRegion
	}
	return ddb.Key{
		PK: fmt.Sprintf("REGION#%s#AUDIT", region),
		SK: fmt.Sprintf("PROMOTION#%s", record.AuditID),
	}, nil
}

// RecordPromotionAudit persists a failover promotion guard decision as an immutable DynamoDB item.
func (s *DDBAuditStore) RecordPromotionAudit(ctx context.Context, record PromotionAuditRecord) error {
	key, err := PromotionAuditKey(record)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("ctrl: marshal promotion audit record: %w", err)
	}

	item := map[string]types.AttributeValue{
		ddb.AttrPK:        &types.AttributeValueMemberS{Value: key.PK},
		ddb.AttrSK:        &types.AttributeValueMemberS{Value: key.SK},
		"audit_id":        &types.AttributeValueMemberS{Value: record.AuditID},
		"region":          &types.AttributeValueMemberS{Value: string(record.Region)},
		"current_epoch":   &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", record.CurrentEpoch)},
		"target_epoch":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", record.TargetEpoch)},
		"allowed":         &types.AttributeValueMemberBOOL{Value: record.Decision.Allowed},
		"guard_type":      &types.AttributeValueMemberS{Value: record.Decision.GuardType},
		"decision_reason": &types.AttributeValueMemberS{Value: record.Decision.Reason},
		"evaluated_at":    &types.AttributeValueMemberS{Value: record.EvaluatedAt.UTC().Format(time.RFC3339Nano)},
		"record_json":     &types.AttributeValueMemberS{Value: string(payload)},
	}
	if record.HomeRegion != "" {
		item["home_region"] = &types.AttributeValueMemberS{Value: string(record.HomeRegion)}
	}
	if record.Decision.OperatorID != "" {
		item["operator_id"] = &types.AttributeValueMemberS{Value: record.Decision.OperatorID}
	}
	if record.Repo != nil {
		item["tenant_id"] = &types.AttributeValueMemberS{Value: string(record.Repo.TenantID)}
		item["repo_id"] = &types.AttributeValueMemberS{Value: string(record.Repo.RepoID)}
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.tableName),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		return fmt.Errorf("ctrl: put promotion audit %s: %w", record.AuditID, err)
	}
	return nil
}

// RecoveryAuditKey returns PK=T#{tenant}#REPO#{repo}#AUDIT, SK=RECOVERY#{audit_id}.
func RecoveryAuditKey(record RecoveryAuditRecord) (ddb.Key, error) {
	if err := record.Repo.Validate(); err != nil {
		return ddb.Key{}, err
	}
	if strings.TrimSpace(record.AuditID) == "" {
		return ddb.Key{}, fmt.Errorf("%w: empty audit_id", ddb.ErrInvalidInput)
	}
	return ddb.Key{
		PK: fmt.Sprintf("%s#AUDIT", record.Repo.DynamoPrefix()),
		SK: fmt.Sprintf("RECOVERY#%s", record.AuditID),
	}, nil
}

// RecordRecoveryAudit persists a published recovery ref record as an immutable DynamoDB item.
func (s *DDBAuditStore) RecordRecoveryAudit(ctx context.Context, record RecoveryAuditRecord) error {
	key, err := RecoveryAuditKey(record)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("ctrl: marshal recovery audit record: %w", err)
	}

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item: map[string]types.AttributeValue{
			ddb.AttrPK:       &types.AttributeValueMemberS{Value: key.PK},
			ddb.AttrSK:       &types.AttributeValueMemberS{Value: key.SK},
			"audit_id":       &types.AttributeValueMemberS{Value: record.AuditID},
			"tenant_id":      &types.AttributeValueMemberS{Value: string(record.Repo.TenantID)},
			"repo_id":        &types.AttributeValueMemberS{Value: string(record.Repo.RepoID)},
			"region":         &types.AttributeValueMemberS{Value: string(record.Region)},
			"failover_epoch": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", record.FailoverEpoch)},
			"source_ref":     &types.AttributeValueMemberS{Value: record.SourceRef},
			"recovery_ref":   &types.AttributeValueMemberS{Value: record.RecoveryRef},
			"commit_oid":     &types.AttributeValueMemberS{Value: record.CommitOID},
			"pack_id":        &types.AttributeValueMemberS{Value: record.PackID},
			"published_at":   &types.AttributeValueMemberS{Value: record.PublishedAt.UTC().Format(time.RFC3339Nano)},
			"record_json":    &types.AttributeValueMemberS{Value: string(payload)},
		},
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		return fmt.Errorf("ctrl: put recovery audit %s: %w", record.AuditID, err)
	}
	return nil
}
