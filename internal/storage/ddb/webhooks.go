package ddb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/tenant"
)

// WebhookRetryConfig controls asynchronous webhook delivery retries and DLQ routing.
type WebhookRetryConfig struct {
	MaxAttempts      int
	InitialBackoffMS int
	MaxBackoffMS     int
	DLQURL           string
}

// WebhookConfig configures a Tier 2 asynchronous webhook target.
type WebhookConfig struct {
	WebhookID       string
	URL             string
	EncryptedSecret []byte
	KMSKeyID        string
	RefGlob         string
	Enabled         bool
	Retry           WebhookRetryConfig
}

// Validate verifies required fields on WebhookConfig.
func (w WebhookConfig) Validate() error {
	if err := validateNonEmpty("webhook_id", w.WebhookID); err != nil {
		return err
	}
	if err := validateNonEmpty("url", w.URL); err != nil {
		return err
	}
	if err := validateNonEmpty("ref_glob", w.RefGlob); err != nil {
		return err
	}
	if w.Retry.MaxAttempts < 0 || w.Retry.InitialBackoffMS < 0 || w.Retry.MaxBackoffMS < 0 {
		return fmt.Errorf("%w: negative retry configuration", ErrInvalidInput)
	}
	return nil
}

// StreamCheckpoint tracks DynamoDB Streams consumer progress per shard.
type StreamCheckpoint struct {
	ShardID        string
	SequenceNumber string
	UpdatedAt      time.Time
	ConsumerID     string
}

// GetWebhooks retrieves the configured Tier 2 webhooks for repo.
func (r *Repository) GetWebhooks(ctx context.Context, repo tenant.RepoKey) ([]WebhookConfig, error) {
	key, err := WebhooksKey(repo)
	if err != nil {
		return nil, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, ErrNotFound
	}
	return unmarshalWebhooks(out.Item)
}

// PutWebhooks persists the Tier 2 webhook targets for repo.
func (r *Repository) PutWebhooks(ctx context.Context, repo tenant.RepoKey, webhooks []WebhookConfig) error {
	key, err := WebhooksKey(repo)
	if err != nil {
		return err
	}
	list := make([]types.AttributeValue, 0, len(webhooks))
	for _, w := range webhooks {
		if err := w.Validate(); err != nil {
			return err
		}
		entry := map[string]types.AttributeValue{
			"webhook_id": attrS(w.WebhookID),
			"url":        attrS(w.URL),
			"ref_glob":   attrS(w.RefGlob),
			"enabled":    attrBool(w.Enabled),
			"retry": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"max_attempts":       attrNInt(w.Retry.MaxAttempts),
					"initial_backoff_ms": attrNInt(w.Retry.InitialBackoffMS),
					"max_backoff_ms":     attrNInt(w.Retry.MaxBackoffMS),
					"dlq_url":            attrS(w.Retry.DLQURL),
				},
			},
		}
		if len(w.EncryptedSecret) > 0 {
			entry["encrypted_secret"] = attrB(w.EncryptedSecret)
		}
		if w.KMSKeyID != "" {
			entry["kms_key_id"] = attrS(w.KMSKeyID)
		}
		list = append(list, &types.AttributeValueMemberM{Value: entry})
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item: map[string]types.AttributeValue{
			AttrPK:     attrS(key.PK),
			AttrSK:     attrS(key.SK),
			"webhooks": &types.AttributeValueMemberL{Value: list},
		},
	})
	return err
}

// GetStreamCheckpoint fetches the saved sequence checkpoint for a DynamoDB Streams shard.
func (r *Repository) GetStreamCheckpoint(ctx context.Context, shardID string) (StreamCheckpoint, error) {
	key, err := StreamCheckpointKey(shardID)
	if err != nil {
		return StreamCheckpoint{}, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return StreamCheckpoint{}, err
	}
	if len(out.Item) == 0 {
		return StreamCheckpoint{}, ErrNotFound
	}
	updatedAt, err := getTime(out.Item, "updated_at")
	if err != nil {
		return StreamCheckpoint{}, err
	}
	return StreamCheckpoint{
		ShardID:        shardID,
		SequenceNumber: getString(out.Item, "sequence_number"),
		UpdatedAt:      updatedAt,
		ConsumerID:     getString(out.Item, "consumer_id"),
	}, nil
}

// PutStreamCheckpoint persists a DynamoDB Streams shard checkpoint.
func (r *Repository) PutStreamCheckpoint(ctx context.Context, cp StreamCheckpoint) error {
	key, err := StreamCheckpointKey(cp.ShardID)
	if err != nil {
		return err
	}
	if err := validateNonEmpty("sequence_number", cp.SequenceNumber); err != nil {
		return err
	}
	updatedAt := cp.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = r.clock.Now().UTC()
	}
	item := map[string]types.AttributeValue{
		AttrPK:            attrS(key.PK),
		AttrSK:            attrS(key.SK),
		"shard_id":        attrS(cp.ShardID),
		"sequence_number": attrS(cp.SequenceNumber),
		"updated_at":      attrS(updatedAt.UTC().Format(time.RFC3339Nano)),
	}
	if cp.ConsumerID != "" {
		item["consumer_id"] = attrS(cp.ConsumerID)
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      item,
	})
	return err
}

func unmarshalWebhooks(item map[string]types.AttributeValue) ([]WebhookConfig, error) {
	av, ok := item["webhooks"]
	if !ok {
		return nil, nil
	}
	l, ok := av.(*types.AttributeValueMemberL)
	if !ok {
		return nil, fmt.Errorf("ddb: webhooks attribute is not a list")
	}
	hooks := make([]WebhookConfig, 0, len(l.Value))
	for _, elem := range l.Value {
		m, ok := elem.(*types.AttributeValueMemberM)
		if !ok {
			return nil, fmt.Errorf("ddb: webhook entry is not a map")
		}
		var retry WebhookRetryConfig
		if rav, ok := m.Value["retry"]; ok {
			rm, ok := rav.(*types.AttributeValueMemberM)
			if !ok {
				return nil, fmt.Errorf("ddb: webhook retry attribute is not a map")
			}
			var err error
			if retry.MaxAttempts, err = getInt(rm.Value, "max_attempts"); err != nil {
				return nil, err
			}
			if retry.InitialBackoffMS, err = getInt(rm.Value, "initial_backoff_ms"); err != nil {
				return nil, err
			}
			if retry.MaxBackoffMS, err = getInt(rm.Value, "max_backoff_ms"); err != nil {
				return nil, err
			}
			retry.DLQURL = getString(rm.Value, "dlq_url")
		}
		hooks = append(hooks, WebhookConfig{
			WebhookID:       getString(m.Value, "webhook_id"),
			URL:             getString(m.Value, "url"),
			EncryptedSecret: getBytes(m.Value, "encrypted_secret"),
			KMSKeyID:        getString(m.Value, "kms_key_id"),
			RefGlob:         getString(m.Value, "ref_glob"),
			Enabled:         getBool(m.Value, "enabled"),
			Retry:           retry,
		})
	}
	return hooks, nil
}
