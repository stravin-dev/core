package ddb

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

// RepoMeta represents the per-repository META routing and fencing item.
type RepoMeta struct {
	Repo           tenant.RepoKey
	HomeRegion     lease.Region
	FailoverEpoch  lease.Epoch
	RefShards      int
	ManifestShards int
	WriteState     lease.Mode
	DefaultBranch  string
}

// Validate checks all required META fields and shard bounds.
func (m RepoMeta) Validate() error {
	if err := m.Repo.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(string(m.HomeRegion)) == "" {
		return lease.ErrEmptyRegion
	}
	if m.FailoverEpoch == 0 {
		return lease.ErrInvalidEpoch
	}
	if err := ValidateRefShards(m.RefShards); err != nil {
		return err
	}
	if err := ValidateManifestShards(m.ManifestShards); err != nil {
		return err
	}
	switch m.WriteState {
	case lease.ModeActive, lease.ModeDraining, lease.ModeReadOnly:
	default:
		return fmt.Errorf("%w: invalid write_state %q", ErrInvalidInput, m.WriteState)
	}
	return nil
}

// GetMeta retrieves the META record for repo.
func (r *Repository) GetMeta(ctx context.Context, repo tenant.RepoKey) (RepoMeta, error) {
	key, err := MetaKey(repo)
	if err != nil {
		return RepoMeta{}, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return RepoMeta{}, err
	}
	if len(out.Item) == 0 {
		return RepoMeta{}, ErrNotFound
	}
	return unmarshalRepoMeta(repo, out.Item)
}

// CreateMeta inserts a new META record if one does not already exist.
func (r *Repository) CreateMeta(ctx context.Context, meta RepoMeta) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	key, err := MetaKey(meta.Repo)
	if err != nil {
		return err
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.tableName),
		Item:                marshalRepoMeta(key, meta),
		ConditionExpression: aws.String("attribute_not_exists(PK)"),
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return ErrAlreadyExists
		}
		return err
	}
	return nil
}

// UpdateMeta updates a repository META record conditioned on expectedEpoch.
func (r *Repository) UpdateMeta(ctx context.Context, meta RepoMeta, expectedEpoch lease.Epoch) error {
	if err := meta.Validate(); err != nil {
		return err
	}
	if expectedEpoch == 0 {
		return lease.ErrInvalidEpoch
	}
	if meta.FailoverEpoch < expectedEpoch {
		return fmt.Errorf("%w: new epoch %d < expected %d", ErrEpochConflict, meta.FailoverEpoch, expectedEpoch)
	}
	key, err := MetaKey(meta.Repo)
	if err != nil {
		return err
	}

	expr := "SET home_region = :home_region, failover_epoch = :epoch, ref_shards = :ref_shards, manifest_shards = :manifest_shards, write_state = :write_state"
	values := map[string]types.AttributeValue{
		":home_region":     attrS(string(meta.HomeRegion)),
		":epoch":           attrNUint64(uint64(meta.FailoverEpoch)),
		":ref_shards":      attrNInt(meta.RefShards),
		":manifest_shards": attrNInt(meta.ManifestShards),
		":write_state":     attrS(string(meta.WriteState)),
		":expected_epoch":  attrNUint64(uint64(expectedEpoch)),
	}
	if meta.DefaultBranch != "" {
		expr += ", default_branch = :default_branch"
		values[":default_branch"] = attrS(meta.DefaultBranch)
	}

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(r.tableName),
		Key:                       key.AttributeMap(),
		UpdateExpression:          aws.String(expr),
		ConditionExpression:       aws.String("failover_epoch = :expected_epoch"),
		ExpressionAttributeValues: values,
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return ErrEpochConflict
		}
		return err
	}
	return nil
}

func marshalRepoMeta(key Key, meta RepoMeta) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		AttrPK:            attrS(key.PK),
		AttrSK:            attrS(key.SK),
		"tenant_id":       attrS(string(meta.Repo.TenantID)),
		"repo_id":         attrS(string(meta.Repo.RepoID)),
		"home_region":     attrS(string(meta.HomeRegion)),
		"failover_epoch":  attrNUint64(uint64(meta.FailoverEpoch)),
		"ref_shards":      attrNInt(meta.RefShards),
		"manifest_shards": attrNInt(meta.ManifestShards),
		"write_state":     attrS(string(meta.WriteState)),
	}
	if meta.DefaultBranch != "" {
		item["default_branch"] = attrS(meta.DefaultBranch)
	}
	return item
}

func unmarshalRepoMeta(repo tenant.RepoKey, item map[string]types.AttributeValue) (RepoMeta, error) {
	epoch, err := getUint64(item, "failover_epoch")
	if err != nil {
		return RepoMeta{}, err
	}
	refShards, err := getInt(item, "ref_shards")
	if err != nil {
		return RepoMeta{}, err
	}
	manifestShards, err := getInt(item, "manifest_shards")
	if err != nil {
		return RepoMeta{}, err
	}
	return RepoMeta{
		Repo:           repo,
		HomeRegion:     lease.Region(getString(item, "home_region")),
		FailoverEpoch:  lease.Epoch(epoch),
		RefShards:      refShards,
		ManifestShards: manifestShards,
		WriteState:     lease.Mode(getString(item, "write_state")),
		DefaultBranch:  getString(item, "default_branch"),
	}, nil
}
