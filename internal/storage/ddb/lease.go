package ddb

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

// ShardLease represents a compaction lease for manifest shard SHARD#{m}.
type ShardLease struct {
	Repo      tenant.RepoKey
	Shard     int
	OwnerID   string
	ExpiresAt time.Time
}

// RegionalWriteLease represents the regional write-authorization lease at
// PK=REGION#{region}#LEASE, SK=WRITE.
type RegionalWriteLease struct {
	Region    lease.Region
	Epoch     lease.Epoch
	Mode      lease.Mode
	HolderID  string
	ExpiresAt time.Time
}

// AcquireShardLease conditionally claims a compaction lease for SHARD#{shard}.
func (r *Repository) AcquireShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ShardLease, error) {
	if err := validateNonEmpty("owner_id", ownerID); err != nil {
		return ShardLease{}, err
	}
	if ttl <= 0 {
		return ShardLease{}, fmt.Errorf("%w: ttl must be positive", ErrInvalidInput)
	}
	key, err := CompactionLeaseKey(repo, shard)
	if err != nil {
		return ShardLease{}, err
	}

	now := r.clock.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item: map[string]types.AttributeValue{
			AttrPK:             attrS(key.PK),
			AttrSK:             attrS(key.SK),
			"owner_id":         attrS(ownerID),
			"lease_expires_at": attrNInt64(expiresAt.UnixMilli()),
		},
		ConditionExpression: aws.String("attribute_not_exists(PK) OR lease_expires_at <= :now OR owner_id = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now":   attrNInt64(now.UnixMilli()),
			":owner": attrS(ownerID),
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return ShardLease{}, ErrLeaseHeld
		}
		return ShardLease{}, err
	}

	return ShardLease{
		Repo:      repo,
		Shard:     shard,
		OwnerID:   ownerID,
		ExpiresAt: expiresAt,
	}, nil
}

// RenewShardLease extends an active compaction shard lease still held by ownerID.
func (r *Repository) RenewShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string, ttl time.Duration) (ShardLease, error) {
	if err := validateNonEmpty("owner_id", ownerID); err != nil {
		return ShardLease{}, err
	}
	if ttl <= 0 {
		return ShardLease{}, fmt.Errorf("%w: ttl must be positive", ErrInvalidInput)
	}
	key, err := CompactionLeaseKey(repo, shard)
	if err != nil {
		return ShardLease{}, err
	}

	now := r.clock.Now().UTC()
	expiresAt := now.Add(ttl)

	_, err = r.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(r.tableName),
		Key:                 key.AttributeMap(),
		UpdateExpression:    aws.String("SET lease_expires_at = :expires"),
		ConditionExpression: aws.String("owner_id = :owner AND lease_expires_at > :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expires": attrNInt64(expiresAt.UnixMilli()),
			":owner":   attrS(ownerID),
			":now":     attrNInt64(now.UnixMilli()),
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return ShardLease{}, ErrLeaseLost
		}
		return ShardLease{}, err
	}

	return ShardLease{
		Repo:      repo,
		Shard:     shard,
		OwnerID:   ownerID,
		ExpiresAt: expiresAt,
	}, nil
}

// ReleaseShardLease releases a compaction shard lease if currently held by ownerID.
func (r *Repository) ReleaseShardLease(ctx context.Context, repo tenant.RepoKey, shard int, ownerID string) error {
	if err := validateNonEmpty("owner_id", ownerID); err != nil {
		return err
	}
	key, err := CompactionLeaseKey(repo, shard)
	if err != nil {
		return err
	}
	_, err = r.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:           aws.String(r.tableName),
		Key:                 key.AttributeMap(),
		ConditionExpression: aws.String("owner_id = :owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":owner": attrS(ownerID),
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return ErrLeaseLost
		}
		return err
	}
	return nil
}

// GetShardLease reads the current compaction shard lease item.
func (r *Repository) GetShardLease(ctx context.Context, repo tenant.RepoKey, shard int) (ShardLease, error) {
	key, err := CompactionLeaseKey(repo, shard)
	if err != nil {
		return ShardLease{}, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return ShardLease{}, err
	}
	if len(out.Item) == 0 {
		return ShardLease{}, ErrNotFound
	}
	expMs, err := getInt64(out.Item, "lease_expires_at")
	if err != nil {
		return ShardLease{}, err
	}
	return ShardLease{
		Repo:      repo,
		Shard:     shard,
		OwnerID:   getString(out.Item, "owner_id"),
		ExpiresAt: time.UnixMilli(expMs).UTC(),
	}, nil
}

// PutRegionalWriteLease writes a regional write lease unconditionally (used by control plane bootstrap/admin).
func (r *Repository) PutRegionalWriteLease(ctx context.Context, l RegionalWriteLease) error {
	if err := validateRegionalWriteLease(l); err != nil {
		return err
	}
	key, err := RegionalWriteLeaseKey(l.Region)
	if err != nil {
		return err
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      marshalRegionalWriteLease(key, l),
	})
	return err
}

// AcquireRegionalWriteLease conditionally acquires or renews a regional write lease
// with monotonic failover_epoch fencing.
func (r *Repository) AcquireRegionalWriteLease(ctx context.Context, region lease.Region, holderID string, epoch lease.Epoch, mode lease.Mode, ttl time.Duration) (RegionalWriteLease, error) {
	if ttl <= 0 {
		return RegionalWriteLease{}, fmt.Errorf("%w: ttl must be positive", ErrInvalidInput)
	}
	now := r.clock.Now().UTC()
	l := RegionalWriteLease{
		Region:    region,
		Epoch:     epoch,
		Mode:      mode,
		HolderID:  holderID,
		ExpiresAt: now.Add(ttl),
	}
	if err := validateRegionalWriteLease(l); err != nil {
		return RegionalWriteLease{}, err
	}
	key, err := RegionalWriteLeaseKey(region)
	if err != nil {
		return RegionalWriteLease{}, err
	}

	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      marshalRegionalWriteLease(key, l),
		ConditionExpression: aws.String(
			"(attribute_not_exists(PK) OR lease_expires_at <= :now OR holder_id = :holder) AND (attribute_not_exists(failover_epoch) OR failover_epoch <= :epoch)",
		),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now":    attrNInt64(now.UnixMilli()),
			":holder": attrS(holderID),
			":epoch":  attrNUint64(uint64(epoch)),
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return RegionalWriteLease{}, ErrLeaseHeld
		}
		return RegionalWriteLease{}, err
	}
	return l, nil
}

// GetRegionalWriteLease fetches the regional write lease for region.
func (r *Repository) GetRegionalWriteLease(ctx context.Context, region lease.Region) (RegionalWriteLease, error) {
	key, err := RegionalWriteLeaseKey(region)
	if err != nil {
		return RegionalWriteLease{}, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return RegionalWriteLease{}, err
	}
	if len(out.Item) == 0 {
		return RegionalWriteLease{}, lease.ErrLeaseNotFound
	}
	epoch, err := getUint64(out.Item, "failover_epoch")
	if err != nil {
		return RegionalWriteLease{}, err
	}
	expMs, err := getInt64(out.Item, "lease_expires_at")
	if err != nil {
		return RegionalWriteLease{}, err
	}
	return RegionalWriteLease{
		Region:    region,
		Epoch:     lease.Epoch(epoch),
		Mode:      lease.Mode(getString(out.Item, "write_state")),
		HolderID:  getString(out.Item, "holder_id"),
		ExpiresAt: time.UnixMilli(expMs).UTC(),
	}, nil
}

// GetLease implements internal/lease.Store by combining repository META routing
// and fencing metadata with the home region's active write lease.
func (r *Repository) GetLease(ctx context.Context, repo tenant.RepoKey) (lease.Lease, error) {
	meta, err := r.GetMeta(ctx, repo)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return lease.Lease{}, lease.ErrLeaseNotFound
		}
		return lease.Lease{}, err
	}
	regLease, err := r.GetRegionalWriteLease(ctx, meta.HomeRegion)
	if err != nil {
		return lease.Lease{}, err
	}
	if err := lease.VerifyEpochFence(meta.FailoverEpoch, regLease.Epoch); err != nil {
		return lease.Lease{}, err
	}

	effectiveMode := meta.WriteState
	if effectiveMode == lease.ModeActive && regLease.Mode != lease.ModeActive {
		effectiveMode = regLease.Mode
	}

	return lease.Lease{
		Repo:       repo,
		HomeRegion: meta.HomeRegion,
		Epoch:      regLease.Epoch,
		Mode:       effectiveMode,
		ExpiresAt:  regLease.ExpiresAt,
		HolderID:   regLease.HolderID,
	}, nil
}

func validateRegionalWriteLease(l RegionalWriteLease) error {
	if strings.TrimSpace(string(l.Region)) == "" {
		return lease.ErrEmptyRegion
	}
	if l.Epoch == 0 {
		return lease.ErrInvalidEpoch
	}
	if err := validateNonEmpty("holder_id", l.HolderID); err != nil {
		return err
	}
	switch l.Mode {
	case lease.ModeActive, lease.ModeDraining, lease.ModeReadOnly:
	default:
		return fmt.Errorf("%w: invalid write_state %q", ErrInvalidInput, l.Mode)
	}
	if l.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at must be set", ErrInvalidInput)
	}
	return nil
}

func marshalRegionalWriteLease(key Key, l RegionalWriteLease) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		AttrPK:             attrS(key.PK),
		AttrSK:             attrS(key.SK),
		"region":           attrS(string(l.Region)),
		"failover_epoch":   attrNUint64(uint64(l.Epoch)),
		"write_state":      attrS(string(l.Mode)),
		"holder_id":        attrS(l.HolderID),
		"lease_expires_at": attrNInt64(l.ExpiresAt.UTC().UnixMilli()),
	}
}
