package ddb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

// PackStatus tracks a packfile's compaction lifecycle state.
type PackStatus string

const (
	PackStatusActive    PackStatus = "ACTIVE"
	PackStatusCompacted PackStatus = "COMPACTED"
)

// PackManifest represents an entry in MS#{m}.
type PackManifest struct {
	Repo         tenant.RepoKey
	ULID         string
	PackID       string
	SizeBytes    int64
	OriginRegion lease.Region
	Status       PackStatus
	CreatedAt    time.Time
}

// Validate verifies required fields on a PackManifest.
func (m PackManifest) Validate() error {
	if err := m.Repo.Validate(); err != nil {
		return err
	}
	if err := validateNonEmpty("ulid", m.ULID); err != nil {
		return err
	}
	if err := validateNonEmpty("pack_id", m.PackID); err != nil {
		return err
	}
	if m.SizeBytes < 0 {
		return fmt.Errorf("%w: negative size_bytes %d", ErrInvalidInput, m.SizeBytes)
	}
	if strings.TrimSpace(string(m.OriginRegion)) == "" {
		return lease.ErrEmptyRegion
	}
	switch m.Status {
	case PackStatusActive, PackStatusCompacted:
	default:
		return fmt.Errorf("%w: invalid pack status %q", ErrInvalidInput, m.Status)
	}
	return nil
}

// PutManifest writes a single PackManifest entry to its deterministic MS#{m} shard.
func (r *Repository) PutManifest(ctx context.Context, m PackManifest, manifestShards int) error {
	if err := m.Validate(); err != nil {
		return err
	}
	key, err := ManifestKey(m.Repo, m.ULID, m.PackID, manifestShards)
	if err != nil {
		return err
	}
	if m.CreatedAt.IsZero() {
		m.CreatedAt = r.clock.Now().UTC()
	}
	_, err = r.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(r.tableName),
		Item:      marshalPackManifest(key, m),
	})
	return err
}

// QueryManifestShard queries MS#{shard} with optional incremental watermark (SK > afterULID).
func (r *Repository) QueryManifestShard(ctx context.Context, repo tenant.RepoKey, shard int, afterULID string, activeOnly bool) ([]PackManifest, error) {
	pk, err := ManifestShardPK(repo, shard)
	if err != nil {
		return nil, err
	}

	keyCond := "PK = :pk AND begins_with(SK, :prefix)"
	values := map[string]types.AttributeValue{
		":pk":     attrS(pk),
		":prefix": attrS("PACK#"),
	}
	if afterULID != "" {
		// '~' (ASCII 126) sorts strictly after '#' (ASCII 35), skipping any items
		// with ULID == afterULID while matching all ULIDs > afterULID.
		keyCond = "PK = :pk AND SK > :watermark"
		values = map[string]types.AttributeValue{
			":pk":        attrS(pk),
			":watermark": attrS("PACK#" + afterULID + "~"),
		}
	}

	var filterExpr *string
	var exprNames map[string]string
	if activeOnly {
		filterExpr = aws.String("#status = :active")
		exprNames = map[string]string{"#status": "status"}
		values[":active"] = attrS(string(PackStatusActive))
	}

	var manifests []PackManifest
	var startKey map[string]types.AttributeValue
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(r.tableName),
			KeyConditionExpression:    aws.String(keyCond),
			FilterExpression:          filterExpr,
			ExpressionAttributeNames:  exprNames,
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			pm, err := unmarshalPackManifest(repo, item)
			if err != nil {
				return nil, err
			}
			manifests = append(manifests, pm)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return manifests, nil
}

// ListManifestsSince queries all manifest shards [0, manifestShards) for entries
// strictly newer than afterULID and sorts them by (ULID, PackID).
func (r *Repository) ListManifestsSince(ctx context.Context, repo tenant.RepoKey, manifestShards int, afterULID string, activeOnly bool) ([]PackManifest, error) {
	if err := repo.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateManifestShards(manifestShards); err != nil {
		return nil, err
	}

	var all []PackManifest
	for shard := 0; shard < manifestShards; shard++ {
		items, err := r.QueryManifestShard(ctx, repo, shard, afterULID, activeOnly)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].ULID != all[j].ULID {
			return all[i].ULID < all[j].ULID
		}
		return all[i].PackID < all[j].PackID
	})
	return all, nil
}

// PublishCompactedPacks executes Phase 1 of two-phase compaction: atomically
// inserting new ACTIVE compacted pack entries into MS#{m}.
func (r *Repository) PublishCompactedPacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []PackManifest) error {
	if err := repo.Validate(); err != nil {
		return err
	}
	if err := ValidateManifestShards(manifestShards); err != nil {
		return err
	}
	if len(packs) == 0 {
		return fmt.Errorf("%w: no compacted packs to publish", ErrInvalidInput)
	}
	if len(packs) > 100 {
		return fmt.Errorf("%w: publish batch %d exceeds DynamoDB 100-item limit", ErrInvalidInput, len(packs))
	}

	now := r.clock.Now().UTC()
	txItems := make([]types.TransactWriteItem, 0, len(packs))
	for _, p := range packs {
		p.Repo = repo
		p.Status = PackStatusActive
		if p.CreatedAt.IsZero() {
			p.CreatedAt = now
		}
		if err := p.Validate(); err != nil {
			return err
		}
		key, err := ManifestKey(repo, p.ULID, p.PackID, manifestShards)
		if err != nil {
			return err
		}
		txItems = append(txItems, types.TransactWriteItem{
			Put: &types.Put{
				TableName:           aws.String(r.tableName),
				Item:                marshalPackManifest(key, p),
				ConditionExpression: aws.String("attribute_not_exists(SK)"),
			},
		})
	}

	_, err := r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: txItems,
	})
	if err != nil {
		return classifyTransactionError(err, nil)
	}
	return nil
}

// SupersedePacks executes Phase 2 of two-phase compaction: atomically marking
// superseded ACTIVE pack entries as COMPACTED.
func (r *Repository) SupersedePacks(ctx context.Context, repo tenant.RepoKey, manifestShards int, packs []PackManifest) error {
	if err := repo.Validate(); err != nil {
		return err
	}
	if err := ValidateManifestShards(manifestShards); err != nil {
		return err
	}
	if len(packs) == 0 {
		return fmt.Errorf("%w: no packs to supersede", ErrInvalidInput)
	}
	if len(packs) > 100 {
		return fmt.Errorf("%w: supersede batch %d exceeds DynamoDB 100-item limit", ErrInvalidInput, len(packs))
	}

	txItems := make([]types.TransactWriteItem, 0, len(packs))
	for _, p := range packs {
		key, err := ManifestKey(repo, p.ULID, p.PackID, manifestShards)
		if err != nil {
			return err
		}
		txItems = append(txItems, types.TransactWriteItem{
			Update: &types.Update{
				TableName:           aws.String(r.tableName),
				Key:                 key.AttributeMap(),
				UpdateExpression:    aws.String("SET #status = :compacted"),
				ConditionExpression: aws.String("#status = :active"),
				ExpressionAttributeNames: map[string]string{
					"#status": "status",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":compacted": attrS(string(PackStatusCompacted)),
					":active":    attrS(string(PackStatusActive)),
				},
			},
		})
	}

	_, err := r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: txItems,
	})
	if err != nil {
		return classifyTransactionError(err, nil)
	}
	return nil
}

func marshalPackManifest(key Key, m PackManifest) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		AttrPK:          attrS(key.PK),
		AttrSK:          attrS(key.SK),
		"ulid":          attrS(m.ULID),
		"pack_id":       attrS(m.PackID),
		"size_bytes":    attrNInt64(m.SizeBytes),
		"origin_region": attrS(string(m.OriginRegion)),
		"status":        attrS(string(m.Status)),
		"created_at":    attrS(m.CreatedAt.UTC().Format(time.RFC3339Nano)),
	}
}

func unmarshalPackManifest(repo tenant.RepoKey, item map[string]types.AttributeValue) (PackManifest, error) {
	sizeBytes, err := getInt64(item, "size_bytes")
	if err != nil {
		return PackManifest{}, err
	}
	createdAt, err := getTime(item, "created_at")
	if err != nil {
		return PackManifest{}, err
	}
	ulid := getString(item, "ulid")
	packID := getString(item, "pack_id")
	if ulid == "" || packID == "" {
		skBody := strings.TrimPrefix(getString(item, AttrSK), "PACK#")
		if parts := strings.SplitN(skBody, "#", 2); len(parts) == 2 {
			if ulid == "" {
				ulid = parts[0]
			}
			if packID == "" {
				packID = parts[1]
			}
		}
	}
	return PackManifest{
		Repo:         repo,
		ULID:         ulid,
		PackID:       packID,
		SizeBytes:    sizeBytes,
		OriginRegion: lease.Region(getString(item, "origin_region")),
		Status:       PackStatus(getString(item, "status")),
		CreatedAt:    createdAt,
	}, nil
}
