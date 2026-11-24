package ddb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/lease"
	"stravin.dev/core/internal/tenant"
)

// RefState represents a single reference entry in RS#{s}.
type RefState struct {
	Repo         tenant.RepoKey
	RefName      string
	OID          string
	PrevOID      string
	PackID       string
	UpdatedAt    time.Time
	Epoch        lease.Epoch
	OriginRegion lease.Region
}

// RefHistory represents an immutable push outbox record in RH#{s}.
type RefHistory struct {
	Repo         tenant.RepoKey
	RefName      string
	ULID         string
	OID          string
	PrevOID      string
	PackID       string
	TTL          int64
	TraceParent  string
	UpdatedAt    time.Time
	Epoch        lease.Epoch
	OriginRegion lease.Region
}

// RefUpdate describes a single reference compare-and-swap transition.
type RefUpdate struct {
	RefName string
	OldOID  string
	NewOID  string
}

// IsCreate reports whether this update creates a brand-new reference.
func (u RefUpdate) IsCreate() bool {
	return u.OldOID == "" || IsZeroOID(u.OldOID)
}

// IsDelete reports whether this update deletes an existing reference.
func (u RefUpdate) IsDelete() bool {
	return IsZeroOID(u.NewOID)
}

// PackUpload describes the S3-persisted packfile recorded alongside a push.
type PackUpload struct {
	PackID       string
	SizeBytes    int64
	OriginRegion lease.Region
}

// CommitPushInput specifies an atomic Ref-CAS + Ref History + optional Manifest transaction.
type CommitPushInput struct {
	Repo           tenant.RepoKey
	RefShards      int
	ManifestShards int
	Epoch          lease.Epoch
	OriginRegion   lease.Region
	TraceParent    string
	ObjectFormat   ObjectFormat
	Updates        []RefUpdate
	Pack           *PackUpload
}

// CommitPushResult returns metadata generated during an atomic push commit.
type CommitPushResult struct {
	ULID        string
	CommittedAt time.Time
	RefCount    int
}

// BuildPushTransaction builds the DynamoDB TransactWriteItems payload for a push,
// including an atomic META ref_version (and manifest_version when a pack is
// uploaded) increment. It returns the transaction items and a map from
// transaction item index to ref name for CancellationReasons attribution.
func BuildPushTransaction(in CommitPushInput, ulid string, now time.Time, tableName string) ([]types.TransactWriteItem, map[int]string, error) {
	if err := validateCommitPushInput(in, ulid, tableName); err != nil {
		return nil, nil, err
	}

	nowUTC := now.UTC()
	nowStr := nowUTC.Format(time.RFC3339Nano)
	ttlEpoch := nowUTC.Add(RefHistoryTTL).Unix()
	packID := ""
	if in.Pack != nil {
		packID = in.Pack.PackID
	}

	items := make([]types.TransactWriteItem, 0, len(in.Updates)*2+2)
	refIndex := make(map[int]string, len(in.Updates))

	for _, u := range in.Updates {
		rsKey, err := RefStateKey(in.Repo, u.RefName, in.RefShards)
		if err != nil {
			return nil, nil, err
		}
		rhKey, err := RefHistoryKey(in.Repo, u.RefName, ulid, in.RefShards)
		if err != nil {
			return nil, nil, err
		}

		zeroOID := ZeroOIDForFormat(in.ObjectFormat)
		if in.ObjectFormat == "" && len(u.NewOID) == 64 {
			zeroOID = ZeroOIDSHA256
		}
		oldOID := u.OldOID
		if oldOID == "" {
			oldOID = zeroOID
		}

		refIndex[len(items)] = u.RefName
		switch {
		case u.IsDelete():
			items = append(items, types.TransactWriteItem{
				Delete: &types.Delete{
					TableName:           aws.String(tableName),
					Key:                 rsKey.AttributeMap(),
					ConditionExpression: aws.String("oid = :old_oid"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":old_oid": attrS(oldOID),
					},
				},
			})
		case u.IsCreate():
			rsItem := map[string]types.AttributeValue{
				AttrPK:          attrS(rsKey.PK),
				AttrSK:          attrS(rsKey.SK),
				"ref_name":      attrS(u.RefName),
				"oid":           attrS(u.NewOID),
				"prev_oid":      attrS(zeroOID),
				"pack_id":       attrS(packID),
				"updated_at":    attrS(nowStr),
				"epoch":         attrNUint64(uint64(in.Epoch)),
				"origin_region": attrS(string(in.OriginRegion)),
			}
			items = append(items, types.TransactWriteItem{
				Put: &types.Put{
					TableName:           aws.String(tableName),
					Item:                rsItem,
					ConditionExpression: aws.String("attribute_not_exists(oid)"),
				},
			})
		default:
			items = append(items, types.TransactWriteItem{
				Update: &types.Update{
					TableName:           aws.String(tableName),
					Key:                 rsKey.AttributeMap(),
					UpdateExpression:    aws.String("SET oid = :new_oid, prev_oid = :old_oid, pack_id = :pack_id, updated_at = :updated_at, epoch = :epoch, origin_region = :origin_region, ref_name = :ref_name"),
					ConditionExpression: aws.String("oid = :old_oid"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":new_oid":       attrS(u.NewOID),
						":old_oid":       attrS(oldOID),
						":pack_id":       attrS(packID),
						":updated_at":    attrS(nowStr),
						":epoch":         attrNUint64(uint64(in.Epoch)),
						":origin_region": attrS(string(in.OriginRegion)),
						":ref_name":      attrS(u.RefName),
					},
				},
			})
		}

		rhItem := map[string]types.AttributeValue{
			AttrPK:          attrS(rhKey.PK),
			AttrSK:          attrS(rhKey.SK),
			"tenant_id":     attrS(string(in.Repo.TenantID)),
			"repo_id":       attrS(string(in.Repo.RepoID)),
			"ref_name":      attrS(u.RefName),
			"ulid":          attrS(ulid),
			"oid":           attrS(u.NewOID),
			"prev_oid":      attrS(oldOID),
			"pack_id":       attrS(packID),
			"ttl":           attrNInt64(ttlEpoch),
			"updated_at":    attrS(nowStr),
			"epoch":         attrNUint64(uint64(in.Epoch)),
			"origin_region": attrS(string(in.OriginRegion)),
		}
		if in.TraceParent != "" {
			rhItem["traceparent"] = attrS(in.TraceParent)
		}
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(tableName),
				Item:      rhItem,
			},
		})
	}

	if in.Pack != nil {
		origin := in.Pack.OriginRegion
		if origin == "" {
			origin = in.OriginRegion
		}
		msKey, err := ManifestKey(in.Repo, ulid, in.Pack.PackID, in.ManifestShards)
		if err != nil {
			return nil, nil, err
		}
		msItem := marshalPackManifest(msKey, PackManifest{
			Repo:         in.Repo,
			ULID:         ulid,
			PackID:       in.Pack.PackID,
			SizeBytes:    in.Pack.SizeBytes,
			OriginRegion: origin,
			Status:       PackStatusActive,
			CreatedAt:    nowUTC,
		})
		items = append(items, types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String(tableName),
				Item:      msItem,
			},
		})
	}

	metaKey, err := MetaKey(in.Repo)
	if err != nil {
		return nil, nil, err
	}
	metaUpdateExpr := "ADD ref_version :one"
	if in.Pack != nil {
		metaUpdateExpr = "ADD ref_version :one, manifest_version :one"
	}
	items = append(items, types.TransactWriteItem{
		Update: &types.Update{
			TableName:        aws.String(tableName),
			Key:              metaKey.AttributeMap(),
			UpdateExpression: aws.String(metaUpdateExpr),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":one": attrNUint64(1),
			},
		},
	})

	return items, refIndex, nil
}

func validateCommitPushInput(in CommitPushInput, ulid, tableName string) error {
	if err := in.Repo.Validate(); err != nil {
		return err
	}
	if err := validateNonEmpty("table_name", tableName); err != nil {
		return err
	}
	if err := validateNonEmpty("ulid", ulid); err != nil {
		return err
	}
	if err := ValidateRefShards(in.RefShards); err != nil {
		return err
	}
	if in.Epoch == 0 {
		return lease.ErrInvalidEpoch
	}
	if strings.TrimSpace(string(in.OriginRegion)) == "" {
		return lease.ErrEmptyRegion
	}
	if len(in.Updates) == 0 {
		return fmt.Errorf("%w: at least one ref update is required", ErrInvalidInput)
	}
	if len(in.Updates) > MaxAtomicPushRefs {
		return fmt.Errorf("%w: got %d refs (max %d)", ErrTooManyRefs, len(in.Updates), MaxAtomicPushRefs)
	}

	if err := ValidateObjectFormat(in.ObjectFormat); err != nil {
		return err
	}

	seen := make(map[string]struct{}, len(in.Updates))
	for _, u := range in.Updates {
		if err := validateNonEmpty("ref_name", u.RefName); err != nil {
			return err
		}
		if _, dup := seen[u.RefName]; dup {
			return fmt.Errorf("%w: duplicate ref %q", ErrInvalidInput, u.RefName)
		}
		seen[u.RefName] = struct{}{}

		validate := ValidateOID
		if in.ObjectFormat != "" {
			validate = func(oid string) error {
				return ValidateOIDForFormat(oid, in.ObjectFormat)
			}
		}

		if u.OldOID != "" {
			if err := validate(u.OldOID); err != nil {
				return fmt.Errorf("old_oid for %s: %w", u.RefName, err)
			}
		}
		if err := validate(u.NewOID); err != nil {
			return fmt.Errorf("new_oid for %s: %w", u.RefName, err)
		}
		if u.OldOID != "" && len(u.OldOID) != len(u.NewOID) {
			return fmt.Errorf("%w: mismatched old_oid/new_oid lengths (%d vs %d) for %s", ErrInvalidOID, len(u.OldOID), len(u.NewOID), u.RefName)
		}
		if u.IsCreate() && u.IsDelete() {
			return fmt.Errorf("%w: ref %q has both zero old_oid and zero new_oid", ErrInvalidInput, u.RefName)
		}
	}

	if in.Pack != nil {
		if err := ValidateManifestShards(in.ManifestShards); err != nil {
			return err
		}
		if err := validateNonEmpty("pack_id", in.Pack.PackID); err != nil {
			return err
		}
		if in.Pack.SizeBytes < 0 {
			return fmt.Errorf("%w: negative pack size %d", ErrInvalidInput, in.Pack.SizeBytes)
		}
	}
	return nil
}

// CommitPush executes an atomic DynamoDB TransactWriteItems push commit.
func (r *Repository) CommitPush(ctx context.Context, in CommitPushInput) (CommitPushResult, error) {
	now := r.clock.Now().UTC()
	ulid, err := r.ulidGen(now)
	if err != nil {
		return CommitPushResult{}, err
	}
	items, refIndex, err := BuildPushTransaction(in, ulid, now, r.tableName)
	if err != nil {
		return CommitPushResult{}, err
	}

	_, err = r.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		return CommitPushResult{}, classifyTransactionError(err, refIndex)
	}
	return CommitPushResult{
		ULID:        ulid,
		CommittedAt: now,
		RefCount:    len(in.Updates),
	}, nil
}

// GetRef fetches the current RefState for refName.
func (r *Repository) GetRef(ctx context.Context, repo tenant.RepoKey, refName string, refShards int) (RefState, error) {
	key, err := RefStateKey(repo, refName, refShards)
	if err != nil {
		return RefState{}, err
	}
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.tableName),
		Key:            key.AttributeMap(),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return RefState{}, err
	}
	if len(out.Item) == 0 {
		return RefState{}, ErrNotFound
	}
	return unmarshalRefState(repo, out.Item)
}

const maxConcurrentRefShardQueries = 16

// ListRefs queries all ref shards [0, refShards) and returns refs sorted by RefName.
// When refShards > 1, shards are queried concurrently with bounded parallelism.
func (r *Repository) ListRefs(ctx context.Context, repo tenant.RepoKey, refShards int) ([]RefState, error) {
	if err := repo.Validate(); err != nil {
		return nil, err
	}
	if err := ValidateRefShards(refShards); err != nil {
		return nil, err
	}

	var refs []RefState
	if refShards == 1 {
		shardRefs, err := r.queryRefShard(ctx, repo, 0)
		if err != nil {
			return nil, err
		}
		refs = shardRefs
	} else {
		qCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		semSize := refShards
		if semSize > maxConcurrentRefShardQueries {
			semSize = maxConcurrentRefShardQueries
		}
		sem := make(chan struct{}, semSize)
		perShard := make([][]RefState, refShards)
		var (
			wg       sync.WaitGroup
			errMu    sync.Mutex
			firstErr error
		)
		recordErr := func(err error) {
			errMu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			errMu.Unlock()
			cancel()
		}

		for shard := 0; shard < refShards; shard++ {
			shard := shard
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-qCtx.Done():
					recordErr(qCtx.Err())
					return
				}
				shardRefs, err := r.queryRefShard(qCtx, repo, shard)
				if err != nil {
					recordErr(err)
					return
				}
				perShard[shard] = shardRefs
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return nil, firstErr
		}

		total := 0
		for _, s := range perShard {
			total += len(s)
		}
		refs = make([]RefState, 0, total)
		for _, s := range perShard {
			refs = append(refs, s...)
		}
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].RefName < refs[j].RefName
	})
	return refs, nil
}

func (r *Repository) queryRefShard(ctx context.Context, repo tenant.RepoKey, shard int) ([]RefState, error) {
	pk, err := RefStateShardPK(repo, shard)
	if err != nil {
		return nil, err
	}
	var (
		refs     []RefState
		startKey map[string]types.AttributeValue
	)
	for {
		out, err := r.client.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(r.tableName),
			KeyConditionExpression: aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk":     attrS(pk),
				":prefix": attrS("REF#"),
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			rs, err := unmarshalRefState(repo, item)
			if err != nil {
				return nil, err
			}
			refs = append(refs, rs)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return refs, nil
}

// ListRefHistory queries RH#{s} for a specific ref, optionally strictly after afterULID.
func (r *Repository) ListRefHistory(ctx context.Context, repo tenant.RepoKey, refName string, refShards int, afterULID string, limit int32) ([]RefHistory, error) {
	shard, err := RefShard(refName, refShards)
	if err != nil {
		return nil, err
	}
	pk, err := RefHistoryShardPK(repo, shard)
	if err != nil {
		return nil, err
	}

	prefix := "REF#" + refName + "#"
	keyCond := "PK = :pk AND begins_with(SK, :prefix)"
	values := map[string]types.AttributeValue{
		":pk":     attrS(pk),
		":prefix": attrS(prefix),
	}
	if afterULID != "" {
		// Bounded range for this ref strictly after afterULID ('~' > all Crockford Base32 chars).
		keyCond = "PK = :pk AND SK BETWEEN :lo AND :hi"
		values = map[string]types.AttributeValue{
			":pk": attrS(pk),
			":lo": attrS(prefix + afterULID + "~"),
			":hi": attrS(prefix + "~"),
		}
	}

	var history []RefHistory
	var startKey map[string]types.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:                 aws.String(r.tableName),
			KeyConditionExpression:    aws.String(keyCond),
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         startKey,
		}
		if limit > 0 {
			remaining := limit - int32(len(history))
			if remaining <= 0 {
				break
			}
			input.Limit = aws.Int32(remaining)
		}
		out, err := r.client.Query(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			h, err := unmarshalRefHistory(repo, item)
			if err != nil {
				return nil, err
			}
			history = append(history, h)
		}
		if len(out.LastEvaluatedKey) == 0 || (limit > 0 && int32(len(history)) >= limit) {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	return history, nil
}

func unmarshalRefState(repo tenant.RepoKey, item map[string]types.AttributeValue) (RefState, error) {
	updatedAt, err := getTime(item, "updated_at")
	if err != nil {
		return RefState{}, err
	}
	epoch, err := getUint64(item, "epoch")
	if err != nil {
		return RefState{}, err
	}
	refName := getString(item, "ref_name")
	if refName == "" {
		refName = strings.TrimPrefix(getString(item, AttrSK), "REF#")
	}
	return RefState{
		Repo:         repo,
		RefName:      refName,
		OID:          getString(item, "oid"),
		PrevOID:      getString(item, "prev_oid"),
		PackID:       getString(item, "pack_id"),
		UpdatedAt:    updatedAt,
		Epoch:        lease.Epoch(epoch),
		OriginRegion: lease.Region(getString(item, "origin_region")),
	}, nil
}

func unmarshalRefHistory(repo tenant.RepoKey, item map[string]types.AttributeValue) (RefHistory, error) {
	updatedAt, err := getTime(item, "updated_at")
	if err != nil {
		return RefHistory{}, err
	}
	ttl, err := getInt64(item, "ttl")
	if err != nil {
		return RefHistory{}, err
	}
	epoch, err := getUint64(item, "epoch")
	if err != nil {
		return RefHistory{}, err
	}
	refName := getString(item, "ref_name")
	ulid := getString(item, "ulid")
	if refName == "" || ulid == "" {
		skBody := strings.TrimPrefix(getString(item, AttrSK), "REF#")
		if idx := strings.LastIndex(skBody, "#"); idx > 0 {
			if refName == "" {
				refName = skBody[:idx]
			}
			if ulid == "" {
				ulid = skBody[idx+1:]
			}
		}
	}
	return RefHistory{
		Repo:         repo,
		RefName:      refName,
		ULID:         ulid,
		OID:          getString(item, "oid"),
		PrevOID:      getString(item, "prev_oid"),
		PackID:       getString(item, "pack_id"),
		TTL:          ttl,
		TraceParent:  getString(item, "traceparent"),
		UpdatedAt:    updatedAt,
		Epoch:        lease.Epoch(epoch),
		OriginRegion: lease.Region(getString(item, "origin_region")),
	}, nil
}
