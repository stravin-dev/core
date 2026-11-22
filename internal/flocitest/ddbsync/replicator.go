package ddbsync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

const (
	// AttrRepOrigin stamps the source region on replicated items for bidirectional loop guarding.
	AttrRepOrigin = "_floci_rep_origin"
	// AttrRepTimestamp stamps unix nanoseconds on replicated items for MREC last-writer-wins (LWW).
	AttrRepTimestamp = "_floci_rep_ts"

	// DefaultPollInterval is the default stream polling interval.
	DefaultPollInterval = 200 * time.Millisecond
	// DefaultBatchLimit is the default GetRecords batch size.
	DefaultBatchLimit = int32(100)

	lwwConditionExpression = "attribute_not_exists(#ts) OR #ts < :ts"
)

// Config configures a directional DynamoDB Streams MREC Replicator.
type Config struct {
	SourceRegion         string
	SourceTable          string
	TargetRegion         string
	TargetTable          string
	PollInterval         time.Duration
	LagDelay             time.Duration
	IteratorType         streamtypes.ShardIteratorType
	DeleteSuppressionTTL time.Duration
	DeleteSuppressor     *DeleteSuppressor
	BatchLimit           int32

	Tables  TableDescriber
	Streams StreamReader
	Target  ItemWriter
	Logger  *slog.Logger

	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
}

// Replicator consumes a source DynamoDB table's stream and replicates INSERT, MODIFY,
// and REMOVE events to a target table with loop guarding, LWW conditional writes,
// bounded-TTL delete suppression, and optional lag injection.
type Replicator struct {
	sourceRegion string
	sourceTable  string
	targetRegion string
	targetTable  string
	pollInterval time.Duration
	lagDelay     time.Duration
	iteratorType streamtypes.ShardIteratorType
	batchLimit   int32

	tables     TableDescriber
	streams    StreamReader
	target     ItemWriter
	suppressor *DeleteSuppressor
	logger     *slog.Logger
	now        func() time.Time
	sleep      func(ctx context.Context, d time.Duration) error

	mu           sync.Mutex
	streamARN    string
	iterators    map[string]string
	closedShards map[string]bool
}

// NewReplicator validates cfg and constructs a Replicator.
func NewReplicator(cfg Config) (*Replicator, error) {
	if strings.TrimSpace(cfg.SourceRegion) == "" {
		return nil, errors.New("ddbsync: empty source region")
	}
	if strings.TrimSpace(cfg.SourceTable) == "" {
		return nil, errors.New("ddbsync: empty source table")
	}
	if strings.TrimSpace(cfg.TargetRegion) == "" {
		return nil, errors.New("ddbsync: empty target region")
	}
	if strings.TrimSpace(cfg.TargetTable) == "" {
		return nil, errors.New("ddbsync: empty target table")
	}
	if cfg.Tables == nil {
		return nil, errors.New("ddbsync: nil table describer")
	}
	if cfg.Streams == nil {
		return nil, errors.New("ddbsync: nil stream reader")
	}
	if cfg.Target == nil {
		return nil, errors.New("ddbsync: nil target item writer")
	}
	if cfg.LagDelay < 0 {
		return nil, errors.New("ddbsync: negative lag delay")
	}

	iterType := cfg.IteratorType
	if iterType == "" {
		iterType = streamtypes.ShardIteratorTypeTrimHorizon
	}
	if iterType != streamtypes.ShardIteratorTypeTrimHorizon && iterType != streamtypes.ShardIteratorTypeLatest {
		return nil, fmt.Errorf("ddbsync: unsupported iterator type %q (want TRIM_HORIZON or LATEST)", iterType)
	}

	pollInterval := cfg.PollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultPollInterval
	}
	batchLimit := cfg.BatchLimit
	if batchLimit <= 0 {
		batchLimit = DefaultBatchLimit
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}
	sleepFn := cfg.Sleep
	if sleepFn == nil {
		sleepFn = sleepContext
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	suppressor := cfg.DeleteSuppressor
	if suppressor == nil {
		suppressor = NewDeleteSuppressor(cfg.DeleteSuppressionTTL, nowFn)
	}

	return &Replicator{
		sourceRegion: cfg.SourceRegion,
		sourceTable:  cfg.SourceTable,
		targetRegion: cfg.TargetRegion,
		targetTable:  cfg.TargetTable,
		pollInterval: pollInterval,
		lagDelay:     cfg.LagDelay,
		iteratorType: iterType,
		batchLimit:   batchLimit,
		tables:       cfg.Tables,
		streams:      cfg.Streams,
		target:       cfg.Target,
		suppressor:   suppressor,
		logger:       logger,
		now:          nowFn,
		sleep:        sleepFn,
		iterators:    make(map[string]string),
		closedShards: make(map[string]bool),
	}, nil
}

// Run polls the source stream on PollInterval until ctx is cancelled.
func (r *Replicator) Run(ctx context.Context) error {
	if err := r.PollOnce(ctx); err != nil && !isContextDone(err) {
		r.logger.WarnContext(ctx, "initial ddb sync poll failed", "error", err)
	}

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.PollOnce(ctx); err != nil && !isContextDone(err) {
				r.logger.WarnContext(ctx, "ddb sync poll iteration failed", "error", err)
			}
		}
	}
}

// PollOnce discovers the stream ARN (if needed), enumerates shards, and drains one batch per open shard.
func (r *Replicator) PollOnce(ctx context.Context) error {
	streamARN, err := r.discoverStreamARN(ctx)
	if err != nil {
		return err
	}

	shards, err := r.listShards(ctx, streamARN)
	if err != nil {
		return err
	}

	var errs []error
	for _, shard := range shards {
		shardID := aws.ToString(shard.ShardId)
		if shardID == "" {
			continue
		}
		isChild := aws.ToString(shard.ParentShardId) != ""
		if err := r.pollShard(ctx, streamARN, shardID, isChild); err != nil {
			errs = append(errs, fmt.Errorf("shard %s: %w", shardID, err))
		}
	}
	return errors.Join(errs...)
}

func (r *Replicator) discoverStreamARN(ctx context.Context) (string, error) {
	r.mu.Lock()
	if r.streamARN != "" {
		arn := r.streamARN
		r.mu.Unlock()
		return arn, nil
	}
	r.mu.Unlock()

	desc, err := r.tables.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(r.sourceTable),
	})
	if err != nil {
		return "", fmt.Errorf("ddbsync: describe table %q: %w", r.sourceTable, err)
	}
	arn := ""
	if desc.Table != nil {
		arn = aws.ToString(desc.Table.LatestStreamArn)
	}
	if arn == "" {
		return "", fmt.Errorf("ddbsync: table %q has no active DynamoDB stream", r.sourceTable)
	}

	r.mu.Lock()
	r.streamARN = arn
	r.mu.Unlock()
	return arn, nil
}

func (r *Replicator) listShards(ctx context.Context, streamARN string) ([]streamtypes.Shard, error) {
	var all []streamtypes.Shard
	var startShardID *string

	for {
		out, err := r.streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: startShardID,
		})
		if err != nil {
			return nil, fmt.Errorf("ddbsync: describe stream: %w", err)
		}
		if out.StreamDescription == nil {
			break
		}
		all = append(all, out.StreamDescription.Shards...)
		if aws.ToString(out.StreamDescription.LastEvaluatedShardId) == "" {
			break
		}
		startShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return all, nil
}

func (r *Replicator) pollShard(ctx context.Context, streamARN, shardID string, isChild bool) error {
	r.mu.Lock()
	if r.closedShards[shardID] {
		r.mu.Unlock()
		return nil
	}
	iter := r.iterators[shardID]
	r.mu.Unlock()

	if iter == "" {
		iterType := r.iteratorType
		if isChild {
			iterType = streamtypes.ShardIteratorTypeTrimHorizon
		}
		resolved, closed, err := r.openShardIterator(ctx, streamARN, shardID, iterType)
		if err != nil {
			return err
		}
		if closed {
			r.markShardClosed(shardID)
			return nil
		}
		iter = resolved
	}

	out, err := r.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(iter),
		Limit:         aws.Int32(r.batchLimit),
	})
	if err != nil {
		if isTrimmedOrExpired(err) {
			r.logger.WarnContext(ctx, "shard iterator trimmed or expired; reopening at TRIM_HORIZON",
				"shard_id", shardID,
				"error", err,
			)
			r.clearIterator(shardID)
			recovered, closed, recErr := r.openShardIterator(ctx, streamARN, shardID, streamtypes.ShardIteratorTypeTrimHorizon)
			if recErr != nil {
				return recErr
			}
			if closed {
				r.markShardClosed(shardID)
				return nil
			}
			out, err = r.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
				ShardIterator: aws.String(recovered),
				Limit:         aws.Int32(r.batchLimit),
			})
			if err != nil {
				if isTrimmedOrExpired(err) {
					r.markShardClosed(shardID)
					return nil
				}
				return fmt.Errorf("ddbsync: get records after recovery: %w", err)
			}
		} else {
			return fmt.Errorf("ddbsync: get records: %w", err)
		}
	}

	for _, rec := range out.Records {
		if err := r.applyRecord(ctx, rec); err != nil {
			return err
		}
	}

	next := aws.ToString(out.NextShardIterator)
	if next == "" {
		r.markShardClosed(shardID)
	} else {
		r.mu.Lock()
		r.iterators[shardID] = next
		r.mu.Unlock()
	}
	return nil
}

func (r *Replicator) openShardIterator(
	ctx context.Context,
	streamARN, shardID string,
	iterType streamtypes.ShardIteratorType,
) (iterator string, closed bool, err error) {
	out, err := r.streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamARN),
		ShardId:           aws.String(shardID),
		ShardIteratorType: iterType,
	})
	if err != nil {
		if isTrimmedDataAccess(err) {
			if iterType != streamtypes.ShardIteratorTypeTrimHorizon {
				return r.openShardIterator(ctx, streamARN, shardID, streamtypes.ShardIteratorTypeTrimHorizon)
			}
			r.logger.WarnContext(ctx, "shard fully trimmed; marking shard closed", "shard_id", shardID)
			return "", true, nil
		}
		return "", false, fmt.Errorf("ddbsync: get shard iterator (%s): %w", iterType, err)
	}
	iter := aws.ToString(out.ShardIterator)
	if iter == "" {
		return "", true, nil
	}
	return iter, false, nil
}

func (r *Replicator) applyRecord(ctx context.Context, rec streamtypes.Record) error {
	if rec.Dynamodb == nil {
		return nil
	}

	switch rec.EventName {
	case streamtypes.OperationTypeInsert, streamtypes.OperationTypeModify:
		return r.applyPut(ctx, rec.Dynamodb.NewImage)
	case streamtypes.OperationTypeRemove:
		return r.applyRemove(ctx, rec.Dynamodb.Keys)
	default:
		return nil
	}
}

func (r *Replicator) applyPut(ctx context.Context, newImage map[string]streamtypes.AttributeValue) error {
	if len(newImage) == 0 {
		return nil
	}
	if origin, ok := newImage[AttrRepOrigin].(*streamtypes.AttributeValueMemberS); ok && origin.Value == r.targetRegion {
		r.logger.DebugContext(ctx, "skipping record originating from target region",
			"origin_region", origin.Value,
			"target_region", r.targetRegion,
		)
		return nil
	}

	if err := r.waitLag(ctx); err != nil {
		return err
	}

	item, err := ConvertAttributeMap(newImage)
	if err != nil {
		return fmt.Errorf("ddbsync: convert NewImage: %w", err)
	}

	ts := strconv.FormatInt(r.now().UnixNano(), 10)
	item[AttrRepOrigin] = &ddbtypes.AttributeValueMemberS{Value: r.sourceRegion}
	item[AttrRepTimestamp] = &ddbtypes.AttributeValueMemberN{Value: ts}

	_, err = r.target.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(r.targetTable),
		Item:                item,
		ConditionExpression: aws.String(lwwConditionExpression),
		ExpressionAttributeNames: map[string]string{
			"#ts": AttrRepTimestamp,
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":ts": &ddbtypes.AttributeValueMemberN{Value: ts},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			r.logger.DebugContext(ctx, "LWW condition skipped stale PutItem", "target_table", r.targetTable)
			return nil
		}
		return fmt.Errorf("ddbsync: target PutItem: %w", err)
	}
	return nil
}

func (r *Replicator) applyRemove(ctx context.Context, keys map[string]streamtypes.AttributeValue) error {
	if len(keys) == 0 {
		return nil
	}
	canonKey := CanonicalKey(keys)
	if r.suppressor.IsSuppressed(canonKey) {
		r.logger.DebugContext(ctx, "suppressing bounced REMOVE event", "key", canonKey)
		return nil
	}

	if err := r.waitLag(ctx); err != nil {
		return err
	}

	ddbKey, err := ConvertAttributeMap(keys)
	if err != nil {
		return fmt.Errorf("ddbsync: convert Keys: %w", err)
	}

	if _, err := r.target.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(r.targetTable),
		Key:       ddbKey,
	}); err != nil {
		return fmt.Errorf("ddbsync: target DeleteItem: %w", err)
	}
	r.suppressor.Record(canonKey)
	return nil
}

func (r *Replicator) waitLag(ctx context.Context) error {
	if r.lagDelay <= 0 {
		return nil
	}
	return r.sleep(ctx, r.lagDelay)
}

func (r *Replicator) clearIterator(shardID string) {
	r.mu.Lock()
	delete(r.iterators, shardID)
	r.mu.Unlock()
}

func (r *Replicator) markShardClosed(shardID string) {
	r.mu.Lock()
	delete(r.iterators, shardID)
	r.closedShards[shardID] = true
	r.mu.Unlock()
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isTrimmedDataAccess(err error) bool {
	var trimmed *streamtypes.TrimmedDataAccessException
	return errors.As(err, &trimmed)
}

func isTrimmedOrExpired(err error) bool {
	var expired *streamtypes.ExpiredIteratorException
	return isTrimmedDataAccess(err) || errors.As(err, &expired)
}

func isConditionalCheckFailed(err error) bool {
	var ccfe *ddbtypes.ConditionalCheckFailedException
	return errors.As(err, &ccfe)
}

func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
