package async

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/storage/ddb"
)

const (
	defaultPollInterval = 500 * time.Millisecond
	defaultBatchLimit   = int32(100)
)

// TableDescriber is the narrow DynamoDB interface needed to discover a table's active stream ARN.
type TableDescriber interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
}

// StreamsAPI is the narrow DynamoDB Streams client interface used by Consumer.
type StreamsAPI interface {
	DescribeStream(ctx context.Context, params *dynamodbstreams.DescribeStreamInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error)
	GetShardIterator(ctx context.Context, params *dynamodbstreams.GetShardIteratorInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, params *dynamodbstreams.GetRecordsInput, optFns ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error)
}

var (
	_ TableDescriber = (*dynamodb.Client)(nil)
	_ StreamsAPI     = (*dynamodbstreams.Client)(nil)
)

// EventObserver receives decoded Ref History outbox events (GoF Observer pattern).
type EventObserver interface {
	OnEvent(ctx context.Context, event RefHistoryEvent) error
}

// ConsumerConfig configures a DynamoDB Streams outbox Consumer.
type ConsumerConfig struct {
	TableName    string
	ConsumerID   string
	PollInterval time.Duration
	BatchLimit   int32
	Tables       TableDescriber
	Streams      StreamsAPI
	Checkpoints  ddb.CheckpointRepository
	Observers    []EventObserver
	Logger       *slog.Logger
}

// Consumer tails a DynamoDB table's stream shards, persists sequence-number checkpoints,
// and notifies registered EventObservers for each Ref History INSERT event.
type Consumer struct {
	tableName    string
	consumerID   string
	pollInterval time.Duration
	batchLimit   int32
	tables       TableDescriber
	streams      StreamsAPI
	checkpoints  ddb.CheckpointRepository
	observers    []EventObserver
	logger       *slog.Logger

	mu           sync.Mutex
	streamARN    string
	iterators    map[string]string
	closedShards map[string]bool
}

// NewConsumer constructs a DynamoDB Streams Ref History outbox Consumer.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if strings.TrimSpace(cfg.TableName) == "" {
		return nil, errors.New("async: empty table name")
	}
	if cfg.Tables == nil {
		return nil, errors.New("async: nil dynamodb table describer")
	}
	if cfg.Streams == nil {
		return nil, errors.New("async: nil dynamodb streams client")
	}
	if cfg.Checkpoints == nil {
		return nil, errors.New("async: nil checkpoint repository")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.BatchLimit <= 0 {
		cfg.BatchLimit = defaultBatchLimit
	}
	if cfg.ConsumerID == "" {
		cfg.ConsumerID = "stravin-hook"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	observers := make([]EventObserver, 0, len(cfg.Observers))
	for _, obs := range cfg.Observers {
		if obs != nil {
			observers = append(observers, obs)
		}
	}

	return &Consumer{
		tableName:    cfg.TableName,
		consumerID:   cfg.ConsumerID,
		pollInterval: cfg.PollInterval,
		batchLimit:   cfg.BatchLimit,
		tables:       cfg.Tables,
		streams:      cfg.Streams,
		checkpoints:  cfg.Checkpoints,
		observers:    observers,
		logger:       cfg.Logger,
		iterators:    make(map[string]string),
		closedShards: make(map[string]bool),
	}, nil
}

// Run polls DynamoDB Streams on PollInterval until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.PollOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		c.logger.WarnContext(ctx, "initial stream poll failed", "error", err)
	}

	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.PollOnce(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				c.logger.WarnContext(ctx, "stream poll iteration failed", "error", err)
			}
		}
	}
}

// PollOnce discovers the stream ARN (if needed), lists shards, and drains one batch
// per open shard, checkpointing sequence numbers after successful dispatch.
func (c *Consumer) PollOnce(ctx context.Context) error {
	streamARN, err := c.discoverStreamARN(ctx)
	if err != nil {
		return err
	}

	shards, err := c.listShards(ctx, streamARN)
	if err != nil {
		return err
	}

	var errs []error
	for _, shard := range shards {
		shardID := aws.ToString(shard.ShardId)
		if shardID == "" {
			continue
		}
		if err := c.pollShard(ctx, streamARN, shardID); err != nil {
			errs = append(errs, fmt.Errorf("shard %s: %w", shardID, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Consumer) discoverStreamARN(ctx context.Context) (string, error) {
	c.mu.Lock()
	if c.streamARN != "" {
		arn := c.streamARN
		c.mu.Unlock()
		return arn, nil
	}
	c.mu.Unlock()

	desc, err := c.tables.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(c.tableName),
	})
	if err != nil {
		return "", fmt.Errorf("async: describe table %q: %w", c.tableName, err)
	}
	if desc.Table == nil || aws.ToString(desc.Table.LatestStreamArn) == "" {
		return "", fmt.Errorf("async: table %q has no active DynamoDB stream", c.tableName)
	}

	arn := aws.ToString(desc.Table.LatestStreamArn)
	c.mu.Lock()
	c.streamARN = arn
	c.mu.Unlock()
	return arn, nil
}

func (c *Consumer) listShards(ctx context.Context, streamARN string) ([]streamtypes.Shard, error) {
	var all []streamtypes.Shard
	var startShardID *string

	for {
		out, err := c.streams.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(streamARN),
			ExclusiveStartShardId: startShardID,
		})
		if err != nil {
			return nil, fmt.Errorf("async: describe stream: %w", err)
		}
		if out.StreamDescription == nil {
			break
		}
		all = append(all, out.StreamDescription.Shards...)
		if out.StreamDescription.LastEvaluatedShardId == nil || *out.StreamDescription.LastEvaluatedShardId == "" {
			break
		}
		startShardID = out.StreamDescription.LastEvaluatedShardId
	}
	return all, nil
}

func (c *Consumer) pollShard(ctx context.Context, streamARN, shardID string) error {
	c.mu.Lock()
	if c.closedShards[shardID] {
		c.mu.Unlock()
		return nil
	}
	iter := c.iterators[shardID]
	c.mu.Unlock()

	if iter == "" {
		resolved, err := c.resolveShardIterator(ctx, streamARN, shardID)
		if err != nil {
			return err
		}
		if resolved == "" {
			c.mu.Lock()
			c.closedShards[shardID] = true
			c.mu.Unlock()
			return nil
		}
		iter = resolved
	}

	out, err := c.streams.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(iter),
		Limit:         aws.Int32(c.batchLimit),
	})
	if err != nil {
		if isIteratorExpired(err) {
			c.mu.Lock()
			delete(c.iterators, shardID)
			c.mu.Unlock()
		}
		return fmt.Errorf("async: get records: %w", err)
	}

	var lastSeq string
	for _, rec := range out.Records {
		if rec.Dynamodb != nil && aws.ToString(rec.Dynamodb.SequenceNumber) != "" {
			lastSeq = aws.ToString(rec.Dynamodb.SequenceNumber)
		}
		ev, ok, parseErr := ParseRefHistoryRecord(rec)
		if parseErr != nil {
			c.logger.WarnContext(ctx, "skipping malformed Ref History stream record",
				"shard_id", shardID,
				"sequence_number", lastSeq,
				"error", parseErr,
			)
			continue
		}
		if !ok {
			continue
		}
		for _, obs := range c.observers {
			if err := obs.OnEvent(ctx, ev); err != nil {
				return fmt.Errorf("async: observer dispatch seq=%s: %w", ev.SequenceNumber, err)
			}
		}
	}

	if lastSeq != "" {
		if err := c.checkpoints.PutStreamCheckpoint(ctx, ddb.StreamCheckpoint{
			ShardID:        shardID,
			SequenceNumber: lastSeq,
			ConsumerID:     c.consumerID,
		}); err != nil {
			return fmt.Errorf("async: save checkpoint seq=%s: %w", lastSeq, err)
		}
	}

	c.mu.Lock()
	next := aws.ToString(out.NextShardIterator)
	if next == "" {
		delete(c.iterators, shardID)
		c.closedShards[shardID] = true
	} else {
		c.iterators[shardID] = next
	}
	c.mu.Unlock()
	return nil
}

func (c *Consumer) resolveShardIterator(ctx context.Context, streamARN, shardID string) (string, error) {
	cp, err := c.checkpoints.GetStreamCheckpoint(ctx, shardID)
	if err != nil && !errors.Is(err, ddb.ErrNotFound) {
		return "", fmt.Errorf("async: load checkpoint: %w", err)
	}

	if err == nil && cp.SequenceNumber != "" {
		out, iterErr := c.streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
			StreamArn:         aws.String(streamARN),
			ShardId:           aws.String(shardID),
			ShardIteratorType: streamtypes.ShardIteratorTypeAfterSequenceNumber,
			SequenceNumber:    aws.String(cp.SequenceNumber),
		})
		if iterErr == nil {
			return aws.ToString(out.ShardIterator), nil
		}
		if !isTrimmedDataAccess(iterErr) {
			return "", fmt.Errorf("async: get shard iterator after sequence %s: %w", cp.SequenceNumber, iterErr)
		}
		c.logger.WarnContext(ctx, "checkpoint sequence trimmed; falling back to TRIM_HORIZON",
			"shard_id", shardID,
			"sequence_number", cp.SequenceNumber,
		)
	}

	out, iterErr := c.streams.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(streamARN),
		ShardId:           aws.String(shardID),
		ShardIteratorType: streamtypes.ShardIteratorTypeTrimHorizon,
	})
	if iterErr != nil {
		return "", fmt.Errorf("async: get shard iterator TRIM_HORIZON: %w", iterErr)
	}
	return aws.ToString(out.ShardIterator), nil
}

func isTrimmedDataAccess(err error) bool {
	var trimmed *streamtypes.TrimmedDataAccessException
	return errors.As(err, &trimmed)
}

func isIteratorExpired(err error) bool {
	var expired *streamtypes.ExpiredIteratorException
	return errors.As(err, &expired)
}
