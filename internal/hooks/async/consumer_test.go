package async

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/storage/ddb"
)

type fakeTableDescriber struct {
	streamARN string
}

func (f *fakeTableDescriber) DescribeTable(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{
		Table: &ddbtypes.TableDescription{
			LatestStreamArn: aws.String(f.streamARN),
		},
	}, nil
}

type fakeStreamsClient struct {
	mu               sync.Mutex
	shards           []streamtypes.Shard
	iterRequests     []*dynamodbstreams.GetShardIteratorInput
	recordsByIter    map[string][]streamtypes.Record
	nextIterByIter   map[string]string
	trimmedOnAfterSq bool
}

func (f *fakeStreamsClient) DescribeStream(_ context.Context, _ *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamtypes.StreamDescription{
			Shards: f.shards,
		},
	}, nil
}

func (f *fakeStreamsClient) GetShardIterator(_ context.Context, in *dynamodbstreams.GetShardIteratorInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.iterRequests = append(f.iterRequests, in)
	if f.trimmedOnAfterSq && in.ShardIteratorType == streamtypes.ShardIteratorTypeAfterSequenceNumber {
		return nil, &streamtypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
	}
	iter := "iter-" + aws.ToString(in.ShardId) + "-" + string(in.ShardIteratorType)
	return &dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String(iter),
	}, nil
}

func (f *fakeStreamsClient) GetRecords(_ context.Context, in *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	iter := aws.ToString(in.ShardIterator)
	recs := f.recordsByIter[iter]
	delete(f.recordsByIter, iter)
	var next *string
	if n, ok := f.nextIterByIter[iter]; ok && n != "" {
		next = aws.String(n)
	}
	return &dynamodbstreams.GetRecordsOutput{
		Records:           recs,
		NextShardIterator: next,
	}, nil
}

type fakeCheckpointRepo struct {
	mu          sync.Mutex
	checkpoints map[string]ddb.StreamCheckpoint
}

func (f *fakeCheckpointRepo) GetStreamCheckpoint(_ context.Context, shardID string) (ddb.StreamCheckpoint, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp, ok := f.checkpoints[shardID]
	if !ok {
		return ddb.StreamCheckpoint{}, ddb.ErrNotFound
	}
	return cp, nil
}

func (f *fakeCheckpointRepo) PutStreamCheckpoint(_ context.Context, cp ddb.StreamCheckpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.checkpoints == nil {
		f.checkpoints = make(map[string]ddb.StreamCheckpoint)
	}
	f.checkpoints[cp.ShardID] = cp
	return nil
}

type recordingObserver struct {
	mu     sync.Mutex
	events []RefHistoryEvent
}

func (o *recordingObserver) OnEvent(_ context.Context, ev RefHistoryEvent) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, ev)
	return nil
}

func TestConsumer_PollCheckpointAndResume(t *testing.T) {
	shardID := "shardId-0001"
	rhRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("seq-100"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK":        &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#app#RH#1"},
				"SK":        &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01J1"},
				"tenant_id": &streamtypes.AttributeValueMemberS{Value: "acme"},
				"repo_id":   &streamtypes.AttributeValueMemberS{Value: "app"},
				"ref_name":  &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
				"oid":       &streamtypes.AttributeValueMemberS{Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				"pack_id":   &streamtypes.AttributeValueMemberS{Value: "pack-1"},
			},
		},
	}
	nonRHRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeInsert,
		Dynamodb: &streamtypes.StreamRecord{
			SequenceNumber: aws.String("seq-101"),
			NewImage: map[string]streamtypes.AttributeValue{
				"PK": &streamtypes.AttributeValueMemberS{Value: "T#acme#REPO#app#RS#1"},
				"SK": &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main"},
			},
		},
	}

	checkpoints := &fakeCheckpointRepo{}
	streams := &fakeStreamsClient{
		shards: []streamtypes.Shard{{ShardId: aws.String(shardID)}},
		recordsByIter: map[string][]streamtypes.Record{
			"iter-shardId-0001-TRIM_HORIZON": {rhRec, nonRHRec},
		},
		nextIterByIter: map[string]string{
			"iter-shardId-0001-TRIM_HORIZON": "iter-next",
		},
	}
	obs := &recordingObserver{}

	consumer, err := NewConsumer(ConsumerConfig{
		TableName:   "stravin-core",
		ConsumerID:  "hook-worker-1",
		Tables:      &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000000000000:table/stravin-core/stream/1"},
		Streams:     streams,
		Checkpoints: checkpoints,
		Observers:   []EventObserver{obs},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	// Only Ref History INSERT delivered to observer.
	if len(obs.events) != 1 || obs.events[0].RefName != "refs/heads/main" || obs.events[0].PackID != "pack-1" {
		t.Fatalf("unexpected observer events: %+v", obs.events)
	}
	// First run used TRIM_HORIZON.
	if len(streams.iterRequests) != 1 || streams.iterRequests[0].ShardIteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Fatalf("expected TRIM_HORIZON on first poll, got %+v", streams.iterRequests)
	}
	// Checkpoint persisted at seq-101 (last record in batch).
	cp, err := checkpoints.GetStreamCheckpoint(context.Background(), shardID)
	if err != nil {
		t.Fatalf("GetStreamCheckpoint: %v", err)
	}
	if cp.SequenceNumber != "seq-101" || cp.ConsumerID != "hook-worker-1" {
		t.Fatalf("unexpected checkpoint: %+v", cp)
	}

	// Simulate restart: new consumer with same checkpoint repo resumes with AFTER_SEQUENCE_NUMBER.
	streams2 := &fakeStreamsClient{
		shards: []streamtypes.Shard{{ShardId: aws.String(shardID)}},
		recordsByIter: map[string][]streamtypes.Record{
			"iter-shardId-0001-AFTER_SEQUENCE_NUMBER": {},
		},
		nextIterByIter: map[string]string{
			"iter-shardId-0001-AFTER_SEQUENCE_NUMBER": "iter-next-2",
		},
	}
	consumer2, err := NewConsumer(ConsumerConfig{
		TableName:   "stravin-core",
		ConsumerID:  "hook-worker-2",
		Tables:      &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000000000000:table/stravin-core/stream/1"},
		Streams:     streams2,
		Checkpoints: checkpoints,
		Observers:   []EventObserver{obs},
	})
	if err != nil {
		t.Fatalf("NewConsumer #2: %v", err)
	}
	if err := consumer2.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce #2: %v", err)
	}
	if len(streams2.iterRequests) != 1 ||
		streams2.iterRequests[0].ShardIteratorType != streamtypes.ShardIteratorTypeAfterSequenceNumber ||
		aws.ToString(streams2.iterRequests[0].SequenceNumber) != "seq-101" {
		t.Fatalf("expected AFTER_SEQUENCE_NUMBER seq-101 on resume, got %+v", streams2.iterRequests)
	}
}

func TestConsumer_TrimmedCheckpointFallsBackToTrimHorizon(t *testing.T) {
	shardID := "shardId-trimmed"
	checkpoints := &fakeCheckpointRepo{
		checkpoints: map[string]ddb.StreamCheckpoint{
			shardID: {ShardID: shardID, SequenceNumber: "expired-seq"},
		},
	}
	streams := &fakeStreamsClient{
		shards:           []streamtypes.Shard{{ShardId: aws.String(shardID)}},
		trimmedOnAfterSq: true,
		recordsByIter:    map[string][]streamtypes.Record{},
		nextIterByIter: map[string]string{
			"iter-shardId-trimmed-TRIM_HORIZON": "iter-after-fallback",
		},
	}

	consumer, err := NewConsumer(ConsumerConfig{
		TableName:   "stravin-core",
		Tables:      &fakeTableDescriber{streamARN: "arn:stream"},
		Streams:     streams,
		Checkpoints: checkpoints,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	if err := consumer.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if len(streams.iterRequests) != 2 ||
		streams.iterRequests[0].ShardIteratorType != streamtypes.ShardIteratorTypeAfterSequenceNumber ||
		streams.iterRequests[1].ShardIteratorType != streamtypes.ShardIteratorTypeTrimHorizon {
		t.Fatalf("expected fallback from AFTER_SEQUENCE_NUMBER to TRIM_HORIZON, got %+v", streams.iterRequests)
	}
}

func TestConsumer_RunStopsOnContextCancel(t *testing.T) {
	streams := &fakeStreamsClient{
		shards:         []streamtypes.Shard{},
		recordsByIter:  map[string][]streamtypes.Record{},
		nextIterByIter: map[string]string{},
	}
	consumer, err := NewConsumer(ConsumerConfig{
		TableName:    "stravin-core",
		PollInterval: 10 * time.Millisecond,
		Tables:       &fakeTableDescriber{streamARN: "arn:stream"},
		Streams:      streams,
		Checkpoints:  &fakeCheckpointRepo{},
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer cancel()
	if err := consumer.Run(ctx); err != nil {
		t.Fatalf("Run returned unexpected error on cancel: %v", err)
	}
}
