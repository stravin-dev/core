package relay

import (
	"context"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"

	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
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

type fakeStreamsAPI struct {
	mu      sync.Mutex
	shards  []streamtypes.Shard
	batches [][]streamtypes.Record
	idx     int
}

func (f *fakeStreamsAPI) DescribeStream(_ context.Context, _ *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamtypes.StreamDescription{
			Shards: f.shards,
		},
	}, nil
}

func (f *fakeStreamsAPI) GetShardIterator(_ context.Context, _ *dynamodbstreams.GetShardIteratorInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	return &dynamodbstreams.GetShardIteratorOutput{
		ShardIterator: aws.String("iter-0"),
	}, nil
}

func (f *fakeStreamsAPI) GetRecords(_ context.Context, _ *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.batches) {
		return &dynamodbstreams.GetRecordsOutput{
			NextShardIterator: aws.String("iter-done"),
		}, nil
	}
	recs := f.batches[f.idx]
	f.idx++
	return &dynamodbstreams.GetRecordsOutput{
		Records:           recs,
		NextShardIterator: aws.String("iter-next"),
	}, nil
}

type memCheckpointRepo struct {
	mu          sync.Mutex
	checkpoints map[string]ddb.StreamCheckpoint
}

func newMemCheckpointRepo() *memCheckpointRepo {
	return &memCheckpointRepo{checkpoints: make(map[string]ddb.StreamCheckpoint)}
}

func (m *memCheckpointRepo) GetStreamCheckpoint(_ context.Context, shardID string) (ddb.StreamCheckpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.checkpoints[shardID]
	if !ok {
		return ddb.StreamCheckpoint{}, ddb.ErrNotFound
	}
	return cp, nil
}

func (m *memCheckpointRepo) PutStreamCheckpoint(_ context.Context, cp ddb.StreamCheckpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkpoints[cp.ShardID] = cp
	return nil
}

func TestConsumer_PollsAllOutboxEventKindsAndCheckpoints(t *testing.T) {
	natsURL, js := startEmbeddedJetStream(t)
	pub, err := NewJetStreamPublisher(JetStreamConfig{URL: natsURL})
	if err != nil {
		t.Fatalf("NewJetStreamPublisher: %v", err)
	}
	defer func() { _ = pub.Close() }()

	tel, err := telemetry.NewInMemoryProvider("stravin-relay-consumer-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	metrics, err := NewMetrics(tel)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	disp, err := NewRelayDispatcher(DispatcherConfig{
		Publisher: pub,
		Telemetry: tel,
		Metrics:   metrics,
	})
	if err != nil {
		t.Fatalf("NewRelayDispatcher: %v", err)
	}

	streams := &fakeStreamsAPI{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-0")}},
		batches: [][]streamtypes.Record{
			{
				{
					EventName: streamtypes.OperationTypeInsert,
					Dynamodb: &streamtypes.StreamRecord{
						SequenceNumber: aws.String("10"),
						NewImage: map[string]streamtypes.AttributeValue{
							"PK":            &streamtypes.AttributeValueMemberS{Value: "T#local#REPO#demo"},
							"SK":            &streamtypes.AttributeValueMemberS{Value: "META"},
							"home_region":   &streamtypes.AttributeValueMemberS{Value: "us-east-1"},
							"object_format": &streamtypes.AttributeValueMemberS{Value: "sha1"},
						},
					},
				},
				{
					EventName: streamtypes.OperationTypeInsert,
					Dynamodb: &streamtypes.StreamRecord{
						SequenceNumber: aws.String("11"),
						NewImage: map[string]streamtypes.AttributeValue{
							"PK":          &streamtypes.AttributeValueMemberS{Value: "T#local#REPO#demo#RH#0"},
							"SK":          &streamtypes.AttributeValueMemberS{Value: "REF#refs/heads/main#01JDF8Z7Q2K9M4N6P8R0T2V4W6"},
							"ref_name":    &streamtypes.AttributeValueMemberS{Value: "refs/heads/main"},
							"ulid":        &streamtypes.AttributeValueMemberS{Value: "01JDF8Z7Q2K9M4N6P8R0T2V4W6"},
							"oid":         &streamtypes.AttributeValueMemberS{Value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
							"prev_oid":    &streamtypes.AttributeValueMemberS{Value: "0000000000000000000000000000000000000000"},
							"traceparent": &streamtypes.AttributeValueMemberS{Value: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
						},
					},
				},
				{
					EventName: streamtypes.OperationTypeRemove,
					Dynamodb: &streamtypes.StreamRecord{
						SequenceNumber: aws.String("12"),
						Keys: map[string]streamtypes.AttributeValue{
							"PK": &streamtypes.AttributeValueMemberS{Value: "T#local#REPO#demo"},
							"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
						},
					},
				},
			},
		},
	}

	cpRepo := newMemCheckpointRepo()
	consumer, err := NewConsumer(ConsumerConfig{
		TableName:   "stravin-core",
		ConsumerID:  "stravin-relay",
		Tables:      &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000000000000:table/stravin-core/stream/1"},
		Streams:     streams,
		Checkpoints: cpRepo,
		Observers:   []EventObserver{disp},
		Telemetry:   tel,
		Metrics:     metrics,
	})
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}

	ctx := context.Background()
	if err := consumer.PollOnce(ctx); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	cp, err := cpRepo.GetStreamCheckpoint(ctx, "shard-0")
	if err != nil {
		t.Fatalf("GetStreamCheckpoint: %v", err)
	}
	if cp.SequenceNumber != "12" || cp.ConsumerID != "stravin-relay" {
		t.Fatalf("checkpoint = %+v, want seq=12 consumer=stravin-relay", cp)
	}

	stream, err := js.Stream(ctx, "STRAVIN_CORE")
	if err != nil {
		t.Fatalf("js.Stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("stream.Info: %v", err)
	}
	if info.State.Msgs != 3 {
		t.Fatalf("STRAVIN_CORE msgs = %d, want 3", info.State.Msgs)
	}
}
