package ddbsync

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
)

type fakeTableDescriber struct {
	mu        sync.Mutex
	streamARN string
	err       error
	calls     int
}

func (f *fakeTableDescriber) DescribeTable(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return &dynamodb.DescribeTableOutput{
		Table: &ddbtypes.TableDescription{
			LatestStreamArn: aws.String(f.streamARN),
		},
	}, nil
}

type fakeStreamReader struct {
	mu                 sync.Mutex
	shards             []streamtypes.Shard
	describeErr        error
	iterators          map[string]string
	iterTypesRequested []streamtypes.ShardIteratorType
	getIterHook        func(in *dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error)
	getRecordsHook     func(in *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error)
}

func (f *fakeStreamReader) DescribeStream(_ context.Context, _ *dynamodbstreams.DescribeStreamInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.DescribeStreamOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	shards := append([]streamtypes.Shard(nil), f.shards...)
	return &dynamodbstreams.DescribeStreamOutput{
		StreamDescription: &streamtypes.StreamDescription{
			Shards: shards,
		},
	}, nil
}

func (f *fakeStreamReader) GetShardIterator(_ context.Context, in *dynamodbstreams.GetShardIteratorInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetShardIteratorOutput, error) {
	f.mu.Lock()
	f.iterTypesRequested = append(f.iterTypesRequested, in.ShardIteratorType)
	hook := f.getIterHook
	iter := ""
	if f.iterators != nil {
		iter = f.iterators[aws.ToString(in.ShardId)]
	}
	f.mu.Unlock()

	if hook != nil {
		return hook(in)
	}
	if iter == "" {
		iter = "iter-" + aws.ToString(in.ShardId)
	}
	return &dynamodbstreams.GetShardIteratorOutput{ShardIterator: aws.String(iter)}, nil
}

func (f *fakeStreamReader) GetRecords(_ context.Context, in *dynamodbstreams.GetRecordsInput, _ ...func(*dynamodbstreams.Options)) (*dynamodbstreams.GetRecordsOutput, error) {
	f.mu.Lock()
	hook := f.getRecordsHook
	f.mu.Unlock()
	if hook != nil {
		return hook(in)
	}
	return &dynamodbstreams.GetRecordsOutput{NextShardIterator: in.ShardIterator}, nil
}

type fakeItemWriter struct {
	mu         sync.Mutex
	puts       []*dynamodb.PutItemInput
	deletes    []*dynamodb.DeleteItemInput
	putHook    func(in *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
	deleteHook func(in *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error)
}

func (f *fakeItemWriter) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.mu.Lock()
	f.puts = append(f.puts, in)
	hook := f.putHook
	f.mu.Unlock()
	if hook != nil {
		return hook(in)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeItemWriter) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.mu.Lock()
	f.deletes = append(f.deletes, in)
	hook := f.deleteHook
	f.mu.Unlock()
	if hook != nil {
		return hook(in)
	}
	return &dynamodb.DeleteItemOutput{}, nil
}

func TestNewReplicator_Validation(t *testing.T) {
	tables := &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000:table/a/stream/1"}
	streams := &fakeStreamReader{}
	writer := &fakeItemWriter{}

	base := Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
	}

	if _, err := NewReplicator(base); err != nil {
		t.Fatalf("valid config failed: %v", err)
	}

	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"missing source region", func(c *Config) { c.SourceRegion = "" }},
		{"missing source table", func(c *Config) { c.SourceTable = "" }},
		{"missing target region", func(c *Config) { c.TargetRegion = "" }},
		{"missing target table", func(c *Config) { c.TargetTable = "" }},
		{"nil tables", func(c *Config) { c.Tables = nil }},
		{"nil streams", func(c *Config) { c.Streams = nil }},
		{"nil target", func(c *Config) { c.Target = nil }},
		{"negative lag", func(c *Config) { c.LagDelay = -time.Millisecond }},
		{"invalid iterator type", func(c *Config) { c.IteratorType = streamtypes.ShardIteratorTypeAtSequenceNumber }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			if _, err := NewReplicator(cfg); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}

func TestReplicator_InsertModifyRemove(t *testing.T) {
	fixedNow := time.Unix(1_700_000_000, 123_456_789)
	tables := &fakeTableDescriber{streamARN: "arn:aws:dynamodb:us-east-1:000:table/a/stream/1"}
	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-1")}},
		getRecordsHook: func(_ *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-2"),
				Records: []streamtypes.Record{
					{
						EventName: streamtypes.OperationTypeInsert,
						Dynamodb: &streamtypes.StreamRecord{
							SequenceNumber: aws.String("1"),
							NewImage: map[string]streamtypes.AttributeValue{
								"PK":   &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK":   &streamtypes.AttributeValueMemberS{Value: "REF#main"},
								"oid":  &streamtypes.AttributeValueMemberS{Value: "aaa"},
								"ver":  &streamtypes.AttributeValueMemberN{Value: "1"},
								"flag": &streamtypes.AttributeValueMemberBOOL{Value: true},
								"meta": &streamtypes.AttributeValueMemberM{Value: map[string]streamtypes.AttributeValue{
									"k": &streamtypes.AttributeValueMemberS{Value: "v"},
								}},
								"list": &streamtypes.AttributeValueMemberL{Value: []streamtypes.AttributeValue{
									&streamtypes.AttributeValueMemberS{Value: "x"},
								}},
							},
						},
					},
					{
						EventName: streamtypes.OperationTypeModify,
						Dynamodb: &streamtypes.StreamRecord{
							SequenceNumber: aws.String("2"),
							NewImage: map[string]streamtypes.AttributeValue{
								"PK":  &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK":  &streamtypes.AttributeValueMemberS{Value: "REF#main"},
								"oid": &streamtypes.AttributeValueMemberS{Value: "bbb"},
							},
						},
					},
					{
						EventName: streamtypes.OperationTypeRemove,
						Dynamodb: &streamtypes.StreamRecord{
							SequenceNumber: aws.String("3"),
							Keys: map[string]streamtypes.AttributeValue{
								"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK": &streamtypes.AttributeValueMemberS{Value: "REF#old"},
							},
						},
					},
				},
			}, nil
		},
	}
	writer := &fakeItemWriter{}

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
		Now:          func() time.Time { return fixedNow },
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}

	if len(writer.puts) != 2 {
		t.Fatalf("expected 2 PutItem calls, got %d", len(writer.puts))
	}
	if len(writer.deletes) != 1 {
		t.Fatalf("expected 1 DeleteItem call, got %d", len(writer.deletes))
	}

	wantTS := strconv.FormatInt(fixedNow.UnixNano(), 10)
	first := writer.puts[0]
	if aws.ToString(first.TableName) != "table-b" {
		t.Errorf("PutItem TableName = %q, want table-b", aws.ToString(first.TableName))
	}
	if got := first.Item[AttrRepOrigin].(*ddbtypes.AttributeValueMemberS).Value; got != "us-east-1" {
		t.Errorf("%s = %q, want us-east-1", AttrRepOrigin, got)
	}
	if got := first.Item[AttrRepTimestamp].(*ddbtypes.AttributeValueMemberN).Value; got != wantTS {
		t.Errorf("%s = %q, want %q", AttrRepTimestamp, got, wantTS)
	}
	if aws.ToString(first.ConditionExpression) != "attribute_not_exists(#ts) OR #ts < :ts" {
		t.Errorf("ConditionExpression = %q", aws.ToString(first.ConditionExpression))
	}
	if first.ExpressionAttributeNames["#ts"] != AttrRepTimestamp {
		t.Errorf("ExpressionAttributeNames[#ts] = %q", first.ExpressionAttributeNames["#ts"])
	}
	if got := first.ExpressionAttributeValues[":ts"].(*ddbtypes.AttributeValueMemberN).Value; got != wantTS {
		t.Errorf("ExpressionAttributeValues[:ts] = %q, want %q", got, wantTS)
	}

	del := writer.deletes[0]
	if aws.ToString(del.TableName) != "table-b" {
		t.Errorf("DeleteItem TableName = %q, want table-b", aws.ToString(del.TableName))
	}
	if got := del.Key["SK"].(*ddbtypes.AttributeValueMemberS).Value; got != "REF#old" {
		t.Errorf("DeleteItem SK = %q, want REF#old", got)
	}
}

func TestReplicator_LoopGuardSkipsTargetOrigin(t *testing.T) {
	tables := &fakeTableDescriber{streamARN: "arn:stream"}
	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-1")}},
		getRecordsHook: func(_ *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-2"),
				Records: []streamtypes.Record{
					{
						EventName: streamtypes.OperationTypeInsert,
						Dynamodb: &streamtypes.StreamRecord{
							NewImage: map[string]streamtypes.AttributeValue{
								"PK":          &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK":          &streamtypes.AttributeValueMemberS{Value: "META"},
								AttrRepOrigin: &streamtypes.AttributeValueMemberS{Value: "us-west-2"},
							},
						},
					},
					{
						EventName: streamtypes.OperationTypeInsert,
						Dynamodb: &streamtypes.StreamRecord{
							NewImage: map[string]streamtypes.AttributeValue{
								"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#2"},
								"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
							},
						},
					},
				},
			}, nil
		},
	}
	writer := &fakeItemWriter{}

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if len(writer.puts) != 1 {
		t.Fatalf("expected 1 PutItem (target-origin record skipped), got %d", len(writer.puts))
	}
	if got := writer.puts[0].Item["PK"].(*ddbtypes.AttributeValueMemberS).Value; got != "REPO#2" {
		t.Errorf("replicated PK = %q, want REPO#2", got)
	}
}

func TestReplicator_LWWConditionalCheckFailedIsNoOp(t *testing.T) {
	tables := &fakeTableDescriber{streamARN: "arn:stream"}
	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-1")}},
		getRecordsHook: func(_ *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-2"),
				Records: []streamtypes.Record{
					{
						EventName: streamtypes.OperationTypeModify,
						Dynamodb: &streamtypes.StreamRecord{
							NewImage: map[string]streamtypes.AttributeValue{
								"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
							},
						},
					},
				},
			}, nil
		},
	}
	writer := &fakeItemWriter{
		putHook: func(_ *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, &ddbtypes.ConditionalCheckFailedException{Message: aws.String("stale write")}
		},
	}

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("ConditionalCheckFailedException should be treated as clean LWW no-op, got: %v", err)
	}
	if len(writer.puts) != 1 {
		t.Fatalf("expected 1 PutItem attempt, got %d", len(writer.puts))
	}
}

func TestReplicator_RemoveBounceSuppressionAndTTLExpiry(t *testing.T) {
	currentTime := time.Unix(1_700_000_000, 0)
	nowFn := func() time.Time { return currentTime }

	tables := &fakeTableDescriber{streamARN: "arn:stream"}
	removeRec := streamtypes.Record{
		EventName: streamtypes.OperationTypeRemove,
		Dynamodb: &streamtypes.StreamRecord{
			Keys: map[string]streamtypes.AttributeValue{
				"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
				"SK": &streamtypes.AttributeValueMemberS{Value: "REF#del"},
			},
		},
	}
	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-1")}},
		getRecordsHook: func(_ *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-next"),
				Records:           []streamtypes.Record{removeRec},
			}, nil
		},
	}
	writer := &fakeItemWriter{}

	rep, err := NewReplicator(Config{
		SourceRegion:         "us-east-1",
		SourceTable:          "table-a",
		TargetRegion:         "us-west-2",
		TargetTable:          "table-b",
		DeleteSuppressionTTL: 2 * time.Second,
		Tables:               tables,
		Streams:              streams,
		Target:               writer,
		Now:                  nowFn,
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	// First REMOVE replicates and records suppression.
	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll 1: %v", err)
	}
	if len(writer.deletes) != 1 {
		t.Fatalf("expected 1 delete after poll 1, got %d", len(writer.deletes))
	}

	// Second REMOVE within TTL is suppressed.
	currentTime = currentTime.Add(1 * time.Second)
	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll 2: %v", err)
	}
	if len(writer.deletes) != 1 {
		t.Fatalf("expected bounce REMOVE within TTL to be suppressed, got %d deletes", len(writer.deletes))
	}

	// Third REMOVE after TTL expires is allowed again.
	currentTime = currentTime.Add(2 * time.Second)
	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll 3: %v", err)
	}
	if len(writer.deletes) != 2 {
		t.Fatalf("expected REMOVE after TTL expiry to replicate, got %d deletes", len(writer.deletes))
	}
}

func TestReplicator_LagDelayAndCancellation(t *testing.T) {
	tables := &fakeTableDescriber{streamARN: "arn:stream"}
	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{{ShardId: aws.String("shard-1")}},
		getRecordsHook: func(_ *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-2"),
				Records: []streamtypes.Record{
					{
						EventName: streamtypes.OperationTypeInsert,
						Dynamodb: &streamtypes.StreamRecord{
							NewImage: map[string]streamtypes.AttributeValue{
								"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#1"},
								"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
							},
						},
					},
				},
			}, nil
		},
	}
	writer := &fakeItemWriter{}

	var sleptDuration time.Duration
	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		LagDelay:     250 * time.Millisecond,
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
		Sleep: func(ctx context.Context, d time.Duration) error {
			sleptDuration = d
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return nil
			}
		},
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce: %v", err)
	}
	if sleptDuration != 250*time.Millisecond {
		t.Fatalf("sleptDuration = %v, want 250ms", sleptDuration)
	}
	if len(writer.puts) != 1 {
		t.Fatalf("expected 1 put after lag, got %d", len(writer.puts))
	}

	// Verify real context cancellation during default lag sleep.
	writer2 := &fakeItemWriter{}
	rep2, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		LagDelay:     5 * time.Second,
		Tables:       tables,
		Streams:      streams,
		Target:       writer2,
	})
	if err != nil {
		t.Fatalf("NewReplicator 2: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = rep2.PollOnce(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded during lag cancel, got %v", err)
	}
	if len(writer2.puts) != 0 {
		t.Fatalf("expected 0 puts on cancelled lag, got %d", len(writer2.puts))
	}
}

func TestReplicator_TrimmedDataAccessRecoveryAndChildShards(t *testing.T) {
	tables := &fakeTableDescriber{streamARN: "arn:stream"}
	var getRecordsCalls atomic.Int32

	streams := &fakeStreamReader{
		shards: []streamtypes.Shard{
			{ShardId: aws.String("shard-parent")},
		},
		getIterHook: func(in *dynamodbstreams.GetShardIteratorInput) (*dynamodbstreams.GetShardIteratorOutput, error) {
			return &dynamodbstreams.GetShardIteratorOutput{
				ShardIterator: aws.String("iter-" + aws.ToString(in.ShardId) + "-" + string(in.ShardIteratorType)),
			}, nil
		},
		getRecordsHook: func(in *dynamodbstreams.GetRecordsInput) (*dynamodbstreams.GetRecordsOutput, error) {
			call := getRecordsCalls.Add(1)
			iter := aws.ToString(in.ShardIterator)
			if call == 1 {
				// Simulate TrimmedDataAccessException on first GetRecords.
				return nil, &streamtypes.TrimmedDataAccessException{Message: aws.String("trimmed")}
			}
			if iter == "iter-shard-parent-TRIM_HORIZON" {
				// Parent shard closes (NextShardIterator == nil).
				return &dynamodbstreams.GetRecordsOutput{
					NextShardIterator: nil,
					Records: []streamtypes.Record{
						{
							EventName: streamtypes.OperationTypeInsert,
							Dynamodb: &streamtypes.StreamRecord{
								NewImage: map[string]streamtypes.AttributeValue{
									"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#parent"},
									"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
								},
							},
						},
					},
				}, nil
			}
			// Child shard record.
			return &dynamodbstreams.GetRecordsOutput{
				NextShardIterator: aws.String("iter-child-2"),
				Records: []streamtypes.Record{
					{
						EventName: streamtypes.OperationTypeInsert,
						Dynamodb: &streamtypes.StreamRecord{
							NewImage: map[string]streamtypes.AttributeValue{
								"PK": &streamtypes.AttributeValueMemberS{Value: "REPO#child"},
								"SK": &streamtypes.AttributeValueMemberS{Value: "META"},
							},
						},
					},
				},
			}, nil
		},
	}
	writer := &fakeItemWriter{}

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceTable:  "table-a",
		TargetRegion: "us-west-2",
		TargetTable:  "table-b",
		IteratorType: streamtypes.ShardIteratorTypeLatest,
		Tables:       tables,
		Streams:      streams,
		Target:       writer,
	})
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	// Poll 1: hits TrimmedDataAccessException, recovers with TRIM_HORIZON and processes parent record.
	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll 1 (trimmed recovery): %v", err)
	}
	if len(writer.puts) != 1 {
		t.Fatalf("expected 1 put after trimmed recovery, got %d", len(writer.puts))
	}

	// Add newly opened child shard.
	streams.mu.Lock()
	streams.shards = append(streams.shards, streamtypes.Shard{
		ShardId:       aws.String("shard-child"),
		ParentShardId: aws.String("shard-parent"),
	})
	streams.mu.Unlock()

	// Poll 2: parent is closed, child shard opens with TRIM_HORIZON and replicates.
	if err := rep.PollOnce(context.Background()); err != nil {
		t.Fatalf("poll 2 (child shard): %v", err)
	}
	if len(writer.puts) != 2 {
		t.Fatalf("expected 2 puts after child shard, got %d", len(writer.puts))
	}
	if got := writer.puts[1].Item["PK"].(*ddbtypes.AttributeValueMemberS).Value; got != "REPO#child" {
		t.Errorf("second put PK = %q, want REPO#child", got)
	}
}
