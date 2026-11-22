package s3sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"sync"
	"testing"
	"time"
)

type fakeQueue struct {
	mu          sync.Mutex
	messages    []QueueMessage
	deleted     []string
	receiveErr  error
	receiveWait chan struct{}
}

func (q *fakeQueue) enqueue(msg QueueMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = append(q.messages, msg)
}

func (q *fakeQueue) Receive(ctx context.Context) ([]QueueMessage, error) {
	q.mu.Lock()
	if q.receiveErr != nil {
		err := q.receiveErr
		q.mu.Unlock()
		return nil, err
	}
	if len(q.messages) > 0 {
		out := append([]QueueMessage(nil), q.messages...)
		q.messages = nil
		q.mu.Unlock()
		return out, nil
	}
	wait := q.receiveWait
	q.mu.Unlock()

	if wait != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait:
			return nil, nil
		}
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, nil
	}
}

func (q *fakeQueue) Delete(_ context.Context, receiptHandle string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted = append(q.deleted, receiptHandle)
	return nil
}

func (q *fakeQueue) deletedHandles() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.deleted...)
}

type fakeBucket struct {
	mu      sync.Mutex
	objects map[string]Object
	getErr  error
	putErr  error
	delErr  error
	puts    int
	deletes []string
	onPut   func(bucket, key string)
}

func newFakeBucket() *fakeBucket {
	return &fakeBucket{
		objects: make(map[string]Object),
	}
}

func (b *fakeBucket) GetObject(_ context.Context, bucket, key string) (Object, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.getErr != nil {
		return Object{}, b.getErr
	}
	obj, ok := b.objects[bucket+"/"+key]
	if !ok {
		return Object{}, ErrObjectNotFound
	}
	return obj, nil
}

func (b *fakeBucket) PutObject(_ context.Context, bucket, key string, obj Object) error {
	b.mu.Lock()
	if b.putErr != nil {
		err := b.putErr
		b.mu.Unlock()
		return err
	}
	copiedMeta := make(map[string]string, len(obj.Metadata))
	for k, v := range obj.Metadata {
		copiedMeta[k] = v
	}
	b.objects[bucket+"/"+key] = Object{
		Body:        append([]byte(nil), obj.Body...),
		ContentType: obj.ContentType,
		Metadata:    copiedMeta,
	}
	b.puts++
	cb := b.onPut
	b.mu.Unlock()

	if cb != nil {
		cb(bucket, key)
	}
	return nil
}

func (b *fakeBucket) DeleteObject(_ context.Context, bucket, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.delErr != nil {
		return b.delErr
	}
	delete(b.objects, bucket+"/"+key)
	b.deletes = append(b.deletes, bucket+"/"+key)
	return nil
}

func (b *fakeBucket) getStored(bucket, key string) (Object, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.objects[bucket+"/"+key]
	return obj, ok
}

func (b *fakeBucket) putCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func s3EventJSON(eventName, bucket, key string) string {
	return fmt.Sprintf(`{"Records":[{"eventName":%q,"s3":{"bucket":{"name":%q},"object":{"key":%q}}}]}`,
		eventName, bucket, url.QueryEscape(key))
}

func TestReplicator_CopiesCreatedObjectAndPreservesMetadataAndContentType(t *testing.T) {
	src := newFakeBucket()
	dst := newFakeBucket()
	q := &fakeQueue{}

	origMeta := map[string]string{"pack-sha256": "deadbeef"}
	srcKey := "ab/local/demo/packs/file name+1.pack"
	_ = src.PutObject(context.Background(), "src-bucket", srcKey, Object{
		Body:        []byte("PACK-bytes"),
		ContentType: "application/x-git-pack",
		Metadata:    origMeta,
	})

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "src-bucket",
		TargetRegion: "us-west-2",
		TargetBucket: "dst-bucket",
		Logger:       discardLogger(),
	}, q, src, dst)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	msg := QueueMessage{
		ID:            "m1",
		ReceiptHandle: "rh-1",
		Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", srcKey),
	}

	if err := rep.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}

	got, ok := dst.getStored("dst-bucket", srcKey)
	if !ok {
		t.Fatalf("target object %q not found", srcKey)
	}
	if !bytes.Equal(got.Body, []byte("PACK-bytes")) {
		t.Errorf("Body = %q, want %q", got.Body, "PACK-bytes")
	}
	if got.ContentType != "application/x-git-pack" {
		t.Errorf("ContentType = %q, want %q", got.ContentType, "application/x-git-pack")
	}
	if got.Metadata["pack-sha256"] != "deadbeef" {
		t.Errorf("Metadata[pack-sha256] = %q, want %q", got.Metadata["pack-sha256"], "deadbeef")
	}
	if got.Metadata[ReplicatedFromMetadataKey] != "us-east-1" {
		t.Errorf("Metadata[%s] = %q, want %q", ReplicatedFromMetadataKey, got.Metadata[ReplicatedFromMetadataKey], "us-east-1")
	}
}

func TestReplicator_TestEventSilentlyDeleted(t *testing.T) {
	src := newFakeBucket()
	dst := newFakeBucket()
	q := &fakeQueue{}
	q.enqueue(QueueMessage{
		ID:            "test-1",
		ReceiptHandle: "rh-test",
		Body:          `{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"src-bucket"}`,
	})

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "src-bucket",
		TargetRegion: "us-west-2",
		TargetBucket: "dst-bucket",
		Logger:       discardLogger(),
	}, q, src, dst)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := rep.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dst.putCount() != 0 {
		t.Fatalf("expected 0 puts for s3:TestEvent, got %d", dst.putCount())
	}
	deleted := q.deletedHandles()
	if len(deleted) != 1 || deleted[0] != "rh-test" {
		t.Fatalf("deleted = %v, want [rh-test]", deleted)
	}
}

func TestReplicator_LoopGuardSkipsTargetRegionOrigin(t *testing.T) {
	src := newFakeBucket()
	dst := newFakeBucket()
	q := &fakeQueue{}

	_ = src.PutObject(context.Background(), "src-bucket", "ab/from-west.pack", Object{
		Body: []byte("replicated"),
		Metadata: map[string]string{
			"Floci-Replicated-From": "us-west-2",
		},
	})
	q.enqueue(QueueMessage{
		ID:            "m-loop",
		ReceiptHandle: "rh-loop",
		Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", "ab/from-west.pack"),
	})

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "src-bucket",
		TargetRegion: "us-west-2",
		TargetBucket: "dst-bucket",
		Logger:       discardLogger(),
	}, q, src, dst)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := rep.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if dst.putCount() != 0 {
		t.Fatalf("expected 0 target puts due to loop guard, got %d", dst.putCount())
	}
	deleted := q.deletedHandles()
	if len(deleted) != 1 || deleted[0] != "rh-loop" {
		t.Fatalf("deleted = %v, want [rh-loop]", deleted)
	}
}

func TestReplicator_BidirectionalNoPingPong(t *testing.T) {
	bucketA := newFakeBucket()
	bucketB := newFakeBucket()
	queueA := &fakeQueue{}
	queueB := &fakeQueue{}

	var msgSeq int
	var seqMu sync.Mutex
	nextReceipt := func(prefix string) string {
		seqMu.Lock()
		defer seqMu.Unlock()
		msgSeq++
		return fmt.Sprintf("%s-%d", prefix, msgSeq)
	}

	bucketA.onPut = func(bucket, key string) {
		queueA.enqueue(QueueMessage{
			ID:            nextReceipt("a"),
			ReceiptHandle: nextReceipt("rh-a"),
			Body:          s3EventJSON("ObjectCreated:Put", bucket, key),
		})
	}
	bucketB.onPut = func(bucket, key string) {
		queueB.enqueue(QueueMessage{
			ID:            nextReceipt("b"),
			ReceiptHandle: nextReceipt("rh-b"),
			Body:          s3EventJSON("ObjectCreated:Put", bucket, key),
		})
	}

	repAtoB, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "bucket-a",
		TargetRegion: "us-west-2",
		TargetBucket: "bucket-b",
		Logger:       discardLogger(),
	}, queueA, bucketA, bucketB)
	if err != nil {
		t.Fatalf("NewReplicator A->B: %v", err)
	}

	repBtoA, err := NewReplicator(Config{
		SourceRegion: "us-west-2",
		SourceBucket: "bucket-b",
		TargetRegion: "us-east-1",
		TargetBucket: "bucket-a",
		Logger:       discardLogger(),
	}, queueB, bucketB, bucketA)
	if err != nil {
		t.Fatalf("NewReplicator B->A: %v", err)
	}

	// Write initial object in Region A.
	if err := bucketA.PutObject(context.Background(), "bucket-a", "packs/one.pack", Object{
		Body:        []byte("hello-bidirectional"),
		ContentType: "application/octet-stream",
	}); err != nil {
		t.Fatalf("initial PutObject: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = repAtoB.Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = repBtoA.Run(ctx)
	}()
	wg.Wait()

	if bucketA.putCount() != 1 {
		t.Fatalf("bucketA puts = %d, want 1 (no ping-pong back from B)", bucketA.putCount())
	}
	if bucketB.putCount() != 1 {
		t.Fatalf("bucketB puts = %d, want 1", bucketB.putCount())
	}
	objB, ok := bucketB.getStored("bucket-b", "packs/one.pack")
	if !ok {
		t.Fatalf("expected object replicated to bucket-b")
	}
	if objB.Metadata[ReplicatedFromMetadataKey] != "us-east-1" {
		t.Errorf("bucketB metadata[%s] = %q, want us-east-1", ReplicatedFromMetadataKey, objB.Metadata[ReplicatedFromMetadataKey])
	}
}

func TestReplicator_DeletePropagation(t *testing.T) {
	t.Run("disabled skips delete", func(t *testing.T) {
		src := newFakeBucket()
		dst := newFakeBucket()
		q := &fakeQueue{}
		_ = dst.PutObject(context.Background(), "dst-bucket", "packs/old.pack", Object{Body: []byte("keep")})

		rep, err := NewReplicator(Config{
			SourceRegion:     "us-east-1",
			SourceBucket:     "src-bucket",
			TargetRegion:     "us-west-2",
			TargetBucket:     "dst-bucket",
			ReplicateDeletes: false,
			Logger:           discardLogger(),
		}, q, src, dst)
		if err != nil {
			t.Fatalf("NewReplicator: %v", err)
		}

		msg := QueueMessage{
			ID:            "del-1",
			ReceiptHandle: "rh-del-1",
			Body:          s3EventJSON("ObjectRemoved:Delete", "src-bucket", "packs/old.pack"),
		}
		if err := rep.ProcessMessage(context.Background(), msg); err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if _, ok := dst.getStored("dst-bucket", "packs/old.pack"); !ok {
			t.Fatalf("expected target object to remain when ReplicateDeletes=false")
		}
	})

	t.Run("enabled deletes target object", func(t *testing.T) {
		src := newFakeBucket()
		dst := newFakeBucket()
		q := &fakeQueue{}
		_ = dst.PutObject(context.Background(), "dst-bucket", "packs/old.pack", Object{Body: []byte("gone")})

		rep, err := NewReplicator(Config{
			SourceRegion:     "us-east-1",
			SourceBucket:     "src-bucket",
			TargetRegion:     "us-west-2",
			TargetBucket:     "dst-bucket",
			ReplicateDeletes: true,
			Logger:           discardLogger(),
		}, q, src, dst)
		if err != nil {
			t.Fatalf("NewReplicator: %v", err)
		}

		msg := QueueMessage{
			ID:            "del-2",
			ReceiptHandle: "rh-del-2",
			Body:          s3EventJSON("ObjectRemoved:Delete", "src-bucket", "packs/old.pack"),
		}
		if err := rep.ProcessMessage(context.Background(), msg); err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if _, ok := dst.getStored("dst-bucket", "packs/old.pack"); ok {
			t.Fatalf("expected target object to be deleted when ReplicateDeletes=true")
		}
	})
}

func TestReplicator_LagDelayAndCancellation(t *testing.T) {
	t.Run("waits lag delay before copying", func(t *testing.T) {
		src := newFakeBucket()
		dst := newFakeBucket()
		q := &fakeQueue{}
		_ = src.PutObject(context.Background(), "src-bucket", "packs/lag.pack", Object{Body: []byte("delayed")})

		rep, err := NewReplicator(Config{
			SourceRegion: "us-east-1",
			SourceBucket: "src-bucket",
			TargetRegion: "us-west-2",
			TargetBucket: "dst-bucket",
			LagDelay:     60 * time.Millisecond,
			Logger:       discardLogger(),
		}, q, src, dst)
		if err != nil {
			t.Fatalf("NewReplicator: %v", err)
		}

		start := time.Now()
		err = rep.ProcessMessage(context.Background(), QueueMessage{
			ID:            "lag-1",
			ReceiptHandle: "rh-lag-1",
			Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", "packs/lag.pack"),
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("ProcessMessage: %v", err)
		}
		if elapsed < 50*time.Millisecond {
			t.Fatalf("elapsed = %v, want >= 50ms", elapsed)
		}
		if _, ok := dst.getStored("dst-bucket", "packs/lag.pack"); !ok {
			t.Fatalf("expected object to be replicated after lag delay")
		}
	})

	t.Run("cancels cleanly during lag delay without deleting message", func(t *testing.T) {
		src := newFakeBucket()
		dst := newFakeBucket()
		q := &fakeQueue{}
		_ = src.PutObject(context.Background(), "src-bucket", "packs/cancel.pack", Object{Body: []byte("delayed")})
		q.enqueue(QueueMessage{
			ID:            "lag-2",
			ReceiptHandle: "rh-lag-2",
			Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", "packs/cancel.pack"),
		})

		rep, err := NewReplicator(Config{
			SourceRegion: "us-east-1",
			SourceBucket: "src-bucket",
			TargetRegion: "us-west-2",
			TargetBucket: "dst-bucket",
			LagDelay:     5 * time.Second,
			Logger:       discardLogger(),
		}, q, src, dst)
		if err != nil {
			t.Fatalf("NewReplicator: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer cancel()

		start := time.Now()
		if err := rep.Run(ctx); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatalf("Run did not cancel promptly during lag delay")
		}
		if dst.putCount() != 0 {
			t.Fatalf("expected 0 puts on canceled lag, got %d", dst.putCount())
		}
		if len(q.deletedHandles()) != 0 {
			t.Fatalf("expected 0 deleted messages on canceled lag, got %v", q.deletedHandles())
		}
	})
}

func TestReplicator_TransientErrorLeavesMessageVisibleForRetry(t *testing.T) {
	src := newFakeBucket()
	dst := newFakeBucket()
	q := &fakeQueue{}
	_ = src.PutObject(context.Background(), "src-bucket", "packs/retry.pack", Object{Body: []byte("data")})
	dst.putErr = errors.New("temporary s3 outage")

	q.enqueue(QueueMessage{
		ID:            "retry-1",
		ReceiptHandle: "rh-retry-1",
		Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", "packs/retry.pack"),
	})

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "src-bucket",
		TargetRegion: "us-west-2",
		TargetBucket: "dst-bucket",
		Logger:       discardLogger(),
	}, q, src, dst)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := rep.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(q.deletedHandles()) != 0 {
		t.Fatalf("expected failed message not to be deleted, got %v", q.deletedHandles())
	}
}

func TestReplicator_MissingSourceObjectCleanSkip(t *testing.T) {
	src := newFakeBucket()
	dst := newFakeBucket()
	q := &fakeQueue{}

	q.enqueue(QueueMessage{
		ID:            "missing-1",
		ReceiptHandle: "rh-missing-1",
		Body:          s3EventJSON("ObjectCreated:Put", "src-bucket", "packs/vanished.pack"),
	})

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: "src-bucket",
		TargetRegion: "us-west-2",
		TargetBucket: "dst-bucket",
		Logger:       discardLogger(),
	}, q, src, dst)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	if err := rep.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	deleted := q.deletedHandles()
	if len(deleted) != 1 || deleted[0] != "rh-missing-1" {
		t.Fatalf("expected vanished source object to be cleanly acknowledged, deleted = %v", deleted)
	}
}
