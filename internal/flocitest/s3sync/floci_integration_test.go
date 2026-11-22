//go:build integration

package s3sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func flociTestEndpoints(t *testing.T) (srcEndpoint, dstEndpoint string) {
	t.Helper()
	srcEndpoint = os.Getenv("FLOCI_SYNC_SOURCE_ENDPOINT")
	if srcEndpoint == "" {
		srcEndpoint = os.Getenv("AWS_ENDPOINT_URL")
	}
	if srcEndpoint == "" {
		t.Skip("skipping Floci s3sync integration test: neither FLOCI_SYNC_SOURCE_ENDPOINT nor AWS_ENDPOINT_URL is set")
	}
	dstEndpoint = os.Getenv("FLOCI_SYNC_TARGET_ENDPOINT")
	if dstEndpoint == "" {
		dstEndpoint = srcEndpoint
	}
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		t.Setenv("AWS_ACCESS_KEY_ID", "test")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	}
	return srcEndpoint, dstEndpoint
}

func TestFlociIntegration_S3ToSQSReplicationAndLoopGuard(t *testing.T) {
	srcEndpoint, dstEndpoint := flociTestEndpoints(t)
	ctx := context.Background()

	srcAWS, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-east-1"))
	if err != nil {
		t.Fatalf("LoadDefaultConfig(source): %v", err)
	}
	dstAWS, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion("us-west-2"))
	if err != nil {
		t.Fatalf("LoadDefaultConfig(target): %v", err)
	}

	srcS3 := s3.NewFromConfig(srcAWS, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(srcEndpoint)
		o.UsePathStyle = true
	})
	dstS3 := s3.NewFromConfig(dstAWS, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(dstEndpoint)
		o.UsePathStyle = true
	})
	srcSQS := sqs.NewFromConfig(srcAWS, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(srcEndpoint)
	})

	nonce := time.Now().UnixNano()
	srcBucket := fmt.Sprintf("floci-s3sync-src-%d", nonce)
	dstBucket := fmt.Sprintf("floci-s3sync-dst-%d", nonce)
	queueName := fmt.Sprintf("floci-s3sync-q-%d", nonce)

	// Verify SQS is enabled on the target Floci instance.
	qOut, err := srcSQS.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		t.Fatalf("CreateQueue(%s): %v", queueName, err)
	}
	queueURL := aws.ToString(qOut.QueueUrl)
	t.Cleanup(func() {
		_, _ = srcSQS.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	})

	attrOut, err := srcSQS.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("GetQueueAttributes: %v", err)
	}
	queueARN := attrOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	if queueARN == "" {
		t.Fatalf("empty QueueArn for %s", queueURL)
	}

	if _, err := srcS3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(srcBucket)}); err != nil {
		t.Fatalf("CreateBucket(src): %v", err)
	}
	t.Cleanup(func() {
		_, _ = srcS3.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(srcBucket)})
	})

	if _, err := dstS3.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(dstBucket),
		CreateBucketConfiguration: &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraintUsWest2,
		},
	}); err != nil {
		t.Fatalf("CreateBucket(dst): %v", err)
	}
	t.Cleanup(func() {
		_, _ = dstS3.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(dstBucket)})
	})

	// Configure S3 -> SQS event notification for ObjectCreated events.
	if _, err := srcS3.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(srcBucket),
		NotificationConfiguration: &s3types.NotificationConfiguration{
			QueueConfigurations: []s3types.QueueConfiguration{
				{
					QueueArn: aws.String(queueARN),
					Events:   []s3types.Event{s3types.Event("s3:ObjectCreated:*")},
				},
			},
		},
	}); err != nil {
		t.Fatalf("PutBucketNotificationConfiguration: %v", err)
	}

	consumer, err := NewSQSConsumer(srcSQS, queueURL)
	if err != nil {
		t.Fatalf("NewSQSConsumer: %v", err)
	}
	consumer.waitTimeSeconds = 1

	reader, err := NewS3ObjectClient(srcS3)
	if err != nil {
		t.Fatalf("NewS3ObjectClient(src): %v", err)
	}
	writer, err := NewS3ObjectClient(dstS3)
	if err != nil {
		t.Fatalf("NewS3ObjectClient(dst): %v", err)
	}

	rep, err := NewReplicator(Config{
		SourceRegion: "us-east-1",
		SourceBucket: srcBucket,
		TargetRegion: "us-west-2",
		TargetBucket: dstBucket,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, consumer, reader, writer)
	if err != nil {
		t.Fatalf("NewReplicator: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	runDone := make(chan error, 1)
	go func() {
		runDone <- rep.Run(runCtx)
	}()
	t.Cleanup(func() {
		cancelRun()
		<-runDone
	})

	// 1. Put normal object in source bucket -> must replicate to target with content-type,
	// custom metadata, and x-amz-meta-floci-replicated-from=us-east-1.
	key := "ab/local/demo/packs/test pack+1.pack"
	payload := []byte("PACK\x00\x00\x00\x02floci-crr-integration")
	if _, err := srcS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(srcBucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/x-git-pack"),
		Metadata: map[string]string{
			"pack-sha256": "abc123",
		},
	}); err != nil {
		t.Fatalf("PutObject(normal): %v", err)
	}
	t.Cleanup(func() {
		_, _ = srcS3.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(srcBucket), Key: aws.String(key)})
		_, _ = dstS3.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(dstBucket), Key: aws.String(key)})
	})

	var replicated Object
	deadline := time.Now().Add(15 * time.Second)
	for {
		obj, getErr := writer.GetObject(ctx, dstBucket, key)
		if getErr == nil {
			replicated = obj
			break
		}
		if !errors.Is(getErr, ErrObjectNotFound) {
			t.Fatalf("GetObject(target): %v", getErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %q to replicate to %s", key, dstBucket)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !bytes.Equal(replicated.Body, payload) {
		t.Errorf("replicated body = %q, want %q", replicated.Body, payload)
	}
	if replicated.ContentType != "application/x-git-pack" {
		t.Errorf("replicated ContentType = %q, want application/x-git-pack", replicated.ContentType)
	}
	if lookupMetadata(replicated.Metadata, "pack-sha256") != "abc123" {
		t.Errorf("replicated metadata[pack-sha256] = %q, want abc123", lookupMetadata(replicated.Metadata, "pack-sha256"))
	}
	if lookupMetadata(replicated.Metadata, ReplicatedFromMetadataKey) != "us-east-1" {
		t.Errorf("replicated metadata[%s] = %q, want us-east-1", ReplicatedFromMetadataKey, lookupMetadata(replicated.Metadata, ReplicatedFromMetadataKey))
	}

	// 2. Loop guard: put an object in source already marked floci-replicated-from=us-west-2
	// (target region), followed by a sentinel object. Once the sentinel replicates, verify
	// the loop-guarded object was NOT copied to target.
	loopKey := "ab/local/demo/packs/loop-guarded.pack"
	sentinelKey := "ab/local/demo/packs/sentinel.pack"
	if _, err := srcS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(loopKey),
		Body:   bytes.NewReader([]byte("must-not-bounce")),
		Metadata: map[string]string{
			ReplicatedFromMetadataKey: "us-west-2",
		},
	}); err != nil {
		t.Fatalf("PutObject(loopKey): %v", err)
	}
	if _, err := srcS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(sentinelKey),
		Body:   bytes.NewReader([]byte("sentinel")),
	}); err != nil {
		t.Fatalf("PutObject(sentinelKey): %v", err)
	}
	t.Cleanup(func() {
		_, _ = srcS3.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(srcBucket), Key: aws.String(loopKey)})
		_, _ = srcS3.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(srcBucket), Key: aws.String(sentinelKey)})
		_, _ = dstS3.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(dstBucket), Key: aws.String(sentinelKey)})
	})

	sentinelDeadline := time.Now().Add(15 * time.Second)
	for {
		if _, getErr := writer.GetObject(ctx, dstBucket, sentinelKey); getErr == nil {
			break
		}
		if time.Now().After(sentinelDeadline) {
			t.Fatalf("timed out waiting for sentinel %q to replicate", sentinelKey)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if _, err := writer.GetObject(ctx, dstBucket, loopKey); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("loop-guarded object %q should not exist in target, got err=%v", loopKey, err)
	}
}
