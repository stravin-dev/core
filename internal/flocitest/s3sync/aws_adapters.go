package s3sync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/smithy-go"
)

const (
	defaultWaitTimeSeconds int32 = 5
	defaultMaxMessages     int32 = 10
)

// SQSConsumer adapts an AWS SDK v2 SQS client to the QueueConsumer port.
type SQSConsumer struct {
	client          *sqs.Client
	queueURL        string
	waitTimeSeconds int32
	maxMessages     int32
}

// NewSQSConsumer returns an SQS-backed QueueConsumer for queueURL.
func NewSQSConsumer(client *sqs.Client, queueURL string) (*SQSConsumer, error) {
	if client == nil {
		return nil, errors.New("s3sync: sqs client is required")
	}
	if queueURL == "" {
		return nil, errors.New("s3sync: queue URL is required")
	}
	return &SQSConsumer{
		client:          client,
		queueURL:        queueURL,
		waitTimeSeconds: defaultWaitTimeSeconds,
		maxMessages:     defaultMaxMessages,
	}, nil
}

// Receive long-polls SQS for S3 notification messages.
func (c *SQSConsumer) Receive(ctx context.Context) ([]QueueMessage, error) {
	out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: c.maxMessages,
		WaitTimeSeconds:     c.waitTimeSeconds,
	})
	if err != nil {
		return nil, fmt.Errorf("sqs receive message: %w", err)
	}

	msgs := make([]QueueMessage, 0, len(out.Messages))
	for _, m := range out.Messages {
		msgs = append(msgs, QueueMessage{
			ID:            aws.ToString(m.MessageId),
			ReceiptHandle: aws.ToString(m.ReceiptHandle),
			Body:          aws.ToString(m.Body),
		})
	}
	return msgs, nil
}

// Delete acknowledges and removes a processed message from SQS.
func (c *SQSConsumer) Delete(ctx context.Context, receiptHandle string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("sqs delete message: %w", err)
	}
	return nil
}

// S3ObjectClient adapts an AWS SDK v2 S3 client to ObjectReader and ObjectWriter.
type S3ObjectClient struct {
	client *s3.Client
}

// NewS3ObjectClient wraps an AWS SDK v2 S3 client.
func NewS3ObjectClient(client *s3.Client) (*S3ObjectClient, error) {
	if client == nil {
		return nil, errors.New("s3sync: s3 client is required")
	}
	return &S3ObjectClient{client: client}, nil
}

// GetObject fetches an object's bytes, content type, and user metadata from S3.
func (c *S3ObjectClient) GetObject(ctx context.Context, bucket, key string) (Object, error) {
	out, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isS3NotFound(err) {
			return Object{}, ErrObjectNotFound
		}
		return Object{}, err
	}
	defer func() {
		_ = out.Body.Close()
	}()

	body, err := io.ReadAll(out.Body)
	if err != nil {
		return Object{}, fmt.Errorf("read s3 object body: %w", err)
	}

	return Object{
		Body:        body,
		ContentType: aws.ToString(out.ContentType),
		Metadata:    out.Metadata,
	}, nil
}

// PutObject uploads an object with its content type and user metadata to S3.
func (c *S3ObjectClient) PutObject(ctx context.Context, bucket, key string, obj Object) error {
	input := &s3.PutObjectInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Body:     bytes.NewReader(obj.Body),
		Metadata: obj.Metadata,
	}
	if obj.ContentType != "" {
		input.ContentType = aws.String(obj.ContentType)
	}
	_, err := c.client.PutObject(ctx, input)
	return err
}

// DeleteObject removes an object from S3.
func (c *S3ObjectClient) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil && isS3NotFound(err) {
		return ErrObjectNotFound
	}
	return err
}

func isS3NotFound(err error) bool {
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) && respErr.HTTPStatusCode() == http.StatusNotFound {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return false
}
