package s3sync

import (
	"context"
	"errors"
)

// ErrObjectNotFound indicates the requested object does not exist in S3.
var ErrObjectNotFound = errors.New("s3sync: object not found")

// QueueMessage represents a single notification message pulled from SQS.
type QueueMessage struct {
	ID            string
	ReceiptHandle string
	Body          string
}

// QueueConsumer polls S3 event notifications from a queue and acknowledges them.
type QueueConsumer interface {
	Receive(ctx context.Context) ([]QueueMessage, error)
	Delete(ctx context.Context, receiptHandle string) error
}

// Object holds an S3 object's payload, content type, and user metadata.
type Object struct {
	Body        []byte
	ContentType string
	Metadata    map[string]string
}

// ObjectReader reads objects from the source S3 endpoint.
type ObjectReader interface {
	GetObject(ctx context.Context, bucket, key string) (Object, error)
}

// ObjectWriter writes and deletes objects on the target S3 endpoint.
type ObjectWriter interface {
	PutObject(ctx context.Context, bucket, key string, obj Object) error
	DeleteObject(ctx context.Context, bucket, key string) error
}
