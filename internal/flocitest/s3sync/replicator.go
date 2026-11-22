package s3sync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// ReplicatedFromMetadataKey is the user-metadata key (exposed over HTTP as
// x-amz-meta-floci-replicated-from) stamped on replicated objects to prevent
// bidirectional replication loops.
const ReplicatedFromMetadataKey = "floci-replicated-from"

const defaultErrorBackoff = 250 * time.Millisecond

// Config configures a unidirectional S3 CRR replicator between two Floci endpoints.
type Config struct {
	SourceRegion     string
	SourceBucket     string
	TargetRegion     string
	TargetBucket     string
	LagDelay         time.Duration
	ReplicateDeletes bool
	ErrorBackoff     time.Duration
	Logger           *slog.Logger
}

// Replicator consumes S3 event notifications from SQS and copies (or deletes)
// objects on a target Floci S3 bucket with configurable lag and loop-guard stamping.
type Replicator struct {
	cfg    Config
	queue  QueueConsumer
	reader ObjectReader
	writer ObjectWriter
	logger *slog.Logger
}

// NewReplicator constructs a validated Replicator wired to the given ports.
func NewReplicator(cfg Config, queue QueueConsumer, reader ObjectReader, writer ObjectWriter) (*Replicator, error) {
	if strings.TrimSpace(cfg.SourceRegion) == "" {
		return nil, errors.New("s3sync: SourceRegion is required")
	}
	if strings.TrimSpace(cfg.TargetRegion) == "" {
		return nil, errors.New("s3sync: TargetRegion is required")
	}
	if strings.TrimSpace(cfg.TargetBucket) == "" {
		return nil, errors.New("s3sync: TargetBucket is required")
	}
	if cfg.LagDelay < 0 {
		return nil, errors.New("s3sync: LagDelay must be non-negative")
	}
	if queue == nil || reader == nil || writer == nil {
		return nil, errors.New("s3sync: queue, reader, and writer ports are required")
	}
	if cfg.ErrorBackoff <= 0 {
		cfg.ErrorBackoff = defaultErrorBackoff
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Replicator{
		cfg:    cfg,
		queue:  queue,
		reader: reader,
		writer: writer,
		logger: logger,
	}, nil
}

// Run polls the source SQS queue and processes notifications until ctx is canceled.
func (r *Replicator) Run(ctx context.Context) error {
	r.logger.Info("floci-test-s3-sync started",
		"source_region", r.cfg.SourceRegion,
		"source_bucket", r.cfg.SourceBucket,
		"target_region", r.cfg.TargetRegion,
		"target_bucket", r.cfg.TargetBucket,
		"lag_delay", r.cfg.LagDelay.String(),
		"replicate_deletes", r.cfg.ReplicateDeletes,
	)

	for {
		if ctx.Err() != nil {
			r.logger.Info("floci-test-s3-sync stopping")
			return nil
		}

		msgs, err := r.queue.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				r.logger.Info("floci-test-s3-sync stopping")
				return nil
			}
			r.logger.Warn("sqs receive failed", "error", err)
			if sleepErr := sleepWithContext(ctx, r.cfg.ErrorBackoff); sleepErr != nil {
				return nil
			}
			continue
		}

		for _, msg := range msgs {
			if err := r.ProcessMessage(ctx, msg); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				r.logger.Warn("s3 sync message failed; leaving visible for retry",
					"message_id", msg.ID,
					"error", err,
				)
				continue
			}

			if delErr := r.queue.Delete(ctx, msg.ReceiptHandle); delErr != nil {
				if ctx.Err() != nil {
					return nil
				}
				r.logger.Warn("sqs delete failed after processing",
					"message_id", msg.ID,
					"error", delErr,
				)
			}
		}
	}
}

// ProcessMessage handles a single SQS message containing an S3 event notification.
// Returning nil signals the caller to acknowledge/delete the SQS message.
func (r *Replicator) ProcessMessage(ctx context.Context, msg QueueMessage) error {
	notification, err := ParseNotification(msg.Body)
	if err != nil {
		return err
	}
	if notification.IsTestEvent {
		r.logger.Debug("ignoring s3:TestEvent", "message_id", msg.ID)
		return nil
	}

	for _, rec := range notification.Records {
		if err := r.processRecord(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func (r *Replicator) processRecord(ctx context.Context, rec EventRecord) error {
	srcBucket := rec.Bucket
	if srcBucket == "" {
		srcBucket = r.cfg.SourceBucket
	}
	if r.cfg.SourceBucket != "" && srcBucket != r.cfg.SourceBucket {
		r.logger.Debug("skipping event for non-matching source bucket",
			"event_bucket", srcBucket,
			"expected_bucket", r.cfg.SourceBucket,
			"key", rec.Key,
		)
		return nil
	}

	switch rec.Kind {
	case EventKindCreated:
		return r.replicateCreated(ctx, srcBucket, rec.Key)
	case EventKindRemoved:
		return r.replicateRemoved(ctx, rec.Key)
	default:
		r.logger.Debug("ignoring unsupported s3 event", "event_name", rec.EventName, "key", rec.Key)
		return nil
	}
}

func (r *Replicator) replicateCreated(ctx context.Context, srcBucket, key string) error {
	if err := sleepWithContext(ctx, r.cfg.LagDelay); err != nil {
		return err
	}

	obj, err := r.reader.GetObject(ctx, srcBucket, key)
	if err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			r.logger.Info("source object vanished before replication; skipping",
				"source_bucket", srcBucket,
				"key", key,
			)
			return nil
		}
		return fmt.Errorf("get source object %s/%s: %w", srcBucket, key, err)
	}

	if origin := lookupMetadata(obj.Metadata, ReplicatedFromMetadataKey); origin == r.cfg.TargetRegion {
		r.logger.Debug("skipping object replicated from target region (loop guard)",
			"key", key,
			"replicated_from", origin,
			"target_region", r.cfg.TargetRegion,
		)
		return nil
	}

	stampedMeta := cloneMetadata(obj.Metadata)
	stampedMeta[ReplicatedFromMetadataKey] = r.cfg.SourceRegion

	outObj := Object{
		Body:        obj.Body,
		ContentType: obj.ContentType,
		Metadata:    stampedMeta,
	}
	if err := r.writer.PutObject(ctx, r.cfg.TargetBucket, key, outObj); err != nil {
		return fmt.Errorf("put target object %s/%s: %w", r.cfg.TargetBucket, key, err)
	}

	r.logger.Debug("replicated object",
		"source_bucket", srcBucket,
		"target_bucket", r.cfg.TargetBucket,
		"key", key,
		"bytes", len(obj.Body),
	)
	return nil
}

func (r *Replicator) replicateRemoved(ctx context.Context, key string) error {
	if !r.cfg.ReplicateDeletes {
		r.logger.Debug("skipping delete event (replicate_deletes=false)", "key", key)
		return nil
	}
	if err := sleepWithContext(ctx, r.cfg.LagDelay); err != nil {
		return err
	}
	if err := r.writer.DeleteObject(ctx, r.cfg.TargetBucket, key); err != nil {
		if errors.Is(err, ErrObjectNotFound) {
			return nil
		}
		return fmt.Errorf("delete target object %s/%s: %w", r.cfg.TargetBucket, key, err)
	}
	r.logger.Debug("replicated object deletion", "target_bucket", r.cfg.TargetBucket, "key", key)
	return nil
}

func lookupMetadata(meta map[string]string, key string) string {
	if v, ok := meta[key]; ok {
		return v
	}
	for k, v := range meta {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

func cloneMetadata(src map[string]string) map[string]string {
	dst := make(map[string]string, len(src)+1)
	for k, v := range src {
		dst[strings.ToLower(k)] = v
	}
	return dst
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
