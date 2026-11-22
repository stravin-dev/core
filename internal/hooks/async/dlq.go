package async

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"stravin.dev/core/internal/storage/ddb"
)

const defaultDLQTTL = 7 * 24 * time.Hour

// DLQEntry captures a webhook delivery that exhausted retries or failed permanently.
type DLQEntry struct {
	EntryID        string
	WebhookID      string
	TargetURL      string
	TenantID       string
	RepoID         string
	RefName        string
	OID            string
	PackID         string
	SequenceNumber string
	TraceParent    string
	Attempts       int
	LastStatusCode int
	LastError      string
	FailedAt       time.Time
}

// DLQSink is the GoF Strategy interface for recording failed webhook deliveries.
type DLQSink interface {
	Record(ctx context.Context, entry DLQEntry) error
}

// LoggingDLQSink emits structured slog error records for dead-lettered webhook deliveries.
type LoggingDLQSink struct {
	logger *slog.Logger
}

// NewLoggingDLQSink constructs a LoggingDLQSink.
func NewLoggingDLQSink(logger *slog.Logger) *LoggingDLQSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LoggingDLQSink{logger: logger}
}

// Record logs the DLQ entry as a structured error.
func (s *LoggingDLQSink) Record(ctx context.Context, entry DLQEntry) error {
	s.logger.ErrorContext(ctx, "webhook delivery moved to DLQ",
		"entry_id", entry.EntryID,
		"hook_id", entry.WebhookID,
		"target_url", entry.TargetURL,
		"tenant_id", entry.TenantID,
		"repo_id", entry.RepoID,
		"ref_name", entry.RefName,
		"oid", entry.OID,
		"pack_id", entry.PackID,
		"sequence_number", entry.SequenceNumber,
		"attempts", entry.Attempts,
		"status_code", entry.LastStatusCode,
		"error", entry.LastError,
	)
	return nil
}

// MemoryDLQSink stores DLQ entries in memory for unit and integration tests.
type MemoryDLQSink struct {
	mu      sync.RWMutex
	entries []DLQEntry
}

// NewMemoryDLQSink returns an empty MemoryDLQSink.
func NewMemoryDLQSink() *MemoryDLQSink {
	return &MemoryDLQSink{}
}

// Record appends entry to the in-memory sink.
func (s *MemoryDLQSink) Record(_ context.Context, entry DLQEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
	return nil
}

// Entries returns a copy of all recorded DLQ entries.
func (s *MemoryDLQSink) Entries() []DLQEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DLQEntry, len(s.entries))
	copy(out, s.entries)
	return out
}

// DynamoDBDLQSink persists dead-lettered webhook entries in the Stravin DynamoDB table.
type DynamoDBDLQSink struct {
	client    ddb.API
	tableName string
	ttl       time.Duration
}

// NewDynamoDBDLQSink constructs a DynamoDB-backed DLQSink.
func NewDynamoDBDLQSink(client ddb.API, tableName string) (*DynamoDBDLQSink, error) {
	if client == nil {
		return nil, errors.New("async: nil dynamodb client for DLQ sink")
	}
	if tableName == "" {
		return nil, errors.New("async: empty table name for DLQ sink")
	}
	return &DynamoDBDLQSink{
		client:    client,
		tableName: tableName,
		ttl:       defaultDLQTTL,
	}, nil
}

// Record persists entry under PK=T#{tenant}#REPO#{repo}#DLQ, SK=HOOK#{webhook_id}#{entry_id}.
func (s *DynamoDBDLQSink) Record(ctx context.Context, entry DLQEntry) error {
	failedAt := entry.FailedAt
	if failedAt.IsZero() {
		failedAt = time.Now().UTC()
	}
	entryID := entry.EntryID
	if entryID == "" {
		var err error
		entryID, err = ddb.NewULID(failedAt)
		if err != nil {
			return fmt.Errorf("async: generate dlq entry id: %w", err)
		}
	}

	pk := fmt.Sprintf("T#%s#REPO#%s#DLQ", entry.TenantID, entry.RepoID)
	sk := fmt.Sprintf("HOOK#%s#%s", entry.WebhookID, entryID)
	ttlEpoch := failedAt.Add(s.ttl).Unix()

	item := map[string]ddbtypes.AttributeValue{
		ddb.AttrPK:        &ddbtypes.AttributeValueMemberS{Value: pk},
		ddb.AttrSK:        &ddbtypes.AttributeValueMemberS{Value: sk},
		"entry_id":        &ddbtypes.AttributeValueMemberS{Value: entryID},
		"webhook_id":      &ddbtypes.AttributeValueMemberS{Value: entry.WebhookID},
		"target_url":      &ddbtypes.AttributeValueMemberS{Value: entry.TargetURL},
		"tenant_id":       &ddbtypes.AttributeValueMemberS{Value: entry.TenantID},
		"repo_id":         &ddbtypes.AttributeValueMemberS{Value: entry.RepoID},
		"ref_name":        &ddbtypes.AttributeValueMemberS{Value: entry.RefName},
		"oid":             &ddbtypes.AttributeValueMemberS{Value: entry.OID},
		"pack_id":         &ddbtypes.AttributeValueMemberS{Value: entry.PackID},
		"sequence_number": &ddbtypes.AttributeValueMemberS{Value: entry.SequenceNumber},
		"attempts":        &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(entry.Attempts)},
		"status_code":     &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(entry.LastStatusCode)},
		"last_error":      &ddbtypes.AttributeValueMemberS{Value: entry.LastError},
		"failed_at":       &ddbtypes.AttributeValueMemberS{Value: failedAt.UTC().Format(time.RFC3339Nano)},
		"ttl":             &ddbtypes.AttributeValueMemberN{Value: strconv.FormatInt(ttlEpoch, 10)},
	}
	if entry.TraceParent != "" {
		item["traceparent"] = &ddbtypes.AttributeValueMemberS{Value: entry.TraceParent}
	}

	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	if err != nil {
		return fmt.Errorf("async: persist DLQ entry: %w", err)
	}
	return nil
}

// MultiDLQSink fans out DLQ entries to multiple sinks (GoF Composite).
type MultiDLQSink struct {
	sinks []DLQSink
}

// NewMultiDLQSink combines non-nil DLQSinks.
func NewMultiDLQSink(sinks ...DLQSink) *MultiDLQSink {
	filtered := make([]DLQSink, 0, len(sinks))
	for _, s := range sinks {
		if s != nil {
			filtered = append(filtered, s)
		}
	}
	return &MultiDLQSink{sinks: filtered}
}

// Record writes entry to every underlying sink and joins any errors.
func (m *MultiDLQSink) Record(ctx context.Context, entry DLQEntry) error {
	var errs []error
	for _, s := range m.sinks {
		if err := s.Record(ctx, entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
