package s3sync

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// EventKind categorizes an S3 notification record.
type EventKind int

const (
	EventKindIgnored EventKind = iota
	EventKindCreated
	EventKindRemoved
)

// EventRecord is a single parsed S3 event notification record.
type EventRecord struct {
	EventName string
	Kind      EventKind
	Bucket    string
	Key       string
}

// Notification is the parsed result of an S3-to-SQS message body.
type Notification struct {
	IsTestEvent bool
	Records     []EventRecord
}

type rawNotification struct {
	Event   string      `json:"Event"`
	Records []rawRecord `json:"Records"`
}

type rawRecord struct {
	EventName string `json:"eventName"`
	S3        struct {
		Bucket struct {
			Name string `json:"name"`
		} `json:"bucket"`
		Object struct {
			Key string `json:"key"`
		} `json:"object"`
	} `json:"s3"`
}

// ParseNotification decodes an S3 event notification JSON payload from SQS.
func ParseNotification(body string) (Notification, error) {
	var raw rawNotification
	if err := json.Unmarshal([]byte(body), &raw); err != nil {
		return Notification{}, fmt.Errorf("parse s3 notification json: %w", err)
	}
	if raw.Event == "s3:TestEvent" {
		return Notification{IsTestEvent: true}, nil
	}

	records := make([]EventRecord, 0, len(raw.Records))
	for _, r := range raw.Records {
		decodedKey, err := url.QueryUnescape(r.S3.Object.Key)
		if err != nil {
			return Notification{}, fmt.Errorf("url-decode s3 object key %q: %w", r.S3.Object.Key, err)
		}
		records = append(records, EventRecord{
			EventName: r.EventName,
			Kind:      classifyEventName(r.EventName),
			Bucket:    r.S3.Bucket.Name,
			Key:       decodedKey,
		})
	}
	return Notification{Records: records}, nil
}

func classifyEventName(name string) EventKind {
	trimmed := strings.TrimPrefix(name, "s3:")
	switch {
	case strings.HasPrefix(trimmed, "ObjectCreated:"):
		return EventKindCreated
	case strings.HasPrefix(trimmed, "ObjectRemoved:"):
		return EventKindRemoved
	default:
		return EventKindIgnored
	}
}
