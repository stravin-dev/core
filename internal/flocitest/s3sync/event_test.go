package s3sync

import (
	"testing"
)

func TestParseNotification_TestEvent(t *testing.T) {
	body := `{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"2026-11-21T00:00:00.000Z","Bucket":"git-objects-us-east-1"}`

	n, err := ParseNotification(body)
	if err != nil {
		t.Fatalf("ParseNotification returned unexpected error: %v", err)
	}
	if !n.IsTestEvent {
		t.Fatalf("IsTestEvent = false, want true")
	}
	if len(n.Records) != 0 {
		t.Fatalf("len(Records) = %d, want 0", len(n.Records))
	}
}

func TestParseNotification_CreatedAndURLEncodedKeys(t *testing.T) {
	cases := []struct {
		name      string
		eventName string
		rawKey    string
		wantKey   string
		wantKind  EventKind
	}{
		{
			name:      "standard ObjectCreated:Put with slashes",
			eventName: "ObjectCreated:Put",
			rawKey:    "ab%2Flocal%2Fdemo%2Fpacks%2Fdeadbeef.pack",
			wantKey:   "ab/local/demo/packs/deadbeef.pack",
			wantKind:  EventKindCreated,
		},
		{
			name:      "prefixed s3:ObjectCreated with plus spaces and literal plus",
			eventName: "s3:ObjectCreated:CompleteMultipartUpload",
			rawKey:    "folder%2Fmy+file%2Bname%20v2.idx",
			wantKey:   "folder/my file+name v2.idx",
			wantKind:  EventKindCreated,
		},
		{
			name:      "standard ObjectRemoved:Delete",
			eventName: "ObjectRemoved:Delete",
			rawKey:    "ab/local/demo/packs/old.pack",
			wantKey:   "ab/local/demo/packs/old.pack",
			wantKind:  EventKindRemoved,
		},
		{
			name:      "prefixed s3:ObjectRemoved:DeleteMarkerCreated",
			eventName: "s3:ObjectRemoved:DeleteMarkerCreated",
			rawKey:    "ab%2Fdeleted.pack",
			wantKey:   "ab/deleted.pack",
			wantKind:  EventKindRemoved,
		},
		{
			name:      "ignored event kind",
			eventName: "ObjectRestore:Post",
			rawKey:    "ab/restored.pack",
			wantKey:   "ab/restored.pack",
			wantKind:  EventKindIgnored,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"Records":[{"eventName":"` + tc.eventName + `","s3":{"bucket":{"name":"git-objects-us-east-1"},"object":{"key":"` + tc.rawKey + `"}}}]}`

			n, err := ParseNotification(body)
			if err != nil {
				t.Fatalf("ParseNotification error: %v", err)
			}
			if n.IsTestEvent {
				t.Fatalf("IsTestEvent = true, want false")
			}
			if len(n.Records) != 1 {
				t.Fatalf("len(Records) = %d, want 1", len(n.Records))
			}
			rec := n.Records[0]
			if rec.Kind != tc.wantKind {
				t.Errorf("Kind = %v, want %v", rec.Kind, tc.wantKind)
			}
			if rec.Bucket != "git-objects-us-east-1" {
				t.Errorf("Bucket = %q, want %q", rec.Bucket, "git-objects-us-east-1")
			}
			if rec.Key != tc.wantKey {
				t.Errorf("Key = %q, want %q", rec.Key, tc.wantKey)
			}
		})
	}
}

func TestParseNotification_Errors(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "malformed json", body: `{not-json`},
		{name: "invalid percent escape", body: `{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"b"},"object":{"key":"bad%ZZkey"}}}]}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseNotification(tc.body); err == nil {
				t.Fatalf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
