package async

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
	"stravin.dev/core/internal/tenant"
)

type fakeWebhookRepo struct {
	mu    sync.RWMutex
	hooks map[tenant.RepoKey][]ddb.WebhookConfig
}

func (f *fakeWebhookRepo) GetWebhooks(_ context.Context, repo tenant.RepoKey) ([]ddb.WebhookConfig, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	hooks, ok := f.hooks[repo]
	if !ok {
		return nil, ddb.ErrNotFound
	}
	out := make([]ddb.WebhookConfig, len(hooks))
	copy(out, hooks)
	return out, nil
}

func (f *fakeWebhookRepo) PutWebhooks(_ context.Context, repo tenant.RepoKey, webhooks []ddb.WebhookConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.hooks == nil {
		f.hooks = make(map[tenant.RepoKey][]ddb.WebhookConfig)
	}
	f.hooks[repo] = webhooks
	return nil
}

type fakeCipher struct{}

func (c fakeCipher) Encrypt(_ context.Context, tid tenant.TenantID, plaintext []byte) ([]byte, error) {
	return append(append([]byte(nil), []byte(string(tid)+":")...), plaintext...), nil
}

func (c fakeCipher) Decrypt(_ context.Context, tid tenant.TenantID, ciphertext []byte) ([]byte, error) {
	pfx := string(tid) + ":"
	if !strings.HasPrefix(string(ciphertext), pfx) {
		return nil, crypto.ErrDecryptionFailed
	}
	return []byte(strings.TrimPrefix(string(ciphertext), pfx)), nil
}

func TestWebhookDispatcher_SignedDeliveryAndTraceLinking(t *testing.T) {
	tel, err := telemetry.NewInMemoryProvider("stravin-hook-test")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	// Create an originating push traceparent.
	originCtx, originSpan := tel.Tracer("stravin-node").Start(context.Background(), "node.push")
	originSC := originSpan.SpanContext()
	originTraceparent := telemetry.TraceparentFromContext(originCtx)
	originSpan.End()
	tel.SpanExporter.Reset()

	secret := []byte("whsec_test_1234567890")
	rk := tenant.RepoKey{TenantID: "acme", RepoID: "core"}
	cipher := fakeCipher{}
	encSecret, err := cipher.Encrypt(context.Background(), rk.TenantID, secret)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}

	var (
		gotBody        []byte
		gotSig         string
		gotEvent       string
		gotTraceparent string
		hits           atomic.Int32
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		gotSig = r.Header.Get(HeaderSignature256)
		gotEvent = r.Header.Get(HeaderEvent)
		gotTraceparent = r.Header.Get("traceparent")
		body, _ := io.ReadAll(r.Body)
		gotBody = body
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	repo := &fakeWebhookRepo{
		hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
			rk: {
				{
					WebhookID:       "wh-match",
					URL:             srv.URL,
					EncryptedSecret: encSecret,
					RefGlob:         "refs/heads/*",
					Enabled:         true,
				},
				{
					WebhookID:       "wh-disabled",
					URL:             srv.URL,
					EncryptedSecret: encSecret,
					RefGlob:         "refs/heads/*",
					Enabled:         false,
				},
				{
					WebhookID:       "wh-nomatch",
					URL:             srv.URL,
					EncryptedSecret: encSecret,
					RefGlob:         "refs/tags/*",
					Enabled:         true,
				},
			},
		},
	}

	dlq := NewMemoryDLQSink()
	disp, err := NewWebhookDispatcher(DispatcherConfig{
		Webhooks:             repo,
		Cipher:               cipher,
		DLQ:                  dlq,
		Telemetry:            tel,
		AllowPrivateNetworks: true,
	})
	if err != nil {
		t.Fatalf("NewWebhookDispatcher: %v", err)
	}

	now := time.Date(2026, 11, 21, 19, 30, 0, 0, time.UTC)
	ev := RefHistoryEvent{
		SequenceNumber: "101",
		Repo:           rk,
		RefName:        "refs/heads/main",
		ULID:           "01JTEST",
		OID:            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PackID:         "pack-42",
		TraceParent:    originTraceparent,
		UpdatedAt:      now,
	}

	if err := disp.OnEvent(context.Background(), ev); err != nil {
		t.Fatalf("OnEvent: %v", err)
	}

	if hits.Load() != 1 {
		t.Fatalf("webhook hits = %d, want 1", hits.Load())
	}
	if len(dlq.Entries()) != 0 {
		t.Fatalf("expected 0 DLQ entries, got %+v", dlq.Entries())
	}
	if gotEvent != EventPush {
		t.Errorf("X-Stravin-Event = %q, want %q", gotEvent, EventPush)
	}
	if !VerifySignature(secret, gotBody, gotSig) {
		t.Errorf("invalid HMAC signature %q for body %s", gotSig, string(gotBody))
	}
	if gotTraceparent == "" {
		t.Error("expected outbound webhook to carry traceparent header")
	}

	var payload WebhookPayload
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatalf("unmarshal webhook payload: %v", err)
	}
	if payload.TenantID != "acme" || payload.RepoID != "core" || payload.Ref != "refs/heads/main" ||
		payload.OID != ev.OID || payload.PackID != "pack-42" || payload.Timestamp != now.Format(time.RFC3339Nano) {
		t.Errorf("unexpected payload: %+v", payload)
	}

	// Verify OTel span hook.async.dispatch links back to originating push trace.
	spans := tel.SpanExporter.GetSpans()
	var found bool
	for _, sp := range spans {
		if sp.Name != SpanAsyncDispatch {
			continue
		}
		found = true
		if len(sp.Links) != 1 {
			t.Fatalf("span links = %d, want 1", len(sp.Links))
		}
		if sp.Links[0].SpanContext.TraceID() != originSC.TraceID() {
			t.Errorf("linked TraceID = %s, want %s", sp.Links[0].SpanContext.TraceID(), originSC.TraceID())
		}
		if sp.Links[0].SpanContext.SpanID() != originSC.SpanID() {
			t.Errorf("linked SpanID = %s, want %s", sp.Links[0].SpanContext.SpanID(), originSC.SpanID())
		}
	}
	if !found {
		t.Fatalf("expected %q span in exported spans: %+v", SpanAsyncDispatch, spans)
	}
}

func TestWebhookDispatcher_RetryBackoffAndDLQ(t *testing.T) {
	rk := tenant.RepoKey{TenantID: "acme", RepoID: "retry-repo"}

	t.Run("succeeds after transient 500s", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) < 3 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		}))
		defer srv.Close()

		var sleeps []time.Duration
		dlq := NewMemoryDLQSink()
		disp, err := NewWebhookDispatcher(DispatcherConfig{
			Webhooks: &fakeWebhookRepo{hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
				rk: {{
					WebhookID: "wh-flaky",
					URL:       srv.URL,
					RefGlob:   "*",
					Enabled:   true,
					Retry:     ddb.WebhookRetryConfig{MaxAttempts: 3, InitialBackoffMS: 50, MaxBackoffMS: 500},
				}},
			}},
			Cipher:               crypto.NoopCipher{},
			DLQ:                  dlq,
			AllowPrivateNetworks: true,
			Sleep: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("NewWebhookDispatcher: %v", err)
		}

		ev := RefHistoryEvent{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("a", 40)}
		if err := disp.OnEvent(context.Background(), ev); err != nil {
			t.Fatalf("OnEvent: %v", err)
		}
		if attempts.Load() != 3 {
			t.Fatalf("attempts = %d, want 3", attempts.Load())
		}
		if len(dlq.Entries()) != 0 {
			t.Fatalf("expected 0 DLQ entries, got %d", len(dlq.Entries()))
		}
		if len(sleeps) != 2 || sleeps[0] != 50*time.Millisecond || sleeps[1] != 100*time.Millisecond {
			t.Fatalf("sleeps = %v, want [50ms 100ms]", sleeps)
		}
	})

	t.Run("exhausted retries record DLQ entry without blocking consumer", func(t *testing.T) {
		var attempts atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("downstream unavailable"))
		}))
		defer srv.Close()

		var sleeps []time.Duration
		dlq := NewMemoryDLQSink()
		disp, err := NewWebhookDispatcher(DispatcherConfig{
			Webhooks: &fakeWebhookRepo{hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
				rk: {{
					WebhookID: "wh-down",
					URL:       srv.URL,
					RefGlob:   "*",
					Enabled:   true,
					Retry:     ddb.WebhookRetryConfig{MaxAttempts: 3, InitialBackoffMS: 25, MaxBackoffMS: 40},
				}},
			}},
			Cipher:               crypto.NoopCipher{},
			DLQ:                  dlq,
			AllowPrivateNetworks: true,
			Sleep: func(_ context.Context, d time.Duration) error {
				sleeps = append(sleeps, d)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("NewWebhookDispatcher: %v", err)
		}

		ev := RefHistoryEvent{
			SequenceNumber: "999",
			Repo:           rk,
			RefName:        "refs/heads/main",
			OID:            strings.Repeat("c", 40),
			PackID:         "pack-9",
		}
		if err := disp.OnEvent(context.Background(), ev); err != nil {
			t.Fatalf("OnEvent should not block consumer after DLQ: %v", err)
		}
		if attempts.Load() != 3 {
			t.Fatalf("attempts = %d, want 3", attempts.Load())
		}
		// Backoff capped at MaxBackoffMS (40ms): 25ms, then min(50ms, 40ms)=40ms.
		if len(sleeps) != 2 || sleeps[0] != 25*time.Millisecond || sleeps[1] != 40*time.Millisecond {
			t.Fatalf("sleeps = %v, want [25ms 40ms]", sleeps)
		}
		entries := dlq.Entries()
		if len(entries) != 1 {
			t.Fatalf("DLQ entries = %d, want 1", len(entries))
		}
		if entries[0].WebhookID != "wh-down" || entries[0].Attempts != 3 || entries[0].LastStatusCode != http.StatusServiceUnavailable {
			t.Errorf("unexpected DLQ entry: %+v", entries[0])
		}
	})

	t.Run("SSRF metadata IP blocked and sent to DLQ without retrying", func(t *testing.T) {
		dlq := NewMemoryDLQSink()
		disp, err := NewWebhookDispatcher(DispatcherConfig{
			Webhooks: &fakeWebhookRepo{hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
				rk: {{
					WebhookID: "wh-ssrf",
					URL:       "http://169.254.169.254/latest/meta-data/",
					RefGlob:   "*",
					Enabled:   true,
					Retry:     ddb.WebhookRetryConfig{MaxAttempts: 3},
				}},
			}},
			Cipher: crypto.NoopCipher{},
			DLQ:    dlq,
		})
		if err != nil {
			t.Fatalf("NewWebhookDispatcher: %v", err)
		}

		ev := RefHistoryEvent{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("d", 40)}
		if err := disp.OnEvent(context.Background(), ev); err != nil {
			t.Fatalf("OnEvent: %v", err)
		}
		entries := dlq.Entries()
		if len(entries) != 1 {
			t.Fatalf("DLQ entries = %d, want 1", len(entries))
		}
		if entries[0].Attempts != 1 || !strings.Contains(entries[0].LastError, "blocked") {
			t.Errorf("unexpected SSRF DLQ entry: %+v", entries[0])
		}
	})

	t.Run("SSRF loopback blocked by default without retrying", func(t *testing.T) {
		var hits atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			hits.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()

		dlq := NewMemoryDLQSink()
		disp, err := NewWebhookDispatcher(DispatcherConfig{
			Webhooks: &fakeWebhookRepo{hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
				rk: {{
					WebhookID: "wh-ssrf-loopback",
					URL:       srv.URL,
					RefGlob:   "*",
					Enabled:   true,
					Retry:     ddb.WebhookRetryConfig{MaxAttempts: 3},
				}},
			}},
			Cipher: crypto.NoopCipher{},
			DLQ:    dlq,
		})
		if err != nil {
			t.Fatalf("NewWebhookDispatcher: %v", err)
		}

		ev := RefHistoryEvent{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("d", 40)}
		if err := disp.OnEvent(context.Background(), ev); err != nil {
			t.Fatalf("OnEvent: %v", err)
		}
		if hits.Load() != 0 {
			t.Fatalf("expected 0 hits to loopback server, got %d", hits.Load())
		}
		entries := dlq.Entries()
		if len(entries) != 1 {
			t.Fatalf("DLQ entries = %d, want 1", len(entries))
		}
		if entries[0].Attempts != 1 || !strings.Contains(entries[0].LastError, "blocked") {
			t.Errorf("unexpected SSRF loopback DLQ entry: %+v", entries[0])
		}
	})

	t.Run("bounded response body read handles large responses safely", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat("x", MaxResponseBodyBytes*3)))
		}))
		defer srv.Close()

		dlq := NewMemoryDLQSink()
		disp, err := NewWebhookDispatcher(DispatcherConfig{
			Webhooks: &fakeWebhookRepo{hooks: map[tenant.RepoKey][]ddb.WebhookConfig{
				rk: {{
					WebhookID: "wh-big-resp",
					URL:       srv.URL,
					RefGlob:   "*",
					Enabled:   true,
				}},
			}},
			Cipher:               crypto.NoopCipher{},
			DLQ:                  dlq,
			AllowPrivateNetworks: true,
		})
		if err != nil {
			t.Fatalf("NewWebhookDispatcher: %v", err)
		}

		ev := RefHistoryEvent{Repo: rk, RefName: "refs/heads/main", OID: strings.Repeat("e", 40)}
		if err := disp.OnEvent(context.Background(), ev); err != nil {
			t.Fatalf("OnEvent: %v", err)
		}
		if len(dlq.Entries()) != 0 {
			t.Fatalf("expected 0 DLQ entries, got %+v", dlq.Entries())
		}
	})
}
