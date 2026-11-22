package async

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/crypto"
	"stravin.dev/core/internal/hooks/wasm"
	"stravin.dev/core/internal/storage/ddb"
	"stravin.dev/core/internal/telemetry"
)

const (
	// SpanAsyncDispatch is the OpenTelemetry span name for async webhook dispatches.
	SpanAsyncDispatch = "hook.async.dispatch"

	defaultRequestTimeout = 10 * time.Second
	defaultMaxAttempts    = 3
	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
)

// WebhookPayload is the JSON body posted to Tier 2 webhook endpoints.
type WebhookPayload struct {
	TenantID  string `json:"tenant_id"`
	RepoID    string `json:"repo_id"`
	Ref       string `json:"ref"`
	OID       string `json:"oid"`
	PackID    string `json:"pack_id"`
	Timestamp string `json:"timestamp"`
}

// DeliveryRequest carries all inputs for a single webhook delivery pipeline run.
type DeliveryRequest struct {
	Event   RefHistoryEvent
	Webhook ddb.WebhookConfig
	Payload []byte
	Secret  []byte
}

// DeliveryResult summarizes the outcome of a webhook delivery pipeline run.
type DeliveryResult struct {
	StatusCode int
	Attempts   int
	Retryable  bool
	Err        error
}

// DeliveryFunc executes one stage of the webhook delivery Chain of Responsibility.
type DeliveryFunc func(ctx context.Context, req DeliveryRequest) DeliveryResult

// DeliveryMiddleware wraps a DeliveryFunc to add cross-cutting behavior (tracing, retries, DLQ).
type DeliveryMiddleware func(next DeliveryFunc) DeliveryFunc

// SleepFunc waits for duration d or returns early if ctx is cancelled.
type SleepFunc func(ctx context.Context, d time.Duration) error

// DispatcherConfig configures WebhookDispatcher.
type DispatcherConfig struct {
	Webhooks             ddb.WebhookRepository
	Cipher               crypto.EnvelopeCipher
	DLQ                  DLQSink
	HTTPClient           *http.Client
	Telemetry            *telemetry.Provider
	Logger               *slog.Logger
	RequestTimeout       time.Duration
	MaxAttempts          int
	InitialBackoff       time.Duration
	MaxBackoff           time.Duration
	Sleep                SleepFunc
	AllowPrivateNetworks bool
}

// WebhookDispatcher implements EventObserver to fan out Ref History events to
// matching Tier 2 webhooks with KMS secret decryption, HMAC-SHA256 signing,
// traceparent span linking, retries, and DLQ recording.
type WebhookDispatcher struct {
	webhooks             ddb.WebhookRepository
	cipher               crypto.EnvelopeCipher
	dlq                  DLQSink
	httpClient           *http.Client
	tracer               trace.Tracer
	logger               *slog.Logger
	maxAttempts          int
	initialBackoff       time.Duration
	maxBackoff           time.Duration
	sleep                SleepFunc
	allowPrivateNetworks bool
	pipeline             DeliveryFunc
}

// NewWebhookDispatcher constructs a WebhookDispatcher with a GoF Chain of Responsibility
// delivery pipeline (Tracing -> Retry/DLQ -> Safe HTTP Sender).
func NewWebhookDispatcher(cfg DispatcherConfig) (*WebhookDispatcher, error) {
	if cfg.Webhooks == nil {
		return nil, errors.New("async: nil webhook repository")
	}
	if cfg.Cipher == nil {
		cfg.Cipher = crypto.NoopCipher{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.DLQ == nil {
		cfg.DLQ = NewLoggingDLQSink(cfg.Logger)
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = defaultRequestTimeout
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = NewSafeHTTPClientWithOptions(cfg.RequestTimeout, SafeClientOptions{
			AllowPrivateNetworks: cfg.AllowPrivateNetworks,
		})
	}
	if cfg.Telemetry == nil {
		cfg.Telemetry = telemetry.NewNoopProvider()
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultInitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultMaxBackoff
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepWithContext
	}

	d := &WebhookDispatcher{
		webhooks:             cfg.Webhooks,
		cipher:               cfg.Cipher,
		dlq:                  cfg.DLQ,
		httpClient:           cfg.HTTPClient,
		tracer:               cfg.Telemetry.Tracer("stravin-hook"),
		logger:               cfg.Logger,
		maxAttempts:          cfg.MaxAttempts,
		initialBackoff:       cfg.InitialBackoff,
		maxBackoff:           cfg.MaxBackoff,
		sleep:                cfg.Sleep,
		allowPrivateNetworks: cfg.AllowPrivateNetworks,
	}

	d.pipeline = chainMiddleware(
		d.sendHTTP,
		d.retryAndDLQMiddleware(),
		d.tracingMiddleware(),
	)

	return d, nil
}

// OnEvent loads matching webhooks for ev.Repo and dispatches signed HTTP deliveries.
func (d *WebhookDispatcher) OnEvent(ctx context.Context, ev RefHistoryEvent) error {
	hooks, err := d.webhooks.GetWebhooks(ctx, ev.Repo)
	if err != nil {
		if errors.Is(err, ddb.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("async: load webhooks for %s/%s: %w", ev.Repo.TenantID, ev.Repo.RepoID, err)
	}

	ts := ev.UpdatedAt
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	payloadBytes, err := json.Marshal(WebhookPayload{
		TenantID:  string(ev.Repo.TenantID),
		RepoID:    string(ev.Repo.RepoID),
		Ref:       ev.RefName,
		OID:       ev.OID,
		PackID:    ev.PackID,
		Timestamp: ts.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return fmt.Errorf("async: marshal webhook payload: %w", err)
	}

	for _, wh := range hooks {
		if !wh.Enabled || !wasm.MatchRefGlob(wh.RefGlob, ev.RefName) {
			continue
		}

		var secret []byte
		if len(wh.EncryptedSecret) > 0 {
			decrypted, decErr := d.cipher.Decrypt(ctx, ev.Repo.TenantID, wh.EncryptedSecret)
			if decErr != nil {
				d.recordDLQ(ctx, ev, wh, DeliveryResult{
					Attempts: 1,
					Err:      fmt.Errorf("decrypt webhook secret: %w", decErr),
				})
				continue
			}
			secret = decrypted
		}

		d.pipeline(ctx, DeliveryRequest{
			Event:   ev,
			Webhook: wh,
			Payload: payloadBytes,
			Secret:  secret,
		})
	}
	return nil
}

func (d *WebhookDispatcher) tracingMiddleware() DeliveryMiddleware {
	return func(next DeliveryFunc) DeliveryFunc {
		return func(ctx context.Context, req DeliveryRequest) DeliveryResult {
			attrs := telemetry.NewSpanAttrs().
				TenantID(string(req.Event.Repo.TenantID)).
				RepoID(string(req.Event.Repo.RepoID)).
				HookID(req.Webhook.WebhookID).
				Build()
			attrs = append(attrs,
				telemetry.AttrRefName.String(req.Event.RefName),
				telemetry.AttrOID.String(req.Event.OID),
				telemetry.AttrPackID.String(req.Event.PackID),
			)

			opts := []trace.SpanStartOption{
				trace.WithSpanKind(trace.SpanKindClient),
				trace.WithAttributes(attrs...),
			}
			if link, ok := telemetry.SpanLinkFromTraceparent(req.Event.TraceParent); ok {
				opts = append(opts, trace.WithLinks(link))
			}

			spanCtx, span := d.tracer.Start(ctx, SpanAsyncDispatch, opts...)
			defer span.End()

			res := next(spanCtx, req)
			if res.StatusCode > 0 {
				span.SetAttributes(telemetry.AttrStatusCode.Int(res.StatusCode))
			}
			span.SetAttributes(attribute.Int("hook.attempts", res.Attempts))
			if res.Err != nil {
				span.RecordError(res.Err)
				span.SetStatus(codes.Error, res.Err.Error())
			} else {
				span.SetStatus(codes.Ok, "")
			}
			return res
		}
	}
}

func (d *WebhookDispatcher) retryAndDLQMiddleware() DeliveryMiddleware {
	return func(next DeliveryFunc) DeliveryFunc {
		return func(ctx context.Context, req DeliveryRequest) DeliveryResult {
			maxAttempts := req.Webhook.Retry.MaxAttempts
			if maxAttempts <= 0 {
				maxAttempts = d.maxAttempts
			}
			initialBackoff := d.initialBackoff
			if req.Webhook.Retry.InitialBackoffMS > 0 {
				initialBackoff = time.Duration(req.Webhook.Retry.InitialBackoffMS) * time.Millisecond
			}
			maxBackoff := d.maxBackoff
			if req.Webhook.Retry.MaxBackoffMS > 0 {
				maxBackoff = time.Duration(req.Webhook.Retry.MaxBackoffMS) * time.Millisecond
			}

			var last DeliveryResult
			backoff := initialBackoff
			for attempt := 1; attempt <= maxAttempts; attempt++ {
				last = next(ctx, req)
				last.Attempts = attempt
				if last.Err == nil {
					return last
				}
				if !last.Retryable || attempt == maxAttempts {
					break
				}
				if sleepErr := d.sleep(ctx, backoff); sleepErr != nil {
					last.Err = sleepErr
					break
				}
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			}

			d.recordDLQ(ctx, req.Event, req.Webhook, last)
			return last
		}
	}
}

func (d *WebhookDispatcher) sendHTTP(ctx context.Context, req DeliveryRequest) DeliveryResult {
	parsedURL, err := ValidateTargetURLWithOptions(req.Webhook.URL, SafeClientOptions{
		AllowPrivateNetworks: d.allowPrivateNetworks,
	})
	if err != nil {
		return DeliveryResult{Retryable: false, Err: err}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, parsedURL.String(), bytes.NewReader(req.Payload))
	if err != nil {
		return DeliveryResult{Retryable: false, Err: fmt.Errorf("async: build request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set(HeaderEvent, EventPush)
	if len(req.Secret) > 0 {
		httpReq.Header.Set(HeaderSignature256, SignPayload(req.Secret, req.Payload))
	}
	telemetry.InjectHTTP(ctx, httpReq.Header)
	if httpReq.Header.Get("traceparent") == "" && req.Event.TraceParent != "" {
		httpReq.Header.Set("traceparent", req.Event.TraceParent)
	}

	resp, err := d.httpClient.Do(httpReq) // #nosec G704 -- target URL validated by ValidateTargetURL + NewSafeHTTPClient dial guard
	if err != nil {
		retryable := !errors.Is(err, ErrBlockedTargetHost) && !errors.Is(err, ErrInvalidTargetURL)
		return DeliveryResult{Retryable: retryable, Err: fmt.Errorf("async: http post: %w", err)}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, MaxResponseBodyBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return DeliveryResult{StatusCode: resp.StatusCode}
	}

	retryable := resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests
	return DeliveryResult{
		StatusCode: resp.StatusCode,
		Retryable:  retryable,
		Err:        fmt.Errorf("async: webhook returned HTTP %d", resp.StatusCode),
	}
}

func (d *WebhookDispatcher) recordDLQ(ctx context.Context, ev RefHistoryEvent, wh ddb.WebhookConfig, res DeliveryResult) {
	errMsg := ""
	if res.Err != nil {
		errMsg = res.Err.Error()
	}
	entry := DLQEntry{
		WebhookID:      wh.WebhookID,
		TargetURL:      wh.URL,
		TenantID:       string(ev.Repo.TenantID),
		RepoID:         string(ev.Repo.RepoID),
		RefName:        ev.RefName,
		OID:            ev.OID,
		PackID:         ev.PackID,
		SequenceNumber: ev.SequenceNumber,
		TraceParent:    ev.TraceParent,
		Attempts:       res.Attempts,
		LastStatusCode: res.StatusCode,
		LastError:      errMsg,
		FailedAt:       time.Now().UTC(),
	}
	if err := d.dlq.Record(ctx, entry); err != nil {
		d.logger.ErrorContext(ctx, "failed to record webhook DLQ entry",
			"hook_id", wh.WebhookID,
			"error", err,
		)
	}
}

func chainMiddleware(final DeliveryFunc, middlewares ...DeliveryMiddleware) DeliveryFunc {
	fn := final
	for _, mw := range middlewares {
		fn = mw(fn)
	}
	return fn
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
