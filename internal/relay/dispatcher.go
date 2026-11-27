package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"stravin.dev/core/internal/telemetry"
)

const (
	// SpanRelayPublish is the OpenTelemetry span name for JetStream outbox publishes.
	SpanRelayPublish = "relay.publish"

	defaultMaxAttempts    = 3
	defaultInitialBackoff = 100 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
)

// SleepFunc waits for duration d or returns early if ctx is cancelled.
type SleepFunc func(ctx context.Context, d time.Duration) error

// DispatcherConfig configures RelayDispatcher.
type DispatcherConfig struct {
	Publisher      Publisher
	Telemetry      *telemetry.Provider
	Metrics        *Metrics
	Logger         *slog.Logger
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Sleep          SleepFunc
	Clock          func() time.Time
}

// RelayDispatcher implements EventObserver to publish DynamoDB Streams outbox
// events to NATS JetStream with deterministic Nats-Msg-Id deduplication, W3C
// traceparent propagation, bounded retries, and OTel observability.
type RelayDispatcher struct {
	publisher      Publisher
	tracer         trace.Tracer
	metrics        *Metrics
	logger         *slog.Logger
	maxAttempts    int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	sleep          SleepFunc
	clock          func() time.Time
}

// NewRelayDispatcher constructs a RelayDispatcher.
func NewRelayDispatcher(cfg DispatcherConfig) (*RelayDispatcher, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("relay: nil publisher")
	}
	if cfg.Telemetry == nil {
		cfg.Telemetry = telemetry.NewNoopProvider()
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
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
	if cfg.Clock == nil {
		cfg.Clock = func() time.Time { return time.Now().UTC() }
	}

	metrics := cfg.Metrics
	if metrics == nil {
		var err error
		metrics, err = NewMetrics(cfg.Telemetry)
		if err != nil {
			return nil, err
		}
	}

	return &RelayDispatcher{
		publisher:      cfg.Publisher,
		tracer:         cfg.Telemetry.Tracer("stravin-relay"),
		metrics:        metrics,
		logger:         cfg.Logger,
		maxAttempts:    cfg.MaxAttempts,
		initialBackoff: cfg.InitialBackoff,
		maxBackoff:     cfg.MaxBackoff,
		sleep:          cfg.Sleep,
		clock:          cfg.Clock,
	}, nil
}

// OnEvent builds the Standard Async Event Envelope and publishes it to NATS JetStream.
func (d *RelayDispatcher) OnEvent(ctx context.Context, ev OutboxEvent) error {
	attrs := telemetry.NewSpanAttrs().
		TenantID(string(ev.Repo.TenantID)).
		RepoID(string(ev.Repo.RepoID)).
		Build()
	attrs = append(attrs, attribute.String("relay.event_kind", string(ev.Kind)))
	if ev.RefName != "" {
		attrs = append(attrs, telemetry.AttrRefName.String(ev.RefName))
	}
	if ev.OID != "" {
		attrs = append(attrs, telemetry.AttrOID.String(ev.OID))
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(attrs...),
	}
	if link, ok := telemetry.SpanLinkFromTraceparent(ev.TraceParent); ok {
		opts = append(opts, trace.WithLinks(link))
	}

	spanCtx, span := d.tracer.Start(ctx, SpanRelayPublish, opts...)
	defer span.End()

	spanTP, spanTS := telemetry.TraceContextFromContext(spanCtx)
	traceparent := ev.TraceParent
	if traceparent == "" {
		traceparent = spanTP
	}

	subject, env, err := BuildEnvelope(ev, traceparent, d.clock())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		d.metrics.RecordPublish(spanCtx, string(ev.Kind), "error")
		return err
	}

	payload, err := json.Marshal(env)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		d.metrics.RecordPublish(spanCtx, env.EventType, "error")
		return fmt.Errorf("relay: marshal envelope: %w", err)
	}

	headers := map[string]string{
		HeaderNatsMsgID:     env.IdempotencyKey,
		HeaderEventType:     env.EventType,
		HeaderSchemaVersion: env.SchemaVersion,
	}
	if traceparent != "" {
		headers[HeaderTraceparent] = traceparent
	}
	if spanTS != "" {
		headers[HeaderTracestate] = spanTS
	}

	msg := Message{
		Subject: subject,
		MsgID:   env.IdempotencyKey,
		Headers: headers,
		Payload: payload,
	}

	span.SetAttributes(
		attribute.String("messaging.destination.name", subject),
		attribute.String("messaging.message.id", msg.MsgID),
	)

	start := time.Now()
	backoff := d.initialBackoff
	var lastErr error
	for attempt := 1; attempt <= d.maxAttempts; attempt++ {
		ack, pubErr := d.publisher.Publish(spanCtx, msg)
		if pubErr == nil {
			result := "ok"
			if ack.Duplicate {
				result = "duplicate"
			}
			d.metrics.RecordPublish(spanCtx, env.EventType, result)
			d.metrics.RecordPublishDuration(spanCtx, env.EventType, result, time.Since(start))
			span.SetAttributes(
				attribute.Int("relay.attempts", attempt),
				attribute.Bool("relay.duplicate", ack.Duplicate),
			)
			span.SetStatus(codes.Ok, "")
			return nil
		}
		lastErr = pubErr
		if attempt == d.maxAttempts {
			break
		}
		d.metrics.RecordPublish(spanCtx, env.EventType, "retry")
		if sleepErr := d.sleep(spanCtx, backoff); sleepErr != nil {
			lastErr = sleepErr
			break
		}
		backoff *= 2
		if backoff > d.maxBackoff {
			backoff = d.maxBackoff
		}
	}

	d.metrics.RecordPublish(spanCtx, env.EventType, "error")
	d.metrics.RecordPublishDuration(spanCtx, env.EventType, "error", time.Since(start))
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, lastErr.Error())
	return lastErr
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
