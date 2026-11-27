package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const defaultPublishTimeout = 5 * time.Second

// Message represents a NATS JetStream message ready to be published.
type Message struct {
	Subject string
	MsgID   string
	Headers map[string]string
	Payload []byte
}

// PublishAck summarizes a JetStream publish acknowledgement.
type PublishAck struct {
	Stream    string
	Sequence  uint64
	Duplicate bool
}

// Publisher abstracts NATS JetStream publishing for clean testability and
// graceful degradation (GoF Adapter / Strategy).
type Publisher interface {
	Publish(ctx context.Context, msg Message) (PublishAck, error)
	Close() error
}

// JetStreamConfig configures a NATS JetStream Publisher.
type JetStreamConfig struct {
	URL            string
	ClientName     string
	PublishTimeout time.Duration
}

// JetStreamPublisher publishes outbox envelopes to NATS JetStream with native
// Nats-Msg-Id deduplication headers.
type JetStreamPublisher struct {
	nc             *nats.Conn
	js             jetstream.JetStream
	publishTimeout time.Duration
}

// NewJetStreamPublisher connects to NATS and initializes a JetStream context.
func NewJetStreamPublisher(cfg JetStreamConfig) (*JetStreamPublisher, error) {
	url := strings.TrimSpace(cfg.URL)
	if url == "" {
		return nil, errors.New("relay: empty NATS URL")
	}
	clientName := strings.TrimSpace(cfg.ClientName)
	if clientName == "" {
		clientName = "stravin-relay"
	}
	timeout := cfg.PublishTimeout
	if timeout <= 0 {
		timeout = defaultPublishTimeout
	}

	nc, err := nats.Connect(url,
		nats.Name(clientName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond),
	)
	if err != nil {
		return nil, fmt.Errorf("relay: connect nats %q: %w", url, err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("relay: init jetstream: %w", err)
	}

	return &JetStreamPublisher{
		nc:             nc,
		js:             js,
		publishTimeout: timeout,
	}, nil
}

// Publish sends msg to NATS JetStream with headers and deterministic MsgID.
func (p *JetStreamPublisher) Publish(ctx context.Context, msg Message) (PublishAck, error) {
	if strings.TrimSpace(msg.Subject) == "" {
		return PublishAck{}, errors.New("relay: empty NATS subject")
	}

	pubCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		pubCtx, cancel = context.WithTimeout(ctx, p.publishTimeout)
		defer cancel()
	}

	natsMsg := nats.NewMsg(msg.Subject)
	natsMsg.Data = msg.Payload
	for k, v := range msg.Headers {
		if k != "" && v != "" {
			natsMsg.Header.Set(k, v)
		}
	}
	if msg.MsgID != "" {
		natsMsg.Header.Set(HeaderNatsMsgID, msg.MsgID)
	}

	ack, err := p.js.PublishMsg(pubCtx, natsMsg)
	if err != nil {
		return PublishAck{}, fmt.Errorf("relay: jetstream publish %q: %w", msg.Subject, err)
	}
	return PublishAck{
		Stream:    ack.Stream,
		Sequence:  ack.Sequence,
		Duplicate: ack.Duplicate,
	}, nil
}

// Close drains and closes the underlying NATS connection.
func (p *JetStreamPublisher) Close() error {
	if p == nil || p.nc == nil {
		return nil
	}
	if err := p.nc.Drain(); err != nil {
		p.nc.Close()
		return err
	}
	return nil
}

// NoopPublisher gracefully drops publishes when STRAVIN_NATS_URL is unconfigured.
type NoopPublisher struct {
	logger *slog.Logger
}

// NewNoopPublisher returns a Publisher that logs and succeeds without NATS connectivity.
func NewNoopPublisher(logger *slog.Logger) *NoopPublisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &NoopPublisher{logger: logger}
}

// Publish logs the skipped outbox message and returns a synthetic ack.
func (n *NoopPublisher) Publish(ctx context.Context, msg Message) (PublishAck, error) {
	n.logger.DebugContext(ctx, "NATS unconfigured; skipping JetStream publish",
		"subject", msg.Subject,
		"msg_id", msg.MsgID,
	)
	return PublishAck{Stream: "noop"}, nil
}

// Close is a no-op for NoopPublisher.
func (n *NoopPublisher) Close() error { return nil }
