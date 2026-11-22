package telemetry

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// TraceHandler decorates a slog.Handler (GoF Decorator/Bridge) to inject
// OpenTelemetry trace_id and span_id fields from context.Context into every log record.
type TraceHandler struct {
	inner slog.Handler
}

// NewTraceHandler wraps inner so active OTel spans automatically correlate with logs.
func NewTraceHandler(inner slog.Handler) *TraceHandler {
	return &TraceHandler{inner: inner}
}

// NewLogger constructs a structured JSON slog.Logger writing to w with OTel trace correlation.
func NewLogger(w io.Writer, level slog.Leveler) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	base := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(NewTraceHandler(base))
}

func (h *TraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		cloned := r.Clone()
		cloned.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
		return h.inner.Handle(ctx, cloned)
	}
	return h.inner.Handle(ctx, r)
}

func (h *TraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &TraceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *TraceHandler) WithGroup(name string) slog.Handler {
	return &TraceHandler{inner: h.inner.WithGroup(name)}
}

// FanoutHandler dispatches slog records to multiple underlying handlers
// (for example, local trace-correlated JSON output plus an OTLP log bridge).
type FanoutHandler struct {
	handlers []slog.Handler
}

// NewFanoutHandler returns a slog.Handler that forwards each enabled record to all non-nil handlers.
func NewFanoutHandler(handlers ...slog.Handler) *FanoutHandler {
	filtered := make([]slog.Handler, 0, len(handlers))
	for _, h := range handlers {
		if h != nil {
			filtered = append(filtered, h)
		}
	}
	return &FanoutHandler{handlers: filtered}
}

func (f *FanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, h := range f.handlers {
		if h.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (f *FanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range f.handlers {
		if h.Enabled(ctx, r.Level) {
			if err := h.Handle(ctx, r.Clone()); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (f *FanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithAttrs(attrs)
	}
	return &FanoutHandler{handlers: next}
}

func (f *FanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(f.handlers))
	for i, h := range f.handlers {
		next[i] = h.WithGroup(name)
	}
	return &FanoutHandler{handlers: next}
}

type levelFilterHandler struct {
	level slog.Leveler
	inner slog.Handler
}

func newLevelFilterHandler(level slog.Leveler, inner slog.Handler) slog.Handler {
	if level == nil {
		return inner
	}
	return &levelFilterHandler{level: level, inner: inner}
}

func (h *levelFilterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.level.Level() && h.inner.Enabled(ctx, level)
}

func (h *levelFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level < h.level.Level() {
		return nil
	}
	return h.inner.Handle(ctx, r)
}

func (h *levelFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelFilterHandler{level: h.level, inner: h.inner.WithAttrs(attrs)}
}

func (h *levelFilterHandler) WithGroup(name string) slog.Handler {
	return &levelFilterHandler{level: h.level, inner: h.inner.WithGroup(name)}
}
