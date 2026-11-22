package telemetry

import (
	"context"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

const (
	headerTraceparent = "traceparent"
	headerTracestate  = "tracestate"
)

// NewPropagator returns Stravin's composite W3C TraceContext + Baggage propagator.
func NewPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

// GRPCMetadataCarrier adapts gRPC metadata.MD to propagation.TextMapCarrier (GoF Adapter).
type GRPCMetadataCarrier struct {
	MD metadata.MD
}

func (c GRPCMetadataCarrier) Get(key string) string {
	vals := c.MD.Get(strings.ToLower(key))
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

func (c GRPCMetadataCarrier) Set(key, val string) {
	c.MD.Set(strings.ToLower(key), val)
}

func (c GRPCMetadataCarrier) Keys() []string {
	keys := make([]string, 0, len(c.MD))
	for k := range c.MD {
		keys = append(keys, k)
	}
	return keys
}

// InjectGRPC injects W3C traceparent/tracestate/baggage from ctx into outgoing gRPC metadata.
func InjectGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		md = md.Copy()
	} else {
		md = metadata.New(nil)
	}
	NewPropagator().Inject(ctx, GRPCMetadataCarrier{MD: md})
	return metadata.NewOutgoingContext(ctx, md)
}

// ExtractGRPC extracts W3C trace context and baggage from incoming gRPC metadata on ctx.
func ExtractGRPC(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	return NewPropagator().Extract(ctx, GRPCMetadataCarrier{MD: md})
}

// InjectHTTP injects W3C trace context and baggage into HTTP headers.
func InjectHTTP(ctx context.Context, h http.Header) {
	NewPropagator().Inject(ctx, propagation.HeaderCarrier(h))
}

// ExtractHTTP extracts W3C trace context and baggage from HTTP headers into ctx.
func ExtractHTTP(ctx context.Context, h http.Header) context.Context {
	return NewPropagator().Extract(ctx, propagation.HeaderCarrier(h))
}

// TraceparentFromContext serializes the active span context into a W3C traceparent
// string suitable for storing on a DynamoDB Ref History outbox item.
func TraceparentFromContext(ctx context.Context) string {
	tp, _ := TraceContextFromContext(ctx)
	return tp
}

// TraceContextFromContext serializes both W3C traceparent and tracestate from ctx.
func TraceContextFromContext(ctx context.Context) (traceparent, tracestate string) {
	carrier := propagation.MapCarrier{}
	NewPropagator().Inject(ctx, carrier)
	return carrier.Get(headerTraceparent), carrier.Get(headerTracestate)
}

// ContextWithTraceparent extracts a remote span context from a persisted W3C traceparent string.
func ContextWithTraceparent(ctx context.Context, traceparent string) context.Context {
	return ContextWithTraceContext(ctx, traceparent, "")
}

// ContextWithTraceContext extracts a remote span context from persisted traceparent/tracestate strings.
func ContextWithTraceContext(ctx context.Context, traceparent, tracestate string) context.Context {
	if traceparent == "" {
		return ctx
	}
	carrier := propagation.MapCarrier{
		headerTraceparent: traceparent,
	}
	if tracestate != "" {
		carrier[headerTracestate] = tracestate
	}
	return NewPropagator().Extract(ctx, carrier)
}

// SpanLinkFromTraceparent builds an OTel trace.Link from a persisted Ref History
// traceparent string for asynchronous DynamoDB Streams consumers such as stravin-hook.
func SpanLinkFromTraceparent(traceparent string) (trace.Link, bool) {
	if traceparent == "" {
		return trace.Link{}, false
	}
	extracted := ContextWithTraceparent(context.Background(), traceparent)
	sc := trace.SpanContextFromContext(extracted)
	if !sc.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}
