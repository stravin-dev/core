package telemetry_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"

	"stravin.dev/core/internal/telemetry"
)

func TestPropagation_GRPCMetadataRoundTrip(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	tracer := prov.Tracer("stravin.edge")
	spanCtx, span := tracer.Start(ctx, "edge.ingress")
	wantSC := span.SpanContext()
	defer span.End()

	outCtx := telemetry.InjectGRPC(spanCtx)
	md, ok := metadata.FromOutgoingContext(outCtx)
	if !ok {
		t.Fatal("expected outgoing gRPC metadata")
	}
	if vals := md.Get("traceparent"); len(vals) == 0 {
		t.Fatal("expected traceparent header in outgoing gRPC metadata")
	}

	inCtx := metadata.NewIncomingContext(context.Background(), md)
	extractedCtx := telemetry.ExtractGRPC(inCtx)
	gotSC := trace.SpanContextFromContext(extractedCtx)

	if gotSC.TraceID() != wantSC.TraceID() {
		t.Errorf("TraceID = %s, want %s", gotSC.TraceID(), wantSC.TraceID())
	}
	if gotSC.SpanID() != wantSC.SpanID() {
		t.Errorf("SpanID = %s, want %s", gotSC.SpanID(), wantSC.SpanID())
	}
}

func TestPropagation_HTTPRoundTrip(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-edge")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	spanCtx, span := prov.Tracer("stravin.edge").Start(ctx, "edge.http")
	wantSC := span.SpanContext()
	defer span.End()

	headers := make(http.Header)
	telemetry.InjectHTTP(spanCtx, headers)
	if headers.Get("Traceparent") == "" {
		t.Fatal("expected Traceparent HTTP header")
	}

	extractedCtx := telemetry.ExtractHTTP(context.Background(), headers)
	gotSC := trace.SpanContextFromContext(extractedCtx)
	if gotSC.TraceID() != wantSC.TraceID() || gotSC.SpanID() != wantSC.SpanID() {
		t.Errorf("HTTP extracted span context mismatch: got (%s, %s), want (%s, %s)",
			gotSC.TraceID(), gotSC.SpanID(), wantSC.TraceID(), wantSC.SpanID())
	}
}

func TestPropagation_DynamoDBRefHistoryStringCarrier(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	spanCtx, span := prov.Tracer("stravin.node").Start(ctx, "ddb.transact")
	wantSC := span.SpanContext()
	defer span.End()

	tp := telemetry.TraceparentFromContext(spanCtx)
	if !strings.HasPrefix(tp, "00-") {
		t.Fatalf("expected W3C traceparent starting with '00-', got %q", tp)
	}

	restoredCtx := telemetry.ContextWithTraceparent(context.Background(), tp)
	gotSC := trace.SpanContextFromContext(restoredCtx)
	if gotSC.TraceID() != wantSC.TraceID() || gotSC.SpanID() != wantSC.SpanID() {
		t.Errorf("restored span context = (%s, %s), want (%s, %s)",
			gotSC.TraceID(), gotSC.SpanID(), wantSC.TraceID(), wantSC.SpanID())
	}

	link, ok := telemetry.SpanLinkFromTraceparent(tp)
	if !ok {
		t.Fatal("expected valid SpanLink from traceparent")
	}
	if link.SpanContext.TraceID() != wantSC.TraceID() {
		t.Errorf("link TraceID = %s, want %s", link.SpanContext.TraceID(), wantSC.TraceID())
	}
}

func TestPropagation_EmptyTraceparentIsNoop(t *testing.T) {
	if _, ok := telemetry.SpanLinkFromTraceparent(""); ok {
		t.Fatal("expected ok=false for empty traceparent")
	}
	restored := telemetry.ContextWithTraceparent(context.Background(), "")
	if trace.SpanContextFromContext(restored).IsValid() {
		t.Fatal("expected invalid span context for empty traceparent")
	}
}
