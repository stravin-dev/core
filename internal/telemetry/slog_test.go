package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"stravin.dev/core/internal/telemetry"
)

func TestTraceHandler_InjectsTraceAndSpanID(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-node")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, slog.LevelInfo)

	spanCtx, span := prov.Tracer("stravin.node").Start(ctx, "node.pack.index_validate")
	wantTraceID := span.SpanContext().TraceID().String()
	wantSpanID := span.SpanContext().SpanID().String()

	logger.InfoContext(spanCtx, "validated pack", "repo_id", "repo-1")
	span.End()

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to decode JSON log: %v (raw: %s)", err, buf.String())
	}

	if record["trace_id"] != wantTraceID {
		t.Errorf("trace_id = %v, want %s", record["trace_id"], wantTraceID)
	}
	if record["span_id"] != wantSpanID {
		t.Errorf("span_id = %v, want %s", record["span_id"], wantSpanID)
	}
	if record["repo_id"] != "repo-1" {
		t.Errorf("repo_id = %v, want repo-1", record["repo_id"])
	}
}

func TestTraceHandler_NoSpanOmitsTraceFields(t *testing.T) {
	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, slog.LevelInfo)

	logger.InfoContext(context.Background(), "startup complete")

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to decode JSON log: %v", err)
	}
	if _, ok := record["trace_id"]; ok {
		t.Errorf("did not expect trace_id without active span, got %v", record["trace_id"])
	}
	if _, ok := record["span_id"]; ok {
		t.Errorf("did not expect span_id without active span, got %v", record["span_id"])
	}
}

func TestTraceHandler_PreservesDecoratorThroughWithAttrsAndWithGroup(t *testing.T) {
	ctx := context.Background()
	prov, err := telemetry.NewInMemoryProvider("stravin-hook")
	if err != nil {
		t.Fatalf("NewInMemoryProvider: %v", err)
	}
	defer func() { _ = prov.Shutdown(ctx) }()

	var buf bytes.Buffer
	logger := telemetry.NewLogger(&buf, slog.LevelInfo).
		With("component", "streams").
		WithGroup("hook")

	spanCtx, span := prov.Tracer("stravin.hook").Start(ctx, "hook.async.dispatch")
	wantTraceID := span.SpanContext().TraceID().String()
	logger.InfoContext(spanCtx, "dispatched", "hook_id", "h-1")
	span.End()

	var record map[string]any
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("decode log: %v", err)
	}
	// Trace fields may sit at top-level or inside group depending on slog.Handler;
	// verify trace_id is present in the serialized JSON payload.
	if !bytes.Contains(buf.Bytes(), []byte(`"trace_id":"`+wantTraceID+`"`)) {
		t.Errorf("expected trace_id %q in JSON log, got: %s", wantTraceID, buf.String())
	}
}

func TestFanoutHandler_DispatchesToAllEnabledHandlers(t *testing.T) {
	var bufA, bufB bytes.Buffer
	hA := telemetry.NewTraceHandler(slog.NewJSONHandler(&bufA, &slog.HandlerOptions{Level: slog.LevelInfo}))
	hB := telemetry.NewTraceHandler(slog.NewJSONHandler(&bufB, &slog.HandlerOptions{Level: slog.LevelWarn}))

	logger := slog.New(telemetry.NewFanoutHandler(hA, hB)).With("service", "stravin-edge").WithGroup("req")
	logger.Info("info-only", "id", 1)
	logger.Warn("warn-both", "id", 2)

	if !bytes.Contains(bufA.Bytes(), []byte("info-only")) || !bytes.Contains(bufA.Bytes(), []byte("warn-both")) {
		t.Errorf("bufA missing expected records: %s", bufA.String())
	}
	if bytes.Contains(bufB.Bytes(), []byte("info-only")) {
		t.Errorf("bufB should not contain info-level record: %s", bufB.String())
	}
	if !bytes.Contains(bufB.Bytes(), []byte("warn-both")) {
		t.Errorf("bufB missing warn-level record: %s", bufB.String())
	}
}
