package main

import (
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"stravin.dev/core/internal/loadgen"
)

type testScenario struct {
	name    string
	persona loadgen.Persona
	runs    *atomic.Int64
	fail    bool
}

func (s *testScenario) Name() string             { return s.name }
func (s *testScenario) Persona() loadgen.Persona { return s.persona }
func (s *testScenario) Weight() int              { return 10 }
func (s *testScenario) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	s.runs.Add(1)
	env.Logger.InfoContext(ctx, "executed scenario", "scenario", s.name)
	if s.fail {
		return &loadgen.GitError{Op: "push", ExitCode: 128, HTTPStatus: 500, Stderr: "HTTP 500"}
	}
	return nil
}

func TestMainCtrlCTriggersImmediateCancelAndFullOTelFlush(t *testing.T) {
	var runs atomic.Int64
	var telBuf bytes.Buffer
	tel, exp, err := loadgen.NewInMemoryTelemetry(&telBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}

	reg := loadgen.NewRegistry()
	_ = reg.Register(&testScenario{name: "dev-smoke", persona: loadgen.PersonaDev, runs: &runs})
	_ = reg.Register(&testScenario{name: "agent-smoke", persona: loadgen.PersonaAgent, runs: &runs})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		// Wait until at least one scenario has executed, then simulate Ctrl+C.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && runs.Load() == 0 {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()

	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := run(ctx, []string{
		"--devs=2",
		"--agents=2",
		"--seed=99",
		"--think-min=5s",
		"--think-max=10s",
		"--flush-timeout=2s",
		"--otel-endpoint=",
	}, runtimeDeps{
		newTelemetry: func(context.Context, string) (*loadgen.Telemetry, error) { return tel, nil },
		newRegistry:  func() *loadgen.Registry { return reg },
	}, &stdout, &stderr)
	elapsed := time.Since(start)

	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr=%s", code, stderr.String())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("Ctrl+C shutdown took %v, want fast abort of 5s-10s think-time", elapsed)
	}
	if len(exp.SpanExporter.Spans()) == 0 {
		t.Fatal("expected spans flushed before exit")
	}
	if !exp.SpanExporter.WasShutdown() {
		t.Fatal("expected OTel TracerProvider Shutdown to be called on Ctrl+C")
	}
	if len(exp.LogExporter.Records()) == 0 {
		t.Fatal("expected OTLP logs flushed before exit")
	}
	if !exp.LogExporter.WasShutdown() {
		t.Fatal("expected OTel LoggerProvider Shutdown to be called on Ctrl+C")
	}

	var summary map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &summary); err != nil {
		t.Fatalf("stdout not valid JSON summary: %v; out=%q", err, stdout.String())
	}
}

func TestMainReturnsNonZeroOnUnexpectedError(t *testing.T) {
	var runs atomic.Int64
	var telBuf bytes.Buffer
	tel, _, err := loadgen.NewInMemoryTelemetry(&telBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}

	reg := loadgen.NewRegistry()
	_ = reg.Register(&testScenario{name: "dev-fail", persona: loadgen.PersonaDev, runs: &runs, fail: true})

	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{
		"--devs=1",
		"--agents=0",
		"--fail-fast",
		"--otel-endpoint=",
	}, runtimeDeps{
		newTelemetry: func(context.Context, string) (*loadgen.Telemetry, error) { return tel, nil },
		newRegistry:  func() *loadgen.Registry { return reg },
	}, &stdout, &stderr)

	if code == 0 {
		t.Fatalf("expected non-zero exit code on unexpected error, got 0; stdout=%s", stdout.String())
	}
}

func TestDefaultRegistryHas22ScenariosAndCLIFiltering(t *testing.T) {
	deps := defaultDeps(&bytes.Buffer{})
	reg := deps.newRegistry()

	devs := reg.Scenarios(loadgen.PersonaDev)
	agents := reg.Scenarios(loadgen.PersonaAgent)
	if len(devs) != 11 || len(agents) != 11 {
		t.Fatalf("default registry counts: dev=%d agent=%d, want 11 and 11", len(devs), len(agents))
	}

	filtered, err := reg.Filter([]string{"dev_clone_and_read", "agent_corrupt_ref_negative_test"})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if len(filtered.Scenarios(loadgen.PersonaDev)) != 1 || len(filtered.Scenarios(loadgen.PersonaAgent)) != 1 {
		t.Fatalf("filtered counts: dev=%d agent=%d, want 1 and 1",
			len(filtered.Scenarios(loadgen.PersonaDev)), len(filtered.Scenarios(loadgen.PersonaAgent)))
	}
}
