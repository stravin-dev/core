package loadgen

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"
)

type stubScenario struct {
	name    string
	persona Persona
	weight  int
	fn      func(ctx context.Context, env *ScenarioEnv) error
}

func (s *stubScenario) Name() string                                    { return s.name }
func (s *stubScenario) Persona() Persona                                { return s.persona }
func (s *stubScenario) Weight() int                                     { return s.weight }
func (s *stubScenario) Run(ctx context.Context, env *ScenarioEnv) error { return s.fn(ctx, env) }

func TestRegistryWeightedSelectionDeterminism(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&stubScenario{name: "dev-a", persona: PersonaDev, weight: 1, fn: func(context.Context, *ScenarioEnv) error { return nil }})
	_ = reg.Register(&stubScenario{name: "dev-b", persona: PersonaDev, weight: 9, fn: func(context.Context, *ScenarioEnv) error { return nil }})
	_ = reg.Register(&stubScenario{name: "agent-a", persona: PersonaAgent, weight: 5, fn: func(context.Context, *ScenarioEnv) error { return nil }})

	rng1 := rand.New(rand.NewPCG(77, 99))
	rng2 := rand.New(rand.NewPCG(77, 99))

	counts := make(map[string]int)
	for i := 0; i < 200; i++ {
		s1, ok1 := reg.Select(PersonaDev, rng1)
		s2, ok2 := reg.Select(PersonaDev, rng2)
		if !ok1 || !ok2 {
			t.Fatal("Select returned !ok")
		}
		if s1.Name() != s2.Name() {
			t.Fatalf("iteration %d: non-deterministic selection %q vs %q", i, s1.Name(), s2.Name())
		}
		counts[s1.Name()]++
	}
	if counts["dev-b"] <= counts["dev-a"] {
		t.Errorf("expected weighted selection to favor dev-b (9) over dev-a (1); counts=%v", counts)
	}
}

func TestRegistryFilterAndValidation(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(&stubScenario{name: "dev-1", persona: PersonaDev, weight: 1, fn: func(context.Context, *ScenarioEnv) error { return nil }}); err != nil {
		t.Fatalf("Register dev-1: %v", err)
	}
	if err := reg.Register(&stubScenario{name: "dev-1", persona: PersonaDev, weight: 1, fn: func(context.Context, *ScenarioEnv) error { return nil }}); err == nil {
		t.Fatal("expected duplicate scenario registration error")
	}
	if err := reg.Register(&stubScenario{name: "agent-1", persona: PersonaAgent, weight: 1, fn: func(context.Context, *ScenarioEnv) error { return nil }}); err != nil {
		t.Fatalf("Register agent-1: %v", err)
	}

	filtered, err := reg.Filter([]string{"dev-1"})
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	if len(filtered.Scenarios(PersonaDev)) != 1 || len(filtered.Scenarios(PersonaAgent)) != 0 {
		t.Fatalf("unexpected filtered counts: dev=%d agent=%d", len(filtered.Scenarios(PersonaDev)), len(filtered.Scenarios(PersonaAgent)))
	}
	if _, err := reg.Filter([]string{"does-not-exist"}); err == nil {
		t.Fatal("expected error when filtering unknown scenario")
	}
}

func TestRunnerParallelDevAndAgentWorkersAndCancellation(t *testing.T) {
	var devRuns atomic.Int64
	var agentRuns atomic.Int64

	reg := NewRegistry()
	_ = reg.Register(&stubScenario{
		name:    "dev-work",
		persona: PersonaDev,
		weight:  10,
		fn: func(ctx context.Context, env *ScenarioEnv) error {
			devRuns.Add(1)
			env.Logger.InfoContext(ctx, "dev tick", "worker", env.WorkerID)
			return nil
		},
	})
	_ = reg.Register(&stubScenario{
		name:    "agent-work",
		persona: PersonaAgent,
		weight:  10,
		fn: func(ctx context.Context, env *ScenarioEnv) error {
			agentRuns.Add(1)
			env.Logger.InfoContext(ctx, "agent tick", "worker", env.WorkerID)
			return nil
		},
	})

	cfg := DefaultConfig()
	cfg.Devs = 3
	cfg.Agents = 4
	cfg.Seed = 12345
	cfg.ThinkMin = 5 * time.Millisecond
	cfg.ThinkMax = 15 * time.Millisecond
	cfg.Duration = 120 * time.Millisecond

	var logBuf bytes.Buffer
	tel, exp, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	oracle := NewOracle()

	runner, err := NewRunner(cfg, reg, tel, oracle)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	res := runner.Run(context.Background())
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Fatalf("tel.Shutdown: %v", err)
	}

	if devRuns.Load() == 0 || agentRuns.Load() == 0 {
		t.Fatalf("expected both dev (%d) and agent (%d) workers to execute", devRuns.Load(), agentRuns.Load())
	}
	if res.Oracle.HasFailures() {
		t.Fatalf("expected 0 failures, got %+v", res.Oracle)
	}
	if len(exp.SpanExporter.Spans()) == 0 {
		t.Fatal("expected exported spans from scenario runs")
	}
	if len(exp.LogExporter.Records()) == 0 {
		t.Fatal("expected exported OTLP log records from scenario runs")
	}
}

func TestRunnerFailFastOnUnexpectedError(t *testing.T) {
	reg := NewRegistry()
	_ = reg.Register(&stubScenario{
		name:    "dev-boom",
		persona: PersonaDev,
		weight:  10,
		fn: func(ctx context.Context, env *ScenarioEnv) error {
			return &GitError{Op: "push", ExitCode: 128, HTTPStatus: 500, Stderr: "HTTP 500"}
		},
	})

	cfg := DefaultConfig()
	cfg.Devs = 2
	cfg.Agents = 0
	cfg.Seed = 1
	cfg.ThinkMin = 5 * time.Second
	cfg.ThinkMax = 10 * time.Second
	cfg.FailFast = true

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	oracle := NewOracle()
	runner, err := NewRunner(cfg, reg, tel, oracle)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	start := time.Now()
	res := runner.Run(context.Background())
	elapsed := time.Since(start)

	if !res.Oracle.HasFailures() || res.Oracle.UnexpectedErrors == 0 {
		t.Fatalf("expected unexpected error recorded, got %+v", res.Oracle)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("FailFast took too long (%v); should cancel think-time immediately", elapsed)
	}
	_ = errors.New
}
