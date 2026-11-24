package loadgen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

type stubSSHScenario struct {
	stubScenario
}

func (s *stubSSHScenario) RequiresSSH() bool { return true }

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
	cfg.RampUp = 0

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
	cfg.RampUp = 0
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

func TestNewRunnerSkipsSSHScenariosWithoutSSHKey(t *testing.T) {
	noop := func(context.Context, *ScenarioEnv) error { return nil }
	makeRegistry := func() *Registry {
		reg := NewRegistry()
		reg.MustRegister(&stubScenario{name: "dev-http", persona: PersonaDev, weight: 10, fn: noop})
		reg.MustRegister(&stubSSHScenario{stubScenario: stubScenario{name: "dev-ssh", persona: PersonaDev, weight: 10, fn: noop}})
		reg.MustRegister(&stubScenario{name: "agent-http", persona: PersonaAgent, weight: 10, fn: noop})
		reg.MustRegister(&stubSSHScenario{stubScenario: stubScenario{name: "agent-ssh", persona: PersonaAgent, weight: 10, fn: noop}})
		return reg
	}

	tests := []struct {
		name           string
		sshKey         string
		sshAddr        string
		scenarios      []string
		devs           int
		agents         int
		wantDevCount   int
		wantAgentCount int
	}{
		{
			name:           "skips ssh scenarios when ssh-key empty",
			sshKey:         "",
			sshAddr:        "127.0.0.1:2222",
			devs:           1,
			agents:         1,
			wantDevCount:   1,
			wantAgentCount: 1,
		},
		{
			name:           "keeps ssh scenarios when ssh-key set",
			sshKey:         "/tmp/id_ed25519",
			sshAddr:        "127.0.0.1:2222",
			devs:           1,
			agents:         1,
			wantDevCount:   2,
			wantAgentCount: 2,
		},
		{
			name:           "keeps ssh scenarios when ssh-addr uses file scheme",
			sshKey:         "",
			sshAddr:        "file:///tmp/repos",
			devs:           1,
			agents:         1,
			wantDevCount:   2,
			wantAgentCount: 2,
		},
		{
			name:           "keeps ssh scenario when explicitly selected",
			sshKey:         "",
			sshAddr:        "127.0.0.1:2222",
			scenarios:      []string{"dev-ssh"},
			devs:           1,
			agents:         0,
			wantDevCount:   1,
			wantAgentCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Devs = tc.devs
			cfg.Agents = tc.agents
			cfg.SSHKey = tc.sshKey
			cfg.SSHAddr = tc.sshAddr
			cfg.Scenarios = tc.scenarios

			var logBuf bytes.Buffer
			tel, _, err := NewInMemoryTelemetry(&logBuf)
			if err != nil {
				t.Fatalf("NewInMemoryTelemetry: %v", err)
			}
			defer func() { _ = tel.Shutdown(context.Background()) }()

			runner, err := NewRunner(cfg, makeRegistry(), tel, NewOracle())
			if err != nil {
				t.Fatalf("NewRunner: %v", err)
			}
			if got := len(runner.registry.Scenarios(PersonaDev)); got != tc.wantDevCount {
				t.Errorf("dev scenarios = %d, want %d", got, tc.wantDevCount)
			}
			if got := len(runner.registry.Scenarios(PersonaAgent)); got != tc.wantAgentCount {
				t.Errorf("agent scenarios = %d, want %d", got, tc.wantAgentCount)
			}
		})
	}
}

func TestRunnerClassifiesPerCommandTimeoutAsTransientNotCanceled(t *testing.T) {
	var runs atomic.Int64
	reg := NewRegistry()
	reg.MustRegister(&stubScenario{
		name:    "dev-timeout",
		persona: PersonaDev,
		weight:  10,
		fn: func(ctx context.Context, env *ScenarioEnv) error {
			runs.Add(1)
			return &GitError{
				Op:       "clone",
				ExitCode: -1,
				Err:      fmt.Errorf("git clone timed out after 30s: %w", context.DeadlineExceeded),
			}
		},
	})

	cfg := DefaultConfig()
	cfg.Devs = 1
	cfg.Agents = 0
	cfg.Seed = 7
	cfg.ThinkMin = 5 * time.Millisecond
	cfg.ThinkMax = 10 * time.Millisecond
	cfg.Duration = 60 * time.Millisecond
	cfg.FailFast = true

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	res := runner.Run(context.Background())
	if runs.Load() == 0 {
		t.Fatal("expected scenario to execute at least once")
	}
	if res.Oracle.TotalOps == 0 {
		t.Fatalf("expected TotalOps > 0 (classified as retryable_transient, not canceled); got %+v", res.Oracle)
	}
	if res.Oracle.HasFailures() {
		t.Fatalf("expected 0 unexpected failures for per-command timeout, got %+v", res.Oracle)
	}
}

func TestBaseCloneCacheIsolationAndSanitization(t *testing.T) {
	baseRoot := filepath.Join(t.TempDir(), "base")
	cache := NewBaseCloneCache(baseRoot)

	if dir, ok := cache.Get("repo-1"); ok || dir != "" {
		t.Fatalf("expected cache miss, got (%q, %v)", dir, ok)
	}

	dir1 := cache.Dir("repo-1")
	dir2 := cache.Dir("org/repo-2")
	dirTraversal := cache.Dir("../../evil")
	if !strings.HasPrefix(dir1, baseRoot+string(filepath.Separator)) {
		t.Fatalf("dir1 %q escaped baseRoot %q", dir1, baseRoot)
	}
	if !strings.HasPrefix(dir2, baseRoot+string(filepath.Separator)) {
		t.Fatalf("dir2 %q escaped baseRoot %q", dir2, baseRoot)
	}
	if !strings.HasPrefix(dirTraversal, baseRoot+string(filepath.Separator)) {
		t.Fatalf("dirTraversal %q escaped baseRoot %q", dirTraversal, baseRoot)
	}
	if dir1 == dir2 {
		t.Fatalf("distinct repos collided on %q", dir1)
	}

	cache.Set("repo-1", dir1)
	got, ok := cache.Get("repo-1")
	if !ok || got != dir1 {
		t.Fatalf("Get(repo-1) = (%q, %v), want (%q, true)", got, ok, dir1)
	}

	cache.Delete("repo-1")
	if _, ok := cache.Get("repo-1"); ok {
		t.Fatal("expected cache miss after Delete")
	}
}

func TestRunnerProvidesIsolatedPerWorkerBaseCloneCache(t *testing.T) {
	var mu sync.Mutex
	cachesByWorker := make(map[string][]*BaseCloneCache)

	reg := NewRegistry()
	record := func(ctx context.Context, env *ScenarioEnv) error {
		if env.BaseClones == nil {
			return errors.New("expected non-nil env.BaseClones")
		}
		key := fmt.Sprintf("%s-%d", env.Persona, env.WorkerID)
		mu.Lock()
		cachesByWorker[key] = append(cachesByWorker[key], env.BaseClones)
		mu.Unlock()
		return nil
	}
	reg.MustRegister(&stubScenario{name: "dev-cache", persona: PersonaDev, weight: 10, fn: record})
	reg.MustRegister(&stubScenario{name: "agent-cache", persona: PersonaAgent, weight: 10, fn: record})

	cfg := DefaultConfig()
	cfg.Devs = 2
	cfg.Agents = 2
	cfg.Seed = 42
	cfg.ThinkMin = 1 * time.Millisecond
	cfg.ThinkMax = 2 * time.Millisecond
	cfg.Duration = 80 * time.Millisecond
	cfg.RampUp = 0

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res := runner.Run(context.Background())
	if res.Oracle.HasFailures() {
		t.Fatalf("unexpected failures: %+v", res.Failures)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(cachesByWorker) != 4 {
		t.Fatalf("expected 4 distinct workers, got %d", len(cachesByWorker))
	}
	seenPtrs := make(map[*BaseCloneCache]string)
	for workerKey, caches := range cachesByWorker {
		if len(caches) < 2 {
			t.Fatalf("worker %s only ran %d time(s); want >= 2 to verify reuse", workerKey, len(caches))
		}
		first := caches[0]
		for _, c := range caches[1:] {
			if c != first {
				t.Fatalf("worker %s received different BaseCloneCache instances across scenarios", workerKey)
			}
		}
		if other, dup := seenPtrs[first]; dup {
			t.Fatalf("workers %s and %s shared the same BaseCloneCache instance", workerKey, other)
		}
		seenPtrs[first] = workerKey
	}
}

func TestBuildWorkerSchedule(t *testing.T) {
	t.Run("single dev worker has zero delay", func(t *testing.T) {
		slots := buildWorkerSchedule(1, 0, 5*time.Second)
		if len(slots) != 1 {
			t.Fatalf("len(slots) = %d, want 1", len(slots))
		}
		want := workerSlot{persona: PersonaDev, workerID: 0, delay: 0}
		if slots[0] != want {
			t.Fatalf("slots[0] = %+v, want %+v", slots[0], want)
		}
	})

	t.Run("single agent worker has zero delay", func(t *testing.T) {
		slots := buildWorkerSchedule(0, 1, 5*time.Second)
		if len(slots) != 1 {
			t.Fatalf("len(slots) = %d, want 1", len(slots))
		}
		want := workerSlot{persona: PersonaAgent, workerID: 0, delay: 0}
		if slots[0] != want {
			t.Fatalf("slots[0] = %+v, want %+v", slots[0], want)
		}
	})

	t.Run("zero ramp-up gives zero delay for all workers", func(t *testing.T) {
		slots := buildWorkerSchedule(2, 3, 0)
		if len(slots) != 5 {
			t.Fatalf("len(slots) = %d, want 5", len(slots))
		}
		for i, s := range slots {
			if s.delay != 0 {
				t.Errorf("slots[%d].delay = %v, want 0", i, s.delay)
			}
		}
	})

	t.Run("even slot spacing across 5s window", func(t *testing.T) {
		slots := buildWorkerSchedule(2, 3, 5*time.Second)
		if len(slots) != 5 {
			t.Fatalf("len(slots) = %d, want 5", len(slots))
		}
		wantDelays := []time.Duration{0, 1 * time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second}
		for i, want := range wantDelays {
			if slots[i].delay != want {
				t.Errorf("slots[%d].delay = %v, want %v", i, slots[i].delay, want)
			}
		}
	})

	t.Run("equal personas alternate cleanly", func(t *testing.T) {
		slots := buildWorkerSchedule(3, 3, 6*time.Second)
		want := []workerSlot{
			{persona: PersonaDev, workerID: 0, delay: 0},
			{persona: PersonaAgent, workerID: 0, delay: 1 * time.Second},
			{persona: PersonaDev, workerID: 1, delay: 2 * time.Second},
			{persona: PersonaAgent, workerID: 1, delay: 3 * time.Second},
			{persona: PersonaDev, workerID: 2, delay: 4 * time.Second},
			{persona: PersonaAgent, workerID: 2, delay: 5 * time.Second},
		}
		if len(slots) != len(want) {
			t.Fatalf("len(slots) = %d, want %d", len(slots), len(want))
		}
		for i := range want {
			if slots[i] != want[i] {
				t.Errorf("slots[%d] = %+v, want %+v", i, slots[i], want[i])
			}
		}
	})

	t.Run("unequal personas interleave proportionally across full window", func(t *testing.T) {
		slots := buildWorkerSchedule(2, 4, 6*time.Second)
		want := []workerSlot{
			{persona: PersonaDev, workerID: 0, delay: 0},
			{persona: PersonaAgent, workerID: 0, delay: 1 * time.Second},
			{persona: PersonaAgent, workerID: 1, delay: 2 * time.Second},
			{persona: PersonaDev, workerID: 1, delay: 3 * time.Second},
			{persona: PersonaAgent, workerID: 2, delay: 4 * time.Second},
			{persona: PersonaAgent, workerID: 3, delay: 5 * time.Second},
		}
		if len(slots) != len(want) {
			t.Fatalf("len(slots) = %d, want %d", len(slots), len(want))
		}
		for i := range want {
			if slots[i] != want[i] {
				t.Errorf("slots[%d] = %+v, want %+v", i, slots[i], want[i])
			}
		}
	})
}

func TestRunnerRampUpStaggersWorkerStartTimes(t *testing.T) {
	var mu sync.Mutex
	firstStart := make(map[string]time.Duration)
	var runStart time.Time

	record := func(_ context.Context, env *ScenarioEnv) error {
		key := fmt.Sprintf("%s-%d", env.Persona, env.WorkerID)
		mu.Lock()
		if _, exists := firstStart[key]; !exists {
			firstStart[key] = time.Since(runStart)
		}
		mu.Unlock()
		return nil
	}

	reg := NewRegistry()
	reg.MustRegister(&stubScenario{name: "dev-ramp", persona: PersonaDev, weight: 10, fn: record})
	reg.MustRegister(&stubScenario{name: "agent-ramp", persona: PersonaAgent, weight: 10, fn: record})

	cfg := DefaultConfig()
	cfg.Devs = 2
	cfg.Agents = 2
	cfg.Seed = 99
	cfg.RampUp = 200 * time.Millisecond // 4 workers => 50ms slots: 0, 50ms, 100ms, 150ms
	cfg.Duration = 260 * time.Millisecond
	cfg.ThinkMin = 10 * time.Millisecond
	cfg.ThinkMax = 10 * time.Millisecond

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	runStart = time.Now()
	res := runner.Run(context.Background())
	if res.Oracle.HasFailures() {
		t.Fatalf("unexpected failures: %+v", res.Failures)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(firstStart) != 4 {
		t.Fatalf("expected all 4 workers to start, got %d: %v", len(firstStart), firstStart)
	}
	// Slot 0 = dev-0 (0ms), slot 1 = agent-0 (50ms), slot 2 = dev-1 (100ms), slot 3 = agent-1 (150ms).
	if firstStart["agent-1"] < 120*time.Millisecond {
		t.Errorf("last worker agent-1 started too early at %v (want >= 120ms); starts=%v", firstStart["agent-1"], firstStart)
	}
	if firstStart["dev-1"] < 70*time.Millisecond {
		t.Errorf("slot-2 worker dev-1 started too early at %v (want >= 70ms); starts=%v", firstStart["dev-1"], firstStart)
	}
	if firstStart["agent-0"] < 25*time.Millisecond {
		t.Errorf("slot-1 worker agent-0 started too early at %v (want >= 25ms); starts=%v", firstStart["agent-0"], firstStart)
	}
}

func TestRunnerRampUpSingleWorkerStartsImmediately(t *testing.T) {
	var runs atomic.Int64
	reg := NewRegistry()
	reg.MustRegister(&stubScenario{
		name:    "dev-single",
		persona: PersonaDev,
		weight:  10,
		fn: func(_ context.Context, _ *ScenarioEnv) error {
			runs.Add(1)
			return nil
		},
	})

	cfg := DefaultConfig()
	cfg.Devs = 1
	cfg.Agents = 0
	cfg.RampUp = 5 * time.Second
	cfg.Duration = 60 * time.Millisecond
	cfg.ThinkMin = 5 * time.Millisecond
	cfg.ThinkMax = 5 * time.Millisecond

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	runner.Run(context.Background())
	if runs.Load() == 0 {
		t.Fatal("expected single worker (N=1) to start immediately despite 5s ramp-up")
	}
}

func TestRunnerMidRampCancellationSkipsWaitingWorkersAndActiveThreads(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var devRuns atomic.Int64
	var agentRuns atomic.Int64

	reg := NewRegistry()
	reg.MustRegister(&stubScenario{
		name:    "dev-cancel",
		persona: PersonaDev,
		weight:  10,
		fn: func(_ context.Context, _ *ScenarioEnv) error {
			devRuns.Add(1)
			cancel()
			return nil
		},
	})
	reg.MustRegister(&stubScenario{
		name:    "agent-cancel",
		persona: PersonaAgent,
		weight:  10,
		fn: func(_ context.Context, _ *ScenarioEnv) error {
			agentRuns.Add(1)
			return nil
		},
	})

	cfg := DefaultConfig()
	cfg.Devs = 3
	cfg.Agents = 3
	cfg.RampUp = 10 * time.Second // slot = 1.66s; slot 0 is dev-0, all others wait >= 1.66s
	cfg.ThinkMin = 5 * time.Millisecond
	cfg.ThinkMax = 5 * time.Millisecond

	var logBuf bytes.Buffer
	tel, exp, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	start := time.Now()
	res := runner.Run(ctx)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Fatalf("mid-ramp cancellation took too long (%v); waiting workers should exit immediately", elapsed)
	}
	if res.Oracle.HasFailures() {
		t.Fatalf("unexpected failures: %+v", res.Failures)
	}
	if devRuns.Load() != 1 {
		t.Errorf("devRuns = %d, want 1 (only slot-0 dev-0)", devRuns.Load())
	}
	if agentRuns.Load() != 0 {
		t.Errorf("agentRuns = %d, want 0 (agent workers cancelled during ramp-up)", agentRuns.Load())
	}

	var rm metricdata.ResourceMetrics
	if err := exp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_loadgen_active_threads" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("active_threads data type = %T, want Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				if val, ok := dp.Attributes.Value("persona"); ok && val.AsString() == string(PersonaAgent) {
					t.Errorf("unexpected active_threads data point for agent persona (value=%d); cancelled workers must not modify active_threads", dp.Value)
				}
				if dp.Value != 0 {
					t.Errorf("active_threads final value = %d for attrs %v, want 0", dp.Value, dp.Attributes)
				}
			}
		}
	}
}

func TestRunnerSelectsRandomRepoPerScenarioAcrossRepoCount(t *testing.T) {
	var mu sync.Mutex
	seenRepos := make(map[string]int)

	reg := NewRegistry()
	record := func(_ context.Context, env *ScenarioEnv) error {
		mu.Lock()
		seenRepos[env.Repo]++
		mu.Unlock()
		return nil
	}
	reg.MustRegister(&stubScenario{name: "dev-repo", persona: PersonaDev, weight: 10, fn: record})

	cfg := DefaultConfig()
	cfg.Devs = 1
	cfg.Agents = 0
	cfg.RepoCount = 3
	cfg.Seed = 99
	cfg.RampUp = 0
	cfg.ThinkMin = 1 * time.Millisecond
	cfg.ThinkMax = 1 * time.Millisecond
	cfg.Duration = 120 * time.Millisecond

	var logBuf bytes.Buffer
	tel, _, err := NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	defer func() { _ = tel.Shutdown(context.Background()) }()

	runner, err := NewRunner(cfg, reg, tel, NewOracle())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	res := runner.Run(context.Background())
	if res.Oracle.HasFailures() {
		t.Fatalf("unexpected failures: %+v", res.Failures)
	}

	mu.Lock()
	defer mu.Unlock()
	allowed := map[string]bool{"repo-1": true, "repo-2": true, "repo-3": true}
	for repo := range seenRepos {
		if !allowed[repo] {
			t.Errorf("unexpected repo %q outside repo-1..repo-3", repo)
		}
	}
	if len(seenRepos) < 2 {
		t.Fatalf("single worker touched only %d repo(s) (%v); expected random per-scenario selection across repo-1..repo-3", len(seenRepos), seenRepos)
	}
}
