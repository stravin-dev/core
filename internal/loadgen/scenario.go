package loadgen

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// Persona identifies the simulated actor class.
type Persona string

const (
	// PersonaDev represents a human developer workflow.
	PersonaDev Persona = "dev"
	// PersonaAgent represents an autonomous AI coding agent workflow.
	PersonaAgent Persona = "agent"
)

// Scenario is the GoF Strategy interface implemented by every loadgen workflow.
type Scenario interface {
	Name() string
	Persona() Persona
	Weight() int
	Run(ctx context.Context, env *ScenarioEnv) error
}

// ScenarioEnv provides per-worker isolated dependencies for executing a Scenario.
type ScenarioEnv struct {
	Config      Config
	WorkerID    int
	Persona     Persona
	Repo        string
	WorkDir     string
	RNG         *rand.Rand
	Git         *GitClient
	Oracle      *Oracle
	Telemetry   *Telemetry
	Logger      *slog.Logger
	RetryPolicy RetryPolicy
}

// RecordOp records operation telemetry and increments oracle operation count.
func (e *ScenarioEnv) RecordOp(ctx context.Context, scenario, op, outcome string, dur time.Duration) {
	if e.Oracle != nil {
		e.Oracle.RecordOp()
	}
	if e.Telemetry != nil {
		e.Telemetry.RecordOp(ctx, e.Persona, scenario, op, outcome, dur)
	}
}

// RecordRetry records a retry event in both Oracle and Telemetry.
func (e *ScenarioEnv) RecordRetry(ctx context.Context, scenario string, reason ErrorClass) {
	if e.Oracle != nil {
		e.Oracle.RecordRetry()
	}
	if e.Telemetry != nil {
		e.Telemetry.RecordRetry(ctx, e.Persona, scenario, string(reason))
	}
}

// RecordFsckFailure records a git fsck failure in Oracle, Telemetry, and structured logs.
func (e *ScenarioEnv) RecordFsckFailure(ctx context.Context, scenario, detail string) {
	if e.Oracle != nil {
		e.Oracle.RecordFsckFailure(ctx, e.Persona, scenario, detail)
	}
	if e.Telemetry != nil {
		e.Telemetry.RecordFsckFailure(ctx, scenario)
	}
	if e.Logger != nil {
		e.Logger.ErrorContext(ctx, "git fsck failure detected",
			"persona", string(e.Persona),
			"scenario", scenario,
			"repo", e.Repo,
			"detail", detail,
		)
	}
}

// RunOp executes fn with bounded exponential backoff on ErrorRetryableTransient errors.
// It returns nil on success or ErrorExpectedReject; otherwise it returns the final error.
func (e *ScenarioEnv) RunOp(
	ctx context.Context,
	scenario, op string,
	opts ClassifyOptions,
	fn func(ctx context.Context) error,
) error {
	maxRetries := e.Config.MaxRetries
	policy := e.RetryPolicy
	if policy.BaseBackoff <= 0 {
		policy = DefaultRetryPolicy()
	}

	for attempt := 0; ; attempt++ {
		start := time.Now()
		err := fn(ctx)
		dur := time.Since(start)

		if err == nil {
			e.RecordOp(ctx, scenario, op, "ok", dur)
			return nil
		}
		if ctx.Err() != nil {
			e.RecordOp(ctx, scenario, op, "canceled", dur)
			return ctx.Err()
		}

		class := ClassifyError(err, opts)
		switch class {
		case ErrorExpectedReject:
			e.RecordOp(ctx, scenario, op, "expected_reject", dur)
			return nil
		case ErrorRetryableTransient:
			if attempt < maxRetries {
				e.RecordRetry(ctx, scenario, class)
				backoff := policy.BackoffDuration(attempt, e.RNG)
				if !sleepContext(ctx, backoff) {
					e.RecordOp(ctx, scenario, op, "canceled", dur)
					return ctx.Err()
				}
				continue
			}
			e.RecordOp(ctx, scenario, op, "retries_exhausted", dur)
			return fmt.Errorf("%s retries exhausted after %d attempts: %w", op, attempt+1, err)
		default:
			e.RecordOp(ctx, scenario, op, string(class), dur)
			return err
		}
	}
}

// Registry stores Scenarios by persona and performs deterministic weighted random selection.
type Registry struct {
	mu        sync.RWMutex
	byName    map[string]Scenario
	byPersona map[Persona][]Scenario
}

// NewRegistry creates an empty Scenario Registry.
func NewRegistry() *Registry {
	return &Registry{
		byName:    make(map[string]Scenario),
		byPersona: make(map[Persona][]Scenario),
	}
}

// Register validates and adds s to the registry.
func (r *Registry) Register(s Scenario) error {
	if s == nil {
		return errors.New("loadgen: cannot register nil scenario")
	}
	if s.Name() == "" {
		return errors.New("loadgen: scenario name must be non-empty")
	}
	if s.Persona() != PersonaDev && s.Persona() != PersonaAgent {
		return fmt.Errorf("loadgen: scenario %q has invalid persona %q", s.Name(), s.Persona())
	}
	if s.Weight() <= 0 {
		return fmt.Errorf("loadgen: scenario %q must have positive weight, got %d", s.Name(), s.Weight())
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.byName[s.Name()]; exists {
		return fmt.Errorf("loadgen: duplicate scenario %q", s.Name())
	}
	r.byName[s.Name()] = s
	r.byPersona[s.Persona()] = append(r.byPersona[s.Persona()], s)
	list := r.byPersona[s.Persona()]
	sort.Slice(list, func(i, j int) bool { return list[i].Name() < list[j].Name() })
	return nil
}

// MustRegister registers s or panics on invalid registration.
func (r *Registry) MustRegister(s Scenario) {
	if err := r.Register(s); err != nil {
		panic(err)
	}
}

// Filter returns a new Registry restricted to the named scenarios.
// When names is empty, it returns a shallow copy containing all registered scenarios.
func (r *Registry) Filter(names []string) (*Registry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := NewRegistry()
	if len(names) == 0 {
		for _, s := range r.byName {
			if err := out.Register(s); err != nil {
				return nil, err
			}
		}
		return out, nil
	}

	for _, name := range names {
		s, ok := r.byName[name]
		if !ok {
			return nil, fmt.Errorf("loadgen: unknown scenario %q", name)
		}
		if err := out.Register(s); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Scenarios returns a sorted copy of registered scenarios for persona.
func (r *Registry) Scenarios(persona Persona) []Scenario {
	r.mu.RLock()
	defer r.mu.RUnlock()
	src := r.byPersona[persona]
	out := make([]Scenario, len(src))
	copy(out, src)
	return out
}

// Select chooses a scenario for persona using weighted random selection driven by rng.
func (r *Registry) Select(persona Persona, rng *rand.Rand) (Scenario, bool) {
	r.mu.RLock()
	list := r.byPersona[persona]
	r.mu.RUnlock()

	if len(list) == 0 {
		return nil, false
	}
	totalWeight := 0
	for _, s := range list {
		totalWeight += s.Weight()
	}
	if totalWeight <= 0 {
		return nil, false
	}

	pick := rng.IntN(totalWeight)
	acc := 0
	for _, s := range list {
		acc += s.Weight()
		if pick < acc {
			return s, true
		}
	}
	return list[len(list)-1], true
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
