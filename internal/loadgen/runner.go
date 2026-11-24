package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// RunResult summarizes a completed loadgen execution.
type RunResult struct {
	Seed     int64                   `json:"seed"`
	Duration time.Duration           `json:"duration_ns"`
	Oracle   OracleSummary           `json:"oracle"`
	Failures []UnexpectedErrorRecord `json:"failures,omitempty"`
}

// Runner orchestrates parallel dev and agent worker goroutines.
type Runner struct {
	cfg      Config
	registry *Registry
	tel      *Telemetry
	oracle   *Oracle
}

// NewRunner validates inputs and constructs a Runner.
func NewRunner(cfg Config, reg *Registry, tel *Telemetry, oracle *Oracle) (*Runner, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if reg == nil {
		return nil, errors.New("loadgen: registry is required")
	}
	filtered, err := reg.Filter(cfg.Scenarios)
	if err != nil {
		return nil, err
	}
	if len(cfg.Scenarios) == 0 && strings.TrimSpace(cfg.SSHKey) == "" && !strings.HasPrefix(cfg.SSHAddr, "file://") {
		filtered, err = filtered.withoutSSH()
		if err != nil {
			return nil, err
		}
	}
	if cfg.Devs > 0 && len(filtered.Scenarios(PersonaDev)) == 0 {
		return nil, errors.New("loadgen: no dev scenarios registered for --devs > 0")
	}
	if cfg.Agents > 0 && len(filtered.Scenarios(PersonaAgent)) == 0 {
		return nil, errors.New("loadgen: no agent scenarios registered for --agents > 0")
	}
	if tel == nil {
		tel, err = NewTelemetry(context.Background(), "", nil)
		if err != nil {
			return nil, err
		}
	}
	if oracle == nil {
		oracle = NewOracle()
	}
	return &Runner{
		cfg:      cfg,
		registry: filtered,
		tel:      tel,
		oracle:   oracle,
	}, nil
}

// Run executes all configured dev and agent workers until ctx is cancelled,
// cfg.Duration elapses, or cfg.FailFast triggers on an unexpected failure.
func (r *Runner) Run(ctx context.Context) RunResult {
	start := time.Now()
	seed := r.cfg.EffectiveSeed()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if r.cfg.Duration > 0 {
		var timeoutCancel context.CancelFunc
		runCtx, timeoutCancel = context.WithTimeout(runCtx, r.cfg.Duration)
		defer timeoutCancel()
	}

	var wg sync.WaitGroup
	for _, s := range buildWorkerSchedule(r.cfg.Devs, r.cfg.Agents, r.cfg.RampUp) {
		wg.Add(1)
		go func(slot workerSlot) {
			defer wg.Done()
			r.runWorker(runCtx, cancel, slot.persona, slot.workerID, seed, slot.delay)
		}(s)
	}

	wg.Wait()

	return RunResult{
		Seed:     seed,
		Duration: time.Since(start),
		Oracle:   r.oracle.Summary(),
		Failures: r.oracle.Records(),
	}
}

func (r *Runner) runWorker(ctx context.Context, cancelRun context.CancelFunc, persona Persona, workerID int, baseSeed int64, startDelay time.Duration) {
	if !sleepContext(ctx, startDelay) {
		return
	}

	metricCtx := context.Background()
	r.tel.AddActiveThreads(metricCtx, persona, 1)
	defer r.tel.AddActiveThreads(metricCtx, persona, -1)

	workDir, err := os.MkdirTemp("", fmt.Sprintf("stravin-loadgen-%s-%d-*", persona, workerID))
	if err != nil {
		r.oracle.RecordUnexpected(ctx, persona, "worker_init", "mkdir_temp", err.Error())
		if r.cfg.FailFast {
			cancelRun()
		}
		return
	}
	defer func() { _ = os.RemoveAll(workDir) }()

	rng := newWorkerRNG(baseSeed, persona, workerID)
	gitClient := NewGitClient(workDir, GitOptions{
		AuthToken:  r.cfg.AuthToken,
		SSHKey:     r.cfg.SSHKey,
		CmdTimeout: r.cfg.CmdTimeout,
	})
	baseClones := NewBaseCloneCache(filepath.Join(workDir, "base"))

	for {
		if ctx.Err() != nil {
			return
		}

		scenario, ok := r.registry.Select(persona, rng)
		if !ok {
			return
		}
		repo := r.cfg.Repos[rng.IntN(len(r.cfg.Repos))]

		r.executeScenario(ctx, cancelRun, persona, workerID, repo, workDir, baseClones, rng, gitClient, scenario)

		if ctx.Err() != nil {
			return
		}
		think := nextThinkDuration(r.cfg.ThinkMin, r.cfg.ThinkMax, rng)
		if !sleepContext(ctx, think) {
			return
		}
	}
}

func (r *Runner) executeScenario(
	ctx context.Context,
	cancelRun context.CancelFunc,
	persona Persona,
	workerID int,
	repo string,
	workDir string,
	baseClones *BaseCloneCache,
	rng *rand.Rand,
	gitClient *GitClient,
	scenario Scenario,
) {
	spanCtx, span := r.tel.Tracer.Start(ctx, "loadgen.scenario",
		trace.WithAttributes(
			attribute.String("persona", string(persona)),
			attribute.String("scenario", scenario.Name()),
			attribute.String("repo", repo),
			attribute.Int("worker_id", workerID),
		),
	)
	defer span.End()

	env := &ScenarioEnv{
		Config:      r.cfg,
		WorkerID:    workerID,
		Persona:     persona,
		Repo:        repo,
		WorkDir:     workDir,
		BaseClones:  baseClones,
		RNG:         rng,
		Git:         gitClient,
		Oracle:      r.oracle,
		Telemetry:   r.tel,
		Logger:      r.tel.Logger,
		RetryPolicy: DefaultRetryPolicy(),
	}

	opStart := time.Now()
	err := scenario.Run(spanCtx, env)
	opDur := time.Since(opStart)

	if err == nil {
		r.oracle.RecordOp()
		r.tel.RecordOp(spanCtx, persona, scenario.Name(), "scenario", "ok", opDur)
		span.SetStatus(codes.Ok, "")
		return
	}

	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		r.tel.RecordOp(spanCtx, persona, scenario.Name(), "scenario", "canceled", opDur)
		return
	}

	class := ClassifyError(err, ClassifyOptions{})
	opName := "scenario"
	var gitErr *GitError
	if errors.As(err, &gitErr) && gitErr.Op != "" {
		opName = gitErr.Op
	}

	switch class {
	case ErrorExpectedReject:
		r.oracle.RecordOp()
		r.tel.RecordOp(spanCtx, persona, scenario.Name(), opName, "expected_reject", opDur)
		span.SetStatus(codes.Ok, "expected_reject")
	case ErrorRetryableConflict, ErrorRetryableTransient:
		r.oracle.RecordOp()
		r.tel.RecordOp(spanCtx, persona, scenario.Name(), opName, string(class), opDur)
		r.tel.Logger.WarnContext(spanCtx, "retryable scenario error surfaced",
			"persona", string(persona),
			"scenario", scenario.Name(),
			"repo", repo,
			"class", string(class),
			"error", err.Error(),
		)
	default:
		r.oracle.RecordOp()
		r.tel.RecordOp(spanCtx, persona, scenario.Name(), opName, "unexpected", opDur)
		if opName == "fsck" {
			r.oracle.RecordFsckFailure(spanCtx, persona, scenario.Name(), err.Error())
			r.tel.RecordFsckFailure(spanCtx, scenario.Name())
		} else {
			r.oracle.RecordUnexpected(spanCtx, persona, scenario.Name(), opName, err.Error())
			r.tel.RecordUnexpectedError(spanCtx, persona, scenario.Name(), opName)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		r.tel.Logger.ErrorContext(spanCtx, "unexpected loadgen error",
			"persona", string(persona),
			"scenario", scenario.Name(),
			"repo", repo,
			"op", opName,
			"error", err.Error(),
		)
		if r.cfg.FailFast {
			cancelRun()
		}
	}
}

func newWorkerRNG(baseSeed int64, persona Persona, workerID int) *rand.Rand {
	var personaSalt uint64 = 1
	if persona == PersonaAgent {
		personaSalt = 2
	}
	stream := (personaSalt << 32) | uint64(workerID+1)
	return rand.New(rand.NewPCG(uint64(baseSeed), stream)) //#nosec G404 -- deterministic PRNG for load simulation, not cryptography
}

func nextThinkDuration(minD, maxD time.Duration, rng *rand.Rand) time.Duration {
	if maxD <= minD {
		return minD
	}
	span := int64(maxD - minD)
	return minD + time.Duration(rng.Int64N(span+1))
}

type workerSlot struct {
	persona  Persona
	workerID int
	delay    time.Duration
}

// buildWorkerSchedule assigns each dev and agent worker an evenly spaced start
// slot across rampUp, interleaving personas proportionally so both ramp
// smoothly across the window.
func buildWorkerSchedule(devs, agents int, rampUp time.Duration) []workerSlot {
	total := devs + agents
	if total <= 0 {
		return nil
	}
	var slot time.Duration
	if total > 1 && rampUp > 0 {
		slot = rampUp / time.Duration(total)
	}
	schedule := make([]workerSlot, 0, total)
	devIdx, agentIdx := 0, 0
	for k := 0; k < total; k++ {
		delay := time.Duration(k) * slot
		if devIdx < devs && (agentIdx >= agents || devIdx*agents <= agentIdx*devs) {
			schedule = append(schedule, workerSlot{
				persona:  PersonaDev,
				workerID: devIdx,
				delay:    delay,
			})
			devIdx++
		} else {
			schedule = append(schedule, workerSlot{
				persona:  PersonaAgent,
				workerID: agentIdx,
				delay:    delay,
			})
			agentIdx++
		}
	}
	return schedule
}
