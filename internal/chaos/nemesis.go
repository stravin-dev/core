package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const (
	defaultInterval      = 5 * time.Second
	defaultFaultDuration = 3 * time.Second
	cleanupTimeout       = 10 * time.Second
)

// DefaultRestartTargets lists the Stravin daemons eligible for optional restart chaos.
var DefaultRestartTargets = []string{
	"stravin-node",
	"stravin-edge",
	"stravin-pack",
	"stravin-hook",
}

// Config configures the Nemesis fault-injection loop.
type Config struct {
	Seed           int64
	Interval       time.Duration
	FaultDuration  time.Duration
	Duration       time.Duration
	MaxFaults      int
	EnableRestarts bool
	RestartTargets []string
	Proxies        []string
}

// Dependencies holds injected collaborators for Nemesis.
type Dependencies struct {
	Proxy   ProxyController
	Service ServiceController
	Tracer  trace.Tracer
	Logger  *slog.Logger
	OnFault func(FaultRecord)
}

// FaultRecord describes one executed chaos fault for logging, tracing, and test verification.
type FaultRecord struct {
	Sequence   int
	Action     string
	Target     string
	ToxicName  string
	Attributes map[string]any
}

// CleanupFunc reverts a single injected fault.
type CleanupFunc func(ctx context.Context) error

// FaultAction (GoF Strategy) encapsulates a single fault-injection behavior.
type FaultAction interface {
	Name() string
	Apply(ctx context.Context, rng *rand.Rand, seq int, proxies []string) (FaultRecord, CleanupFunc, error)
}

// Nemesis runs a deterministic, seeded fault-injection loop against Toxiproxy and optional services.
type Nemesis struct {
	cfg     Config
	proxy   ProxyController
	tracer  trace.Tracer
	logger  *slog.Logger
	onFault func(FaultRecord)
	actions []FaultAction
}

// NewNemesis validates cfg and builds a Nemesis with its FaultAction strategy catalog.
func NewNemesis(cfg Config, deps Dependencies) (*Nemesis, error) {
	if deps.Proxy == nil {
		return nil, errors.New("chaos: ProxyController is required")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = defaultInterval
	}
	if cfg.FaultDuration <= 0 {
		cfg.FaultDuration = defaultFaultDuration
	}
	if cfg.Duration < 0 {
		return nil, errors.New("chaos: duration must be >= 0")
	}

	targets := cfg.RestartTargets
	if len(targets) == 0 {
		targets = append([]string(nil), DefaultRestartTargets...)
	}
	if cfg.EnableRestarts {
		if deps.Service == nil {
			return nil, errors.New("chaos: ServiceController is required when restarts are enabled")
		}
		for _, target := range targets {
			trimmed := strings.TrimSpace(target)
			if IsProtectedService(trimmed) {
				return nil, fmt.Errorf("chaos: protected service %q cannot be in restart targets", trimmed)
			}
			if !isSafeServiceName(trimmed) {
				return nil, fmt.Errorf("chaos: invalid restart target %q", trimmed)
			}
		}
	}

	tracer := deps.Tracer
	if tracer == nil {
		tracer = tracenoop.NewTracerProvider().Tracer("stravin-chaos")
	}
	logger := deps.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	actions := []FaultAction{
		newToxicFaultAction("latency", deps.Proxy, func(rng *rand.Rand) map[string]any {
			return map[string]any{
				"latency": 50 + rng.Intn(351),
				"jitter":  10 + rng.Intn(91),
			}
		}),
		newToxicFaultAction("bandwidth", deps.Proxy, func(rng *rand.Rand) map[string]any {
			return map[string]any{
				"rate": 64 + rng.Intn(449),
			}
		}),
		newToxicFaultAction("reset_peer", deps.Proxy, func(rng *rand.Rand) map[string]any {
			return map[string]any{
				"timeout": 50 + rng.Intn(201),
			}
		}),
		newToxicFaultAction("timeout", deps.Proxy, func(rng *rand.Rand) map[string]any {
			return map[string]any{
				"timeout": 200 + rng.Intn(801),
			}
		}),
		newToxicFaultAction("slow_close", deps.Proxy, func(rng *rand.Rand) map[string]any {
			return map[string]any{
				"delay": 100 + rng.Intn(401),
			}
		}),
	}

	if cfg.EnableRestarts {
		guarded := NewGuardedServiceController(deps.Service, targets)
		actions = append(actions, &serviceRestartAction{
			controller: guarded,
			targets:    targets,
		})
	}

	return &Nemesis{
		cfg:     cfg,
		proxy:   deps.Proxy,
		tracer:  tracer,
		logger:  logger,
		onFault: deps.OnFault,
		actions: actions,
	}, nil
}

// Run executes the chaos loop until ctx is cancelled, Duration elapses, or MaxFaults is reached.
// It ALWAYS calls ProxyController.ResetAll on exit so the cluster returns to a clean state.
func (n *Nemesis) Run(ctx context.Context) error {
	runCtx := ctx
	var cancel context.CancelFunc
	if n.cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(ctx, n.cfg.Duration)
		defer cancel()
	}

	var cleanupErr error
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cleanupCancel()
		if err := n.proxy.ResetAll(cleanupCtx); err != nil {
			n.logger.ErrorContext(cleanupCtx, "chaos teardown reset failed", slog.String("error", err.Error()))
			cleanupErr = err
			return
		}
		n.logger.InfoContext(cleanupCtx, "chaos teardown cleared all toxics")
	}()

	proxies, err := n.resolveProxies(runCtx)
	if err != nil {
		return err
	}
	if len(proxies) == 0 {
		return errors.New("chaos: no Toxiproxy proxies available")
	}

	// #nosec G404 -- deterministic PRNG required for reproducible chaos schedules under --seed.
	rng := rand.New(rand.NewSource(n.cfg.Seed))

	for seq := 1; n.cfg.MaxFaults == 0 || seq <= n.cfg.MaxFaults; seq++ {
		if runCtx.Err() != nil {
			break
		}

		action := n.actions[rng.Intn(len(n.actions))]
		if err := n.executeFault(runCtx, rng, seq, action, proxies); err != nil {
			if runCtx.Err() != nil {
				break
			}
			n.logger.WarnContext(runCtx, "chaos fault execution failed",
				slog.Int("seq", seq),
				slog.String("action", action.Name()),
				slog.String("error", err.Error()),
			)
		}

		if n.cfg.MaxFaults > 0 && seq >= n.cfg.MaxFaults {
			break
		}
		if !sleepWithContext(runCtx, n.cfg.Interval) {
			break
		}
	}

	return cleanupErr
}

func (n *Nemesis) resolveProxies(ctx context.Context) ([]string, error) {
	if len(n.cfg.Proxies) > 0 {
		return append([]string(nil), n.cfg.Proxies...), nil
	}
	listed, err := n.proxy.ListProxies(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: discover proxies: %w", err)
	}
	names := make([]string, 0, len(listed))
	for _, p := range listed {
		if p.Enabled {
			names = append(names, p.Name)
		}
	}
	return names, nil
}

func (n *Nemesis) executeFault(
	ctx context.Context,
	rng *rand.Rand,
	seq int,
	action FaultAction,
	proxies []string,
) error {
	spanCtx, span := n.tracer.Start(ctx, "chaos.fault.inject",
		trace.WithAttributes(
			attribute.String("chaos.fault.type", action.Name()),
			attribute.Int64("chaos.seed", n.cfg.Seed),
			attribute.Int("chaos.seq", seq),
		),
	)
	defer span.End()

	rec, cleanup, err := action.Apply(spanCtx, rng, seq, proxies)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	span.SetAttributes(
		attribute.String("chaos.target", rec.Target),
		attribute.String("chaos.toxic.name", rec.ToxicName),
	)
	n.logger.InfoContext(spanCtx, "chaos fault injected",
		slog.Int("seq", rec.Sequence),
		slog.String("action", rec.Action),
		slog.String("target", rec.Target),
		slog.String("toxic", rec.ToxicName),
	)

	if n.onFault != nil {
		n.onFault(rec)
	}

	if cleanup != nil {
		_ = sleepWithContext(spanCtx, n.cfg.FaultDuration)
		cleanCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()
		if err := cleanup(cleanCtx); err != nil {
			span.RecordError(err)
			n.logger.WarnContext(spanCtx, "chaos fault cleanup failed",
				slog.String("target", rec.Target),
				slog.String("toxic", rec.ToxicName),
				slog.String("error", err.Error()),
			)
			return err
		}
		n.logger.InfoContext(spanCtx, "chaos fault recovered",
			slog.Int("seq", rec.Sequence),
			slog.String("action", rec.Action),
			slog.String("target", rec.Target),
		)
	}

	return nil
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
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

type toxicFaultAction struct {
	faultType string
	proxy     ProxyController
	attrsFn   func(*rand.Rand) map[string]any
}

func newToxicFaultAction(
	faultType string,
	proxy ProxyController,
	attrsFn func(*rand.Rand) map[string]any,
) FaultAction {
	return &toxicFaultAction{
		faultType: faultType,
		proxy:     proxy,
		attrsFn:   attrsFn,
	}
}

func (a *toxicFaultAction) Name() string {
	return a.faultType
}

func (a *toxicFaultAction) Apply(
	ctx context.Context,
	rng *rand.Rand,
	seq int,
	proxies []string,
) (FaultRecord, CleanupFunc, error) {
	proxyName := proxies[rng.Intn(len(proxies))]
	toxicName := fmt.Sprintf("stravin_chaos_%s_%d", a.faultType, seq)
	attrs := a.attrsFn(rng)

	toxic := Toxic{
		Name:       toxicName,
		Type:       a.faultType,
		Stream:     "downstream",
		Toxicity:   1.0,
		Attributes: attrs,
	}
	created, err := a.proxy.AddToxic(ctx, proxyName, toxic)
	if err != nil {
		return FaultRecord{}, nil, err
	}

	rec := FaultRecord{
		Sequence:   seq,
		Action:     a.faultType,
		Target:     proxyName,
		ToxicName:  created.Name,
		Attributes: attrs,
	}
	cleanup := func(cleanCtx context.Context) error {
		return a.proxy.RemoveToxic(cleanCtx, proxyName, created.Name)
	}
	return rec, cleanup, nil
}

type serviceRestartAction struct {
	controller ServiceController
	targets    []string
}

func (a *serviceRestartAction) Name() string {
	return "service_restart"
}

func (a *serviceRestartAction) Apply(
	ctx context.Context,
	rng *rand.Rand,
	seq int,
	_ []string,
) (FaultRecord, CleanupFunc, error) {
	target := a.targets[rng.Intn(len(a.targets))]
	if err := a.controller.Restart(ctx, target); err != nil {
		return FaultRecord{}, nil, err
	}
	return FaultRecord{
		Sequence: seq,
		Action:   a.Name(),
		Target:   target,
	}, nil, nil
}
