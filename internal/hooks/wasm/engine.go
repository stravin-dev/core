package wasm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
)

// Engine executes Tier 1 synchronous Wasm hooks using a pure-Go wazero runtime
// with pre-compiled module LRU caching, strict resource guardrails, and a bulkhead semaphore.
type Engine struct {
	loader   WasmLoader
	store    HookConfigStore
	cfg      EngineConfig
	bulkhead *bulkhead
	cache    *moduleCache

	rtMu     sync.Mutex
	runtimes map[uint32]wazero.Runtime
}

// NewEngine constructs a Tier 1 Wasm hook engine backed by narrow consumer interfaces.
func NewEngine(loader WasmLoader, store HookConfigStore, cfg EngineConfig) *Engine {
	norm := cfg.withDefaults()
	return &Engine{
		loader:   loader,
		store:    store,
		cfg:      norm,
		bulkhead: newBulkhead(norm.MaxConcurrency),
		cache:    newModuleCache(norm.ModuleCacheSize),
		runtimes: make(map[uint32]wazero.Runtime),
	}
}

// Close releases all cached compiled modules and underlying wazero runtimes.
func (e *Engine) Close(ctx context.Context) error {
	e.cache.Close(ctx)

	e.rtMu.Lock()
	rts := make([]wazero.Runtime, 0, len(e.runtimes))
	for _, rt := range e.runtimes {
		rts = append(rts, rt)
	}
	clear(e.runtimes)
	e.rtMu.Unlock()

	var firstErr error
	for _, rt := range rts {
		if err := rt.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// EvaluatePush runs all configured hooks whose RefGlob matches at least one ref update.
// Hooks execute in configured order and short-circuit on the first REJECT verdict.
func (e *Engine) EvaluatePush(ctx context.Context, pushCtx PushContext, reader ObjectReader) (PipelineResult, error) {
	result := PipelineResult{Allowed: true}
	if e.store == nil || len(pushCtx.Updates) == 0 {
		return result, nil
	}

	hooks, err := e.store.ListHooks(ctx, pushCtx.TenantID, pushCtx.RepoID, pushCtx.OrgID)
	if err != nil {
		return PipelineResult{}, fmt.Errorf("hooks/wasm: list hooks: %w", err)
	}

	for _, hook := range hooks {
		matched := filterMatchingUpdates(hook.RefGlob, pushCtx.Updates)
		if len(matched) == 0 {
			continue
		}

		scopedCtx := pushCtx
		scopedCtx.Updates = matched

		outcome := e.runHook(ctx, hook, scopedCtx, reader)
		result.HookOutcomes = append(result.HookOutcomes, outcome)
		if outcome.Warning != "" {
			result.Warnings = append(result.Warnings, outcome.Warning)
		}
		if outcome.Decision == VerdictReject {
			result.Allowed = false
			result.RejectedBy = hook.HookID
			result.Message = outcome.Message
			result.RefVerdicts = outcome.RefVerdicts
			return result, nil
		}
	}

	return result, nil
}

func (e *Engine) runHook(ctx context.Context, hook HookConfig, pushCtx PushContext, reader ObjectReader) HookOutcome {
	start := time.Now()
	policy := policyFor(hook.FailMode)

	if err := e.bulkhead.Acquire(ctx); err != nil {
		out := policy.handleFailure(hook.HookID, err, e.cfg.Logger)
		out.Duration = time.Since(start)
		return out
	}
	defer e.bulkhead.Release()

	timeout := e.effectiveTimeout(hook.TimeoutMS)
	memPages := e.effectiveMemoryPages(hook.MemoryLimitMB)

	rt, err := e.runtimeForMemoryLimit(ctx, memPages)
	if err != nil {
		out := policy.handleFailure(hook.HookID, err, e.cfg.Logger)
		out.Duration = time.Since(start)
		return out
	}

	cached, cacheHit, err := e.getOrCompile(ctx, rt, hook.WasmSHA256, memPages)
	if err != nil {
		out := policy.handleFailure(hook.HookID, err, e.cfg.Logger)
		out.Duration = time.Since(start)
		return out
	}
	defer cached.release(context.Background())

	verdict, err := e.executeModule(ctx, rt, cached.compiled, pushCtx, reader, timeout)
	if err != nil {
		out := policy.handleFailure(hook.HookID, err, e.cfg.Logger)
		out.Duration = time.Since(start)
		out.CacheHit = cacheHit
		return out
	}

	return HookOutcome{
		HookID:      hook.HookID,
		Decision:    verdict.Decision,
		Message:     verdict.Message,
		RefVerdicts: verdict.RefVerdicts,
		Duration:    time.Since(start),
		CacheHit:    cacheHit,
	}
}

func (e *Engine) executeModule(
	ctx context.Context,
	rt wazero.Runtime,
	compiled wazero.CompiledModule,
	pushCtx PushContext,
	reader ObjectReader,
	timeout time.Duration,
) (HookVerdict, error) {
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	state := &invocationState{
		pushCtx: pushCtx,
		reader:  newBudgetedObjectReader(reader, e.cfg.MaxCommitsWalked, e.cfg.MaxBlobBytesRead, e.cfg.MaxHostCalls),
	}
	execCtx = context.WithValue(execCtx, invocationStateKey{}, state)

	// Anonymous module config isolates each invocation with zero WASI, env, args, fs, or network.
	mod, err := rt.InstantiateModule(execCtx, compiled, wazero.NewModuleConfig().WithName(""))
	if err != nil {
		return HookVerdict{}, classifyExecError(execCtx, err)
	}
	defer func() {
		_ = mod.Close(context.Background())
	}()

	fn := mod.ExportedFunction("run")
	if fn == nil {
		fn = mod.ExportedFunction("_start")
	}
	if fn == nil {
		return HookVerdict{}, fmt.Errorf("%w: missing exported run or _start function", ErrInvalidConfig)
	}

	if _, err := fn.Call(execCtx); err != nil {
		return HookVerdict{}, classifyExecError(execCtx, err)
	}

	if v, ok := state.getVerdict(); ok {
		return v, nil
	}
	return HookVerdict{Decision: VerdictAllow}, nil
}

func (e *Engine) runtimeForMemoryLimit(ctx context.Context, memPages uint32) (wazero.Runtime, error) {
	e.rtMu.Lock()
	defer e.rtMu.Unlock()

	if rt, ok := e.runtimes[memPages]; ok {
		return rt, nil
	}

	rc := wazero.NewRuntimeConfig().
		WithCloseOnContextDone(true).
		WithMemoryLimitPages(memPages)
	rt := wazero.NewRuntimeWithConfig(ctx, rc)
	if err := registerHostABI(ctx, rt); err != nil {
		_ = rt.Close(context.Background())
		return nil, err
	}
	e.runtimes[memPages] = rt
	return rt, nil
}

func (e *Engine) getOrCompile(ctx context.Context, rt wazero.Runtime, shaHex string, memPages uint32) (*cachedModule, bool, error) {
	normSHA := strings.ToLower(strings.TrimSpace(shaHex))
	if normSHA == "" {
		return nil, false, fmt.Errorf("%w: empty wasm_sha256", ErrInvalidConfig)
	}
	key := moduleCacheKey{sha256: normSHA, memoryLimitPages: memPages}
	if mod, ok := e.cache.Get(key); ok {
		return mod, true, nil
	}

	if e.loader == nil {
		return nil, false, ErrMissingArtifact
	}
	raw, err := e.loader.LoadWasm(ctx, normSHA)
	if err != nil {
		return nil, false, fmt.Errorf("load wasm %s: %w", normSHA, err)
	}
	actual := sha256.Sum256(raw)
	if hex.EncodeToString(actual[:]) != normSHA {
		return nil, false, fmt.Errorf("%w: expected %s", ErrChecksumMismatch, normSHA)
	}

	compiled, err := rt.CompileModule(ctx, raw)
	if err != nil {
		return nil, false, classifyExecError(ctx, err)
	}
	return e.cache.Put(ctx, key, compiled), false, nil
}

func (e *Engine) effectiveTimeout(timeoutMS int) time.Duration {
	ms := timeoutMS
	if ms <= 0 {
		ms = e.cfg.DefaultTimeoutMS
	}
	if ms > e.cfg.MaxTimeoutMS {
		ms = e.cfg.MaxTimeoutMS
	}
	return time.Duration(ms) * time.Millisecond
}

func (e *Engine) effectiveMemoryPages(memoryLimitMB int) uint32 {
	mb := memoryLimitMB
	if mb <= 0 {
		mb = e.cfg.DefaultMemoryLimitMB
	}
	if mb > e.cfg.MaxMemoryLimitMB {
		mb = e.cfg.MaxMemoryLimitMB
	}
	return uint32(mb) * wasmPagesPerMB
}

func classifyExecError(ctx context.Context, err error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", ErrTimeout, err)
	}
	msg := err.Error()
	if strings.Contains(msg, "memory") && (strings.Contains(msg, "limit") || strings.Contains(msg, "exceeds")) {
		return fmt.Errorf("%w: %w", ErrMemoryLimitExceeded, err)
	}
	return fmt.Errorf("%w: %w", ErrModuleTrap, err)
}
