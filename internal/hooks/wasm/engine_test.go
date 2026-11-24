package wasm

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type memWasmLoader struct {
	mu      sync.Mutex
	modules map[string][]byte
	loads   map[string]int
}

func newMemWasmLoader() *memWasmLoader {
	return &memWasmLoader{
		modules: make(map[string][]byte),
		loads:   make(map[string]int),
	}
}

func (l *memWasmLoader) Put(wasmBytes []byte) string {
	sum := sha256Hex(wasmBytes)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.modules[sum] = wasmBytes
	return sum
}

func (l *memWasmLoader) LoadWasm(_ context.Context, sha string) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loads[sha]++
	b, ok := l.modules[sha]
	if !ok {
		return nil, ErrMissingArtifact
	}
	return b, nil
}

func (l *memWasmLoader) LoadCount(sha string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loads[sha]
}

type staticConfigStore struct {
	hooks []HookConfig
	err   error
}

func (s *staticConfigStore) ListHooks(_ context.Context, _, _, _ string) ([]HookConfig, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.hooks, nil
}

type stubObjectReader struct {
	mu          sync.Mutex
	commits     []CommitMeta
	changes     []TreeChange
	blobs       map[[20]byte][]byte
	commitCalls int
	diffCalls   int
	blobCalls   int
}

func (r *stubObjectReader) ListNewCommits(_, _ [20]byte, limit uint32) ([]CommitMeta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.commitCalls++
	if limit > 0 && int(limit) < len(r.commits) {
		return r.commits[:limit], nil
	}
	return r.commits, nil
}

func (r *stubObjectReader) DiffTree(_, _ [20]byte, limit uint32) ([]TreeChange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diffCalls++
	if limit > 0 && int(limit) < len(r.changes) {
		return r.changes[:limit], nil
	}
	return r.changes, nil
}

func (r *stubObjectReader) ReadBlobChunk(blobOID [20]byte, offset uint64, maxBytes uint32) ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blobCalls++
	data, ok := r.blobs[blobOID]
	if !ok {
		return nil, errors.New("blob not found")
	}
	if offset >= uint64(len(data)) {
		return []byte{}, nil
	}
	end := offset + uint64(maxBytes)
	if end > uint64(len(data)) {
		end = uint64(len(data))
	}
	return data[offset:end], nil
}

func oidByte(b byte) [20]byte {
	var o [20]byte
	o[0] = b
	return o
}

func TestEngineAllowAndRejectHooks(t *testing.T) {
	loader := newMemWasmLoader()
	allowSHA := loader.Put(wasmVerdictFixture(0, "all good"))
	rejectSHA := loader.Put(wasmVerdictFixture(1, "secret detected in commit"))

	pushCtx := PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		ActorID:  "alice",
		Updates: []RefUpdate{
			{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)},
		},
	}

	t.Run("allow hook succeeds", func(t *testing.T) {
		store := &staticConfigStore{hooks: []HookConfig{
			{HookID: "lint", WasmSHA256: allowSHA, RefGlob: "refs/heads/*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
		}}
		eng := NewEngine(loader, store, EngineConfig{})
		defer func() { _ = eng.Close(context.Background()) }()

		res, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
		if err != nil {
			t.Fatalf("EvaluatePush error: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("expected Allowed=true, got false (msg=%q)", res.Message)
		}
		if len(res.HookOutcomes) != 1 || res.HookOutcomes[0].Decision != VerdictAllow {
			t.Fatalf("unexpected outcomes: %+v", res.HookOutcomes)
		}
	})

	t.Run("reject hook rejects with stderr message", func(t *testing.T) {
		store := &staticConfigStore{hooks: []HookConfig{
			{HookID: "secret-scan", WasmSHA256: rejectSHA, RefGlob: "refs/heads/*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
			{HookID: "never-reached", WasmSHA256: allowSHA, RefGlob: "*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
		}}
		eng := NewEngine(loader, store, EngineConfig{})
		defer func() { _ = eng.Close(context.Background()) }()

		res, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
		if err != nil {
			t.Fatalf("EvaluatePush error: %v", err)
		}
		if res.Allowed {
			t.Fatal("expected Allowed=false, got true")
		}
		if res.RejectedBy != "secret-scan" {
			t.Fatalf("RejectedBy = %q, want %q", res.RejectedBy, "secret-scan")
		}
		if !strings.Contains(res.Message, "secret detected in commit") {
			t.Fatalf("Message = %q, expected stderr diagnostic", res.Message)
		}
		if len(res.HookOutcomes) != 1 {
			t.Fatalf("expected short-circuit after reject, got %d outcomes", len(res.HookOutcomes))
		}
	})
}

func TestEngineInfiniteLoopTimeoutFailModes(t *testing.T) {
	loader := newMemWasmLoader()
	loopSHA := loader.Put(wasmInfiniteLoopFixture())

	pushCtx := PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		ActorID:  "alice",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}

	t.Run("fail_mode closed rejects on timeout", func(t *testing.T) {
		store := &staticConfigStore{hooks: []HookConfig{
			{HookID: "hang-closed", WasmSHA256: loopSHA, RefGlob: "*", TimeoutMS: 30, MemoryLimitMB: 16, FailMode: FailModeClosed},
		}}
		eng := NewEngine(loader, store, EngineConfig{})
		defer func() { _ = eng.Close(context.Background()) }()

		start := time.Now()
		res, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("EvaluatePush returned unexpected error: %v", err)
		}
		if res.Allowed {
			t.Fatal("expected Allowed=false for closed timeout")
		}
		if res.RejectedBy != "hang-closed" {
			t.Fatalf("RejectedBy = %q, want hang-closed", res.RejectedBy)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("timeout took too long: %v", elapsed)
		}
	})

	t.Run("fail_mode open warns and allows on timeout", func(t *testing.T) {
		store := &staticConfigStore{hooks: []HookConfig{
			{HookID: "hang-open", WasmSHA256: loopSHA, RefGlob: "*", TimeoutMS: 30, MemoryLimitMB: 16, FailMode: FailModeOpen},
		}}
		eng := NewEngine(loader, store, EngineConfig{})
		defer func() { _ = eng.Close(context.Background()) }()

		res, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
		if err != nil {
			t.Fatalf("EvaluatePush returned unexpected error: %v", err)
		}
		if !res.Allowed {
			t.Fatalf("expected Allowed=true for open timeout, got msg=%q", res.Message)
		}
		if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "hang-open") {
			t.Fatalf("expected side-band warning mentioning hang-open, got %v", res.Warnings)
		}
	})
}

func TestEngineMemoryLimitAndTrapAndMissingArtifact(t *testing.T) {
	loader := newMemWasmLoader()
	// 32 pages = 2 MB > 1 MB limit (16 pages)
	oomSHA := loader.Put(wasmMemoryExceedFixture(32))
	trapSHA := loader.Put(wasmTrapFixture())
	missingSHA := strings.Repeat("a", 64)

	pushCtx := PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}

	cases := []struct {
		name      string
		sha       string
		memMB     int
		failMode  FailMode
		wantAllow bool
	}{
		{name: "oom closed", sha: oomSHA, memMB: 1, failMode: FailModeClosed, wantAllow: false},
		{name: "oom open", sha: oomSHA, memMB: 1, failMode: FailModeOpen, wantAllow: true},
		{name: "trap closed", sha: trapSHA, memMB: 16, failMode: FailModeClosed, wantAllow: false},
		{name: "trap open", sha: trapSHA, memMB: 16, failMode: FailModeOpen, wantAllow: true},
		{name: "missing artifact closed", sha: missingSHA, memMB: 16, failMode: FailModeClosed, wantAllow: false},
		{name: "missing artifact open", sha: missingSHA, memMB: 16, failMode: FailModeOpen, wantAllow: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &staticConfigStore{hooks: []HookConfig{
				{HookID: tc.name, WasmSHA256: tc.sha, RefGlob: "*", TimeoutMS: 50, MemoryLimitMB: tc.memMB, FailMode: tc.failMode},
			}}
			eng := NewEngine(loader, store, EngineConfig{})
			defer func() { _ = eng.Close(context.Background()) }()

			res, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
			if err != nil {
				t.Fatalf("EvaluatePush error: %v", err)
			}
			if res.Allowed != tc.wantAllow {
				t.Fatalf("Allowed = %v, want %v (msg=%q warnings=%v)", res.Allowed, tc.wantAllow, res.Message, res.Warnings)
			}
			if tc.wantAllow && len(res.Warnings) == 0 {
				t.Fatal("expected warning for fail_mode=open failure")
			}
		})
	}
}

func TestEngineHookHostABIEndToEnd(t *testing.T) {
	oldOID := oidByte(0x11)
	newOID := oidByte(0x22)
	oldTree := oidByte(0x33)
	newTree := oidByte(0x44)
	blobOID := oidByte(0x55)

	loader := newMemWasmLoader()
	abiSHA := loader.Put(wasmABIExerciserFixture(oldOID, newOID, oldTree, newTree, blobOID))

	reader := &stubObjectReader{
		commits: []CommitMeta{{OID: newOID, TreeOID: newTree, Message: "feat: add hook"}},
		changes: []TreeChange{{Path: "README.md", Kind: ChangeAdd, NewOID: blobOID, NewSize: 12}},
		blobs:   map[[20]byte][]byte{blobOID: []byte("hello world!")},
	}

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "abi-check", WasmSHA256: abiSHA, RefGlob: "refs/heads/*", TimeoutMS: 100, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{})
	defer func() { _ = eng.Close(context.Background()) }()

	res, err := eng.EvaluatePush(context.Background(), PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		ActorID:  "bob",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oldOID, NewOID: newOID}},
	}, reader)
	if err != nil {
		t.Fatalf("EvaluatePush error: %v", err)
	}
	if !res.Allowed {
		t.Fatalf("expected Allowed=true, got false (msg=%q)", res.Message)
	}
	if reader.commitCalls != 1 || reader.diffCalls != 1 || reader.blobCalls != 1 {
		t.Fatalf("expected 1 call each to commit/diff/blob, got %d/%d/%d", reader.commitCalls, reader.diffCalls, reader.blobCalls)
	}
}

func TestEngineHostCallBudgetEnforcement(t *testing.T) {
	blobOID := oidByte(0x77)
	loader := newMemWasmLoader()
	spamSHA := loader.Put(wasmBlobBudgetSpammerFixture(blobOID, 1024))

	reader := &stubObjectReader{
		blobs: map[[20]byte][]byte{blobOID: make([]byte, 4096)},
	}
	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "blob-spam", WasmSHA256: spamSHA, RefGlob: "*", TimeoutMS: 200, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{
		MaxBlobBytesRead: 2048,
	})
	defer func() { _ = eng.Close(context.Background()) }()

	res, err := eng.EvaluatePush(context.Background(), PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}, reader)
	if err != nil {
		t.Fatalf("EvaluatePush error: %v", err)
	}
	if res.Allowed {
		t.Fatal("expected budget exhaustion to trigger REJECT")
	}
	if !strings.Contains(res.Message, "budget-hit") {
		t.Fatalf("expected budget-hit message, got %q", res.Message)
	}
	if reader.blobCalls != 2 {
		t.Fatalf("expected exactly 2 blob reads within 2048-byte budget, got %d", reader.blobCalls)
	}
}

func TestEngineModuleCacheHitAvoidsRecompilation(t *testing.T) {
	loader := newMemWasmLoader()
	allowSHA := loader.Put(wasmVerdictFixture(0, "ok"))

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "cached-hook", WasmSHA256: allowSHA, RefGlob: "*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{ModuleCacheSize: 8})
	defer func() { _ = eng.Close(context.Background()) }()

	pushCtx := PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}

	res1, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
	if err != nil || !res1.Allowed {
		t.Fatalf("first eval failed: res=%+v err=%v", res1, err)
	}
	if res1.HookOutcomes[0].CacheHit {
		t.Fatal("expected CacheHit=false on first invocation")
	}

	res2, err := eng.EvaluatePush(context.Background(), pushCtx, &stubObjectReader{})
	if err != nil || !res2.Allowed {
		t.Fatalf("second eval failed: res=%+v err=%v", res2, err)
	}
	if !res2.HookOutcomes[0].CacheHit {
		t.Fatal("expected CacheHit=true on second invocation")
	}
	if got := loader.LoadCount(allowSHA); got != 1 {
		t.Fatalf("expected WasmLoader called once, got %d", got)
	}
}

func TestEngineRefGlobFiltering(t *testing.T) {
	loader := newMemWasmLoader()
	rejectSHA := loader.Put(wasmVerdictFixture(1, "protected branch"))

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "protect-main", WasmSHA256: rejectSHA, RefGlob: "refs/heads/main", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{})
	defer func() { _ = eng.Close(context.Background()) }()

	// Push to feature branch should NOT trigger protect-main.
	res, err := eng.EvaluatePush(context.Background(), PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates:  []RefUpdate{{Ref: "refs/heads/feat-1", OldOID: oidByte(1), NewOID: oidByte(2)}},
	}, &stubObjectReader{})
	if err != nil {
		t.Fatalf("EvaluatePush error: %v", err)
	}
	if !res.Allowed || len(res.HookOutcomes) != 0 {
		t.Fatalf("expected feature push allowed with 0 hook runs, got allowed=%v outcomes=%d", res.Allowed, len(res.HookOutcomes))
	}
	if loader.LoadCount(rejectSHA) != 0 {
		t.Fatalf("expected skipped hook not to load wasm, got %d loads", loader.LoadCount(rejectSHA))
	}
}

func TestEngineStructuredPerRefVerdict(t *testing.T) {
	payload, err := json.Marshal(HookVerdict{
		Decision: VerdictReject,
		Message:  "policy violation",
		RefVerdicts: []RefVerdict{
			{Ref: "refs/heads/main", Decision: VerdictReject, Message: "force-push forbidden"},
			{Ref: "refs/heads/dev", Decision: VerdictAllow},
		},
	})
	if err != nil {
		t.Fatalf("marshal verdict: %v", err)
	}

	loader := newMemWasmLoader()
	sha := loader.Put(wasmVerdictFixture(int32(verdictDecisionJSON), string(payload)))

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "ref-policy", WasmSHA256: sha, RefGlob: "refs/heads/*", TimeoutMS: 50, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{})
	defer func() { _ = eng.Close(context.Background()) }()

	res, err := eng.EvaluatePush(context.Background(), PushContext{
		TenantID: "t1",
		RepoID:   "r1",
		Updates: []RefUpdate{
			{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)},
			{Ref: "refs/heads/dev", OldOID: oidByte(3), NewOID: oidByte(4)},
		},
	}, &stubObjectReader{})
	if err != nil {
		t.Fatalf("EvaluatePush error: %v", err)
	}
	if res.Allowed {
		t.Fatal("expected Allowed=false")
	}
	if len(res.RefVerdicts) != 2 || res.RefVerdicts[0].Ref != "refs/heads/main" {
		t.Fatalf("unexpected RefVerdicts: %+v", res.RefVerdicts)
	}
}

func TestEngineBulkheadAndCacheUnderRace(t *testing.T) {
	loader := newMemWasmLoader()
	sha1 := loader.Put(wasmVerdictFixture(0, "ok-1"))
	sha2 := loader.Put(wasmVerdictFixture(0, "ok-2"))
	sha3 := loader.Put(wasmVerdictFixture(0, "ok-3"))

	store := &staticConfigStore{hooks: []HookConfig{
		{HookID: "concurrent-hook-1", WasmSHA256: sha1, RefGlob: "*", TimeoutMS: 100, MemoryLimitMB: 16, FailMode: FailModeClosed},
		{HookID: "concurrent-hook-2", WasmSHA256: sha2, RefGlob: "*", TimeoutMS: 100, MemoryLimitMB: 16, FailMode: FailModeClosed},
		{HookID: "concurrent-hook-3", WasmSHA256: sha3, RefGlob: "*", TimeoutMS: 100, MemoryLimitMB: 16, FailMode: FailModeClosed},
	}}
	eng := NewEngine(loader, store, EngineConfig{
		MaxConcurrency:  4,
		ModuleCacheSize: 2,
	})
	defer func() { _ = eng.Close(context.Background()) }()

	const callers = 24
	var wg sync.WaitGroup
	var succeeded atomic.Int32

	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := eng.EvaluatePush(context.Background(), PushContext{
				TenantID: "t1",
				RepoID:   "r1",
				Updates:  []RefUpdate{{Ref: "refs/heads/main", OldOID: oidByte(1), NewOID: oidByte(2)}},
			}, &stubObjectReader{})
			if err != nil {
				t.Errorf("EvaluatePush error: %v", err)
				return
			}
			if !res.Allowed {
				t.Errorf("expected Allowed=true, got false: %s", res.Message)
				return
			}
			succeeded.Add(1)
		}()
	}
	wg.Wait()

	if got := succeeded.Load(); got != callers {
		t.Fatalf("succeeded = %d, want %d", got, callers)
	}
}

func TestEffectiveMemoryPagesBounds(t *testing.T) {
	eng := &Engine{
		cfg: EngineConfig{
			DefaultMemoryLimitMB: 16,
			MaxMemoryLimitMB:     math.MaxInt,
		},
	}

	if got := eng.effectiveMemoryPages(0); got != 16*wasmPagesPerMB {
		t.Fatalf("effectiveMemoryPages(0) = %d, want %d", got, 16*wasmPagesPerMB)
	}
	wantMax := uint32(math.MaxUint32/wasmPagesPerMB) * wasmPagesPerMB
	if got := eng.effectiveMemoryPages(math.MaxInt); got != wantMax {
		t.Fatalf("effectiveMemoryPages(math.MaxInt) = %d, want %d", got, wantMax)
	}

	negEng := &Engine{
		cfg: EngineConfig{
			DefaultMemoryLimitMB: -1,
			MaxMemoryLimitMB:     64,
		},
	}
	if got := negEng.effectiveMemoryPages(-5); got != 0 {
		t.Fatalf("effectiveMemoryPages(-5) with negative default = %d, want 0", got)
	}
}
