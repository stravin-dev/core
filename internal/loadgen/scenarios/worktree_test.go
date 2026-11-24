package scenarios

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"stravin.dev/core/internal/loadgen"
)

func collectOpCounts(t *testing.T, exp *loadgen.InMemoryExporters) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := exp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	counts := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_loadgen_ops_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				var op string
				for _, kv := range dp.Attributes.ToSlice() {
					if string(kv.Key) == "op" {
						op = kv.Value.AsString()
					}
				}
				counts[op] += dp.Value
			}
		}
	}
	return counts
}

func newTelemetryTestEnv(t *testing.T, root string, persona loadgen.Persona, workerID int, seed uint64) (*loadgen.ScenarioEnv, *loadgen.InMemoryExporters) {
	t.Helper()
	var logBuf bytes.Buffer
	tel, exp, err := loadgen.NewInMemoryTelemetry(&logBuf)
	if err != nil {
		t.Fatalf("NewInMemoryTelemetry: %v", err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })

	oracle := loadgen.NewOracle()
	env := newTestEnv(t, root, persona, workerID, seed, oracle)
	env.Telemetry = tel
	env.Logger = tel.Logger
	return env, exp
}

func assertNoWorktreeLeaks(t *testing.T, baseDir string) {
	t.Helper()
	wtMetaDir := filepath.Join(baseDir, ".git", "worktrees")
	entries, err := os.ReadDir(wtMetaDir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ReadDir(%s): %v", wtMetaDir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 leftover worktree metadata entries in %s, got %d", wtMetaDir, len(entries))
	}
}

func TestWorktreeBaseCloneReuse(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 1)

	env, exp := newTelemetryTestEnv(t, root, loadgen.PersonaDev, 0, 101)
	s := &devFeatureBranchPR{}

	// 1st run: should clone once into worker's base cache.
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("1st run failed: %v", err)
	}
	ops1 := collectOpCounts(t, exp)
	if ops1["clone"] != 1 {
		t.Fatalf("after 1st run: clone ops = %d, want 1 (all ops: %v)", ops1["clone"], ops1)
	}
	baseDir, ok := env.BaseClones.Get("repo-1")
	if !ok {
		t.Fatal("expected repo-1 to be cached in env.BaseClones after 1st run")
	}
	assertNoWorktreeLeaks(t, baseDir)

	// 2nd run: should reuse base clone via fetch + detached worktree, NOT clone again.
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("2nd run failed: %v", err)
	}
	ops2 := collectOpCounts(t, exp)
	if ops2["clone"] != 1 {
		t.Fatalf("after 2nd run: clone ops = %d, want 1 (reused base clone); all ops: %v", ops2["clone"], ops2)
	}
	if ops2["fetch"] <= ops1["fetch"] {
		t.Fatalf("expected fetch count to increase on 2nd run: before=%d after=%d", ops1["fetch"], ops2["fetch"])
	}
	assertNoWorktreeLeaks(t, baseDir)
}

func TestWorktreeMultiRepoSwitchingPerWorker(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 1)
	initBareRepo(t, root, "loadgen", "repo-2", 1)

	env, exp := newTelemetryTestEnv(t, root, loadgen.PersonaAgent, 0, 202)
	s := &agentRapidCheckpointBurst{}

	env.Repo = "repo-1"
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("repo-1 1st run failed: %v", err)
	}
	env.Repo = "repo-2"
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("repo-2 1st run failed: %v", err)
	}
	env.Repo = "repo-1"
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("repo-1 2nd run failed: %v", err)
	}

	dir1, ok1 := env.BaseClones.Get("repo-1")
	dir2, ok2 := env.BaseClones.Get("repo-2")
	if !ok1 || !ok2 {
		t.Fatalf("expected both repo-1 (%v) and repo-2 (%v) cached", ok1, ok2)
	}
	if dir1 == dir2 {
		t.Fatalf("repo-1 and repo-2 must have distinct base clone directories, both got %q", dir1)
	}

	ops := collectOpCounts(t, exp)
	if ops["clone"] != 2 {
		t.Fatalf("clone ops across 3 runs (2 repos) = %d, want 2 (all ops: %v)", ops["clone"], ops)
	}
	assertNoWorktreeLeaks(t, dir1)
	assertNoWorktreeLeaks(t, dir2)
}

func TestWorktreeEmptyRepoFallbackAndSubsequentReuse(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 0)

	env, exp := newTelemetryTestEnv(t, root, loadgen.PersonaDev, 0, 303)
	s := &devFeatureBranchPR{}

	// 1st run on empty repo seeds main via fresh clone fallback.
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("1st run on empty repo failed: %v", err)
	}
	opsAfterFirst := collectOpCounts(t, exp)

	// 2nd run fetches seeded main into base clone and uses detached worktree (no extra clone).
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("2nd run after seed failed: %v", err)
	}
	opsAfterSecond := collectOpCounts(t, exp)
	if opsAfterSecond["clone"] != opsAfterFirst["clone"] {
		t.Fatalf("expected 2nd run after empty-repo seed to reuse base clone without cloning: before=%d after=%d",
			opsAfterFirst["clone"], opsAfterSecond["clone"])
	}
	baseDir, ok := env.BaseClones.Get("repo-1")
	if !ok {
		t.Fatal("expected repo-1 base clone cached")
	}
	assertNoWorktreeLeaks(t, baseDir)
}

func TestWorktreeCleanupOnContextCancellation(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 1)

	env, _ := newTelemetryTestEnv(t, root, loadgen.PersonaAgent, 0, 404)
	ent := NewEntropy(env.RNG)

	ctx, cancel := context.WithCancel(context.Background())
	g, cleanup, err := acquireWorktree(ctx, env, "test_cancel", repoURL(env), ent, "wt-cancel")
	if err != nil {
		t.Fatalf("acquireWorktree: %v", err)
	}
	wtDir := g.WorkDir()
	if _, statErr := os.Stat(wtDir); statErr != nil {
		t.Fatalf("expected worktree dir %s to exist: %v", wtDir, statErr)
	}

	// Cancel scenario context before calling cleanup.
	cancel()
	cleanup()

	if _, statErr := os.Stat(wtDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected worktree dir %s to be deleted after canceled cleanup, statErr=%v", wtDir, statErr)
	}
	baseDir, ok := env.BaseClones.Get("repo-1")
	if !ok {
		t.Fatal("expected base clone to remain cached")
	}
	assertNoWorktreeLeaks(t, baseDir)
}

func TestFreshCloneExemptionsStillCloneFresh(t *testing.T) {
	cases := []struct {
		name     string
		scenario loadgen.Scenario
		wantOp   string
		minDelta int64
	}{
		{name: "dev_clone_and_read", scenario: &devCloneAndRead{}, wantOp: "clone", minDelta: 1},
		{name: "dev_shallow_clone_unshallow", scenario: &devShallowCloneUnshallow{}, wantOp: "clone_shallow", minDelta: 1},
		{name: "dev_ssh_full_lifecycle", scenario: &devSSHFullLifecycle{}, wantOp: "clone", minDelta: 1},
		{name: "agent_deep_tree_refactor_fsck_verify", scenario: &agentDeepTreeRefactor{}, wantOp: "clone", minDelta: 1},
		{name: "dev_large_binary_asset_fsck_verify", scenario: &devLargeBinaryAsset{}, wantOp: "clone", minDelta: 1},
		{name: "dev_tag_and_release_verify", scenario: &devTagAndRelease{}, wantOp: "clone", minDelta: 1},
		{name: "agent_wasm_hook_allow_and_reject_verify", scenario: &agentWasmHookAllowAndReject{}, wantOp: "clone", minDelta: 1},
		{name: "agent_high_churn_compaction_trigger_verify", scenario: &agentHighChurnCompactionTrigger{}, wantOp: "clone", minDelta: 1},
		{name: "agent_protocol_switch_http_ssh_verify", scenario: &agentProtocolSwitchHTTPSSH{}, wantOp: "clone", minDelta: 2},
		{name: "agent_badref_negative_test_verify", scenario: &agentCorruptRefNegativeTest{}, wantOp: "clone", minDelta: 1},
	}

	for idx, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			initBareRepo(t, root, "loadgen", "repo-1", 2)

			env, exp := newTelemetryTestEnv(t, root, tc.scenario.Persona(), idx, uint64(500+idx))
			// Warm the base clone cache first so any subsequent clone op must come from an explicit fresh clone.
			warmEnt := NewEntropy(env.RNG)
			_, warmCleanup, err := acquireWorktree(context.Background(), env, "warmup", repoURL(env), warmEnt, "warm")
			if err != nil {
				t.Fatalf("warmup acquireWorktree: %v", err)
			}
			warmCleanup()

			before := collectOpCounts(t, exp)[tc.wantOp]
			if err := tc.scenario.Run(context.Background(), env); err != nil {
				t.Fatalf("scenario %s failed: %v", tc.name, err)
			}
			after := collectOpCounts(t, exp)[tc.wantOp]
			if after-before < tc.minDelta {
				t.Fatalf("scenario %s: %s ops delta = %d, want >= %d (before=%d after=%d)",
					tc.name, tc.wantOp, after-before, tc.minDelta, before, after)
			}
		})
	}
}

func collectGCRunCount(t *testing.T, exp *loadgen.InMemoryExporters) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := exp.MetricReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("MetricReader.Collect: %v", err)
	}
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "stravin_loadgen_gc_runs_total" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
		}
	}
	return total
}

func TestAcquireWorktreeCleanupTriggersOnDemandGCAndRecordsTelemetry(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 1)

	env, exp := newTelemetryTestEnv(t, root, loadgen.PersonaAgent, 0, 606)
	env.BaseClones.SetGCThresholds(loadgen.GCThresholds{MaxPacks: 2, MaxLooseObjects: loadgen.DefaultGCLooseThreshold})

	// Warm base clone via acquireWorktree.
	ent := NewEntropy(env.RNG)
	_, cleanup1, err := acquireWorktree(ctx, env, "test_gc", repoURL(env), ent, "wt-gc-1")
	if err != nil {
		t.Fatalf("1st acquireWorktree: %v", err)
	}
	cleanup1()

	if got := collectGCRunCount(t, exp); got != 0 {
		t.Fatalf("expected 0 GC runs below threshold, got %d", got)
	}

	baseDir, ok := env.BaseClones.Get("repo-1")
	if !ok {
		t.Fatal("expected repo-1 cached in BaseClones")
	}

	// Acquire a second worktree, push a commit to create a 2nd packfile in baseDir,
	// then run cleanup() and verify on-demand GC fires after worktree removal.
	wtGit, cleanup2, err := acquireWorktree(ctx, env, "test_gc", repoURL(env), ent, "wt-gc-2")
	if err != nil {
		t.Fatalf("2nd acquireWorktree: %v", err)
	}
	baseGit := env.Git.WithWorkDir(baseDir)
	if _, err := baseGit.Run(ctx, "repack", "-d"); err != nil {
		t.Fatalf("1st base repack: %v", err)
	}
	if err := wtGit.CheckoutNewBranch(ctx, "gc-test-branch"); err != nil {
		t.Fatalf("CheckoutNewBranch: %v", err)
	}
	if err := wtGit.WriteFile("extra.txt", []byte("extra pack\n")); err != nil {
		t.Fatal(err)
	}
	if err := wtGit.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wtGit.Commit(ctx, "extra commit"); err != nil {
		t.Fatal(err)
	}
	if _, err := baseGit.Run(ctx, "repack", "-d"); err != nil {
		t.Fatalf("2nd base repack: %v", err)
	}

	cleanup2()

	assertNoWorktreeLeaks(t, baseDir)
	if got := collectGCRunCount(t, exp); got != 1 {
		t.Fatalf("stravin_loadgen_gc_runs_total = %d, want 1", got)
	}

	foundGCSpan := false
	for _, sp := range exp.SpanExporter.Spans() {
		if sp.Name() == "loadgen.base_clone.gc" {
			foundGCSpan = true
			break
		}
	}
	if !foundGCSpan {
		t.Fatal("expected loadgen.base_clone.gc span to be exported")
	}

	packEntries, err := os.ReadDir(filepath.Join(baseDir, ".git", "objects", "pack"))
	if err != nil {
		t.Fatalf("ReadDir pack: %v", err)
	}
	idxCount := 0
	for _, entry := range packEntries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".idx") {
			idxCount++
		}
	}
	if idxCount != 1 {
		t.Fatalf("expected packfiles consolidated to 1 .idx after GC, got %d", idxCount)
	}
}
