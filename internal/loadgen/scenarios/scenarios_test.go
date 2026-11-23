package scenarios

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"stravin.dev/core/internal/loadgen"
)

func TestRegisterAllAndCatalogInvariants(t *testing.T) {
	if err := RegisterAll(nil); err == nil {
		t.Fatal("expected error for nil registry")
	}

	reg := loadgen.NewRegistry()
	if err := RegisterAll(reg); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	if err := RegisterAll(reg); err == nil {
		t.Fatal("expected duplicate registration error on second RegisterAll")
	}

	devs := reg.Scenarios(loadgen.PersonaDev)
	agents := reg.Scenarios(loadgen.PersonaAgent)
	if len(devs) != 11 {
		t.Fatalf("dev scenarios = %d, want 11", len(devs))
	}
	if len(agents) != 11 {
		t.Fatalf("agent scenarios = %d, want 11", len(agents))
	}
	if len(devs)+len(agents) < 22 {
		t.Fatalf("total scenarios = %d, want >= 22", len(devs)+len(agents))
	}

	wantNames := map[string]loadgen.Persona{
		"dev_clone_and_read":                  loadgen.PersonaDev,
		"dev_feature_branch_pr":               loadgen.PersonaDev,
		"dev_incremental_thin_pack":           loadgen.PersonaDev,
		"dev_rebase_and_force_push":           loadgen.PersonaDev,
		"dev_concurrent_main_push_retry":      loadgen.PersonaDev,
		"dev_tag_and_release":                 loadgen.PersonaDev,
		"dev_large_binary_asset":              loadgen.PersonaDev,
		"dev_branch_cleanup_cycle":            loadgen.PersonaDev,
		"dev_shallow_clone_unshallow":         loadgen.PersonaDev,
		"dev_ssh_full_lifecycle":              loadgen.PersonaDev,
		"dev_noop_empty_push_and_fetch":       loadgen.PersonaDev,
		"agent_rapid_checkpoint_burst":        loadgen.PersonaAgent,
		"agent_parallel_branch_fanout":        loadgen.PersonaAgent,
		"agent_shared_branch_race":            loadgen.PersonaAgent,
		"agent_deep_tree_refactor":            loadgen.PersonaAgent,
		"agent_orphan_branch_artifact":        loadgen.PersonaAgent,
		"agent_merge_commit_diamond":          loadgen.PersonaAgent,
		"agent_cherry_pick_backport":          loadgen.PersonaAgent,
		"agent_wasm_hook_allow_and_reject":    loadgen.PersonaAgent,
		"agent_high_churn_compaction_trigger": loadgen.PersonaAgent,
		"agent_protocol_switch_http_ssh":      loadgen.PersonaAgent,
		"agent_corrupt_ref_negative_test":     loadgen.PersonaAgent,
	}

	seen := make(map[string]bool, len(wantNames))
	for _, s := range append(devs, agents...) {
		if s.Weight() <= 0 {
			t.Errorf("scenario %q has non-positive weight %d", s.Name(), s.Weight())
		}
		wantPersona, ok := wantNames[s.Name()]
		if !ok {
			t.Errorf("unexpected scenario %q", s.Name())
		}
		if s.Persona() != wantPersona {
			t.Errorf("scenario %q persona = %q, want %q", s.Name(), s.Persona(), wantPersona)
		}
		wantSSH := s.Name() == "dev_ssh_full_lifecycle" || s.Name() == "agent_protocol_switch_http_ssh"
		sshScen, isSSH := s.(loadgen.SSHScenario)
		gotSSH := isSSH && sshScen.RequiresSSH()
		if gotSSH != wantSSH {
			t.Errorf("scenario %q RequiresSSH() = %v, want %v", s.Name(), gotSSH, wantSSH)
		}
		seen[s.Name()] = true
	}
	for name := range wantNames {
		if !seen[name] {
			t.Errorf("missing required scenario %q", name)
		}
	}

	// Verify CLI --scenarios filtering via Registry.Filter.
	filtered, err := reg.Filter([]string{"dev_clone_and_read", "agent_corrupt_ref_negative_test"})
	if err != nil {
		t.Fatalf("Filter valid scenarios: %v", err)
	}
	if len(filtered.Scenarios(loadgen.PersonaDev)) != 1 || len(filtered.Scenarios(loadgen.PersonaAgent)) != 1 {
		t.Fatalf("unexpected filtered counts: dev=%d agent=%d",
			len(filtered.Scenarios(loadgen.PersonaDev)), len(filtered.Scenarios(loadgen.PersonaAgent)))
	}
	if _, err := reg.Filter([]string{"nonexistent_scenario"}); err == nil {
		t.Fatal("expected error filtering unknown scenario")
	}
}

func TestEntropyDeterministicUnderSeed(t *testing.T) {
	e1 := NewEntropy(rand.New(rand.NewPCG(42, 7)))
	e2 := NewEntropy(rand.New(rand.NewPCG(42, 7)))
	e3 := NewEntropy(rand.New(rand.NewPCG(99, 7)))

	if e1.IntRange(1, 100) != e2.IntRange(1, 100) {
		t.Fatal("IntRange not deterministic")
	}
	if e1.Slug("x") != e2.Slug("x") {
		t.Fatal("Slug not deterministic")
	}
	if e1.BranchName("feat") != e2.BranchName("feat") {
		t.Fatal("BranchName not deterministic")
	}
	if e1.TagName("rel") != e2.TagName("rel") {
		t.Fatal("TagName not deterministic")
	}
	if e1.CommitMessage("dev") != e2.CommitMessage("dev") {
		t.Fatal("CommitMessage not deterministic")
	}
	p1, p2 := e1.TextPayload(12), e2.TextPayload(12)
	if !bytes.Equal(p1, p2) {
		t.Fatal("TextPayload not deterministic")
	}
	if !bytes.Equal(e1.SmallDelta(p1), e2.SmallDelta(p2)) {
		t.Fatal("SmallDelta not deterministic")
	}
	b1, b2 := e1.IncompressibleBlob(1027), e2.IncompressibleBlob(1027)
	if !bytes.Equal(b1, b2) || len(b1) != 1027 {
		t.Fatal("IncompressibleBlob not deterministic or wrong size")
	}
	paths1, paths2 := e1.DeepTreePaths(4, 5), e2.DeepTreePaths(4, 5)
	if strings.Join(paths1, "|") != strings.Join(paths2, "|") {
		t.Fatal("DeepTreePaths not deterministic")
	}
	if e1.InvalidRefName() != e2.InvalidRefName() {
		t.Fatal("InvalidRefName not deterministic")
	}
	if e1.WeightedFileSize() != e2.WeightedFileSize() {
		t.Fatal("WeightedFileSize not deterministic")
	}
	wp1, wp2 := e1.WeightedPayload(), e2.WeightedPayload()
	if !bytes.Equal(wp1, wp2) {
		t.Fatal("WeightedPayload not deterministic")
	}
	if e1.BranchName("feat") == e3.BranchName("feat") {
		t.Fatal("different seeds unexpectedly produced identical branch name")
	}
}

func TestWeightedFileSizeAndPayloadDistribution(t *testing.T) {
	const (
		samples      = 2000
		minBytes     = 256
		smallMax     = 4 * 1024
		largeMin     = 128 * 1024
		maxBytes     = 512 * 1024
		sampleSeed   = 1337
		sampleStream = 42
	)

	ent := NewEntropy(rand.New(rand.NewPCG(sampleSeed, sampleStream)))
	sizes := make([]int, samples)
	var sum int64
	var smallCount, extraLargeCount int

	for i := 0; i < samples; i++ {
		sz := ent.WeightedFileSize()
		if sz < minBytes || sz > maxBytes {
			t.Fatalf("sample %d: WeightedFileSize() = %d, want [%d, %d]", i, sz, minBytes, maxBytes)
		}
		sizes[i] = sz
		sum += int64(sz)
		if sz <= smallMax {
			smallCount++
		}
		if sz > largeMin {
			extraLargeCount++
		}
	}

	slices.Sort(sizes)
	median := float64(sizes[samples/2])
	mean := float64(sum) / float64(samples)

	smallRatio := float64(smallCount) / float64(samples)
	extraLargeRatio := float64(extraLargeCount) / float64(samples)

	if smallRatio <= 0.60 {
		t.Fatalf("small tier (<=4KiB) ratio = %.3f, want > 0.60", smallRatio)
	}
	if extraLargeCount == 0 || extraLargeRatio >= 0.10 {
		t.Fatalf("extra-large tier (>128KiB) count=%d ratio=%.3f, want >0 and <0.10", extraLargeCount, extraLargeRatio)
	}
	if median >= mean {
		t.Fatalf("expected skewed distribution with median < mean, got median=%.1f mean=%.1f", median, mean)
	}

	// Verify WeightedPayload length matches WeightedFileSize for identical seeds.
	for i := 0; i < 64; i++ {
		seed := uint64(777 + i)
		eSize := NewEntropy(rand.New(rand.NewPCG(seed, 1)))
		ePayload := NewEntropy(rand.New(rand.NewPCG(seed, 1)))
		wantLen := eSize.WeightedFileSize()
		payload := ePayload.WeightedPayload()
		if len(payload) != wantLen {
			t.Fatalf("iter %d: len(WeightedPayload()) = %d, want %d", i, len(payload), wantLen)
		}
	}
}

// initBareRepo creates <root>/<tenant>/<repo>.git and optionally seeds commits on main.
func initBareRepo(t *testing.T, root, tenant, repo string, seedCommits int) string {
	t.Helper()
	ctx := context.Background()
	bareDir := filepath.Join(root, tenant, repo+".git")
	if err := os.MkdirAll(bareDir, 0o750); err != nil {
		t.Fatal(err)
	}
	bareGit := loadgen.NewGitClient(bareDir, loadgen.GitOptions{})
	if _, err := bareGit.Run(ctx, "init", "--bare", "-b", "main"); err != nil {
		t.Fatalf("git init --bare: %v", err)
	}
	if seedCommits <= 0 {
		return bareDir
	}

	seedDir := filepath.Join(t.TempDir(), "seed")
	g := loadgen.NewGitClient(seedDir, loadgen.GitOptions{})
	if err := g.Clone(ctx, bareDir, seedDir); err != nil {
		t.Fatalf("clone bare for seed: %v", err)
	}
	if err := g.ConfigUser(ctx, "Seed Bot", "seed@stravin.local"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < seedCommits; i++ {
		if err := g.WriteFile("README.md", []byte(strings.Repeat("stravin-seed\n", i+1))); err != nil {
			t.Fatal(err)
		}
		if err := g.AddAll(ctx); err != nil {
			t.Fatal(err)
		}
		if err := g.Commit(ctx, "seed commit"); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.Push(ctx, "origin", "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("push seed: %v", err)
	}
	return bareDir
}

func newTestEnv(t *testing.T, root string, persona loadgen.Persona, workerID int, seed uint64, oracle *loadgen.Oracle) *loadgen.ScenarioEnv {
	t.Helper()
	workDir := t.TempDir()
	cfg := loadgen.DefaultConfig()
	cfg.HTTPURL = "file://" + root
	cfg.SSHAddr = "file://" + root
	cfg.Tenant = "loadgen"
	cfg.Repos = []string{"repo-1"}
	cfg.MaxRetries = 5
	return &loadgen.ScenarioEnv{
		Config:      cfg,
		WorkerID:    workerID,
		Persona:     persona,
		Repo:        "repo-1",
		WorkDir:     workDir,
		RNG:         rand.New(rand.NewPCG(seed, uint64(workerID+1))),
		Git:         loadgen.NewGitClient(workDir, loadgen.GitOptions{}),
		Oracle:      oracle,
		RetryPolicy: loadgen.RetryPolicy{BaseBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond},
	}
}

func TestAll22ScenariosAgainstLocalBareRepo(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 2)

	for idx, s := range All() {
		t.Run(s.Name(), func(t *testing.T) {
			oracle := loadgen.NewOracle()
			env := newTestEnv(t, root, s.Persona(), idx, uint64(1000+idx), oracle)
			if err := s.Run(context.Background(), env); err != nil {
				t.Fatalf("scenario %s failed: %v (oracle=%+v)", s.Name(), err, oracle.Summary())
			}
			if oracle.HasFailures() {
				t.Fatalf("scenario %s recorded oracle failures: %+v records=%+v", s.Name(), oracle.Summary(), oracle.Records())
			}
			if oracle.Summary().TotalOps == 0 {
				t.Fatalf("scenario %s recorded 0 ops", s.Name())
			}
		})
	}
}

func TestScenariosOnEmptyBareRepo(t *testing.T) {
	candidates := []loadgen.Scenario{
		&devCloneAndRead{},
		&devShallowCloneUnshallow{},
		&devNoopEmptyPushAndFetch{},
		&agentOrphanBranchArtifact{},
		&devFeatureBranchPR{},
	}
	for idx, s := range candidates {
		t.Run(s.Name(), func(t *testing.T) {
			root := t.TempDir()
			initBareRepo(t, root, "loadgen", "repo-1", 0)
			oracle := loadgen.NewOracle()
			env := newTestEnv(t, root, s.Persona(), idx, uint64(2000+idx), oracle)
			if err := s.Run(context.Background(), env); err != nil {
				t.Fatalf("empty repo scenario %s failed: %v", s.Name(), err)
			}
			if oracle.HasFailures() {
				t.Fatalf("empty repo scenario %s recorded oracle failures: %+v", s.Name(), oracle.Records())
			}
		})
	}
}

func TestConcurrentSharedBranchPushRace(t *testing.T) {
	cases := []struct {
		name     string
		scenario loadgen.Scenario
		branch   string
	}{
		{name: "dev_concurrent_main_push_retry", scenario: &devConcurrentMainPushRetry{}, branch: "main"},
		{name: "agent_shared_branch_race", scenario: &agentSharedBranchRace{}, branch: "agent/shared-race"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			bareDir := initBareRepo(t, root, "loadgen", "repo-1", 1)
			oracle := loadgen.NewOracle()

			const workers = 4
			var wg sync.WaitGroup
			errs := make([]error, workers)
			for i := 0; i < workers; i++ {
				env := newTestEnv(t, root, tc.scenario.Persona(), i, uint64(3000+i), oracle)
				env.Config.MaxRetries = 8
				wg.Add(1)
				go func(idx int, e *loadgen.ScenarioEnv) {
					defer wg.Done()
					errs[idx] = tc.scenario.Run(context.Background(), e)
				}(i, env)
			}
			wg.Wait()

			for i, err := range errs {
				if err != nil {
					t.Errorf("worker %d failed: %v", i, err)
				}
			}
			if oracle.HasFailures() {
				t.Fatalf("unexpected oracle failures under concurrent race: %+v", oracle.Records())
			}

			// Verify remote branch contains at least workers+1 commits (initial + 4 workers).
			bareGit := loadgen.NewGitClient(bareDir, loadgen.GitOptions{})
			out, err := bareGit.Run(context.Background(), "--git-dir", bareDir, "rev-list", "--count", "refs/heads/"+tc.branch)
			if err != nil {
				t.Fatalf("rev-list --count %s: %v", tc.branch, err)
			}
			if strings.TrimSpace(out) == "0" || strings.TrimSpace(out) == "1" {
				t.Fatalf("expected multiple commits on %s, got count=%s", tc.branch, strings.TrimSpace(out))
			}
		})
	}
}

func TestWasmHookRejectClassification(t *testing.T) {
	root := t.TempDir()
	bareDir := initBareRepo(t, root, "loadgen", "repo-1", 1)

	// Install a pre-receive hook that declines refs/heads/reject/* cleanly.
	hookPath := filepath.Join(bareDir, "hooks", "pre-receive")
	hookScript := "#!/bin/sh\nwhile read oldrev newrev refname; do\n  case \"$refname\" in\n    refs/heads/reject/*) echo \"rejected by policy (wasm hook)\" >&2; exit 1 ;;\n  esac\ndone\nexit 0\n"
	if err := os.WriteFile(hookPath, []byte(hookScript), 0o755); err != nil {
		t.Fatalf("write pre-receive hook: %v", err)
	}

	oracle := loadgen.NewOracle()
	env := newTestEnv(t, root, loadgen.PersonaAgent, 0, 4242, oracle)
	s := &agentWasmHookAllowAndReject{}
	if err := s.Run(context.Background(), env); err != nil {
		t.Fatalf("agent_wasm_hook_allow_and_reject failed: %v", err)
	}
	if oracle.HasFailures() {
		t.Fatalf("expected zero oracle failures on clean hook reject, got: %+v", oracle.Records())
	}

	// Verify no reject/* branch exists in bare repo, while hook-allow branch does.
	bareGit := loadgen.NewGitClient(bareDir, loadgen.GitOptions{})
	refsOut, err := bareGit.Run(context.Background(), "--git-dir", bareDir, "for-each-ref", "--format=%(refname)")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(refsOut, "refs/heads/reject/") {
		t.Fatalf("rejected ref unexpectedly landed in bare repo:\n%s", refsOut)
	}
	if !strings.Contains(refsOut, "hook-allow") {
		t.Fatalf("allowed ref missing from bare repo:\n%s", refsOut)
	}
}

func TestLargeBinarySHA256MismatchDetection(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "asset.bin")
	if err := os.WriteFile(filePath, []byte("corrupted-asset-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	oracle := loadgen.NewOracle()
	env := &loadgen.ScenarioEnv{Persona: loadgen.PersonaDev, Oracle: oracle}

	err := verifyFileSHA256(context.Background(), env, "dev_large_binary_asset", filePath, sha256Hex([]byte("original-asset-bytes")))
	if err == nil {
		t.Fatal("expected SHA256 mismatch error")
	}
	if !oracle.HasFailures() || oracle.Summary().UnexpectedErrors != 1 {
		t.Fatalf("expected 1 unexpected oracle error on SHA256 mismatch, got %+v", oracle.Summary())
	}
}

func TestFsckFailureCallsRecordFsckFailure(t *testing.T) {
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "corrupt-repo")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		t.Fatal(err)
	}
	g := loadgen.NewGitClient(workDir, loadgen.GitOptions{})
	if _, err := g.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatal(err)
	}
	if err := g.ConfigUser(ctx, "Test", "test@local"); err != nil {
		t.Fatal(err)
	}
	if _, err := commitFile(ctx, g, "a.txt", []byte("hello\n"), "init"); err != nil {
		t.Fatal(err)
	}

	// Corrupt a loose object in .git/objects.
	corrupted := false
	err := filepath.Walk(filepath.Join(workDir, ".git", "objects"), func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() || corrupted {
			return walkErr
		}
		parent := filepath.Base(filepath.Dir(p))
		if len(parent) == 2 && parent != "in" && parent != "pa" {
			_ = os.Chmod(p, 0o600)
			if writeErr := os.WriteFile(p, []byte("bad-object"), 0o600); writeErr != nil {
				return writeErr
			}
			corrupted = true
		}
		return nil
	})
	if err != nil || !corrupted {
		t.Fatalf("failed to corrupt object: err=%v corrupted=%v", err, corrupted)
	}

	oracle := loadgen.NewOracle()
	env := &loadgen.ScenarioEnv{Persona: loadgen.PersonaDev, Oracle: oracle}
	if fsckErr := runFsck(ctx, env, "dev_clone_and_read", g); fsckErr == nil {
		t.Fatal("expected runFsck to fail on corrupted repo")
	}
	if oracle.Summary().FsckFailures != 1 {
		t.Fatalf("FsckFailures = %d, want 1", oracle.Summary().FsckFailures)
	}
}

func TestCanceledContextStopsScenarioCleanly(t *testing.T) {
	root := t.TempDir()
	initBareRepo(t, root, "loadgen", "repo-1", 1)
	oracle := loadgen.NewOracle()
	env := newTestEnv(t, root, loadgen.PersonaDev, 0, 9001, oracle)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := (&devCloneAndRead{}).Run(ctx, env)
	if err == nil {
		t.Fatal("expected canceled context error")
	}
	if oracle.HasFailures() {
		t.Fatalf("canceled context must not record oracle failure: %+v", oracle.Records())
	}
}
