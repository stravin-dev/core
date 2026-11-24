package scenarios

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"stravin.dev/core/internal/loadgen"
)

// 12. agent_rapid_checkpoint_burst: burst of 5-15 rapid sequential commits/pushes.
type agentRapidCheckpointBurst struct{}

func (s *agentRapidCheckpointBurst) Name() string             { return "agent_rapid_checkpoint_burst" }
func (s *agentRapidCheckpointBurst) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentRapidCheckpointBurst) Weight() int              { return 14 }

func (s *agentRapidCheckpointBurst) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-burst")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("checkpoint")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	count := ent.IntRange(5, 15)
	var headSHA string
	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		relPath := fmt.Sprintf("checkpoints/%s/step-%02d.txt", ent.HexID(), i)
		headSHA, err = commitFile(ctx, g, relPath, ent.WeightedPayload(), ent.CommitMessage("checkpoint"))
		if err != nil {
			return err
		}
		if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
			return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
		}); err != nil {
			return err
		}
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, branch, headSHA)
}

// 13. agent_parallel_branch_fanout: 3-5 topic branches pushed in quick succession.
type agentParallelBranchFanout struct{}

func (s *agentParallelBranchFanout) Name() string             { return "agent_parallel_branch_fanout" }
func (s *agentParallelBranchFanout) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentParallelBranchFanout) Weight() int              { return 12 }

func (s *agentParallelBranchFanout) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-fanout")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}
	baseSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}

	fanout := ent.IntRange(3, 5)
	pushed := make(map[string]string, fanout)
	for i := 0; i < fanout; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := g.Checkout(ctx, baseSHA); err != nil {
			return err
		}
		branch := ent.BranchName(fmt.Sprintf("fanout-%d", i))
		if err := g.CheckoutNewBranch(ctx, branch); err != nil {
			return err
		}
		sha, commitErr := commitFile(ctx, g, fmt.Sprintf("fanout/%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("fanout"))
		if commitErr != nil {
			return commitErr
		}
		if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
			return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
		}); err != nil {
			return err
		}
		pushed[branch] = sha
	}

	for branch, expectedSHA := range pushed {
		if err := fetchAndVerifySHA(ctx, env, s.Name(), g, branch, expectedSHA); err != nil {
			return err
		}
	}
	return nil
}

// 14. agent_shared_branch_race: concurrent agents racing on shared branch with fetch+rebase+retry.
type agentSharedBranchRace struct{}

func (s *agentSharedBranchRace) Name() string             { return "agent_shared_branch_race" }
func (s *agentSharedBranchRace) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentSharedBranchRace) Weight() int              { return 12 }

func (s *agentSharedBranchRace) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	const sharedBranch = "agent/shared-race"
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-race")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	// Start from remote shared branch if it already exists; otherwise branch from HEAD.
	// Use a unique local branch name to avoid worktree branch-lock contention.
	localBranch := ent.BranchName("shared-race")
	if fetchErr := g.Fetch(ctx, "origin", sharedBranch); fetchErr == nil {
		if _, err := g.Run(ctx, "checkout", "-B", localBranch, "FETCH_HEAD"); err != nil {
			return err
		}
	} else {
		if _, err := g.Run(ctx, "checkout", "-B", localBranch); err != nil {
			return err
		}
	}

	relPath := fmt.Sprintf("shared-race/agent-%d-%s.txt", env.WorkerID, ent.HexID())
	if _, err := commitFile(ctx, g, relPath, ent.WeightedPayload(), ent.CommitMessage("race")); err != nil {
		return err
	}

	committedSHA, err := pushWithConflictRetry(ctx, env, s.Name(), g, sharedBranch)
	if err != nil {
		return err
	}
	return verifyCommittedAncestor(ctx, env, s.Name(), g, sharedBranch, committedSHA)
}

// 15. agent_deep_tree_refactor: nested tree + bulk rename/move/delete -> push -> clone + git fsck --full --strict.
type agentDeepTreeRefactor struct{}

func (s *agentDeepTreeRefactor) Name() string             { return "agent_deep_tree_refactor" }
func (s *agentDeepTreeRefactor) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentDeepTreeRefactor) Weight() int              { return 10 }

func (s *agentDeepTreeRefactor) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	remote := repoURL(env)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), remote, ent, "agent-tree")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("refactor")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	paths := ent.DeepTreePaths(ent.IntRange(4, 6), ent.IntRange(6, 10))
	for i, p := range paths {
		// Use a weighted payload on the primary file and small text on the rest
		// to exercise weighted sizes while keeping multi-file refactors bounded.
		var payload []byte
		if i == 0 {
			payload = ent.WeightedPayload()
		} else {
			payload = ent.TextPayload(ent.IntRange(3, 8))
		}
		if err := g.WriteFile(p, payload); err != nil {
			return err
		}
	}
	if err := g.AddAll(ctx); err != nil {
		return err
	}
	if err := g.Commit(ctx, ent.CommitMessage("tree-create")); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_initial", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}

	// Bulk move first half and delete two files.
	movedPaths := make([]string, 0, len(paths)/2)
	deletedPaths := make([]string, 0, 2)
	for i, p := range paths {
		switch {
		case i < len(paths)/2:
			dst := filepath.ToSlash(filepath.Join("refactored", ent.HexID(), filepath.Base(p)))
			if err := os.MkdirAll(filepath.Dir(filepath.Join(g.WorkDir(), dst)), 0o750); err != nil {
				return err
			}
			if _, err := g.Run(ctx, "mv", p, dst); err != nil {
				return err
			}
			movedPaths = append(movedPaths, dst)
		case len(deletedPaths) < 2:
			if _, err := g.Run(ctx, "rm", p); err != nil {
				return err
			}
			deletedPaths = append(deletedPaths, p)
		}
	}
	if err := g.Commit(ctx, ent.CommitMessage("tree-refactor")); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_refactor", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}

	verifyDir, verifyCleanup := newTempDir(env, ent, "agent-tree-verify")
	defer verifyCleanup()
	vg, err := cloneFresh(ctx, env, s.Name(), remote, verifyDir)
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return vg.Fetch(opCtx, "origin", branch)
	}); err != nil {
		return err
	}
	if err := vg.Checkout(ctx, "FETCH_HEAD"); err != nil {
		return err
	}
	for _, dst := range movedPaths {
		if _, statErr := os.Stat(filepath.Join(verifyDir, dst)); statErr != nil {
			return fmt.Errorf("expected refactored file %s missing: %w", dst, statErr)
		}
	}
	for _, del := range deletedPaths {
		if _, statErr := os.Stat(filepath.Join(verifyDir, del)); !errors.Is(statErr, os.ErrNotExist) {
			return fmt.Errorf("expected deleted file %s to be absent", del)
		}
	}
	return runFsck(ctx, env, s.Name(), vg)
}

// 16. agent_orphan_branch_artifact: orphan branch commit + push + isolated fetch verify.
type agentOrphanBranchArtifact struct{}

func (s *agentOrphanBranchArtifact) Name() string             { return "agent_orphan_branch_artifact" }
func (s *agentOrphanBranchArtifact) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentOrphanBranchArtifact) Weight() int              { return 8 }

func (s *agentOrphanBranchArtifact) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	remote := repoURL(env)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), remote, ent, "agent-orphan")
	if err != nil {
		return err
	}
	defer cleanup()

	branch := ent.BranchName("artifact")
	if _, err := g.Run(ctx, "checkout", "--orphan", branch); err != nil {
		return err
	}
	_, _ = g.Run(ctx, "rm", "-rf", "--ignore-unmatch", ".")

	artifactSHA, err := commitFile(ctx, g, fmt.Sprintf("artifacts/%s.json", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("orphan"))
	if err != nil {
		return err
	}

	parentsOut, err := g.Run(ctx, "rev-list", "--parents", "-n", "1", "HEAD")
	if err != nil {
		return err
	}
	if len(strings.Fields(parentsOut)) != 1 {
		return fmt.Errorf("orphan commit expected 0 parents, got %q", strings.TrimSpace(parentsOut))
	}

	if err := env.RunOp(ctx, s.Name(), "push_orphan", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}

	// Isolated fetch into an empty local repo.
	isoDir, isoCleanup := newTempDir(env, ent, "agent-orphan-iso")
	defer isoCleanup()
	if err := os.MkdirAll(isoDir, 0o750); err != nil {
		return err
	}
	isoGit := env.Git.WithWorkDir(isoDir)
	if _, err := isoGit.Run(ctx, "init", "-b", "main"); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "fetch_orphan", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return isoGit.Fetch(opCtx, remote, "refs/heads/"+branch)
	}); err != nil {
		return err
	}
	gotSHA, err := isoGit.RevParse(ctx, "FETCH_HEAD")
	if err != nil {
		return err
	}
	if env.Oracle != nil {
		if err := env.Oracle.VerifySHA(ctx, env.Persona, s.Name(), artifactSHA, gotSHA); err != nil {
			return err
		}
	} else if gotSHA != artifactSHA {
		return fmt.Errorf("orphan fetch SHA mismatch: got %q, want %q", gotSHA, artifactSHA)
	}
	return runFsck(ctx, env, s.Name(), isoGit)
}

// 17. agent_merge_commit_diamond: two branches merged into diamond history -> push -> parent verify.
type agentMergeCommitDiamond struct{}

func (s *agentMergeCommitDiamond) Name() string             { return "agent_merge_commit_diamond" }
func (s *agentMergeCommitDiamond) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentMergeCommitDiamond) Weight() int              { return 10 }

func (s *agentMergeCommitDiamond) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-diamond")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}
	baseSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}

	leftBranch := ent.BranchName("diamond-left")
	if err := g.CheckoutNewBranch(ctx, leftBranch); err != nil {
		return err
	}
	leftSHA, err := commitFile(ctx, g, fmt.Sprintf("diamond/left-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("diamond-left"))
	if err != nil {
		return err
	}

	if err := g.Checkout(ctx, baseSHA); err != nil {
		return err
	}
	rightBranch := ent.BranchName("diamond-right")
	if err := g.CheckoutNewBranch(ctx, rightBranch); err != nil {
		return err
	}
	rightSHA, err := commitFile(ctx, g, fmt.Sprintf("diamond/right-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("diamond-right"))
	if err != nil {
		return err
	}

	if _, err := g.Run(ctx, "merge", "--no-ff", "-m", ent.CommitMessage("diamond-merge"), leftBranch); err != nil {
		return err
	}
	mergeSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}

	if err := env.RunOp(ctx, s.Name(), "push_diamond", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+rightBranch)
	}); err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), g, rightBranch, mergeSHA); err != nil {
		return err
	}

	parentsOut, err := g.Run(ctx, "rev-list", "--parents", "-n", "1", "FETCH_HEAD")
	if err != nil {
		return err
	}
	fields := strings.Fields(parentsOut)
	if len(fields) != 3 || fields[0] != mergeSHA || fields[1] != rightSHA || fields[2] != leftSHA {
		detail := fmt.Sprintf("diamond merge parents mismatch: got %v, want [%s %s %s]", fields, mergeSHA, rightSHA, leftSHA)
		if env.Oracle != nil {
			env.Oracle.RecordUnexpected(ctx, env.Persona, s.Name(), "verify_parents", detail)
		}
		return errors.New(detail)
	}
	return nil
}

// 18. agent_cherry_pick_backport: cherry-pick feature commit onto release branch -> push both.
type agentCherryPickBackport struct{}

func (s *agentCherryPickBackport) Name() string             { return "agent_cherry_pick_backport" }
func (s *agentCherryPickBackport) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentCherryPickBackport) Weight() int              { return 8 }

func (s *agentCherryPickBackport) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-cherry")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}
	baseSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}

	releaseBranch := ent.BranchName("release")
	if err := g.CheckoutNewBranch(ctx, releaseBranch); err != nil {
		return err
	}
	if _, err := commitFile(ctx, g, fmt.Sprintf("backport/rel-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("release-base")); err != nil {
		return err
	}

	if err := g.Checkout(ctx, baseSHA); err != nil {
		return err
	}
	featBranch := ent.BranchName("feature")
	if err := g.CheckoutNewBranch(ctx, featBranch); err != nil {
		return err
	}
	if _, err := commitFile(ctx, g, fmt.Sprintf("backport/feat1-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("feat-1")); err != nil {
		return err
	}
	pickFile := fmt.Sprintf("backport/fix-%s.txt", ent.HexID())
	pickSHA, err := commitFile(ctx, g, pickFile, ent.WeightedPayload(), ent.CommitMessage("feat-fix"))
	if err != nil {
		return err
	}

	if err := g.Checkout(ctx, releaseBranch); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "cherry-pick", pickSHA); err != nil {
		return err
	}
	releaseSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(g.WorkDir(), pickFile)); err != nil {
		return fmt.Errorf("cherry-picked file %s missing on release branch: %w", pickFile, err)
	}

	if err := env.RunOp(ctx, s.Name(), "push_both", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin",
			"refs/heads/"+featBranch+":refs/heads/"+featBranch,
			"refs/heads/"+releaseBranch+":refs/heads/"+releaseBranch,
		)
	}); err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), g, featBranch, pickSHA); err != nil {
		return err
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, releaseBranch, releaseSHA)
}

// 19. agent_wasm_hook_allow_and_reject: allowed push + rejected push verifying clean side-band reject & zero corruption.
type agentWasmHookAllowAndReject struct{}

func (s *agentWasmHookAllowAndReject) Name() string             { return "agent_wasm_hook_allow_and_reject" }
func (s *agentWasmHookAllowAndReject) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentWasmHookAllowAndReject) Weight() int              { return 8 }

func (s *agentWasmHookAllowAndReject) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), repoURL(env), ent, "agent-hook")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	// Phase 1: Allowed push on normal exclusive branch.
	allowedBranch := ent.BranchName("hook-allow")
	if err := g.CheckoutNewBranch(ctx, allowedBranch); err != nil {
		return err
	}
	allowedSHA, err := commitFile(ctx, g, fmt.Sprintf("hooks/ok-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("hook-allow"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_allow", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+allowedBranch)
	}); err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), g, allowedBranch, allowedSHA); err != nil {
		return err
	}

	// Phase 2: Push designed to trigger pre-receive / Wasm policy hook rejection.
	rejectBranch := "reject/" + ent.Slug("policy")
	if err := g.CheckoutNewBranch(ctx, rejectBranch); err != nil {
		return err
	}
	if _, err := commitFile(ctx, g, fmt.Sprintf("hooks/reject-%s.txt", ent.HexID()), []byte("STRAVIN_HOOK_REJECT=true\n"), "reject: trigger policy hook"); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_reject", loadgen.ClassifyOptions{ExpectReject: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+rejectBranch)
	}); err != nil {
		return err
	}

	// Verify repository integrity remains clean after allow + reject sequence
	// using an isolated fresh clone rather than the shared-base linked worktree.
	return runFsckFreshClone(ctx, env, s.Name(), repoURL(env), ent, "agent-hook-verify")
}

// 20. agent_high_churn_compaction_trigger: rapid small packs triggering stravin-pack compaction under concurrent reads.
type agentHighChurnCompactionTrigger struct{}

func (s *agentHighChurnCompactionTrigger) Name() string             { return "agent_high_churn_compaction_trigger" }
func (s *agentHighChurnCompactionTrigger) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentHighChurnCompactionTrigger) Weight() int              { return 8 }

func (s *agentHighChurnCompactionTrigger) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	remote := repoURL(env)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), remote, ent, "agent-churn")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("churn")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	// Spawn a concurrent reader performing ls-remote polls while rapid small packs are pushed.
	readCtx, stopReader := context.WithCancel(ctx)
	var readerErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		readerGit := env.Git.WithWorkDir(env.WorkDir)
		for i := 0; i < 6 && readCtx.Err() == nil; i++ {
			if err := env.RunOp(readCtx, s.Name(), "concurrent_ls_remote", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
				_, lsErr := readerGit.LsRemote(opCtx, remote)
				return lsErr
			}); err != nil && readCtx.Err() == nil {
				readerErr = err
				return
			}
		}
	}()

	packs := ent.IntRange(6, 12)
	var headSHA string
	for i := 0; i < packs; i++ {
		if ctx.Err() != nil {
			stopReader()
			wg.Wait()
			return ctx.Err()
		}
		headSHA, err = commitFile(ctx, g, fmt.Sprintf("churn/%s/p-%02d.txt", ent.HexID(), i), ent.WeightedPayload(), ent.CommitMessage("churn"))
		if err != nil {
			stopReader()
			wg.Wait()
			return err
		}
		if err := env.RunOp(ctx, s.Name(), "push_pack", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
			return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
		}); err != nil {
			stopReader()
			wg.Wait()
			return err
		}
	}

	stopReader()
	wg.Wait()
	if readerErr != nil {
		return readerErr
	}
	verifyDir, verifyCleanup := newTempDir(env, ent, "agent-churn-verify")
	defer verifyCleanup()
	vg, err := cloneFresh(ctx, env, s.Name(), remote, verifyDir)
	if err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), vg, branch, headSHA); err != nil {
		return err
	}
	return runFsck(ctx, env, s.Name(), vg)
}

// 21. agent_protocol_switch_http_ssh: push over Smart HTTP -> immediately fetch/verify over SSH (and reverse).
type agentProtocolSwitchHTTPSSH struct{}

func (s *agentProtocolSwitchHTTPSSH) Name() string             { return "agent_protocol_switch_http_ssh" }
func (s *agentProtocolSwitchHTTPSSH) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentProtocolSwitchHTTPSSH) Weight() int              { return 8 }
func (s *agentProtocolSwitchHTTPSSH) RequiresSSH() bool        { return true }

func (s *agentProtocolSwitchHTTPSSH) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	sshDir, sshCleanup := newTempDir(env, ent, "agent-proto-ssh")
	defer sshCleanup()

	httpURL := repoURL(env)
	sshURL := sshRepoURL(env)

	// Step A: Push over Smart HTTP -> immediately fetch/verify over SSH.
	httpGit, httpCleanup, err := acquireWorktree(ctx, env, s.Name(), httpURL, ent, "agent-proto-http")
	if err != nil {
		return err
	}
	defer httpCleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), httpGit, ent); err != nil {
		return err
	}
	httpBranch := ent.BranchName("http-to-ssh")
	if err := httpGit.CheckoutNewBranch(ctx, httpBranch); err != nil {
		return err
	}
	httpSHA, err := commitFile(ctx, httpGit, fmt.Sprintf("proto/http-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("http-push"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_http", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return httpGit.Push(opCtx, "origin", "HEAD:refs/heads/"+httpBranch)
	}); err != nil {
		return err
	}

	sshGit, err := cloneFresh(ctx, env, s.Name(), sshURL, sshDir)
	if err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), sshGit, httpBranch, httpSHA); err != nil {
		return err
	}

	// Step B: Push over SSH -> immediately fetch/verify over Smart HTTP.
	if err := sshGit.Checkout(ctx, "FETCH_HEAD"); err != nil {
		return err
	}
	sshBranch := ent.BranchName("ssh-to-http")
	if err := sshGit.CheckoutNewBranch(ctx, sshBranch); err != nil {
		return err
	}
	sshSHA, err := commitFile(ctx, sshGit, fmt.Sprintf("proto/ssh-%s.txt", ent.HexID()), ent.WeightedPayload(), ent.CommitMessage("ssh-push"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_ssh", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return sshGit.Push(opCtx, "origin", "HEAD:refs/heads/"+sshBranch)
	}); err != nil {
		return err
	}
	verifyDir, verifyCleanup := newTempDir(env, ent, "agent-proto-verify")
	defer verifyCleanup()
	vg, err := cloneFresh(ctx, env, s.Name(), httpURL, verifyDir)
	if err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), vg, sshBranch, sshSHA); err != nil {
		return err
	}
	return runFsck(ctx, env, s.Name(), vg)
}

// 22. agent_corrupt_ref_negative_test: invalid ref name rejected cleanly without harming repo health.
type agentCorruptRefNegativeTest struct{}

func (s *agentCorruptRefNegativeTest) Name() string             { return "agent_corrupt_ref_negative_test" }
func (s *agentCorruptRefNegativeTest) Persona() loadgen.Persona { return loadgen.PersonaAgent }
func (s *agentCorruptRefNegativeTest) Weight() int              { return 6 }

func (s *agentCorruptRefNegativeTest) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	remote := repoURL(env)
	g, cleanup, err := acquireWorktree(ctx, env, s.Name(), remote, ent, "agent-badref")
	if err != nil {
		return err
	}
	defer cleanup()
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	badRef := ent.InvalidRefName()
	start := time.Now()
	pushErr := g.Push(ctx, "origin", "HEAD:"+badRef)
	dur := time.Since(start)
	if pushErr == nil {
		detail := fmt.Sprintf("invalid ref %q was unexpectedly accepted", badRef)
		env.RecordOp(ctx, s.Name(), "push_invalid_ref", "unexpected", dur)
		if env.Oracle != nil {
			env.Oracle.RecordUnexpected(ctx, env.Persona, s.Name(), "push_invalid_ref", detail)
		}
		return errors.New(detail)
	}
	if ctx.Err() != nil {
		env.RecordOp(ctx, s.Name(), "push_invalid_ref", "canceled", dur)
		return ctx.Err()
	}

	class := loadgen.ClassifyError(pushErr, loadgen.ClassifyOptions{ExpectReject: true})
	if class != loadgen.ErrorExpectedReject {
		env.RecordOp(ctx, s.Name(), "push_invalid_ref", string(class), dur)
		return pushErr
	}
	env.RecordOp(ctx, s.Name(), "push_invalid_ref", "expected_reject", dur)

	// Confirm remote and local repository health are unharmed.
	if err := env.RunOp(ctx, s.Name(), "ls-remote", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		_, lsErr := g.LsRemote(opCtx, remote)
		return lsErr
	}); err != nil {
		return err
	}
	return runFsckFreshClone(ctx, env, s.Name(), remote, ent, "agent-badref-verify")
}
