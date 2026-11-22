package scenarios

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"stravin.dev/core/internal/loadgen"
)

// 1. dev_clone_and_read: fresh clone + git fsck --full --strict + log/status.
type devCloneAndRead struct{}

func (s *devCloneAndRead) Name() string             { return "dev_clone_and_read" }
func (s *devCloneAndRead) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devCloneAndRead) Weight() int              { return 15 }

func (s *devCloneAndRead) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-read")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := runFsck(ctx, env, s.Name(), g); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "status", "--short"); err != nil {
		return err
	}
	if hasHead(ctx, g) {
		limit := strconv.Itoa(ent.IntRange(1, 10))
		if _, err := g.Run(ctx, "log", "-n", limit, "--oneline"); err != nil {
			return err
		}
	}
	return nil
}

// 2. dev_feature_branch_pr: clone -> branch -> 1-5 commits -> push -> fetch verify.
type devFeatureBranchPR struct{}

func (s *devFeatureBranchPR) Name() string             { return "dev_feature_branch_pr" }
func (s *devFeatureBranchPR) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devFeatureBranchPR) Weight() int              { return 15 }

func (s *devFeatureBranchPR) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-feat")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("feat")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	commits := ent.IntRange(1, 5)
	var headSHA string
	for i := 0; i < commits; i++ {
		relPath := fmt.Sprintf("features/%s/part-%d.txt", ent.HexID(), i)
		headSHA, err = commitFile(ctx, g, relPath, ent.TextPayload(ent.IntRange(4, 16)), ent.CommitMessage("feat"))
		if err != nil {
			return err
		}
	}

	if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, branch, headSHA)
}

// 3. dev_incremental_thin_pack: fetch -> small delta commit -> thin-pack push -> verify.
type devIncrementalThinPack struct{}

func (s *devIncrementalThinPack) Name() string             { return "dev_incremental_thin_pack" }
func (s *devIncrementalThinPack) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devIncrementalThinPack) Weight() int              { return 12 }

func (s *devIncrementalThinPack) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-thin")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("thin")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	relPath := fmt.Sprintf("thin/%s.txt", ent.Slug("base"))
	basePayload := ent.TextPayload(ent.IntRange(80, 160))
	if _, err := commitFile(ctx, g, relPath, basePayload, ent.CommitMessage("thin-base")); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}

	if err := env.RunOp(ctx, s.Name(), "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin")
	}); err != nil {
		return err
	}

	updatedPayload := ent.SmallDelta(basePayload)
	headSHA, err := commitFile(ctx, g, relPath, updatedPayload, ent.CommitMessage("thin-delta"))
	if err != nil {
		return err
	}

	if err := env.RunOp(ctx, s.Name(), "push_thin", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		_, pushErr := g.Run(opCtx, "push", "--thin", "origin", "HEAD:refs/heads/"+branch)
		return pushErr
	}); err != nil {
		return err
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, branch, headSHA)
}

// 4. dev_rebase_and_force_push: amend/rebase branch -> --force-with-lease push -> verify.
type devRebaseAndForcePush struct{}

func (s *devRebaseAndForcePush) Name() string             { return "dev_rebase_and_force_push" }
func (s *devRebaseAndForcePush) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devRebaseAndForcePush) Weight() int              { return 10 }

func (s *devRebaseAndForcePush) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-force")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("rebase")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	relPath := fmt.Sprintf("rebase/%s.txt", ent.Slug("wip"))
	if _, err := commitFile(ctx, g, relPath, ent.TextPayload(6), ent.CommitMessage("wip-1")); err != nil {
		return err
	}
	if _, err := commitFile(ctx, g, relPath, ent.TextPayload(8), ent.CommitMessage("wip-2")); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin", branch)
	}); err != nil {
		return err
	}

	// Amend the tip commit to rewrite history, then push with --force-with-lease.
	if err := g.WriteFile(relPath, ent.TextPayload(10)); err != nil {
		return err
	}
	if err := g.AddAll(ctx); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "commit", "--amend", "-m", ent.CommitMessage("amend")); err != nil {
		return err
	}
	amendedSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		return err
	}

	if err := env.RunOp(ctx, s.Name(), "push_force_with_lease", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.PushForceWithLease(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, branch, amendedSHA)
}

// 5. dev_concurrent_main_push_retry: push shared main -> handle Ref-CAS conflict via fetch+rebase+retry -> verify committed SHA.
type devConcurrentMainPushRetry struct{}

func (s *devConcurrentMainPushRetry) Name() string             { return "dev_concurrent_main_push_retry" }
func (s *devConcurrentMainPushRetry) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devConcurrentMainPushRetry) Weight() int              { return 10 }

func (s *devConcurrentMainPushRetry) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-main")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "checkout", "-B", "main"); err != nil {
		return err
	}

	relPath := fmt.Sprintf("main-updates/w%d-%s.txt", env.WorkerID, ent.HexID())
	if _, err := commitFile(ctx, g, relPath, ent.TextPayload(5), ent.CommitMessage("main")); err != nil {
		return err
	}

	committedSHA, err := pushWithConflictRetry(ctx, env, s.Name(), g, "main")
	if err != nil {
		return err
	}
	return verifyCommittedAncestor(ctx, env, s.Name(), g, "main", committedSHA)
}

// 6. dev_tag_and_release: annotated & lightweight tag push -> fetch --tags verify.
type devTagAndRelease struct{}

func (s *devTagAndRelease) Name() string             { return "dev_tag_and_release" }
func (s *devTagAndRelease) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devTagAndRelease) Weight() int              { return 8 }

func (s *devTagAndRelease) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-tag")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("release")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}
	targetSHA, err := commitFile(ctx, g, fmt.Sprintf("releases/%s.txt", ent.HexID()), ent.TextPayload(5), ent.CommitMessage("release"))
	if err != nil {
		return err
	}

	lwTag := ent.TagName("lw")
	annTag := ent.TagName("ann")
	if _, err := g.Run(ctx, "tag", lwTag); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "tag", "-a", annTag, "-m", ent.CommitMessage("tag")); err != nil {
		return err
	}

	if err := env.RunOp(ctx, s.Name(), "push_tags", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch, "refs/tags/"+lwTag, "refs/tags/"+annTag)
	}); err != nil {
		return err
	}

	verifyDir, verifyCleanup := newTempDir(env, ent, "dev-tag-verify")
	defer verifyCleanup()
	vg, err := cloneFresh(ctx, env, s.Name(), repoURL(env), verifyDir)
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "fetch_tags", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return vg.Fetch(opCtx, "origin", "--tags")
	}); err != nil {
		return err
	}

	for _, tag := range []string{lwTag, annTag} {
		gotSHA, revErr := vg.RevParse(ctx, "refs/tags/"+tag+"^{commit}")
		if revErr != nil {
			return revErr
		}
		if env.Oracle != nil {
			if err := env.Oracle.VerifySHA(ctx, env.Persona, s.Name(), targetSHA, gotSHA); err != nil {
				return err
			}
		} else if gotSHA != targetSHA {
			return fmt.Errorf("tag %s resolved to %q, want %q", tag, gotSHA, targetSHA)
		}
	}
	return nil
}

// 7. dev_large_binary_asset: 1-4 MiB incompressible blob push -> clone -> SHA256 verify.
type devLargeBinaryAsset struct{}

func (s *devLargeBinaryAsset) Name() string             { return "dev_large_binary_asset" }
func (s *devLargeBinaryAsset) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devLargeBinaryAsset) Weight() int              { return 6 }

func (s *devLargeBinaryAsset) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-bin")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), repoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("binary")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}

	sizeBytes := ent.IntRange(1<<20, 4<<20)
	blob := ent.IncompressibleBlob(sizeBytes)
	expectedHash := sha256Hex(blob)
	relPath := fmt.Sprintf("assets/%s.bin", ent.Slug("blob"))

	if _, err := commitFile(ctx, g, relPath, blob, ent.CommitMessage("binary")); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}

	verifyDir, verifyCleanup := newTempDir(env, ent, "dev-bin-verify")
	defer verifyCleanup()
	vg, err := cloneFresh(ctx, env, s.Name(), repoURL(env), verifyDir)
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
	if err := verifyFileSHA256(ctx, env, s.Name(), filepath.Join(verifyDir, relPath), expectedHash); err != nil {
		return err
	}
	return runFsck(ctx, env, s.Name(), vg)
}

// 8. dev_branch_cleanup_cycle: create -> push -> delete -> recreate branch -> verify.
type devBranchCleanupCycle struct{}

func (s *devBranchCleanupCycle) Name() string             { return "dev_branch_cleanup_cycle" }
func (s *devBranchCleanupCycle) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devBranchCleanupCycle) Weight() int              { return 8 }

func (s *devBranchCleanupCycle) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-cleanup")
	defer cleanup()

	remote := repoURL(env)
	g, err := cloneFresh(ctx, env, s.Name(), remote, cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("cleanup")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}
	firstSHA, err := commitFile(ctx, g, fmt.Sprintf("cleanup/%s-v1.txt", ent.HexID()), ent.TextPayload(4), ent.CommitMessage("create"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), g, branch, firstSHA); err != nil {
		return err
	}

	// Delete remote branch and verify removal via ls-remote.
	if err := env.RunOp(ctx, s.Name(), "push_delete", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "--delete", branch)
	}); err != nil {
		return err
	}
	var lsOut string
	if err := env.RunOp(ctx, s.Name(), "ls-remote", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		var lsErr error
		lsOut, lsErr = g.LsRemote(opCtx, remote, "refs/heads/"+branch)
		return lsErr
	}); err != nil {
		return err
	}
	if got := parseLsRemoteSHA(lsOut, "refs/heads/"+branch); got != "" {
		detail := fmt.Sprintf("deleted branch %s still present at %s", branch, got)
		if env.Oracle != nil {
			env.Oracle.RecordUnexpected(ctx, env.Persona, s.Name(), "verify_delete", detail)
		}
		return fmt.Errorf("%s", detail)
	}

	// Recreate branch with a fresh commit and verify.
	secondSHA, err := commitFile(ctx, g, fmt.Sprintf("cleanup/%s-v2.txt", ent.HexID()), ent.TextPayload(6), ent.CommitMessage("recreate"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_recreate", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	return fetchAndVerifySHA(ctx, env, s.Name(), g, branch, secondSHA)
}

// 9. dev_shallow_clone_unshallow: clone --depth=1 -> fetch --unshallow -> git fsck --full --strict.
type devShallowCloneUnshallow struct{}

func (s *devShallowCloneUnshallow) Name() string             { return "dev_shallow_clone_unshallow" }
func (s *devShallowCloneUnshallow) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devShallowCloneUnshallow) Weight() int              { return 8 }

func (s *devShallowCloneUnshallow) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-shallow")
	defer cleanup()

	remote := repoURL(env)
	if err := os.MkdirAll(filepath.Dir(cloneDir), 0o750); err != nil {
		return err
	}
	parent := env.Git.WithWorkDir(filepath.Dir(cloneDir))
	if err := env.RunOp(ctx, s.Name(), "clone_shallow", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		_, cloneErr := parent.Run(opCtx, "clone", "--depth=1", remote, cloneDir)
		return cloneErr
	}); err != nil {
		return err
	}

	g := env.Git.WithWorkDir(cloneDir)
	shallowMarker := filepath.Join(cloneDir, ".git", "shallow")
	if _, statErr := os.Stat(shallowMarker); statErr == nil {
		if err := env.RunOp(ctx, s.Name(), "fetch_unshallow", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
			return g.Fetch(opCtx, "origin", "--unshallow")
		}); err != nil {
			return err
		}
	} else {
		if err := env.RunOp(ctx, s.Name(), "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
			return g.Fetch(opCtx, "origin")
		}); err != nil {
			return err
		}
	}
	return runFsck(ctx, env, s.Name(), g)
}

// 10. dev_ssh_full_lifecycle: SSH :2222 clone -> commit -> push -> fetch -> git fsck --full --strict.
type devSSHFullLifecycle struct{}

func (s *devSSHFullLifecycle) Name() string             { return "dev_ssh_full_lifecycle" }
func (s *devSSHFullLifecycle) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devSSHFullLifecycle) Weight() int              { return 8 }

func (s *devSSHFullLifecycle) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-ssh")
	defer cleanup()

	g, err := cloneFresh(ctx, env, s.Name(), sshRepoURL(env), cloneDir)
	if err != nil {
		return err
	}
	if err := ensureBaseCommit(ctx, env, s.Name(), g, ent); err != nil {
		return err
	}

	branch := ent.BranchName("ssh")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}
	headSHA, err := commitFile(ctx, g, fmt.Sprintf("ssh/%s.txt", ent.HexID()), ent.TextPayload(6), ent.CommitMessage("ssh"))
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_ssh", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	if err := fetchAndVerifySHA(ctx, env, s.Name(), g, branch, headSHA); err != nil {
		return err
	}
	return runFsck(ctx, env, s.Name(), g)
}

// 11. dev_noop_empty_push_and_fetch: ls-remote + up-to-date fetch + no-op push.
type devNoopEmptyPushAndFetch struct{}

func (s *devNoopEmptyPushAndFetch) Name() string             { return "dev_noop_empty_push_and_fetch" }
func (s *devNoopEmptyPushAndFetch) Persona() loadgen.Persona { return loadgen.PersonaDev }
func (s *devNoopEmptyPushAndFetch) Weight() int              { return 10 }

func (s *devNoopEmptyPushAndFetch) Run(ctx context.Context, env *loadgen.ScenarioEnv) error {
	ent := NewEntropy(env.RNG)
	cloneDir, cleanup := newTempDir(env, ent, "dev-noop")
	defer cleanup()

	remote := repoURL(env)
	if err := env.RunOp(ctx, s.Name(), "ls-remote", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		_, err := env.Git.LsRemote(opCtx, remote)
		return err
	}); err != nil {
		return err
	}

	g, err := cloneFresh(ctx, env, s.Name(), remote, cloneDir)
	if err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin")
	}); err != nil {
		return err
	}
	if !hasHead(ctx, g) {
		return nil
	}

	// Push an exclusive branch once, then perform a guaranteed no-op push on that same ref.
	branch := ent.BranchName("noop")
	if err := g.CheckoutNewBranch(ctx, branch); err != nil {
		return err
	}
	if err := env.RunOp(ctx, s.Name(), "push_init", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	}); err != nil {
		return err
	}
	return env.RunOp(ctx, s.Name(), "push_noop", loadgen.ClassifyOptions{ExclusiveBranch: true}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/"+branch)
	})
}
