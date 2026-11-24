package scenarios

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"stravin.dev/core/internal/loadgen"
)

func repoURL(env *loadgen.ScenarioEnv) string {
	return env.Config.RepoHTTPURL(env.Repo)
}

func sshRepoURL(env *loadgen.ScenarioEnv) string {
	if strings.HasPrefix(env.Config.SSHAddr, "file://") {
		base := strings.TrimRight(env.Config.SSHAddr, "/")
		return fmt.Sprintf("%s/%s/%s.git", base, env.Config.Tenant, strings.TrimSuffix(env.Repo, ".git"))
	}
	return env.Config.RepoSSHURL(env.Repo)
}

func newTempDir(env *loadgen.ScenarioEnv, ent *Entropy, prefix string) (string, func()) {
	dir := filepath.Join(env.WorkDir, fmt.Sprintf("%s-%d-%s", prefix, env.WorkerID, ent.HexID()))
	return dir, func() { _ = os.RemoveAll(dir) }
}

func cloneFresh(ctx context.Context, env *loadgen.ScenarioEnv, scenario, remoteURL, destDir string) (*loadgen.GitClient, error) {
	if err := env.RunOp(ctx, scenario, "clone", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return env.Git.Clone(opCtx, remoteURL, destDir)
	}); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("Stravin %s %d", env.Persona, env.WorkerID)
	email := fmt.Sprintf("%s-%d@loadgen.stravin.local", env.Persona, env.WorkerID)
	return env.Git.WithWorkDir(destDir).WithAuthor(name, email), nil
}

func hasHead(ctx context.Context, g *loadgen.GitClient) bool {
	_, err := g.RevParse(ctx, "HEAD")
	return err == nil
}

// ensureBaseCommit guarantees the local working tree has at least one commit on HEAD.
// On an empty remote repository it seeds an initial commit and pushes it to main,
// cleanly handling races where another worker seeds main first.
func ensureBaseCommit(ctx context.Context, env *loadgen.ScenarioEnv, scenario string, g *loadgen.GitClient, ent *Entropy) error {
	if hasHead(ctx, g) {
		return nil
	}
	if _, err := g.Run(ctx, "checkout", "-B", "main"); err != nil {
		return err
	}
	if err := g.WriteFile("README.md", ent.WeightedPayload()); err != nil {
		return err
	}
	if err := g.AddAll(ctx); err != nil {
		return err
	}
	if err := g.Commit(ctx, ent.CommitMessage("seed")); err != nil {
		return err
	}

	err := env.RunOp(ctx, scenario, "push_seed", loadgen.ClassifyOptions{ExclusiveBranch: false}, func(opCtx context.Context) error {
		return g.Push(opCtx, "origin", "HEAD:refs/heads/main")
	})
	if err == nil {
		return nil
	}
	if loadgen.ClassifyError(err, loadgen.ClassifyOptions{ExclusiveBranch: false}) != loadgen.ErrorRetryableConflict {
		return err
	}

	// Another worker seeded main first; fetch and align onto origin/main.
	env.RecordRetry(ctx, scenario, loadgen.ErrorRetryableConflict)
	if fetchErr := env.RunOp(ctx, scenario, "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin", "main")
	}); fetchErr != nil {
		return fetchErr
	}
	_, checkoutErr := g.Run(ctx, "checkout", "-B", "main", "FETCH_HEAD")
	return checkoutErr
}

func commitFile(ctx context.Context, g *loadgen.GitClient, relPath string, content []byte, msg string) (string, error) {
	if err := g.WriteFile(relPath, content); err != nil {
		return "", err
	}
	if err := g.AddAll(ctx); err != nil {
		return "", err
	}
	if err := g.Commit(ctx, msg); err != nil {
		return "", err
	}
	return g.RevParse(ctx, "HEAD")
}

func runFsck(ctx context.Context, env *loadgen.ScenarioEnv, scenario string, g *loadgen.GitClient) error {
	start := time.Now()
	err := g.Fsck(ctx)
	dur := time.Since(start)
	if err != nil {
		if ctx.Err() != nil {
			env.RecordOp(ctx, scenario, "fsck", "canceled", dur)
			return ctx.Err()
		}
		env.RecordOp(ctx, scenario, "fsck", "fsck_failure", dur)
		env.RecordFsckFailure(ctx, scenario, err.Error())
		return err
	}
	env.RecordOp(ctx, scenario, "fsck", "ok", dur)
	return nil
}

func fetchAndVerifySHA(ctx context.Context, env *loadgen.ScenarioEnv, scenario string, g *loadgen.GitClient, branch, expectedSHA string) error {
	if err := env.RunOp(ctx, scenario, "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin", branch)
	}); err != nil {
		return err
	}
	actualSHA, err := g.RevParse(ctx, "FETCH_HEAD")
	if err != nil {
		return err
	}
	if env.Oracle != nil {
		return env.Oracle.VerifySHA(ctx, env.Persona, scenario, expectedSHA, actualSHA)
	}
	if actualSHA != expectedSHA {
		return fmt.Errorf("fetched SHA mismatch for %s: got %q, want %q", branch, actualSHA, expectedSHA)
	}
	return nil
}

// pushWithConflictRetry pushes the current HEAD to remote branch, handling
// Ref-CAS / non-fast-forward contention via fetch + rebase + retry.
// It returns the final pushed commit SHA.
func pushWithConflictRetry(ctx context.Context, env *loadgen.ScenarioEnv, scenario string, g *loadgen.GitClient, branch string) (string, error) {
	maxAttempts := env.Config.MaxRetries + 1
	if maxAttempts < 4 {
		maxAttempts = 4
	}
	refspec := "HEAD:refs/heads/" + branch
	opts := loadgen.ClassifyOptions{ExclusiveBranch: false}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		sha, err := g.RevParse(ctx, "HEAD")
		if err != nil {
			return "", err
		}
		pushErr := env.RunOp(ctx, scenario, "push", opts, func(opCtx context.Context) error {
			return g.Push(opCtx, "origin", refspec)
		})
		if pushErr == nil {
			return sha, nil
		}
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		if loadgen.ClassifyError(pushErr, opts) != loadgen.ErrorRetryableConflict {
			return "", pushErr
		}
		if attempt+1 >= maxAttempts {
			return "", fmt.Errorf("shared branch %s push conflict retries exhausted: %w", branch, pushErr)
		}

		env.RecordRetry(ctx, scenario, loadgen.ErrorRetryableConflict)
		if fetchErr := env.RunOp(ctx, scenario, "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
			return g.Fetch(opCtx, "origin", branch)
		}); fetchErr != nil {
			return "", fetchErr
		}
		if rebaseErr := g.Rebase(ctx, "FETCH_HEAD"); rebaseErr != nil {
			_ = g.RebaseAbort(ctx)
			return "", fmt.Errorf("rebase onto %s failed: %w", branch, rebaseErr)
		}
	}
	return "", errors.New("unreachable push retry loop exit")
}

// verifyCommittedAncestor fetches branch from origin and verifies committedSHA
// is equal to or an ancestor of the remote tip (accounting for concurrent pushes).
func verifyCommittedAncestor(ctx context.Context, env *loadgen.ScenarioEnv, scenario string, g *loadgen.GitClient, branch, committedSHA string) error {
	if err := env.RunOp(ctx, scenario, "fetch", loadgen.ClassifyOptions{}, func(opCtx context.Context) error {
		return g.Fetch(opCtx, "origin", branch)
	}); err != nil {
		return err
	}
	if _, err := g.Run(ctx, "merge-base", "--is-ancestor", committedSHA, "FETCH_HEAD"); err != nil {
		detail := fmt.Sprintf("committed SHA %s missing from %s history: %v", committedSHA, branch, err)
		if env.Oracle != nil {
			env.Oracle.RecordUnexpected(ctx, env.Persona, scenario, "verify_ancestor", detail)
		}
		return errors.New(detail)
	}
	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func verifyFileSHA256(ctx context.Context, env *loadgen.ScenarioEnv, scenario, filePath, expectedHex string) error {
	cleanPath := filepath.Clean(filePath)
	data, err := os.ReadFile(cleanPath) //#nosec G304 -- path is within worker's isolated temp clone directory
	if err != nil {
		return err
	}
	actualHex := sha256Hex(data)
	if actualHex != expectedHex {
		detail := fmt.Sprintf("binary SHA256 mismatch for %s: expected %s, got %s", filepath.Base(cleanPath), expectedHex, actualHex)
		if env.Oracle != nil {
			env.Oracle.RecordUnexpected(ctx, env.Persona, scenario, "verify_sha256", detail)
		}
		return errors.New(detail)
	}
	return nil
}

func parseLsRemoteSHA(output, ref string) string {
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[1] == ref {
			return fields[0]
		}
	}
	return ""
}
