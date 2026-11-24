package loadgen

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitClientLocalLifecycleAndFsck(t *testing.T) {
	ctx := context.Background()
	bareDir := filepath.Join(t.TempDir(), "remote.git")
	workDir := filepath.Join(t.TempDir(), "work")

	bareGit := NewGitClient(bareDir, GitOptions{})
	if err := os.MkdirAll(bareDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := bareGit.Run(ctx, "init", "--bare", "-b", "main"); err != nil {
		t.Fatalf("git init --bare: %v", err)
	}

	g := NewGitClient(workDir, GitOptions{})
	if err := g.Clone(ctx, bareDir, workDir); err != nil {
		t.Fatalf("Clone: %v", err)
	}
	if err := g.ConfigUser(ctx, "Loadgen Dev", "dev@loadgen.local"); err != nil {
		t.Fatalf("ConfigUser: %v", err)
	}
	if err := g.WriteFile("README.md", []byte("hello stravin\n")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := g.AddAll(ctx); err != nil {
		t.Fatalf("AddAll: %v", err)
	}
	if err := g.Commit(ctx, "initial commit"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	sha, err := g.RevParse(ctx, "HEAD")
	if err != nil {
		t.Fatalf("RevParse: %v", err)
	}
	if len(sha) != 40 {
		t.Fatalf("RevParse HEAD = %q, want 40-char SHA", sha)
	}
	if err := g.Push(ctx, "origin", "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("Push: %v", err)
	}

	// Verify ls-remote sees pushed SHA.
	out, err := g.LsRemote(ctx, bareDir, "refs/heads/main")
	if err != nil {
		t.Fatalf("LsRemote: %v", err)
	}
	if !strings.Contains(out, sha) {
		t.Fatalf("LsRemote output %q missing SHA %q", out, sha)
	}

	// Verify git fsck --full --strict succeeds on clean repo.
	if err := g.Fsck(ctx); err != nil {
		t.Fatalf("Fsck on clean repo: %v", err)
	}
}

func TestGitClientFsckDetectsCorruption(t *testing.T) {
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "corrupt")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		t.Fatal(err)
	}

	g := NewGitClient(workDir, GitOptions{})
	if _, err := g.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if _, err := g.Run(ctx, "config", "maintenance.auto", "false"); err != nil {
		t.Fatalf("disable maintenance.auto: %v", err)
	}
	if _, err := g.Run(ctx, "config", "gc.auto", "0"); err != nil {
		t.Fatalf("disable gc.auto: %v", err)
	}
	if err := g.ConfigUser(ctx, "Loadgen", "loadgen@local"); err != nil {
		t.Fatal(err)
	}
	if err := g.WriteFile("data.txt", []byte("integrity test\n")); err != nil {
		t.Fatal(err)
	}
	if err := g.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(ctx, "commit 1"); err != nil {
		t.Fatal(err)
	}

	// Corrupt a loose object in .git/objects to trigger git fsck --full --strict failure.
	corrupted := false
	objectsDir := filepath.Join(workDir, ".git", "objects")
	err := filepath.Walk(objectsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if info.IsDir() || corrupted || strings.HasSuffix(info.Name(), ".lock") {
			return nil
		}
		parent := filepath.Base(filepath.Dir(path))
		if isHexFanoutDir(parent) {
			if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
				return chmodErr
			}
			if writeErr := os.WriteFile(path, []byte("corrupted-bytes"), 0o600); writeErr != nil {
				return writeErr
			}
			corrupted = true
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil || !corrupted {
		t.Fatalf("failed to corrupt object: err=%v corrupted=%v", err, corrupted)
	}

	fsckErr := g.Fsck(ctx)
	if fsckErr == nil {
		t.Fatal("expected git fsck --full --strict to fail on corrupted repo")
	}
	if got := ClassifyError(fsckErr, ClassifyOptions{}); got != ErrorUnexpected {
		t.Errorf("ClassifyError(fsckErr) = %q, want %q", got, ErrorUnexpected)
	}
}

func isHexFanoutDir(name string) bool {
	if len(name) != 2 {
		return false
	}
	for i := 0; i < 2; i++ {
		c := name[i]
		isDigit := c >= '0' && c <= '9'
		isHexLower := c >= 'a' && c <= 'f'
		if !isDigit && !isHexLower {
			return false
		}
	}
	return true
}

func TestExtractHTTPStatusAndRedactToken(t *testing.T) {
	if got := extractHTTPStatus("fatal: unable to access 'http://x/': The requested URL returned error: 503"); got != 503 {
		t.Errorf("extractHTTPStatus = %d, want 503", got)
	}
	if got := extractHTTPStatus("remote: HTTP 500 Internal Server Error"); got != 500 {
		t.Errorf("extractHTTPStatus = %d, want 500", got)
	}
	if got := extractHTTPStatus("no status here"); got != 0 {
		t.Errorf("extractHTTPStatus = %d, want 0", got)
	}

	redacted := redactSecret("Authorization: Bearer super-secret-token failed", "super-secret-token")
	if strings.Contains(redacted, "super-secret-token") {
		t.Errorf("secret not redacted: %q", redacted)
	}
}

func TestGitClientBuildEnvSSHConnectTimeout(t *testing.T) {
	tests := []struct {
		name        string
		sshKey      string
		wantKeyFlag bool
	}{
		{name: "without ssh key", sshKey: "", wantKeyFlag: false},
		{name: "with ssh key", sshKey: "/tmp/test_key", wantKeyFlag: true},
	}

	requiredFlags := []string{
		"-o StrictHostKeyChecking=no",
		"-o UserKnownHostsFile=/dev/null",
		"-o BatchMode=yes",
		"-o ConnectTimeout=5",
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			g := NewGitClient(t.TempDir(), GitOptions{SSHKey: tc.sshKey})
			env := g.buildEnv()

			var sshCmd string
			for _, kv := range env {
				if strings.HasPrefix(kv, "GIT_SSH_COMMAND=") {
					sshCmd = strings.TrimPrefix(kv, "GIT_SSH_COMMAND=")
				}
			}
			if sshCmd == "" {
				t.Fatal("expected GIT_SSH_COMMAND to always be set")
			}
			for _, flag := range requiredFlags {
				if !strings.Contains(sshCmd, flag) {
					t.Errorf("GIT_SSH_COMMAND = %q, want %q", sshCmd, flag)
				}
			}
			keyFlag := "-i " + tc.sshKey
			if tc.wantKeyFlag && !strings.Contains(sshCmd, keyFlag) {
				t.Errorf("GIT_SSH_COMMAND = %q, want %q", sshCmd, keyFlag)
			}
			if !tc.wantKeyFlag && strings.Contains(sshCmd, " -i ") {
				t.Errorf("GIT_SSH_COMMAND = %q, must not include -i when SSHKey is empty", sshCmd)
			}
		})
	}
}

func TestGitClientWithAuthorEnvPropagationAndOverride(t *testing.T) {
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "author-env")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		t.Fatal(err)
	}

	base := NewGitClient(workDir, GitOptions{
		ExtraEnv: []string{
			"CUSTOM_VAR=keep",
			"GIT_AUTHOR_NAME=Old Name",
			"GIT_COMMITTER_EMAIL=old@example.com",
		},
	})
	g := base.WithAuthor("Stravin Agent 7", "agent-7@loadgen.stravin.local")

	// Verify parent ExtraEnv was not mutated.
	if len(base.opts.ExtraEnv) != 3 || base.opts.ExtraEnv[1] != "GIT_AUTHOR_NAME=Old Name" {
		t.Fatalf("base ExtraEnv mutated: %v", base.opts.ExtraEnv)
	}

	// Verify overriding author replaces prior author/committer vars without duplicates.
	g2 := g.WithAuthor("Override User", "override@stravin.local")
	wantVars := map[string]string{
		"CUSTOM_VAR":          "keep",
		"GIT_AUTHOR_NAME":     "Override User",
		"GIT_AUTHOR_EMAIL":    "override@stravin.local",
		"GIT_COMMITTER_NAME":  "Override User",
		"GIT_COMMITTER_EMAIL": "override@stravin.local",
	}
	seen := make(map[string]int, len(wantVars))
	for _, kv := range g2.opts.ExtraEnv {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", kv)
		}
		seen[k]++
		if want, exists := wantVars[k]; exists && v != want {
			t.Errorf("%s = %q, want %q", k, v, want)
		}
	}
	for k := range wantVars {
		if seen[k] != 1 {
			t.Errorf("env var %s count = %d, want 1 (ExtraEnv=%v)", k, seen[k], g2.opts.ExtraEnv)
		}
	}

	// Verify git commit succeeds without git config user.name/user.email.
	if _, err := g.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := g.WriteFile("note.txt", []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if err := g.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(ctx, "env author commit"); err != nil {
		t.Fatalf("Commit with WithAuthor failed: %v", err)
	}
	authorOut, err := g.Run(ctx, "log", "-1", "--format=%an <%ae> | %cn <%ce>")
	if err != nil {
		t.Fatalf("git log: %v", err)
	}
	wantAuthor := "Stravin Agent 7 <agent-7@loadgen.stravin.local> | Stravin Agent 7 <agent-7@loadgen.stravin.local>"
	if strings.TrimSpace(authorOut) != wantAuthor {
		t.Fatalf("commit identity = %q, want %q", strings.TrimSpace(authorOut), wantAuthor)
	}
}

func TestGitClientRevParseHeadFastPathAndFallbacks(t *testing.T) {
	ctx := context.Background()
	workDir := filepath.Join(t.TempDir(), "revparse")
	if err := os.MkdirAll(workDir, 0o750); err != nil {
		t.Fatal(err)
	}

	g := NewGitClient(workDir, GitOptions{}).WithAuthor("Test User", "test@stravin.local")
	if _, err := g.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}

	// 1. Unborn HEAD: fast path returns false and fallback git rev-parse HEAD fails.
	if sha, ok := resolveHeadSHA(workDir); ok {
		t.Fatalf("expected unborn HEAD fast path miss, got %q", sha)
	}
	if _, err := g.RevParse(ctx, "HEAD"); err == nil {
		t.Fatal("expected RevParse(HEAD) error on unborn repo")
	}

	// 2. Symbolic ref HEAD -> refs/heads/main (loose ref fast path).
	if err := g.WriteFile("a.txt", []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if err := g.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := g.Commit(ctx, "first"); err != nil {
		t.Fatal(err)
	}
	wantSHAOut, err := g.Run(ctx, "rev-parse", "HEAD")
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	wantSHA := strings.TrimSpace(wantSHAOut)

	fastSHA, ok := resolveHeadSHA(workDir)
	if !ok || fastSHA != wantSHA {
		t.Fatalf("resolveHeadSHA(symbolic) = (%q, %v), want (%q, true)", fastSHA, ok, wantSHA)
	}
	gotSHA, err := g.RevParse(ctx, "HEAD")
	if err != nil || gotSHA != wantSHA {
		t.Fatalf("RevParse(HEAD) = (%q, %v), want (%q, nil)", gotSHA, err, wantSHA)
	}

	// 3. Packed refs fallback: pack-refs --all removes loose ref; fast path misses, RevParse still succeeds.
	if _, err := g.Run(ctx, "pack-refs", "--all"); err != nil {
		t.Fatalf("git pack-refs: %v", err)
	}
	if sha, ok := resolveHeadSHA(workDir); ok {
		t.Fatalf("expected packed-refs fast path miss, got %q", sha)
	}
	gotPacked, err := g.RevParse(ctx, "HEAD")
	if err != nil || gotPacked != wantSHA {
		t.Fatalf("RevParse(HEAD) packed-refs fallback = (%q, %v), want (%q, nil)", gotPacked, err, wantSHA)
	}

	// 4. Detached HEAD fast path.
	if _, err := g.Run(ctx, "checkout", "--detach", wantSHA); err != nil {
		t.Fatalf("git checkout --detach: %v", err)
	}
	detachedSHA, ok := resolveHeadSHA(workDir)
	if !ok || detachedSHA != wantSHA {
		t.Fatalf("resolveHeadSHA(detached) = (%q, %v), want (%q, true)", detachedSHA, ok, wantSHA)
	}
	gotDetached, err := g.RevParse(ctx, "HEAD")
	if err != nil || gotDetached != wantSHA {
		t.Fatalf("RevParse(HEAD) detached = (%q, %v), want (%q, nil)", gotDetached, err, wantSHA)
	}
}

func TestGitClientWorktreeLifecycleAndRevParseFastPath(t *testing.T) {
	ctx := context.Background()
	baseDir := filepath.Join(t.TempDir(), "base")
	if err := os.MkdirAll(baseDir, 0o750); err != nil {
		t.Fatal(err)
	}

	baseGit := NewGitClient(baseDir, GitOptions{}).WithAuthor("Test User", "test@stravin.local")
	if _, err := baseGit.Run(ctx, "init", "-b", "main"); err != nil {
		t.Fatalf("git init: %v", err)
	}
	if err := baseGit.WriteFile("README.md", []byte("base\n")); err != nil {
		t.Fatal(err)
	}
	if err := baseGit.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := baseGit.Commit(ctx, "initial"); err != nil {
		t.Fatal(err)
	}
	baseSHA, err := baseGit.RevParse(ctx, "HEAD")
	if err != nil {
		t.Fatalf("RevParse base HEAD: %v", err)
	}

	wtDir := filepath.Join(t.TempDir(), "wt-1")
	if err := baseGit.WorktreeAddDetach(ctx, wtDir, baseSHA); err != nil {
		t.Fatalf("WorktreeAddDetach: %v", err)
	}

	// Linked detached worktree should hit resolveHeadSHA fast path.
	wtSHA, ok := resolveHeadSHA(wtDir)
	if !ok || wtSHA != baseSHA {
		t.Fatalf("resolveHeadSHA(linked detached) = (%q, %v), want (%q, true)", wtSHA, ok, baseSHA)
	}

	wtGit := baseGit.WithWorkDir(wtDir)
	gotSHA, err := wtGit.RevParse(ctx, "HEAD")
	if err != nil || gotSHA != baseSHA {
		t.Fatalf("RevParse(HEAD) in linked detached worktree = (%q, %v), want (%q, nil)", gotSHA, err, baseSHA)
	}

	// Switching to a new branch inside the linked worktree should also hit fast path via commondir.
	if err := wtGit.CheckoutNewBranch(ctx, "wt-branch-1"); err != nil {
		t.Fatalf("CheckoutNewBranch: %v", err)
	}
	if err := wtGit.WriteFile("wt.txt", []byte("from worktree\n")); err != nil {
		t.Fatal(err)
	}
	if err := wtGit.AddAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wtGit.Commit(ctx, "wt commit"); err != nil {
		t.Fatal(err)
	}
	wantBranchSHAOut, err := wtGit.Run(ctx, "rev-parse", "HEAD")
	if err != nil {
		t.Fatalf("git rev-parse HEAD: %v", err)
	}
	wantBranchSHA := strings.TrimSpace(wantBranchSHAOut)

	branchFastSHA, ok := resolveHeadSHA(wtDir)
	if !ok || branchFastSHA != wantBranchSHA {
		t.Fatalf("resolveHeadSHA(linked branch) = (%q, %v), want (%q, true)", branchFastSHA, ok, wantBranchSHA)
	}

	// Remove worktree and verify directory and metadata are cleaned up.
	if err := baseGit.WorktreeRemove(ctx, wtDir); err != nil {
		t.Fatalf("WorktreeRemove: %v", err)
	}
	if _, statErr := os.Stat(wtDir); !os.IsNotExist(statErr) {
		t.Fatalf("expected worktree dir %s to be removed, statErr=%v", wtDir, statErr)
	}

	// Verify WorktreePrune cleans up manually deleted worktree metadata.
	wtDir2 := filepath.Join(t.TempDir(), "wt-2")
	if err := baseGit.WorktreeAddDetach(ctx, wtDir2, baseSHA); err != nil {
		t.Fatalf("WorktreeAddDetach 2: %v", err)
	}
	if err := os.RemoveAll(wtDir2); err != nil {
		t.Fatal(err)
	}
	if err := baseGit.WorktreePrune(ctx); err != nil {
		t.Fatalf("WorktreePrune: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(baseDir, ".git", "worktrees"))
	if err == nil && len(entries) != 0 {
		t.Fatalf("expected 0 stale worktree metadata entries after prune, got %d", len(entries))
	}
}
