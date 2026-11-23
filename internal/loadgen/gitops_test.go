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
		if walkErr != nil || info.IsDir() || corrupted {
			return walkErr
		}
		parent := filepath.Base(filepath.Dir(path))
		if len(parent) == 2 && parent != "in" && parent != "pa" {
			if chmodErr := os.Chmod(path, 0o600); chmodErr != nil {
				return chmodErr
			}
			if writeErr := os.WriteFile(path, []byte("corrupted-bytes"), 0o600); writeErr != nil {
				return writeErr
			}
			corrupted = true
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
	g := NewGitClient(t.TempDir(), GitOptions{SSHKey: "/tmp/test_key"})
	env := g.buildEnv()

	var sshCmd string
	for _, kv := range env {
		if strings.HasPrefix(kv, "GIT_SSH_COMMAND=") {
			sshCmd = strings.TrimPrefix(kv, "GIT_SSH_COMMAND=")
			break
		}
	}
	if sshCmd == "" {
		t.Fatal("expected GIT_SSH_COMMAND to be set when SSHKey is configured")
	}
	if !strings.Contains(sshCmd, "-o ConnectTimeout=5") {
		t.Errorf("GIT_SSH_COMMAND = %q, want -o ConnectTimeout=5", sshCmd)
	}
}
