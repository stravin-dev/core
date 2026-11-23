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
