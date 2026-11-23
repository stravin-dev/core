package loadgen

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGitClient_ProtocolV2FallbackAndSHA256Lifecycle(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}
	ctx := context.Background()

	bareDir := filepath.Join(t.TempDir(), "remote-sha256.git")
	bareClient := NewGitClient(filepath.Dir(bareDir), GitOptions{})
	if _, err := bareClient.Run(ctx, "init", "--bare", "--initial-branch=main", "--object-format=sha256", bareDir); err != nil {
		t.Fatalf("init bare sha256: %v", err)
	}

	workDir := filepath.Join(t.TempDir(), "work-sha256")
	work := NewGitClient(workDir, GitOptions{})
	if _, err := NewGitClient(filepath.Dir(workDir), GitOptions{}).Run(ctx, "init", "--initial-branch=main", "--object-format=sha256", workDir); err != nil {
		t.Fatalf("init work sha256: %v", err)
	}
	if err := work.ConfigUser(ctx, "Loadgen Test", "loadgen@stravin.dev"); err != nil {
		t.Fatalf("ConfigUser: %v", err)
	}
	if err := work.WriteFile("README.md", []byte("sha256 loadgen test\n")); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := work.AddAll(ctx); err != nil {
		t.Fatalf("AddAll: %v", err)
	}
	if err := work.Commit(ctx, "initial sha256 commit"); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	oid, err := work.RevParse(ctx, "HEAD")
	if err != nil {
		t.Fatalf("RevParse: %v", err)
	}
	if len(oid) != 64 {
		t.Fatalf("SHA-256 HEAD OID len = %d (%q), want 64", len(oid), oid)
	}
	if _, err := work.Run(ctx, "-c", "protocol.version=2", "push", bareDir, "HEAD:refs/heads/main"); err != nil {
		t.Fatalf("push v2 sha256: %v", err)
	}

	// Clone with Protocol v2 + partial clone filter.
	cloneV2Dir := filepath.Join(t.TempDir(), "clone-v2-sha256")
	if _, err := NewGitClient(filepath.Dir(cloneV2Dir), GitOptions{}).Run(ctx, "-c", "protocol.version=2", "clone", "--filter=blob:none", "file://"+bareDir, cloneV2Dir); err != nil {
		t.Fatalf("clone v2 partial sha256: %v", err)
	}
	gotV2, err := NewGitClient(cloneV2Dir, GitOptions{}).RevParse(ctx, "HEAD")
	if err != nil || gotV2 != oid {
		t.Fatalf("clone v2 HEAD = (%q, %v), want %q", gotV2, err, oid)
	}

	// Clone with Protocol v0 fallback + strict fsck oracle.
	cloneV0Dir := filepath.Join(t.TempDir(), "clone-v0-sha256")
	if _, err := NewGitClient(filepath.Dir(cloneV0Dir), GitOptions{}).Run(ctx, "-c", "protocol.version=0", "clone", "file://"+bareDir, cloneV0Dir); err != nil {
		t.Fatalf("clone v0 sha256: %v", err)
	}
	cloneV0 := NewGitClient(cloneV0Dir, GitOptions{})
	if err := cloneV0.Fsck(ctx); err != nil {
		t.Fatalf("Fsck(sha256): %v", err)
	}

	// Verify oracle classifies SHA-256 corruption as ErrorUnexpected.
	if cls := ClassifyError(&GitError{Op: "fetch", Stderr: "fatal: invalid sha256 object"}, ClassifyOptions{}); cls != ErrorUnexpected {
		t.Fatalf("ClassifyError(invalid sha256) = %q, want %q", cls, ErrorUnexpected)
	}
}
