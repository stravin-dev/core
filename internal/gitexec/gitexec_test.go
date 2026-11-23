package gitexec

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"stravin.dev/core/internal/tenant"
)

func TestScrubbedEnvStripsHostAndAWSSecrets(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
	t.Setenv("AWS_SESSION_TOKEN", "FwoGZXIvYXdzEBYaDExampleToken")
	t.Setenv("STRAVIN_HOST_SECRET", "super-secret-value")

	env := scrubbedEnv("/usr/bin:/bin")

	want := map[string]string{
		"PATH":                "/usr/bin:/bin",
		"HOME":                "/nonexistent",
		"GIT_CONFIG_NOSYSTEM": "1",
		"GIT_CONFIG_GLOBAL":   "/dev/null",
		"GIT_TERMINAL_PROMPT": "0",
	}

	got := make(map[string]string, len(env))
	for _, kv := range env {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("malformed env entry %q", kv)
		}
		got[k] = v
	}

	if len(got) != len(want) {
		t.Fatalf("expected exactly %d allowlisted env vars, got %d: %v", len(want), len(got), got)
	}
	for k, wv := range want {
		if gv, ok := got[k]; !ok || gv != wv {
			t.Errorf("env[%q] = %q (present=%v), want %q", k, gv, ok, wv)
		}
	}
	for _, forbidden := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "STRAVIN_HOST_SECRET"} {
		if _, leaked := got[forbidden]; leaked {
			t.Errorf("forbidden env var %q leaked into scrubbed env", forbidden)
		}
	}
}

func TestLockdownArgsPrependedBeforeSubcommand(t *testing.T) {
	sub := []string{"upload-pack", "--stateless-rpc", "--advertise-refs", "/nvme/t/acme/repo"}
	args := buildGitArgs(sub...)

	expectedPrefix := []string{
		"-c", "core.hooksPath=/dev/null",
		"-c", "core.fsmonitor=false",
		"-c", "core.alternateRefsCommand=exit 0",
		"-c", "core.sshCommand=false",
		"-c", "protocol.ext.allow=never",
		"-c", "safe.bareRepository=explicit",
	}

	if len(args) != len(expectedPrefix)+len(sub) {
		t.Fatalf("len(args) = %d, want %d: %v", len(args), len(expectedPrefix)+len(sub), args)
	}
	for i, want := range expectedPrefix {
		if args[i] != want {
			t.Errorf("args[%d] = %q, want %q (full: %v)", i, args[i], want, args)
		}
	}
	for i, want := range sub {
		gotIdx := len(expectedPrefix) + i
		if args[gotIdx] != want {
			t.Errorf("args[%d] = %q, want %q", gotIdx, args[gotIdx], want)
		}
	}
}

func TestLimitedBufferCapsOutput(t *testing.T) {
	buf := newLimitedBuffer(10)
	n, err := buf.Write([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if n != 16 {
		t.Fatalf("Write returned %d, want 16 (io.Writer contract)", n)
	}
	if !buf.Truncated() {
		t.Fatal("expected buffer to report truncated=true")
	}
	if got := buf.String(); got != "0123456789" {
		t.Fatalf("buf.String() = %q, want %q", got, "0123456789")
	}
}

func TestSubprocessEnvIsolationAtRuntime(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIA_MUST_NOT_LEAK")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "SECRET_MUST_NOT_LEAK")
	t.Setenv("AWS_SESSION_TOKEN", "TOKEN_MUST_NOT_LEAK")

	helperPath := filepath.Join(t.TempDir(), "dump-env.sh")
	script := "#!/bin/sh\nenv\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}

	var stdout bytes.Buffer
	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:       t.TempDir(),
		AdvertiseRefs: true,
		Stdout:        &stdout,
	})
	if err != nil {
		t.Fatalf("ExecUploadPack with env-dump helper failed: %v", err)
	}

	out := stdout.String()
	for _, forbidden := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AKIA_MUST_NOT_LEAK"} {
		if strings.Contains(out, forbidden) {
			t.Errorf("subprocess observed forbidden env string %q in:\n%s", forbidden, out)
		}
	}
	for _, required := range []string{"GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null", "GIT_TERMINAL_PROMPT=0", "HOME=/nonexistent"} {
		if !strings.Contains(out, required) {
			t.Errorf("subprocess missing required env %q in:\n%s", required, out)
		}
	}
}

func TestExecUploadPackAdvertiseRefsRealGit(t *testing.T) {
	requireGit(t)
	bareDir, commitSHA := createBareRepoWithCommit(t)

	repoKey, err := tenant.NewRepoKey("acme", "demo")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err = execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		Repo:          repoKey,
		RepoDir:       bareDir,
		AdvertiseRefs: true,
		StatelessRPC:  true,
		Stdout:        &stdout,
		Stderr:        &stderr,
	})
	if err != nil {
		t.Fatalf("ExecUploadPack failed: %v (stderr: %s)", err, stderr.String())
	}

	out := stdout.String()
	if !strings.Contains(out, commitSHA) {
		t.Errorf("advertise-refs output missing commit SHA %s:\n%s", commitSHA, out)
	}
	if !strings.Contains(out, "refs/heads/main") {
		t.Errorf("advertise-refs output missing refs/heads/main:\n%s", out)
	}
}

func TestExecUploadPackPassesAllowReachableSHA1InWantBeforeSubcommand(t *testing.T) {
	helperPath := filepath.Join(t.TempDir(), "dump-args.sh")
	script := "#!/bin/sh\nfor a in \"$@\"; do printf '%s\\n' \"$a\"; done\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}

	repoDir := t.TempDir()
	var stdout bytes.Buffer
	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:      repoDir,
		StatelessRPC: true,
		Stdout:       &stdout,
	})
	if err != nil {
		t.Fatalf("ExecUploadPack with arg-dump helper failed: %v", err)
	}

	args := strings.Split(strings.TrimSuffix(stdout.String(), "\n"), "\n")
	flagIdx := -1
	subcmdIdx := -1
	for i := 0; i < len(args); i++ {
		if args[i] == "-c" && i+1 < len(args) && args[i+1] == "uploadpack.allowReachableSHA1InWant=true" {
			flagIdx = i
		}
		if args[i] == "upload-pack" && subcmdIdx == -1 {
			subcmdIdx = i
		}
	}
	if flagIdx == -1 {
		t.Fatalf("expected -c uploadpack.allowReachableSHA1InWant=true in args: %v", args)
	}
	if subcmdIdx == -1 {
		t.Fatalf("expected upload-pack subcommand in args: %v", args)
	}
	if flagIdx >= subcmdIdx {
		t.Fatalf("expected -c uploadpack.allowReachableSHA1InWant=true (idx %d) before upload-pack (idx %d): %v", flagIdx, subcmdIdx, args)
	}
}

func TestExecUploadPackAllowsReachableNonTipSHA1RealGit(t *testing.T) {
	requireGit(t)
	bareDir, commits := createBareRepoWithCommits(t, 2)
	firstCommitSHA, tipCommitSHA := commits[0], commits[1]
	if firstCommitSHA == tipCommitSHA {
		t.Fatalf("expected distinct commit SHAs, both were %s", firstCommitSHA)
	}

	// Simulate a Smart HTTP client requesting a recently-superseded but still
	// reachable ancestor commit (refs/heads/main already advanced to tipCommitSHA).
	reqBody := "0032want " + firstCommitSHA + "\n00000009done\n"
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:      bareDir,
		StatelessRPC: true,
		Stdin:        strings.NewReader(reqBody),
		Stdout:       &stdout,
		Stderr:       &stderr,
	})
	if err != nil {
		t.Fatalf("ExecUploadPack for reachable non-tip SHA %s failed: %v (stderr: %s)", firstCommitSHA, err, stderr.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("PACK")) {
		t.Errorf("expected packfile payload in upload-pack response, got %d bytes", stdout.Len())
	}
}

func TestExecIndexPackStdinRealGit(t *testing.T) {
	requireGit(t)
	bareDir, commitSHA := createBareRepoWithCommit(t)

	// Build a valid packfile from the bare repo using git pack-objects.
	packCmd := exec.Command("git", "--git-dir", bareDir, "pack-objects", "--stdout", "--revs")
	packCmd.Stdin = strings.NewReader(commitSHA + "\n")
	packBytes, err := packCmd.Output()
	if err != nil {
		t.Fatalf("git pack-objects failed: %v", err)
	}

	stagingDir := t.TempDir()
	outPack := filepath.Join(stagingDir, "incoming.pack")
	outIdx := filepath.Join(stagingDir, "incoming.idx")

	var stdout bytes.Buffer
	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err = execImpl.ExecIndexPack(context.Background(), IndexPackRequest{
		RepoDir:        bareDir,
		StagingDir:     stagingDir,
		FromStdin:      true,
		FixThin:        true,
		OutputPackPath: outPack,
		Stdin:          bytes.NewReader(packBytes),
		Stdout:         &stdout,
	})
	if err != nil {
		t.Fatalf("ExecIndexPack failed: %v", err)
	}

	if _, err := os.Stat(outPack); err != nil {
		t.Fatalf("expected output packfile at %s: %v", outPack, err)
	}
	if _, err := os.Stat(outIdx); err != nil {
		t.Fatalf("expected output index at %s: %v", outIdx, err)
	}
}

func TestExecRepackGeneratesMIDXAndCommitGraphRealGit(t *testing.T) {
	requireGit(t)
	bareDir, _ := createBareRepoWithCommit(t)

	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	err := execImpl.ExecRepack(context.Background(), RepackRequest{
		RepoDir:          bareDir,
		Repack:           true,
		All:              true,
		DeleteRedundant:  true,
		WriteMIDX:        true,
		WriteCommitGraph: true,
	})
	if err != nil {
		t.Fatalf("ExecRepack failed: %v", err)
	}

	midxPath := filepath.Join(bareDir, "objects", "pack", "multi-pack-index")
	if _, err := os.Stat(midxPath); err != nil {
		t.Fatalf("expected multi-pack-index at %s: %v", midxPath, err)
	}

	graphPath := filepath.Join(bareDir, "objects", "info", "commit-graph")
	if _, err := os.Stat(graphPath); err != nil {
		t.Fatalf("expected commit-graph at %s: %v", graphPath, err)
	}
}

func TestContextCancellationKillsProcessGroup(t *testing.T) {
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	helperPath := filepath.Join(t.TempDir(), "slow-git.sh")
	script := "#!/bin/sh\nsleep 30 &\necho $! > " + pidFile + "\nwait\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write slow helper: %v", err)
	}

	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := execImpl.ExecUploadPack(ctx, UploadPackRequest{
		RepoDir:       t.TempDir(),
		AdvertiseRefs: true,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error on context timeout, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected error wrapping context.DeadlineExceeded, got: %v", err)
	}
	if elapsed > 3*time.Second {
		t.Errorf("cancellation took too long (%v); child process may not have been killed", elapsed)
	}

	// If child wrote its grandchild PID, verify the grandchild is no longer running.
	if raw, readErr := os.ReadFile(pidFile); readErr == nil {
		if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw))); parseErr == nil && pid > 0 {
			deadline := time.Now().Add(500 * time.Millisecond)
			for time.Now().Before(deadline) {
				if !isProcessRunning(pid) {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			if isProcessRunning(pid) {
				t.Errorf("grandchild pid %d still running after process-group kill", pid)
			}
		}
	}
}

func isProcessRunning(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return false
	}
	// In Linux containers where PID 1 does not reap orphaned grandchildren,
	// a SIGKILLed grandchild transitions to zombie ('Z') state until container exit.
	if statBytes, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat"); err == nil {
		stat := string(statBytes)
		if idx := strings.LastIndex(stat, ") "); idx != -1 && idx+2 < len(stat) {
			if stat[idx+2] == 'Z' {
				return false
			}
		}
	}
	return true
}

func TestRequestValidation(t *testing.T) {
	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{}); !errors.Is(err, ErrEmptyRepoDir) {
		t.Errorf("ExecUploadPack empty repo dir: got %v, want ErrEmptyRepoDir", err)
	}
	if err := execImpl.ExecIndexPack(context.Background(), IndexPackRequest{}); !errors.Is(err, ErrEmptyRepoDir) {
		t.Errorf("ExecIndexPack empty repo dir: got %v, want ErrEmptyRepoDir", err)
	}
	if err := execImpl.ExecRepack(context.Background(), RepackRequest{}); !errors.Is(err, ErrEmptyRepoDir) {
		t.Errorf("ExecRepack empty repo dir: got %v, want ErrEmptyRepoDir", err)
	}
	if err := execImpl.ExecRepack(context.Background(), RepackRequest{RepoDir: t.TempDir()}); !errors.Is(err, ErrNoRepackAction) {
		t.Errorf("ExecRepack no action: got %v, want ErrNoRepackAction", err)
	}
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not found in PATH")
	}
}

func createBareRepoWithCommit(t *testing.T) (bareDir string, commitSHA string) {
	t.Helper()
	bareDir, commits := createBareRepoWithCommits(t, 1)
	return bareDir, commits[0]
}

func createBareRepoWithCommits(t *testing.T, count int) (bareDir string, commits []string) {
	t.Helper()
	workDir := filepath.Join(t.TempDir(), "work")
	bareDir = filepath.Join(t.TempDir(), "repo.git")

	run := func(dir string, args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = []string{
			"PATH=" + os.Getenv("PATH"),
			"HOME=/nonexistent",
			"GIT_CONFIG_NOSYSTEM=1",
			"GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_AUTHOR_NAME=Stravin Test",
			"GIT_AUTHOR_EMAIL=test@stravin.dev",
			"GIT_COMMITTER_NAME=Stravin Test",
			"GIT_COMMITTER_EMAIL=test@stravin.dev",
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v in %s failed: %v\n%s", args, dir, err, string(out))
		}
		return strings.TrimSpace(string(out))
	}

	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir workDir: %v", err)
	}
	run(workDir, "init", "-b", "main")
	commits = make([]string, 0, count)
	for i := 1; i <= count; i++ {
		content := "hello stravin commit " + strconv.Itoa(i) + "\n"
		if err := os.WriteFile(filepath.Join(workDir, "README.md"), []byte(content), 0o644); err != nil {
			t.Fatalf("write README %d: %v", i, err)
		}
		run(workDir, "add", "README.md")
		run(workDir, "commit", "-m", "commit "+strconv.Itoa(i))
		commits = append(commits, run(workDir, "rev-parse", "HEAD"))
	}

	run(filepath.Dir(bareDir), "clone", "--bare", workDir, bareDir)
	return bareDir, commits
}
