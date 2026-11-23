package gitexec

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

func TestGit3ExplicitGitDirArgs(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "invocations.log")
	helperPath := filepath.Join(t.TempDir(), "record-args.sh")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' '---' >> " + logPath + "\n" +
		"for a in \"$@\"; do printf '%s\\n' \"$a\" >> " + logPath + "; done\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}

	repoDir := filepath.Join(t.TempDir(), "bare.git")
	if err := os.MkdirAll(repoDir, 0o750); err != nil {
		t.Fatalf("mkdir repoDir: %v", err)
	}

	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	readInvocations := func(t *testing.T) [][]string {
		t.Helper()
		raw, err := os.ReadFile(logPath)
		if err != nil {
			t.Fatalf("read invocations log: %v", err)
		}
		if err := os.Truncate(logPath, 0); err != nil {
			t.Fatalf("truncate invocations log: %v", err)
		}
		chunks := strings.Split(strings.TrimSpace(string(raw)), "---\n")
		var invocations [][]string
		for _, chunk := range chunks {
			chunk = strings.TrimSpace(chunk)
			if chunk == "" {
				continue
			}
			invocations = append(invocations, strings.Split(chunk, "\n"))
		}
		return invocations
	}

	assertSafeBareRepoFlag := func(t *testing.T, args []string, subcmd string) int {
		t.Helper()
		subIdx := slices.Index(args, subcmd)
		if subIdx == -1 {
			t.Fatalf("missing subcommand %q in args: %v", subcmd, args)
		}
		found := false
		for i := 0; i < subIdx-1; i++ {
			if args[i] == "-c" && args[i+1] == "safe.bareRepository=explicit" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing -c safe.bareRepository=explicit before %q in args: %v", subcmd, args)
		}
		return subIdx
	}

	assertExplicitGitDir := func(t *testing.T, args []string, subcmd, wantDir string) {
		t.Helper()
		subIdx := assertSafeBareRepoFlag(t, args, subcmd)
		foundGitDir := false
		for i := 0; i < subIdx; i++ {
			if args[i] == "-C" {
				t.Errorf("subcommand %q must use --git-dir instead of -C: %v", subcmd, args)
			}
			if args[i] == "--git-dir" && i+1 < subIdx && args[i+1] == wantDir {
				foundGitDir = true
			}
			if args[i] == "--git-dir="+wantDir {
				foundGitDir = true
			}
		}
		if !foundGitDir {
			t.Errorf("subcommand %q missing explicit --git-dir %q before subcommand: %v", subcmd, wantDir, args)
		}
	}

	t.Run("ExecUploadPack", func(t *testing.T) {
		err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
			RepoDir:       repoDir,
			StatelessRPC:  true,
			AdvertiseRefs: true,
		})
		if err != nil {
			t.Fatalf("ExecUploadPack: %v", err)
		}
		invocations := readInvocations(t)
		if len(invocations) != 1 {
			t.Fatalf("expected 1 invocation, got %d", len(invocations))
		}
		args := invocations[0]
		assertSafeBareRepoFlag(t, args, "upload-pack")
		if args[len(args)-1] != repoDir {
			t.Errorf("expected upload-pack target dir %q as final arg, got %v", repoDir, args)
		}
	})

	t.Run("ExecIndexPack", func(t *testing.T) {
		err := execImpl.ExecIndexPack(context.Background(), IndexPackRequest{
			RepoDir:   repoDir,
			FromStdin: true,
			FixThin:   true,
			Stdin:     bytes.NewReader(nil),
		})
		if err != nil {
			t.Fatalf("ExecIndexPack: %v", err)
		}
		invocations := readInvocations(t)
		if len(invocations) != 1 {
			t.Fatalf("expected 1 invocation, got %d", len(invocations))
		}
		assertExplicitGitDir(t, invocations[0], "index-pack", repoDir)
	})

	t.Run("ExecRepack", func(t *testing.T) {
		err := execImpl.ExecRepack(context.Background(), RepackRequest{
			RepoDir:          repoDir,
			Repack:           true,
			All:              true,
			DeleteRedundant:  true,
			WriteMIDX:        true,
			WriteCommitGraph: true,
		})
		if err != nil {
			t.Fatalf("ExecRepack: %v", err)
		}
		invocations := readInvocations(t)
		if len(invocations) != 3 {
			t.Fatalf("expected 3 invocations (repack, multi-pack-index, commit-graph), got %d", len(invocations))
		}
		assertExplicitGitDir(t, invocations[0], "repack", repoDir)
		assertExplicitGitDir(t, invocations[1], "multi-pack-index", repoDir)
		assertExplicitGitDir(t, invocations[2], "commit-graph", repoDir)
	})
}

func TestGit3SafeBareRepositoryExplicitRealGit(t *testing.T) {
	requireGit(t)
	t.Setenv("GIT_CONFIG_PARAMETERS", "'safe.bareRepository=explicit'")

	bareDir, commitSHA := createBareRepoWithCommit(t)

	// Verify SHA-1 default object format remains intact for pre-Git-3 compatibility.
	if len(commitSHA) != 40 {
		t.Fatalf("expected 40-char SHA-1 commit OID, got %q (len=%d)", commitSHA, len(commitSHA))
	}
	fmtCmd := exec.Command("git", "-c", "safe.bareRepository=explicit", "--git-dir", bareDir, "rev-parse", "--show-object-format")
	fmtOut, err := fmtCmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git rev-parse --show-object-format failed: %v\n%s", err, string(fmtOut))
	}
	if got := strings.TrimSpace(string(fmtOut)); got != "sha1" {
		t.Fatalf("bare repo object format = %q, want %q", got, "sha1")
	}

	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// 1. ExecUploadPack under safe.bareRepository=explicit.
	var advOut bytes.Buffer
	if err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:       bareDir,
		AdvertiseRefs: true,
		StatelessRPC:  true,
		Stdout:        &advOut,
	}); err != nil {
		t.Fatalf("ExecUploadPack failed under safe.bareRepository=explicit: %v", err)
	}
	if !strings.Contains(advOut.String(), commitSHA) {
		t.Errorf("advertise-refs output missing commit %s", commitSHA)
	}

	// 2. ExecIndexPack under safe.bareRepository=explicit.
	packCmd := exec.Command("git", "-c", "safe.bareRepository=explicit", "--git-dir", bareDir, "pack-objects", "--stdout", "--revs")
	packCmd.Stdin = strings.NewReader(commitSHA + "\n")
	packBytes, err := packCmd.Output()
	if err != nil {
		t.Fatalf("git pack-objects failed: %v", err)
	}
	stagingDir := t.TempDir()
	outPack := filepath.Join(stagingDir, "git3-check.pack")
	if err := execImpl.ExecIndexPack(context.Background(), IndexPackRequest{
		RepoDir:        bareDir,
		StagingDir:     stagingDir,
		FromStdin:      true,
		FixThin:        true,
		OutputPackPath: outPack,
		Stdin:          bytes.NewReader(packBytes),
	}); err != nil {
		t.Fatalf("ExecIndexPack failed under safe.bareRepository=explicit: %v", err)
	}

	// 3. ExecRepack (repack + multi-pack-index + commit-graph) under safe.bareRepository=explicit.
	if err := execImpl.ExecRepack(context.Background(), RepackRequest{
		RepoDir:          bareDir,
		Repack:           true,
		All:              true,
		DeleteRedundant:  true,
		WriteMIDX:        true,
		WriteCommitGraph: true,
	}); err != nil {
		t.Fatalf("ExecRepack failed under safe.bareRepository=explicit: %v", err)
	}
}

func TestGit3RemovedCommandsAndLayoutsAbsent(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}

	forbidden := []struct {
		name string
		re   *regexp.Regexp
	}{
		{name: "git whatchanged", re: regexp.MustCompile(`\bwhatchanged\b`)},
		{name: "git pack-redundant", re: regexp.MustCompile(`\bpack-redundant\b`)},
		{name: ".git/info/grafts", re: regexp.MustCompile(`\.git/info/grafts|info/grafts`)},
		{name: ".git/branches/", re: regexp.MustCompile(`\.git/branches/`)},
		{name: ".git/remotes/", re: regexp.MustCompile(`\.git/remotes/`)},
		{name: "name-rev --stdin", re: regexp.MustCompile(`name-rev[^\n]*--stdin`)},
		{name: "core.preferSymlinkRefs", re: regexp.MustCompile(`core\.preferSymlinkRefs`)},
	}

	scanDirs := []string{"internal", "cmd", "scripts"}
	for _, relDir := range scanDirs {
		absDir := filepath.Join(repoRoot, relDir)
		err := filepath.WalkDir(absDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			ext := filepath.Ext(path)
			if ext != ".go" && ext != ".sh" {
				return nil
			}
			if filepath.Base(path) == "git3_compat_test.go" {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relPath, _ := filepath.Rel(repoRoot, path)
			for _, rule := range forbidden {
				if rule.re.Match(content) {
					t.Errorf("%s contains Git 3.0 removed/deprecated pattern %q", relPath, rule.name)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", relDir, err)
		}
	}
}
