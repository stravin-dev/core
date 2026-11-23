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
	"slices"
	"strings"
	"testing"
)

func TestValidateBundleURI(t *testing.T) {
	valid := []string{
		"https://s3.us-east-1.amazonaws.com/git-objects/bundles/acme/core/latest.bundle?X-Amz-Signature=abc123",
		"http://127.0.0.1:4566/git-objects-us-east-1/bundles/acme/core/latest.bundle?X-Amz-Expires=900",
		"http://floci:4566/bucket/key.bundle",
	}
	for _, u := range valid {
		if err := ValidateBundleURI(u); err != nil {
			t.Errorf("ValidateBundleURI(%q) unexpected error: %v", u, err)
		}
	}

	invalid := []string{
		"",
		"   ",
		"file:///etc/passwd",
		"ssh://git@example.com/repo.bundle",
		"ext::sh -c id",
		"ftp://example.com/repo.bundle",
		"https:///missing-host.bundle",
		"https://example.com/repo.bundle\n[core]",
		"https://example.com/repo.bundle\r\nX-Inject: 1",
		"https://example.com/repo with space.bundle",
		"https://example.com/repo\x00.bundle",
		"https://example.com/" + strings.Repeat("a", 2050),
	}
	for _, u := range invalid {
		if err := ValidateBundleURI(u); err == nil {
			t.Errorf("ValidateBundleURI(%q) = nil, want error", u)
		}
	}
}

func TestExecUploadPack_BundleURIArgsAndValidation(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "invocations.log")
	helperPath := filepath.Join(t.TempDir(), "record-args.sh")
	script := "#!/bin/sh\n" +
		"for a in \"$@\"; do printf '%s\\n' \"$a\" >> " + logPath + "; done\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	repoDir := t.TempDir()

	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	// Invalid bundle URI must be rejected before invoking git.
	err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:   repoDir,
		BundleURI: "file:///etc/passwd",
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("ExecUploadPack invalid BundleURI err = %v, want ErrInvalidRequest", err)
	}

	// Valid bundle URI must inject safe -c bundle.* args before upload-pack.
	uri := "http://127.0.0.1:4566/bucket/bundles/acme/core/latest.bundle?sig=1"
	if err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:     repoDir,
		GitProtocol: "version=2",
		BundleURI:   uri,
	}); err != nil {
		t.Fatalf("ExecUploadPack valid BundleURI: %v", err)
	}

	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	args := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for _, want := range []string{
		"uploadpack.advertiseBundleURIs=true",
		"bundle.version=1",
		"bundle.mode=all",
		"bundle.stravin.uri=" + uri,
	} {
		if !slices.Contains(args, want) {
			t.Errorf("missing arg %q in %v", want, args)
		}
	}
}

func TestReftableBareRepo_MIDXBitmapCommitGraphAndBundle_DualHash(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	execImpl := NewSubprocessExecutor(Config{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	for _, objFmt := range []string{"sha1", "sha256"} {
		t.Run(objFmt, func(t *testing.T) {
			// Create a source commit in a scratch repo of matching object format.
			srcDir := filepath.Join(t.TempDir(), "src")
			runGitCmd(t, "", "init", "--initial-branch=main", "--object-format="+objFmt, srcDir)
			runGitCmd(t, srcDir, "config", "user.name", "Stravin Test")
			runGitCmd(t, srcDir, "config", "user.email", "test@stravin.dev")
			if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello "+objFmt+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runGitCmd(t, srcDir, "add", "hello.txt")
			runGitCmd(t, srcDir, "commit", "-m", "commit 1")
			c1 := strings.TrimSpace(runGitCmd(t, srcDir, "rev-parse", "HEAD"))

			if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("hello 2 "+objFmt+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			runGitCmd(t, srcDir, "add", "hello.txt")
			runGitCmd(t, srcDir, "commit", "-m", "commit 2")
			c2 := strings.TrimSpace(runGitCmd(t, srcDir, "rev-parse", "HEAD"))

			// Export all objects into a packfile.
			packCmd := exec.Command("git", "--git-dir", filepath.Join(srcDir, ".git"), "pack-objects", "--stdout", "--revs")
			packCmd.Stdin = strings.NewReader(c2 + "\n")
			packBytes, err := packCmd.Output()
			if err != nil {
				t.Fatalf("git pack-objects: %v", err)
			}

			// 1. Initialize bare repo with reftable + matching object-format.
			bareDir := filepath.Join(t.TempDir(), "bare.git")
			if err := execImpl.ExecInitBare(ctx, InitBareRequest{
				RepoDir:       bareDir,
				ObjectFormat:  objFmt,
				RefFormat:     "reftable",
				DefaultBranch: "refs/heads/main",
			}); err != nil {
				t.Fatalf("ExecInitBare: %v", err)
			}

			gotRefFmt := strings.TrimSpace(runGitCmd(t, "", "--git-dir", bareDir, "rev-parse", "--show-ref-format"))
			if gotRefFmt != "reftable" {
				t.Fatalf("ref format = %q, want reftable", gotRefFmt)
			}
			gotObjFmt := strings.TrimSpace(runGitCmd(t, "", "--git-dir", bareDir, "rev-parse", "--show-object-format"))
			if gotObjFmt != objFmt {
				t.Fatalf("object format = %q, want %s", gotObjFmt, objFmt)
			}

			// 2. Index the pack into bareDir/objects/pack.
			outPack := filepath.Join(bareDir, "objects", "pack", "pack-seed.pack")
			if err := execImpl.ExecIndexPack(ctx, IndexPackRequest{
				RepoDir:        bareDir,
				FromStdin:      true,
				OutputPackPath: outPack,
				Stdin:          bytes.NewReader(packBytes),
			}); err != nil {
				t.Fatalf("ExecIndexPack: %v", err)
			}

			// 3. Update refs via ExecUpdateRef and verify via ExecForEachRef (including D/F replacement).
			if err := execImpl.ExecUpdateRef(ctx, UpdateRefRequest{
				RepoDir: bareDir,
				Updates: []RefUpdateSpec{
					{RefName: "refs/heads/main", NewOID: c1},
					{RefName: "refs/heads/a/b", NewOID: c1},
				},
			}); err != nil {
				t.Fatalf("ExecUpdateRef(1): %v", err)
			}
			refs, err := execImpl.ExecForEachRef(ctx, ForEachRefRequest{RepoDir: bareDir})
			if err != nil {
				t.Fatalf("ExecForEachRef(1): %v", err)
			}
			if refs["refs/heads/main"] != c1 || refs["refs/heads/a/b"] != c1 {
				t.Fatalf("unexpected refs(1): %+v", refs)
			}

			// Replace refs/heads/a/b with refs/heads/a and advance main -> c2.
			if err := execImpl.ExecUpdateRef(ctx, UpdateRefRequest{
				RepoDir: bareDir,
				Updates: []RefUpdateSpec{
					{RefName: "refs/heads/a/b", Delete: true},
					{RefName: "refs/heads/a", NewOID: c2},
					{RefName: "refs/heads/main", NewOID: c2},
				},
			}); err != nil {
				t.Fatalf("ExecUpdateRef(2): %v", err)
			}
			if err := execImpl.ExecSymbolicRef(ctx, SymbolicRefRequest{
				RepoDir: bareDir,
				Name:    "HEAD",
				Target:  "refs/heads/main",
			}); err != nil {
				t.Fatalf("ExecSymbolicRef: %v", err)
			}

			refs2, err := execImpl.ExecForEachRef(ctx, ForEachRefRequest{RepoDir: bareDir})
			if err != nil {
				t.Fatalf("ExecForEachRef(2): %v", err)
			}
			if refs2["refs/heads/main"] != c2 || refs2["refs/heads/a"] != c2 || refs2["refs/heads/a/b"] != "" {
				t.Fatalf("unexpected refs(2): %+v", refs2)
			}

			// 4. Write MIDX + bitmap + commit-graph.
			if err := execImpl.ExecRepack(ctx, RepackRequest{
				RepoDir:          bareDir,
				WriteMIDX:        true,
				WriteBitmap:      true,
				WriteCommitGraph: true,
			}); err != nil {
				t.Fatalf("ExecRepack(MIDX+bitmap+CG): %v", err)
			}

			if _, err := os.Stat(filepath.Join(bareDir, "objects", "pack", "multi-pack-index")); err != nil {
				t.Fatalf("missing multi-pack-index: %v", err)
			}
			bitmaps, err := filepath.Glob(filepath.Join(bareDir, "objects", "pack", "*.bitmap"))
			if err != nil || len(bitmaps) == 0 {
				t.Fatalf("missing .bitmap file in objects/pack (glob=%v, err=%v)", bitmaps, err)
			}
			if _, err := os.Stat(filepath.Join(bareDir, "objects", "info", "commit-graph")); err != nil {
				t.Fatalf("missing commit-graph: %v", err)
			}

			// 5. Create a Git bundle via ExecBundleCreate and verify it.
			bundlePath := filepath.Join(t.TempDir(), "latest.bundle")
			if err := execImpl.ExecBundleCreate(ctx, BundleCreateRequest{
				RepoDir:    bareDir,
				OutputPath: bundlePath,
			}); err != nil {
				t.Fatalf("ExecBundleCreate: %v", err)
			}
			info, err := os.Stat(bundlePath)
			if err != nil || info.Size() == 0 {
				t.Fatalf("bundle stat = (%v, %v), want non-empty file", info, err)
			}
			runGitCmd(t, "", "--git-dir", bareDir, "bundle", "verify", bundlePath)
		})
	}
}

func runGitCmd(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = append(os.Environ(),
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
	return string(out)
}
