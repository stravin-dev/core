package gitexec

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func TestExecUploadPack_GitProtocolEnvPassingAndValidation(t *testing.T) {
	helperPath := filepath.Join(t.TempDir(), "dump-env.sh")
	script := "#!/bin/sh\nenv\n"
	if err := os.WriteFile(helperPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper script: %v", err)
	}

	execImpl := NewSubprocessExecutor(Config{
		GitBinary: helperPath,
	})
	repoDir := t.TempDir()
	rk, _ := tenant.NewRepoKey("acme", "proto")

	t.Run("PassesGitProtocolWhenSet", func(t *testing.T) {
		var stdout bytes.Buffer
		err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
			Repo:        rk,
			RepoDir:     repoDir,
			GitProtocol: "version=2",
			Stdout:      &stdout,
		})
		if err != nil {
			t.Fatalf("ExecUploadPack: %v", err)
		}
		if !strings.Contains(stdout.String(), "GIT_PROTOCOL=version=2\n") {
			t.Fatalf("expected GIT_PROTOCOL=version=2 in env, got:\n%s", stdout.String())
		}
	})

	t.Run("OmitsGitProtocolWhenEmpty", func(t *testing.T) {
		var stdout bytes.Buffer
		err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
			Repo:    rk,
			RepoDir: repoDir,
			Stdout:  &stdout,
		})
		if err != nil {
			t.Fatalf("ExecUploadPack: %v", err)
		}
		if strings.Contains(stdout.String(), "GIT_PROTOCOL=") {
			t.Fatalf("expected no GIT_PROTOCOL when empty, got:\n%s", stdout.String())
		}
	})

	t.Run("RejectsMalformedGitProtocol", func(t *testing.T) {
		for _, bad := range []string{
			"version=2\nLD_PRELOAD=/tmp/evil.so",
			"version=2; id",
			"version=2\x00",
			strings.Repeat("a", 129),
		} {
			err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
				Repo:        rk,
				RepoDir:     repoDir,
				GitProtocol: bad,
			})
			if !errors.Is(err, ErrInvalidRequest) {
				t.Errorf("ExecUploadPack(%q) err = %v, want ErrInvalidRequest", bad, err)
			}
		}
	})
}

func TestExecUploadPack_ProtocolV2AndSHA256AdvertiseRefs(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not installed")
	}

	execImpl := NewSubprocessExecutor(Config{})
	rk, _ := tenant.NewRepoKey("acme", "sha256-v2")

	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			bareDir := filepath.Join(t.TempDir(), format+".git")
			initCmd := exec.Command("git", "init", "--bare", "--object-format="+format, bareDir)
			if out, err := initCmd.CombinedOutput(); err != nil {
				t.Fatalf("git init --bare --object-format=%s: %v\n%s", format, err, out)
			}

			var stdout bytes.Buffer
			err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
				Repo:          rk,
				RepoDir:       bareDir,
				AdvertiseRefs: true,
				StatelessRPC:  true,
				GitProtocol:   "version=2",
				Stdout:        &stdout,
			})
			if err != nil {
				t.Fatalf("ExecUploadPack v2 (%s): %v", format, err)
			}
			out := stdout.String()
			for _, want := range []string{"version 2\n", "ls-refs", "filter", "object-format=" + format} {
				if !strings.Contains(out, want) {
					t.Errorf("v2 advertisement (%s) missing %q:\n%q", format, want, out)
				}
			}
		})
	}
}
