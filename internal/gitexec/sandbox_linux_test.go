//go:build linux

package gitexec

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"stravin.dev/core/internal/tenant"
)

func TestLinuxSandboxConfiguratorBuildsSysProcAttrAndCgroupFiles(t *testing.T) {
	cgroupRoot := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := SandboxConfig{
		EnableNamespaces: true,
		EnableCgroup:     true,
		CgroupRoot:       cgroupRoot,
		MemoryMaxBytes:   256 * 1024 * 1024,
		CPUQuota:         "50000 100000",
		PidsMax:          64,
	}

	sb := NewLinuxSandboxConfigurator(cfg, logger)
	repoKey, err := tenant.NewRepoKey("acme", "core")
	if err != nil {
		t.Fatalf("NewRepoKey: %v", err)
	}

	prepared, err := sb.Prepare(context.Background(), SandboxSpec{
		Repo:     repoKey,
		RepoDir:  t.TempDir(),
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	attr := prepared.SysProcAttr
	if attr == nil {
		t.Fatal("expected non-nil SysProcAttr")
	}
	if !attr.Setpgid {
		t.Error("expected Setpgid=true")
	}
	if attr.Pdeathsig != syscall.SIGKILL {
		t.Errorf("Pdeathsig = %v, want SIGKILL", attr.Pdeathsig)
	}
	wantFlags := uintptr(syscall.CLONE_NEWNET | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS)
	if attr.Cloneflags&wantFlags != wantFlags {
		t.Errorf("Cloneflags = %#x, missing expected namespace flags %#x", attr.Cloneflags, wantFlags)
	}
	if !attr.UseCgroupFD || attr.CgroupFD < 0 {
		t.Errorf("expected UseCgroupFD=true and CgroupFD>=0, got UseCgroupFD=%v CgroupFD=%d", attr.UseCgroupFD, attr.CgroupFD)
	}
	if !prepared.Isolated {
		t.Error("expected Isolated=true")
	}

	// Verify cgroup limit files were written inside cgroupRoot.
	entries, err := os.ReadDir(cgroupRoot)
	if err != nil {
		t.Fatalf("ReadDir cgroupRoot: %v", err)
	}
	if len(entries) != 1 || !entries[0].IsDir() {
		t.Fatalf("expected 1 cgroup subdirectory in %s, got %d", cgroupRoot, len(entries))
	}
	cgDir := filepath.Join(cgroupRoot, entries[0].Name())

	assertFileContent := func(name, want string) {
		t.Helper()
		raw, readErr := os.ReadFile(filepath.Join(cgDir, name))
		if readErr != nil {
			t.Fatalf("read %s: %v", name, readErr)
		}
		if got := strings.TrimSpace(string(raw)); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}

	assertFileContent("memory.max", "268435456")
	assertFileContent("cpu.max", "50000 100000")
	assertFileContent("pids.max", "64")

	// Cleanup should close FD and remove the ephemeral cgroup dir.
	prepared.Cleanup()

	if _, statErr := os.Stat(cgDir); !os.IsNotExist(statErr) {
		t.Errorf("expected cgroup dir %s to be removed on Cleanup, stat err=%v", cgDir, statErr)
	}
}

func TestLinuxSandboxConfiguratorGracefulFallbackWhenCgroupUnavailable(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := SandboxConfig{
		EnableNamespaces: true,
		EnableCgroup:     true,
		CgroupRoot:       filepath.Join(t.TempDir(), "nonexistent", "cgroup"),
		MemoryMaxBytes:   128 * 1024 * 1024,
	}

	sb := NewLinuxSandboxConfigurator(cfg, logger)
	prepared, err := sb.Prepare(context.Background(), SandboxSpec{
		RepoDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Prepare should degrade gracefully when cgroup root is unavailable, got: %v", err)
	}
	defer prepared.Cleanup()

	if prepared.SysProcAttr == nil {
		t.Fatal("expected non-nil SysProcAttr")
	}
	if prepared.SysProcAttr.UseCgroupFD {
		t.Error("expected UseCgroupFD=false when cgroup root is unavailable")
	}
	if !prepared.SysProcAttr.Setpgid {
		t.Error("expected Setpgid=true even after cgroup fallback")
	}
}

func TestLinuxSubprocessExecutorGracefulFallbackWithNamespacesEnabled(t *testing.T) {
	requireGit(t)
	bareDir, commitSHA := createBareRepoWithCommit(t)

	var logBuf strings.Builder
	logger := slog.New(slog.NewTextHandler(&logBuf, nil))

	execImpl := NewSubprocessExecutor(Config{
		Sandbox: SandboxConfig{
			EnableNamespaces: true,
			EnableCgroup:     true,
			CgroupRoot:       filepath.Join(t.TempDir(), "nonexistent-cgroup"),
			MemoryMaxBytes:   128 * 1024 * 1024,
		},
		Logger: logger,
	})

	var stdout strings.Builder
	err := execImpl.ExecUploadPack(context.Background(), UploadPackRequest{
		RepoDir:       bareDir,
		AdvertiseRefs: true,
		StatelessRPC:  true,
		Stdout:        &stdout,
	})
	if err != nil {
		t.Fatalf("ExecUploadPack with namespaces enabled should succeed (directly or via graceful fallback): %v", err)
	}
	if !strings.Contains(stdout.String(), commitSHA) {
		t.Errorf("expected commit SHA %s in upload-pack output, got:\n%s", commitSHA, stdout.String())
	}
}
