package node

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"stravin.dev/core/internal/gitexec"
)

func TestLoadDaemonConfigFromEnvGitExecDefaults(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "")
	t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")

	cfg := LoadDaemonConfigFromEnv()

	if !cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected stravin-node default GitExec.Sandbox.EnableNamespaces = true")
	}
	if !cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected stravin-node default GitExec.Sandbox.EnableCgroup = true")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes <= 0 {
		t.Errorf("expected positive default MemoryMaxBytes, got %d", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota == "" {
		t.Error("expected non-empty default CPUQuota")
	}
	if cfg.GitExec.Sandbox.PidsMax <= 0 {
		t.Errorf("expected positive default PidsMax, got %d", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", cfg.GitExec.DefaultTimeout, defaultGitExecTimeout)
	}
}

func TestLoadDaemonConfigFromEnvGitExecOverrides(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "false")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "false")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "268435456")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "100000 100000")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "32")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "90s")

	cfg := LoadDaemonConfigFromEnv()

	if cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected EnableNamespaces = false when overridden")
	}
	if cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected EnableCgroup = false when overridden")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes != 268435456 {
		t.Errorf("MemoryMaxBytes = %d, want 268435456", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota != "100000 100000" {
		t.Errorf("CPUQuota = %q, want %q", cfg.GitExec.Sandbox.CPUQuota, "100000 100000")
	}
	if cfg.GitExec.Sandbox.PidsMax != 32 {
		t.Errorf("PidsMax = %d, want 32", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != 90*time.Second {
		t.Errorf("DefaultTimeout = %v, want 90s", cfg.GitExec.DefaultTimeout)
	}
}

func TestResolveGitExecConfigZeroValueUsesSecureDefaults(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	resolved := resolveGitExecConfig(gitexec.Config{}, logger)

	if !resolved.Sandbox.EnableNamespaces || !resolved.Sandbox.EnableCgroup {
		t.Errorf("expected secure sandbox defaults for zero-value Config, got %+v", resolved.Sandbox)
	}
	if resolved.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", resolved.DefaultTimeout, defaultGitExecTimeout)
	}
	if resolved.Logger != logger {
		t.Error("expected logger to be wired into resolved gitexec.Config")
	}
}
