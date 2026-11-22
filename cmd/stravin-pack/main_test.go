package main

import (
	"testing"
	"time"

	"stravin.dev/core/internal/gitexec"
)

func TestLoadConfigGitExecDefaults(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "")
	t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")

	cfg := loadConfig()

	if !cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected stravin-pack default GitExec.Sandbox.EnableNamespaces = true")
	}
	if !cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected stravin-pack default GitExec.Sandbox.EnableCgroup = true")
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

func TestLoadConfigGitExecOverrides(t *testing.T) {
	t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "false")
	t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "false")
	t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "1073741824")
	t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "150000 100000")
	t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "128")
	t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "15m")

	cfg := loadConfig()

	if cfg.GitExec.Sandbox.EnableNamespaces {
		t.Error("expected EnableNamespaces = false when overridden")
	}
	if cfg.GitExec.Sandbox.EnableCgroup {
		t.Error("expected EnableCgroup = false when overridden")
	}
	if cfg.GitExec.Sandbox.MemoryMaxBytes != 1073741824 {
		t.Errorf("MemoryMaxBytes = %d, want 1073741824", cfg.GitExec.Sandbox.MemoryMaxBytes)
	}
	if cfg.GitExec.Sandbox.CPUQuota != "150000 100000" {
		t.Errorf("CPUQuota = %q, want %q", cfg.GitExec.Sandbox.CPUQuota, "150000 100000")
	}
	if cfg.GitExec.Sandbox.PidsMax != 128 {
		t.Errorf("PidsMax = %d, want 128", cfg.GitExec.Sandbox.PidsMax)
	}
	if cfg.GitExec.DefaultTimeout != 15*time.Minute {
		t.Errorf("DefaultTimeout = %v, want 15m", cfg.GitExec.DefaultTimeout)
	}
}

func TestResolveGitExecConfigZeroValueUsesSecureDefaults(t *testing.T) {
	resolved := resolveGitExecConfig(gitexec.Config{})

	if !resolved.Sandbox.EnableNamespaces || !resolved.Sandbox.EnableCgroup {
		t.Errorf("expected secure sandbox defaults for zero-value Config, got %+v", resolved.Sandbox)
	}
	if resolved.DefaultTimeout != defaultGitExecTimeout {
		t.Errorf("DefaultTimeout = %v, want %v", resolved.DefaultTimeout, defaultGitExecTimeout)
	}
}
