package gitexec

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestDefaultSandboxConfigEnablesIsolationAndLimits(t *testing.T) {
	cfg := DefaultSandboxConfig()
	if !cfg.EnableNamespaces {
		t.Error("expected DefaultSandboxConfig().EnableNamespaces = true")
	}
	if !cfg.EnableCgroup {
		t.Error("expected DefaultSandboxConfig().EnableCgroup = true")
	}
	if cfg.CgroupRoot != DefaultCgroupRoot {
		t.Errorf("CgroupRoot = %q, want %q", cfg.CgroupRoot, DefaultCgroupRoot)
	}
	if cfg.MemoryMaxBytes != DefaultMemoryMaxBytes || cfg.MemoryMaxBytes <= 0 {
		t.Errorf("MemoryMaxBytes = %d, want %d (>0)", cfg.MemoryMaxBytes, DefaultMemoryMaxBytes)
	}
	if cfg.CPUQuota != DefaultCPUQuota || cfg.CPUQuota == "" {
		t.Errorf("CPUQuota = %q, want %q (non-empty)", cfg.CPUQuota, DefaultCPUQuota)
	}
	if cfg.PidsMax != DefaultPidsMax || cfg.PidsMax <= 0 {
		t.Errorf("PidsMax = %d, want %d (>0)", cfg.PidsMax, DefaultPidsMax)
	}
}

func TestLoadSandboxConfigFromEnvDefaultsAndOverrides(t *testing.T) {
	t.Run("defaults when env unset", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "")
		t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "")
		t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "")
		t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "")
		t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "")
		t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "")

		got := LoadSandboxConfigFromEnv()
		want := DefaultSandboxConfig()
		if got != want {
			t.Errorf("LoadSandboxConfigFromEnv() = %+v, want %+v", got, want)
		}
	})

	t.Run("overrides from env", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "false")
		t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "false")
		t.Setenv("STRAVIN_GITEXEC_CGROUP_ROOT", "/custom/cgroup")
		t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "1073741824")
		t.Setenv("STRAVIN_GITEXEC_CPU_QUOTA", "100000 100000")
		t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "128")

		got := LoadSandboxConfigFromEnv()
		if got.EnableNamespaces {
			t.Error("expected EnableNamespaces = false")
		}
		if got.EnableCgroup {
			t.Error("expected EnableCgroup = false")
		}
		if got.CgroupRoot != "/custom/cgroup" {
			t.Errorf("CgroupRoot = %q, want /custom/cgroup", got.CgroupRoot)
		}
		if got.MemoryMaxBytes != 1073741824 {
			t.Errorf("MemoryMaxBytes = %d, want 1073741824", got.MemoryMaxBytes)
		}
		if got.CPUQuota != "100000 100000" {
			t.Errorf("CPUQuota = %q, want %q", got.CPUQuota, "100000 100000")
		}
		if got.PidsMax != 128 {
			t.Errorf("PidsMax = %d, want 128", got.PidsMax)
		}
	})

	t.Run("invalid env values fall back to secure defaults", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES", "not-a-bool")
		t.Setenv("STRAVIN_GITEXEC_ENABLE_CGROUP", "maybe")
		t.Setenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES", "-10")
		t.Setenv("STRAVIN_GITEXEC_PIDS_MAX", "0")

		got := LoadSandboxConfigFromEnv()
		want := DefaultSandboxConfig()
		if got != want {
			t.Errorf("LoadSandboxConfigFromEnv() with invalid values = %+v, want %+v", got, want)
		}
	})
}

func TestLoadConfigFromEnvTimeoutAndLogger(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("uses caller default timeout", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")
		cfg := LoadConfigFromEnv(10*time.Minute, logger)
		if cfg.DefaultTimeout != 10*time.Minute {
			t.Errorf("DefaultTimeout = %v, want 10m", cfg.DefaultTimeout)
		}
		if cfg.Logger != logger {
			t.Error("expected Logger to be preserved")
		}
		if !cfg.Sandbox.EnableNamespaces || !cfg.Sandbox.EnableCgroup {
			t.Errorf("expected sandbox enabled by default, got %+v", cfg.Sandbox)
		}
	})

	t.Run("falls back to package DefaultTimeout when caller passes 0", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "")
		cfg := LoadConfigFromEnv(0, logger)
		if cfg.DefaultTimeout != DefaultTimeout {
			t.Errorf("DefaultTimeout = %v, want %v", cfg.DefaultTimeout, DefaultTimeout)
		}
	})

	t.Run("overrides timeout from STRAVIN_GITEXEC_TIMEOUT", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "2m30s")
		cfg := LoadConfigFromEnv(5*time.Minute, logger)
		if cfg.DefaultTimeout != 150*time.Second {
			t.Errorf("DefaultTimeout = %v, want 2m30s", cfg.DefaultTimeout)
		}
	})

	t.Run("ignores invalid or non-positive timeout", func(t *testing.T) {
		t.Setenv("STRAVIN_GITEXEC_TIMEOUT", "-5s")
		cfg := LoadConfigFromEnv(5*time.Minute, logger)
		if cfg.DefaultTimeout != 5*time.Minute {
			t.Errorf("DefaultTimeout = %v, want 5m", cfg.DefaultTimeout)
		}
	})
}
