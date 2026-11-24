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

func TestDefaultPackConfig(t *testing.T) {
	cfg := DefaultPackConfig()
	if cfg.Threads != 1 {
		t.Errorf("Threads = %d, want 1", cfg.Threads)
	}
	if cfg.WindowMemory != "256m" {
		t.Errorf("WindowMemory = %q, want 256m", cfg.WindowMemory)
	}
	if cfg.Compression != 1 {
		t.Errorf("Compression = %d, want 1", cfg.Compression)
	}
	if cfg.AllowReuse != "multi" {
		t.Errorf("AllowReuse = %q, want multi", cfg.AllowReuse)
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("DefaultPackConfig().Validate() = %v, want nil", err)
	}
}

func TestParsePackConfigFromEnv(t *testing.T) {
	clearEnv := func(t *testing.T) {
		t.Helper()
		t.Setenv("STRAVIN_GIT_PACK_THREADS", "")
		t.Setenv("STRAVIN_GIT_PACK_WINDOW_MEMORY", "")
		t.Setenv("STRAVIN_GIT_PACK_COMPRESSION", "")
		t.Setenv("STRAVIN_GIT_PACK_ALLOW_REUSE", "")
	}

	t.Run("defaults when unset", func(t *testing.T) {
		clearEnv(t)
		got, err := ParsePackConfigFromEnv()
		if err != nil {
			t.Fatalf("ParsePackConfigFromEnv: %v", err)
		}
		if got != DefaultPackConfig() {
			t.Errorf("got %+v, want %+v", got, DefaultPackConfig())
		}
	})

	t.Run("valid overrides", func(t *testing.T) {
		clearEnv(t)
		t.Setenv("STRAVIN_GIT_PACK_THREADS", "4")
		t.Setenv("STRAVIN_GIT_PACK_WINDOW_MEMORY", "512M")
		t.Setenv("STRAVIN_GIT_PACK_COMPRESSION", "0")
		t.Setenv("STRAVIN_GIT_PACK_ALLOW_REUSE", "SINGLE")

		got, err := ParsePackConfigFromEnv()
		if err != nil {
			t.Fatalf("ParsePackConfigFromEnv: %v", err)
		}
		want := PackConfig{
			Threads:      4,
			WindowMemory: "512M",
			Compression:  0,
			AllowReuse:   "single",
		}
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	})

	t.Run("valid allow_reuse and compression boundaries", func(t *testing.T) {
		for _, reuse := range []string{"single", "multi", "true", "false"} {
			clearEnv(t)
			t.Setenv("STRAVIN_GIT_PACK_ALLOW_REUSE", reuse)
			t.Setenv("STRAVIN_GIT_PACK_COMPRESSION", "9")
			t.Setenv("STRAVIN_GIT_PACK_WINDOW_MEMORY", "1g")
			got, err := ParsePackConfigFromEnv()
			if err != nil {
				t.Fatalf("reuse=%q err=%v", reuse, err)
			}
			if got.AllowReuse != reuse || got.Compression != 9 || got.WindowMemory != "1g" {
				t.Errorf("got %+v for reuse=%q", got, reuse)
			}
		}
	})

	invalidCases := []struct {
		name string
		env  string
		val  string
	}{
		{name: "threads zero", env: "STRAVIN_GIT_PACK_THREADS", val: "0"},
		{name: "threads negative", env: "STRAVIN_GIT_PACK_THREADS", val: "-2"},
		{name: "threads non-int", env: "STRAVIN_GIT_PACK_THREADS", val: "two"},
		{name: "window memory zero", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "0"},
		{name: "window memory zero unit", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "0m"},
		{name: "window memory bad unit", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "256x"},
		{name: "window memory negative", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "-128m"},
		{name: "window memory injection", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "256m -c core.sshCommand=evil"},
		{name: "window memory newline", env: "STRAVIN_GIT_PACK_WINDOW_MEMORY", val: "256m\nevil"},
		{name: "compression negative", env: "STRAVIN_GIT_PACK_COMPRESSION", val: "-1"},
		{name: "compression too high", env: "STRAVIN_GIT_PACK_COMPRESSION", val: "10"},
		{name: "compression non-int", env: "STRAVIN_GIT_PACK_COMPRESSION", val: "fast"},
		{name: "allow reuse unknown", env: "STRAVIN_GIT_PACK_ALLOW_REUSE", val: "always"},
		{name: "allow reuse control char", env: "STRAVIN_GIT_PACK_ALLOW_REUSE", val: "multi\x01"},
		{name: "allow reuse newline", env: "STRAVIN_GIT_PACK_ALLOW_REUSE", val: "multi\nevil"},
	}

	for _, tc := range invalidCases {
		t.Run("invalid "+tc.name, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tc.env, tc.val)
			if _, err := ParsePackConfigFromEnv(); err == nil {
				t.Errorf("ParsePackConfigFromEnv(%s=%q) = nil, want error", tc.env, tc.val)
			}
		})
	}
}

func TestLoadConfigFromEnvPackFallback(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	t.Setenv("STRAVIN_GIT_PACK_THREADS", "0")
	t.Setenv("STRAVIN_GIT_PACK_WINDOW_MEMORY", "bad")
	t.Setenv("STRAVIN_GIT_PACK_COMPRESSION", "99")
	t.Setenv("STRAVIN_GIT_PACK_ALLOW_REUSE", "invalid")

	cfg := LoadConfigFromEnv(5*time.Minute, logger)
	if cfg.Pack != DefaultPackConfig() {
		t.Errorf("LoadConfigFromEnv invalid pack env = %+v, want defaults %+v", cfg.Pack, DefaultPackConfig())
	}
}
