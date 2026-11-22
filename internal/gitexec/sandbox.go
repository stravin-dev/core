package gitexec

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultCgroupRoot is the standard Linux cgroup v2 mount point.
	DefaultCgroupRoot = "/sys/fs/cgroup"
	// DefaultMemoryMaxBytes caps git subprocess memory at 512 MiB.
	DefaultMemoryMaxBytes int64 = 512 * 1024 * 1024
	// DefaultCPUQuota caps git subprocess CPU at 200% (200ms per 100ms period).
	DefaultCPUQuota = "200000 100000"
	// DefaultPidsMax caps git subprocess tasks/threads at 64.
	DefaultPidsMax int64 = 64
	// DefaultTimeout is the fallback subprocess execution timeout when not specified.
	DefaultTimeout = 5 * time.Minute
)

// DefaultSandboxConfig returns secure Linux namespace and cgroup v2 defaults.
func DefaultSandboxConfig() SandboxConfig {
	return SandboxConfig{
		EnableNamespaces: true,
		EnableCgroup:     true,
		CgroupRoot:       DefaultCgroupRoot,
		MemoryMaxBytes:   DefaultMemoryMaxBytes,
		CPUQuota:         DefaultCPUQuota,
		PidsMax:          DefaultPidsMax,
	}
}

// LoadSandboxConfigFromEnv loads SandboxConfig from STRAVIN_GITEXEC_* env vars,
// falling back to DefaultSandboxConfig for unset or invalid values.
func LoadSandboxConfigFromEnv() SandboxConfig {
	cfg := DefaultSandboxConfig()

	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_ENABLE_NAMESPACES")); raw != "" {
		if v, err := strconv.ParseBool(raw); err == nil {
			cfg.EnableNamespaces = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_ENABLE_CGROUP")); raw != "" {
		if v, err := strconv.ParseBool(raw); err == nil {
			cfg.EnableCgroup = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_CGROUP_ROOT")); raw != "" {
		cfg.CgroupRoot = raw
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_MEMORY_MAX_BYTES")); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			cfg.MemoryMaxBytes = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_CPU_QUOTA")); raw != "" {
		cfg.CPUQuota = raw
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_PIDS_MAX")); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			cfg.PidsMax = v
		}
	}

	return cfg
}

// LoadConfigFromEnv builds a gitexec.Config with secure sandbox defaults and
// a non-zero execution timeout (overridable via STRAVIN_GITEXEC_TIMEOUT).
func LoadConfigFromEnv(defaultTimeout time.Duration, logger *slog.Logger) Config {
	timeout := defaultTimeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GITEXEC_TIMEOUT")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			timeout = d
		}
	}
	return Config{
		DefaultTimeout: timeout,
		Sandbox:        LoadSandboxConfigFromEnv(),
		Logger:         logger,
	}
}

// SandboxConfig configures OS-level isolation and cgroup v2 resource caps for
// sandboxed git subprocesses.
type SandboxConfig struct {
	// EnableNamespaces enables Linux network, PID, and mount namespaces
	// (CLONE_NEWNET | CLONE_NEWPID | CLONE_NEWNS) when supported by the kernel
	// and container privilege level.
	EnableNamespaces bool
	// EnableCgroup enables per-invocation cgroup v2 resource limits when
	// CgroupRoot is writable.
	EnableCgroup bool
	// CgroupRoot overrides the cgroup v2 hierarchy root (defaults to /sys/fs/cgroup).
	CgroupRoot string
	// MemoryMaxBytes sets cgroup v2 memory.max when > 0.
	MemoryMaxBytes int64
	// CPUQuota sets cgroup v2 cpu.max (e.g. "50000 100000") when non-empty.
	CPUQuota string
	// PidsMax sets cgroup v2 pids.max when > 0.
	PidsMax int64
}

// SandboxSpec describes the target repository and access mode for a single git
// subprocess invocation.
type SandboxSpec struct {
	Repo       tenant.RepoKey
	RepoDir    string
	StagingDir string
	ReadOnly   bool
}

// PreparedSandbox holds the OS process attributes and cleanup callback for one
// subprocess execution.
type PreparedSandbox struct {
	SysProcAttr *syscall.SysProcAttr
	Cleanup     func()
	Isolated    bool
}

// SandboxConfigurator is the GoF Strategy/Adapter interface that constructs
// platform-specific process isolation attributes for git subprocesses.
type SandboxConfigurator interface {
	Prepare(ctx context.Context, spec SandboxSpec) (PreparedSandbox, error)
}

func noopCleanup() {}
