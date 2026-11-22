package gitexec

import (
	"context"
	"syscall"

	"stravin.dev/core/internal/tenant"
)

// DefaultCgroupRoot is the standard Linux cgroup v2 mount point.
const DefaultCgroupRoot = "/sys/fs/cgroup"

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
