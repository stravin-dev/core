package gitexec

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"regexp"
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

	// DefaultPackThreads bounds git pack-objects worker threads per upload-pack.
	DefaultPackThreads = 1
	// DefaultPackWindowMemory caps delta-search window memory per pack-objects thread.
	DefaultPackWindowMemory = "256m"
	// DefaultPackCompression sets fast zlib compression level (0..9) for on-the-fly packs.
	DefaultPackCompression = 1
	// DefaultPackAllowReuse enables single-pack bitmap reuse during upload-pack
	// (avoiding Git 2.47.3 pack.allowPackReuse=multi missing-object drops with MIDX bitmaps).
	DefaultPackAllowReuse = "single"
)

var gitByteSizePattern = regexp.MustCompile(`^[1-9][0-9]*[kmgKMG]?$`)

// PackConfig tunes git pack-objects execution during upload-pack.
type PackConfig struct {
	Threads      int
	WindowMemory string
	Compression  int
	AllowReuse   string
}

// DefaultPackConfig returns safe, portable defaults for git pack-objects tuning.
func DefaultPackConfig() PackConfig {
	return PackConfig{
		Threads:      DefaultPackThreads,
		WindowMemory: DefaultPackWindowMemory,
		Compression:  DefaultPackCompression,
		AllowReuse:   DefaultPackAllowReuse,
	}
}

// Validate checks that all PackConfig fields are within safe, allowlisted bounds.
func (p PackConfig) Validate() error {
	if p.Threads < 1 {
		return fmt.Errorf("pack.threads must be >= 1, got %d", p.Threads)
	}
	if !gitByteSizePattern.MatchString(p.WindowMemory) {
		return fmt.Errorf("pack.windowMemory %q must match ^[1-9][0-9]*[kmgKMG]?$", p.WindowMemory)
	}
	if p.Compression < 0 || p.Compression > 9 {
		return fmt.Errorf("pack.compression must be in [0, 9], got %d", p.Compression)
	}
	switch p.AllowReuse {
	case "single", "multi", "true", "false":
	default:
		return fmt.Errorf("pack.allowPackReuse %q must be one of single, multi, true, false", p.AllowReuse)
	}
	return nil
}

// ParsePackConfigFromEnv parses and strictly validates STRAVIN_GIT_PACK_* env vars.
func ParsePackConfigFromEnv() (PackConfig, error) {
	cfg := DefaultPackConfig()

	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_THREADS")); raw != "" {
		n, err := parsePackThreads(raw)
		if err != nil {
			return PackConfig{}, err
		}
		cfg.Threads = n
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_WINDOW_MEMORY")); raw != "" {
		v, err := parsePackWindowMemory(raw)
		if err != nil {
			return PackConfig{}, err
		}
		cfg.WindowMemory = v
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_COMPRESSION")); raw != "" {
		n, err := parsePackCompression(raw)
		if err != nil {
			return PackConfig{}, err
		}
		cfg.Compression = n
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_ALLOW_REUSE")); raw != "" {
		v, err := parsePackAllowReuse(raw)
		if err != nil {
			return PackConfig{}, err
		}
		cfg.AllowReuse = v
	}
	return cfg, cfg.Validate()
}

func loadPackConfigFromEnv(logger *slog.Logger) PackConfig {
	cfg := DefaultPackConfig()

	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_THREADS")); raw != "" {
		if n, err := parsePackThreads(raw); err == nil {
			cfg.Threads = n
		} else if logger != nil {
			logger.Warn("gitexec: ignoring invalid STRAVIN_GIT_PACK_THREADS", "value", raw, "error", err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_WINDOW_MEMORY")); raw != "" {
		if v, err := parsePackWindowMemory(raw); err == nil {
			cfg.WindowMemory = v
		} else if logger != nil {
			logger.Warn("gitexec: ignoring invalid STRAVIN_GIT_PACK_WINDOW_MEMORY", "value", raw, "error", err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_COMPRESSION")); raw != "" {
		if n, err := parsePackCompression(raw); err == nil {
			cfg.Compression = n
		} else if logger != nil {
			logger.Warn("gitexec: ignoring invalid STRAVIN_GIT_PACK_COMPRESSION", "value", raw, "error", err)
		}
	}
	if raw := strings.TrimSpace(os.Getenv("STRAVIN_GIT_PACK_ALLOW_REUSE")); raw != "" {
		if v, err := parsePackAllowReuse(raw); err == nil {
			cfg.AllowReuse = v
		} else if logger != nil {
			logger.Warn("gitexec: ignoring invalid STRAVIN_GIT_PACK_ALLOW_REUSE", "value", raw, "error", err)
		}
	}
	return cfg
}

func parsePackThreads(raw string) (int, error) {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("invalid STRAVIN_GIT_PACK_THREADS %q: must be integer >= 1", raw)
	}
	return n, nil
}

func parsePackWindowMemory(raw string) (string, error) {
	if !gitByteSizePattern.MatchString(raw) {
		return "", fmt.Errorf("invalid STRAVIN_GIT_PACK_WINDOW_MEMORY %q: must match ^[1-9][0-9]*[kmgKMG]?$", raw)
	}
	return raw, nil
}

func parsePackCompression(raw string) (int, error) {
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 || n > 9 {
		return 0, fmt.Errorf("invalid STRAVIN_GIT_PACK_COMPRESSION %q: must be integer in [0, 9]", raw)
	}
	return n, nil
}

func parsePackAllowReuse(raw string) (string, error) {
	v := strings.ToLower(raw)
	switch v {
	case "single", "multi", "true", "false":
		return v, nil
	default:
		return "", fmt.Errorf("invalid STRAVIN_GIT_PACK_ALLOW_REUSE %q: must be one of single, multi, true, false", raw)
	}
}

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
		Pack:           loadPackConfigFromEnv(logger),
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
