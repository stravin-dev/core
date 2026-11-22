//go:build linux

package gitexec

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
)

// LinuxSandboxConfigurator builds Linux namespace and cgroup v2 SysProcAttr
// configurations with automatic graceful fallback in unprivileged environments.
type LinuxSandboxConfigurator struct {
	cfg          SandboxConfig
	logger       *slog.Logger
	cgroupSeq    atomic.Uint64
	cgroupWarned sync.Once
	nsDisabled   atomic.Bool
	nsWarned     sync.Once
}

// DefaultSandboxConfigurator returns the platform-native Linux sandbox configurator.
func DefaultSandboxConfigurator(cfg SandboxConfig, logger *slog.Logger) SandboxConfigurator {
	return NewLinuxSandboxConfigurator(cfg, logger)
}

// NewLinuxSandboxConfigurator creates a LinuxSandboxConfigurator with the given config.
func NewLinuxSandboxConfigurator(cfg SandboxConfig, logger *slog.Logger) *LinuxSandboxConfigurator {
	if cfg.CgroupRoot == "" {
		cfg.CgroupRoot = DefaultCgroupRoot
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LinuxSandboxConfigurator{
		cfg:    cfg,
		logger: logger,
	}
}

// Prepare constructs Linux SysProcAttr with process-group isolation, parent-death
// SIGKILL, optional namespaces, and optional cgroup v2 limits. If cgroup creation
// fails due to permissions or missing mount, it logs once and degrades gracefully.
func (l *LinuxSandboxConfigurator) Prepare(_ context.Context, spec SandboxSpec) (PreparedSandbox, error) {
	attr := &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}

	isolated := false
	if l.cfg.EnableNamespaces && !l.nsDisabled.Load() {
		attr.Cloneflags = syscall.CLONE_NEWNET | syscall.CLONE_NEWPID | syscall.CLONE_NEWNS
		isolated = true
	}

	cleanup := noopCleanup
	if l.cfg.EnableCgroup && l.hasCgroupLimits() {
		fd, cgCleanup, err := l.prepareCgroup(spec)
		if err != nil {
			l.cgroupWarned.Do(func() {
				l.logger.Warn("gitexec: cgroup v2 unavailable, falling back to locked-down subprocess without cgroup limits",
					"cgroup_root", l.cfg.CgroupRoot,
					"error", err,
				)
			})
		} else {
			attr.UseCgroupFD = true
			attr.CgroupFD = fd
			cleanup = cgCleanup
			isolated = true
		}
	}

	return PreparedSandbox{
		SysProcAttr: attr,
		Cleanup:     cleanup,
		Isolated:    isolated,
	}, nil
}

// DisableNamespaces permanently disables namespace Cloneflags on this configurator
// after an unprivileged container returns EPERM/EACCES.
func (l *LinuxSandboxConfigurator) DisableNamespaces(cause error) {
	l.nsDisabled.Store(true)
	l.nsWarned.Do(func() {
		l.logger.Warn("gitexec: Linux namespaces unavailable in current environment, falling back to locked-down subprocess mode",
			"error", cause,
		)
	})
}

func (l *LinuxSandboxConfigurator) hasCgroupLimits() bool {
	return l.cfg.MemoryMaxBytes > 0 || l.cfg.CPUQuota != "" || l.cfg.PidsMax > 0
}

func (l *LinuxSandboxConfigurator) prepareCgroup(spec SandboxSpec) (int, func(), error) {
	seq := l.cgroupSeq.Add(1)
	name := formatCgroupName(spec, seq)
	cgDir := filepath.Join(l.cfg.CgroupRoot, name)

	if err := os.Mkdir(cgDir, 0o750); err != nil {
		return -1, noopCleanup, fmt.Errorf("mkdir cgroup %s: %w", cgDir, err)
	}

	removeDir := func() {
		if err := os.Remove(cgDir); errors.Is(err, syscall.ENOTEMPTY) {
			_ = os.RemoveAll(cgDir)
		}
	}

	if l.cfg.MemoryMaxBytes > 0 {
		if err := writeCgroupFile(cgDir, "memory.max", strconv.FormatInt(l.cfg.MemoryMaxBytes, 10)); err != nil {
			removeDir()
			return -1, noopCleanup, err
		}
	}
	if l.cfg.CPUQuota != "" {
		if err := writeCgroupFile(cgDir, "cpu.max", l.cfg.CPUQuota); err != nil {
			removeDir()
			return -1, noopCleanup, err
		}
	}
	if l.cfg.PidsMax > 0 {
		if err := writeCgroupFile(cgDir, "pids.max", strconv.FormatInt(l.cfg.PidsMax, 10)); err != nil {
			removeDir()
			return -1, noopCleanup, err
		}
	}

	// #nosec G304 -- cgDir is constructed from configured CgroupRoot and sanitized name
	f, err := os.Open(cgDir)
	if err != nil {
		removeDir()
		return -1, noopCleanup, fmt.Errorf("open cgroup dir %s: %w", cgDir, err)
	}

	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			_ = f.Close()
			removeDir()
		})
	}
	return int(f.Fd()), cleanup, nil
}

func writeCgroupFile(cgDir, filename, value string) error {
	path := filepath.Join(cgDir, filename)
	// #nosec G306 -- cgroup v2 control files use standard 0644/0600 permissions
	if err := os.WriteFile(path, []byte(value+"\n"), 0o600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func formatCgroupName(spec SandboxSpec, seq uint64) string {
	pid := os.Getpid()
	if spec.Repo.TenantID != "" && spec.Repo.RepoID != "" {
		safeTenant := sanitizeCgroupSegment(string(spec.Repo.TenantID))
		safeRepo := sanitizeCgroupSegment(string(spec.Repo.RepoID))
		return fmt.Sprintf("stravin-%s-%s-%d-%d", safeTenant, safeRepo, pid, seq)
	}
	return fmt.Sprintf("stravin-git-%d-%d", pid, seq)
}

func sanitizeCgroupSegment(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteByte(c)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func fallbackSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid:   true,
		Pdeathsig: syscall.SIGKILL,
	}
}

func isSandboxPermissionError(err error) bool {
	return errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) || errors.Is(err, syscall.EINVAL)
}

func killProcessGroup(pid int) error {
	if pid <= 0 {
		return nil
	}
	err := syscall.Kill(-pid, syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
