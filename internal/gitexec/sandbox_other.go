//go:build !linux

package gitexec

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"syscall"
)

// FallbackSandboxConfigurator provides locked-down subprocess execution with
// process-group isolation on non-Linux hosts (e.g. macOS local development).
type FallbackSandboxConfigurator struct {
	logger *slog.Logger
	warned sync.Once
}

// DefaultSandboxConfigurator returns the non-Linux fallback sandbox configurator.
func DefaultSandboxConfigurator(_ SandboxConfig, logger *slog.Logger) SandboxConfigurator {
	if logger == nil {
		logger = slog.Default()
	}
	return &FallbackSandboxConfigurator{logger: logger}
}

// Prepare returns a process-group-isolated SysProcAttr without Linux namespaces
// or cgroup v2 controls, logging a one-time warning.
func (f *FallbackSandboxConfigurator) Prepare(_ context.Context, _ SandboxSpec) (PreparedSandbox, error) {
	f.warned.Do(func() {
		f.logger.Warn("gitexec: Linux namespace/cgroup sandbox unavailable on host OS, running locked-down subprocess mode",
			"goos", runtime.GOOS,
		)
	})
	return PreparedSandbox{
		SysProcAttr: fallbackSysProcAttr(),
		Cleanup:     noopCleanup,
		Isolated:    false,
	}, nil
}

func fallbackSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Setpgid: true,
	}
}

func isSandboxPermissionError(_ error) bool {
	return false
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
