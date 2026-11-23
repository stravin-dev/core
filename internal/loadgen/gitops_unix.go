//go:build unix

package loadgen

import (
	"errors"
	"os/exec"
	"syscall"
	"time"
)

const gitCmdWaitDelay = 2 * time.Second

// configureGitCommand isolates the git command in its own process group so
// context cancellation terminates git and all descendant helpers
// (git-remote-http, ssh, git-pack-objects) instead of leaving orphaned
// processes holding stdout/stderr pipes open.
func configureGitCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = gitCmdWaitDelay
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}
}
