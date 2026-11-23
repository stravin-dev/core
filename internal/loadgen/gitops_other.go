//go:build !unix

package loadgen

import (
	"os/exec"
	"time"
)

const gitCmdWaitDelay = 2 * time.Second

func configureGitCommand(cmd *exec.Cmd) {
	cmd.WaitDelay = gitCmdWaitDelay
}
