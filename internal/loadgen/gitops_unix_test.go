//go:build unix

package loadgen

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestGitClientRunCancelsOrphanedChildProcesses(t *testing.T) {
	workDir := t.TempDir()
	pidFile := filepath.Join(workDir, "child.pid")

	g := NewGitClient(workDir, GitOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spawn a git alias that forks a background sleeping child inheriting
	// stdout/stderr pipes, mimicking git-remote-http / ssh / git-pack-objects.
	alias := fmt.Sprintf("alias.hang=!sh -c 'sleep 30 & echo $! > \"$1\"; sleep 30' _ %s", pidFile)

	done := make(chan error, 1)
	go func() {
		_, err := g.Run(ctx, "-c", alias, "hang")
		done <- err
	}()

	// Wait deterministically until the background child PID is recorded.
	var childPID int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(pidFile) //#nosec G304 -- test reads temp pid file created inside t.TempDir()
		if err == nil {
			if pid, parseErr := strconv.Atoi(strings.TrimSpace(string(raw))); parseErr == nil && pid > 0 {
				childPID = pid
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID == 0 {
		cancel()
		t.Fatal("timed out waiting for git child process to start")
	}
	t.Cleanup(func() {
		_ = syscall.Kill(childPID, syscall.SIGKILL)
	})

	cancelAt := time.Now()
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected GitClient.Run to return an error after context cancellation")
		}
		if elapsed := time.Since(cancelAt); elapsed >= gitCmdWaitDelay {
			t.Fatalf("GitClient.Run took %s after cancel, want < %s (process group kill should close pipes immediately)", elapsed, gitCmdWaitDelay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("GitClient.Run hung after context cancellation (orphaned child held pipes open)")
	}

	// Verify the spawned child process was terminated along with the process group.
	killDeadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(killDeadline) {
		if isProcessGoneOrZombie(childPID) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("orphaned child process %d still alive after context cancellation", childPID)
}

func isProcessGoneOrZombie(pid int) bool {
	if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
		return true
	}
	// Inside containers without an init reaper, killed grandchildren reparented
	// to PID 1 remain zombies where kill(pid, 0) still returns nil.
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) //#nosec G304 -- reads proc stat for test-spawned child PID
	if err != nil {
		return false
	}
	if idx := strings.LastIndexByte(string(stat), ')'); idx >= 0 && idx+2 < len(stat) {
		return stat[idx+2] == 'Z'
	}
	return false
}
