// Package gitexec implements the strict Apache 2.0 / GPLv2 subprocess boundary
// for invoking upstream git binaries (git-upload-pack, git-index-pack,
// git-repack, multi-pack-index, commit-graph).
//
// Stravin Core NEVER links libgit2 or git via cgo; all invocations cross an OS
// process boundary over pipes with a scrubbed environment, mandatory -c config
// lockdown flags, bounded stderr capture, process-group cancellation cleanup,
// and platform-adaptive Linux namespace/cgroup v2 sandboxing.
package gitexec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"stravin.dev/core/internal/tenant"
)

const (
	// DefaultAllowedPATH is the minimal PATH allowlisted for git subprocesses.
	DefaultAllowedPATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	// DefaultMaxStderrBytes caps stderr captured in memory per git invocation.
	DefaultMaxStderrBytes = 64 * 1024
	// defaultWaitDelay bounds pipe drain time after killing a cancelled process group.
	defaultWaitDelay = 2 * time.Second
)

var (
	// ErrEmptyRepoDir is returned when a request omits RepoDir.
	ErrEmptyRepoDir = errors.New("gitexec: empty repo directory")
	// ErrNoRepackAction is returned when RepackRequest requests no operation.
	ErrNoRepackAction = errors.New("gitexec: repack request must enable at least one action")
	// ErrInvalidRequest is returned when a request has invalid or conflicting fields.
	ErrInvalidRequest = errors.New("gitexec: invalid request")
)

// GitExecutor is the pluggable GoF Strategy interface for executing git
// operations behind an isolation boundary (direct Linux sandbox, locked-down
// subprocess, or future tenant-pinned microVM).
type GitExecutor interface {
	ExecUploadPack(ctx context.Context, req UploadPackRequest) error
	ExecIndexPack(ctx context.Context, req IndexPackRequest) error
	ExecRepack(ctx context.Context, req RepackRequest) error
}

// UploadPackRequest configures a read-only git-upload-pack subprocess.
type UploadPackRequest struct {
	Repo          tenant.RepoKey
	RepoDir       string
	AdvertiseRefs bool
	StatelessRPC  bool
	Stdin         io.Reader
	Stdout        io.Writer
	Stderr        io.Writer
	Timeout       time.Duration
}

// IndexPackRequest configures a git index-pack subprocess with an optional
// ephemeral writable staging directory.
type IndexPackRequest struct {
	Repo           tenant.RepoKey
	RepoDir        string
	StagingDir     string
	FromStdin      bool
	FixThin        bool
	PackPath       string
	OutputPackPath string
	Stdin          io.Reader
	Stdout         io.Writer
	Stderr         io.Writer
	Timeout        time.Duration
}

// RepackRequest configures offline compaction commands (git repack,
// git multi-pack-index write, git commit-graph write) for stravin-pack.
type RepackRequest struct {
	Repo             tenant.RepoKey
	RepoDir          string
	Repack           bool
	All              bool
	DeleteRedundant  bool
	Geometric        int
	WriteBitmap      bool
	Quiet            bool
	WriteMIDX        bool
	WriteCommitGraph bool
	Stdout           io.Writer
	Stderr           io.Writer
	Timeout          time.Duration
}

// ExecError reports a failed git subprocess invocation with bounded stderr.
type ExecError struct {
	Subcommand string
	ExitCode   int
	Stderr     string
	Truncated  bool
	Err        error
}

func (e *ExecError) Error() string {
	suffix := ""
	if e.Truncated {
		suffix = " (truncated)"
	}
	if e.Stderr != "" {
		return fmt.Sprintf("gitexec: git %s failed (exit %d): %v: %s%s", e.Subcommand, e.ExitCode, e.Err, e.Stderr, suffix)
	}
	return fmt.Sprintf("gitexec: git %s failed (exit %d): %v", e.Subcommand, e.ExitCode, e.Err)
}

func (e *ExecError) Unwrap() error {
	return e.Err
}

// Config configures SubprocessExecutor.
type Config struct {
	// GitBinary is the git executable name or path (defaults to "git").
	GitBinary string
	// AllowedPATH overrides the minimal PATH passed to git subprocesses.
	AllowedPATH string
	// DefaultTimeout applies when a request does not specify Timeout (>0).
	DefaultTimeout time.Duration
	// MaxStderrBytes caps in-memory stderr capture (defaults to 64 KiB).
	MaxStderrBytes int
	// Sandbox configures Linux namespace / cgroup v2 isolation.
	Sandbox SandboxConfig
	// SandboxConfigurator optionally overrides the platform default strategy.
	SandboxConfigurator SandboxConfigurator
	// Logger receives structured fallback and diagnostic logs.
	Logger *slog.Logger
}

// SubprocessExecutor executes git strictly as an OS subprocess over pipes with
// scrubbed environment variables, mandatory -c lockdown flags, and sandbox
// attributes from a pluggable SandboxConfigurator.
type SubprocessExecutor struct {
	gitBinary      string
	allowedPATH    string
	defaultTimeout time.Duration
	maxStderrBytes int
	sandbox        SandboxConfigurator
	logger         *slog.Logger
	fallbackWarned atomic.Bool
}

// NewSubprocessExecutor creates a SubprocessExecutor with safe defaults.
func NewSubprocessExecutor(cfg Config) *SubprocessExecutor {
	gitBin := cfg.GitBinary
	if gitBin == "" {
		gitBin = "git"
	}
	allowedPATH := cfg.AllowedPATH
	if allowedPATH == "" {
		allowedPATH = DefaultAllowedPATH
	}
	maxStderr := cfg.MaxStderrBytes
	if maxStderr <= 0 {
		maxStderr = DefaultMaxStderrBytes
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	sb := cfg.SandboxConfigurator
	if sb == nil {
		sb = DefaultSandboxConfigurator(cfg.Sandbox, logger)
	}
	return &SubprocessExecutor{
		gitBinary:      gitBin,
		allowedPATH:    allowedPATH,
		defaultTimeout: cfg.DefaultTimeout,
		maxStderrBytes: maxStderr,
		sandbox:        sb,
		logger:         logger,
	}
}

// ExecUploadPack invokes `git upload-pack` in read-only mode against req.RepoDir.
func (e *SubprocessExecutor) ExecUploadPack(ctx context.Context, req UploadPackRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}

	args := []string{"-c", "uploadpack.allowReachableSHA1InWant=true", "upload-pack"}
	if req.StatelessRPC {
		args = append(args, "--stateless-rpc")
	}
	if req.AdvertiseRefs {
		args = append(args, "--advertise-refs")
	}
	args = append(args, req.RepoDir)

	spec := SandboxSpec{
		Repo:     req.Repo,
		RepoDir:  req.RepoDir,
		ReadOnly: true,
	}
	return e.run(ctx, "upload-pack", spec, req.RepoDir, args, req.Stdin, req.Stdout, req.Stderr, req.Timeout)
}

// ExecIndexPack invokes `git index-pack` for validating/indexing a packfile
// stream or staged packfile.
func (e *SubprocessExecutor) ExecIndexPack(ctx context.Context, req IndexPackRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !req.FromStdin && strings.TrimSpace(req.PackPath) == "" {
		return fmt.Errorf("%w: index-pack requires FromStdin or PackPath", ErrInvalidRequest)
	}

	args := []string{"-C", req.RepoDir, "index-pack"}
	if req.FromStdin {
		args = append(args, "--stdin")
	}
	if req.FixThin {
		args = append(args, "--fix-thin")
	}
	if req.FromStdin {
		if req.OutputPackPath != "" {
			args = append(args, req.OutputPackPath)
		}
	} else {
		args = append(args, req.PackPath)
	}

	spec := SandboxSpec{
		Repo:       req.Repo,
		RepoDir:    req.RepoDir,
		StagingDir: req.StagingDir,
		ReadOnly:   req.StagingDir != "",
	}
	return e.run(ctx, "index-pack", spec, req.RepoDir, args, req.Stdin, req.Stdout, req.Stderr, req.Timeout)
}

// ExecRepack runs configured compaction steps (git repack, multi-pack-index,
// commit-graph) sequentially against req.RepoDir.
func (e *SubprocessExecutor) ExecRepack(ctx context.Context, req RepackRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !req.Repack && !req.WriteMIDX && !req.WriteCommitGraph {
		return ErrNoRepackAction
	}

	spec := SandboxSpec{
		Repo:     req.Repo,
		RepoDir:  req.RepoDir,
		ReadOnly: false,
	}

	if req.Repack {
		args := []string{"-C", req.RepoDir, "repack"}
		if req.All {
			args = append(args, "-a")
		}
		if req.DeleteRedundant {
			args = append(args, "-d")
		}
		if req.Geometric > 0 {
			args = append(args, "--geometric="+strconv.Itoa(req.Geometric))
		}
		if req.WriteBitmap {
			args = append(args, "-b")
		}
		if req.Quiet {
			args = append(args, "-q")
		}
		if err := e.run(ctx, "repack", spec, req.RepoDir, args, nil, req.Stdout, req.Stderr, req.Timeout); err != nil {
			return err
		}
	}

	if req.WriteMIDX {
		args := []string{"-C", req.RepoDir, "multi-pack-index", "write"}
		if req.WriteBitmap {
			args = append(args, "--bitmap")
		}
		if err := e.run(ctx, "multi-pack-index", spec, req.RepoDir, args, nil, req.Stdout, req.Stderr, req.Timeout); err != nil {
			return err
		}
	}

	if req.WriteCommitGraph {
		args := []string{"-C", req.RepoDir, "commit-graph", "write", "--reachable", "--changed-paths"}
		if err := e.run(ctx, "commit-graph", spec, req.RepoDir, args, nil, req.Stdout, req.Stderr, req.Timeout); err != nil {
			return err
		}
	}

	return nil
}

func (e *SubprocessExecutor) run(
	ctx context.Context,
	subcommand string,
	spec SandboxSpec,
	workDir string,
	subArgs []string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	timeout time.Duration,
) error {
	effectiveTimeout := timeout
	if effectiveTimeout <= 0 {
		effectiveTimeout = e.defaultTimeout
	}
	if effectiveTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, effectiveTimeout)
		defer cancel()
	}

	prepared, err := e.sandbox.Prepare(ctx, spec)
	if err != nil {
		return fmt.Errorf("gitexec: prepare sandbox for %s: %w", subcommand, err)
	}
	defer func() {
		if prepared.Cleanup != nil {
			prepared.Cleanup()
		}
	}()

	fullArgs := buildGitArgs(subArgs...)
	env := scrubbedEnv(e.allowedPATH)

	stderrBuf := newLimitedBuffer(e.maxStderrBytes)
	stderrWriter := io.Writer(stderrBuf)
	if stderr != nil {
		stderrWriter = io.MultiWriter(stderr, stderrBuf)
	}

	cmd := e.buildCmd(ctx, workDir, fullArgs, env, stdin, stdout, stderrWriter, prepared.SysProcAttr)
	if err := cmd.Start(); err != nil {
		if prepared.Isolated && isSandboxPermissionError(err) {
			e.handleSandboxFallback(err)
			if prepared.Cleanup != nil {
				prepared.Cleanup()
				prepared.Cleanup = noopCleanup
			}
			cmd = e.buildCmd(ctx, workDir, fullArgs, env, stdin, stdout, stderrWriter, fallbackSysProcAttr())
			if retryErr := cmd.Start(); retryErr != nil {
				return fmt.Errorf("gitexec: start git %s (fallback): %w", subcommand, retryErr)
			}
		} else {
			return fmt.Errorf("gitexec: start git %s: %w", subcommand, err)
		}
	}

	waitErr := cmd.Wait()
	if waitErr == nil {
		return nil
	}

	exitCode := -1
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		exitCode = exitErr.ExitCode()
	}

	cause := waitErr
	if ctxErr := ctx.Err(); ctxErr != nil {
		cause = fmt.Errorf("%w: %w", ctxErr, waitErr)
	}

	return &ExecError{
		Subcommand: subcommand,
		ExitCode:   exitCode,
		Stderr:     strings.TrimSpace(stderrBuf.String()),
		Truncated:  stderrBuf.Truncated(),
		Err:        cause,
	}
}

func (e *SubprocessExecutor) buildCmd(
	ctx context.Context,
	workDir string,
	args []string,
	env []string,
	stdin io.Reader,
	stdout io.Writer,
	stderr io.Writer,
	sysProcAttr *syscall.SysProcAttr,
) *exec.Cmd {
	// #nosec G204 -- gitBinary is configured by the service; args are built with mandatory -c lockdown flags
	cmd := exec.CommandContext(ctx, e.gitBinary, args...)
	cmd.Dir = workDir
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = sysProcAttr
	cmd.WaitDelay = defaultWaitDelay
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return killProcessGroup(cmd.Process.Pid)
	}
	return cmd
}

type namespaceDisabler interface {
	DisableNamespaces(cause error)
}

func (e *SubprocessExecutor) handleSandboxFallback(cause error) {
	if d, ok := e.sandbox.(namespaceDisabler); ok {
		d.DisableNamespaces(cause)
		return
	}
	if e.fallbackWarned.CompareAndSwap(false, true) {
		e.logger.Warn("gitexec: sandboxed subprocess start denied, falling back to locked-down subprocess mode",
			"error", cause,
		)
	}
}

// scrubbedEnv constructs the minimal allowlisted environment for every git
// subprocess, preventing host secrets or AWS credentials from leaking.
func scrubbedEnv(allowedPATH string) []string {
	if allowedPATH == "" {
		allowedPATH = DefaultAllowedPATH
	}
	return []string{
		"PATH=" + allowedPATH,
		"HOME=/nonexistent",
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_CONFIG_GLOBAL=/dev/null",
		"GIT_TERMINAL_PROMPT=0",
	}
}

// buildGitArgs prepends mandatory config attack-surface lockdown flags before
// the requested git subcommand and its arguments.
func buildGitArgs(subcommandArgs ...string) []string {
	args := make([]string, 0, 10+len(subcommandArgs))
	args = append(args,
		"-c", "core.hooksPath=/dev/null",
		"-c", "core.fsmonitor=false",
		"-c", "core.alternateRefsCommand=exit 0",
		"-c", "core.sshCommand=false",
		"-c", "protocol.ext.allow=never",
	)
	args = append(args, subcommandArgs...)
	return args
}

// limitedBuffer captures up to maxBytes of output without failing writes.
type limitedBuffer struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	maxBytes  int
	truncated bool
}

func newLimitedBuffer(maxBytes int) *limitedBuffer {
	return &limitedBuffer{maxBytes: maxBytes}
}

func (l *limitedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	total := len(p)
	remaining := l.maxBytes - l.buf.Len()
	if remaining <= 0 {
		if total > 0 {
			l.truncated = true
		}
		return total, nil
	}
	if total > remaining {
		_, _ = l.buf.Write(p[:remaining])
		l.truncated = true
		return total, nil
	}
	_, _ = l.buf.Write(p)
	return total, nil
}

func (l *limitedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *limitedBuffer) Truncated() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncated
}
