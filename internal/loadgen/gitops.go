package loadgen

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	reReturnedError = regexp.MustCompile(`returned error:\s*(\d{3})`)
	reHTTPStatus    = regexp.MustCompile(`\bHTTP(?:/[0-9.]+)?\s+(\d{3})\b`)
)

// GitError captures structured failure details from a local git CLI invocation.
type GitError struct {
	Op         string
	Args       []string
	ExitCode   int
	Stdout     string
	Stderr     string
	HTTPStatus int
	Err        error
}

func (e *GitError) Error() string {
	if e.HTTPStatus > 0 {
		return fmt.Sprintf("git %s failed (exit=%d, http=%d): %s", e.Op, e.ExitCode, e.HTTPStatus, strings.TrimSpace(e.Stderr))
	}
	msg := strings.TrimSpace(e.Stderr)
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	return fmt.Sprintf("git %s failed (exit=%d): %s", e.Op, e.ExitCode, msg)
}

func (e *GitError) Unwrap() error { return e.Err }

// GitOptions configures auth and SSH settings for GitClient.
type GitOptions struct {
	AuthToken string
	SSHKey    string
	ExtraEnv  []string
}

// GitClient wraps local git CLI operations inside an isolated per-worker directory.
type GitClient struct {
	workDir string
	opts    GitOptions
}

// NewGitClient creates a GitClient scoped to workDir.
func NewGitClient(workDir string, opts GitOptions) *GitClient {
	return &GitClient{
		workDir: workDir,
		opts:    opts,
	}
}

// WorkDir returns the client's working directory.
func (g *GitClient) WorkDir() string { return g.workDir }

// WithWorkDir returns a clone of GitClient pointing at a different working directory.
func (g *GitClient) WithWorkDir(dir string) *GitClient {
	return &GitClient{
		workDir: dir,
		opts:    g.opts,
	}
}

// Run executes a git command with context cancellation and structured error capture.
func (g *GitClient) Run(ctx context.Context, args ...string) (string, error) {
	op := "git"
	if len(args) > 0 {
		op = args[0]
	}

	fullArgs := g.buildArgs(args)
	cmd := exec.CommandContext(ctx, "git", fullArgs...) //#nosec G204 -- loadgen invokes local git CLI with controlled arguments
	configureGitCommand(cmd)
	if g.workDir != "" {
		if info, err := os.Stat(g.workDir); err == nil && info.IsDir() {
			cmd.Dir = g.workDir
		}
	}
	cmd.Env = g.buildEnv()

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	outStr := redactSecret(stdout.String(), g.opts.AuthToken)
	errStr := redactSecret(stderr.String(), g.opts.AuthToken)
	if err != nil {
		exitCode := -1
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		}
		return outStr, &GitError{
			Op:         op,
			Args:       args,
			ExitCode:   exitCode,
			Stdout:     outStr,
			Stderr:     errStr,
			HTTPStatus: extractHTTPStatus(errStr),
			Err:        err,
		}
	}
	return outStr, nil
}

// Clone clones repoURL into destDir.
func (g *GitClient) Clone(ctx context.Context, repoURL, destDir string) error {
	if err := os.MkdirAll(filepath.Dir(destDir), 0o750); err != nil {
		return err
	}
	// Run clone with no pre-existing cwd requirement by using a temporary parent-scoped client.
	parent := g.WithWorkDir(filepath.Dir(destDir))
	_, err := parent.Run(ctx, "clone", repoURL, destDir)
	return err
}

// Fetch fetches from remote (or "origin" when empty).
func (g *GitClient) Fetch(ctx context.Context, remote string, refspecs ...string) error {
	if remote == "" {
		remote = "origin"
	}
	args := append([]string{"fetch", remote}, refspecs...)
	_, err := g.Run(ctx, args...)
	return err
}

// Checkout checks out ref.
func (g *GitClient) Checkout(ctx context.Context, ref string) error {
	_, err := g.Run(ctx, "checkout", ref)
	return err
}

// CheckoutNewBranch creates and switches to branch.
func (g *GitClient) CheckoutNewBranch(ctx context.Context, branch string) error {
	_, err := g.Run(ctx, "checkout", "-b", branch)
	return err
}

// ConfigUser sets local user.name and user.email in the repository.
func (g *GitClient) ConfigUser(ctx context.Context, name, email string) error {
	if _, err := g.Run(ctx, "config", "user.name", name); err != nil {
		return err
	}
	_, err := g.Run(ctx, "config", "user.email", email)
	return err
}

// WriteFile writes content to relPath inside workDir, preventing path traversal.
func (g *GitClient) WriteFile(relPath string, content []byte) error {
	cleanRel := filepath.Clean(relPath)
	if filepath.IsAbs(cleanRel) || strings.HasPrefix(cleanRel, "..") {
		return fmt.Errorf("gitops: invalid relative path %q", relPath)
	}
	target := filepath.Join(g.workDir, cleanRel)
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return err
	}
	return os.WriteFile(target, content, 0o600)
}

// AddAll stages all changes in workDir.
func (g *GitClient) AddAll(ctx context.Context) error {
	_, err := g.Run(ctx, "add", "-A")
	return err
}

// Commit creates a git commit with message.
func (g *GitClient) Commit(ctx context.Context, message string) error {
	_, err := g.Run(ctx, "commit", "-m", message)
	return err
}

// Push pushes refspecs to remote.
func (g *GitClient) Push(ctx context.Context, remote string, refspecs ...string) error {
	if remote == "" {
		remote = "origin"
	}
	args := append([]string{"push", remote}, refspecs...)
	_, err := g.Run(ctx, args...)
	return err
}

// PushForceWithLease pushes with --force-with-lease.
func (g *GitClient) PushForceWithLease(ctx context.Context, remote string, refspecs ...string) error {
	if remote == "" {
		remote = "origin"
	}
	args := append([]string{"push", "--force-with-lease", remote}, refspecs...)
	_, err := g.Run(ctx, args...)
	return err
}

// Rebase rebases the current branch onto upstream.
func (g *GitClient) Rebase(ctx context.Context, upstream string) error {
	_, err := g.Run(ctx, "rebase", upstream)
	return err
}

// RebaseAbort aborts an in-progress rebase.
func (g *GitClient) RebaseAbort(ctx context.Context) error {
	_, err := g.Run(ctx, "rebase", "--abort")
	return err
}

// LsRemote runs git ls-remote against repoURL.
func (g *GitClient) LsRemote(ctx context.Context, repoURL string, patterns ...string) (string, error) {
	args := append([]string{"ls-remote", repoURL}, patterns...)
	return g.Run(ctx, args...)
}

// RevParse resolves rev to a trimmed commit SHA.
func (g *GitClient) RevParse(ctx context.Context, rev string) (string, error) {
	out, err := g.Run(ctx, "rev-parse", rev)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// Fsck runs git fsck --full --strict in workDir.
func (g *GitClient) Fsck(ctx context.Context) error {
	_, err := g.Run(ctx, "fsck", "--full", "--strict")
	return err
}

func (g *GitClient) buildArgs(args []string) []string {
	var prefix []string
	if g.opts.AuthToken != "" {
		header := formatAuthHeader(g.opts.AuthToken)
		prefix = append(prefix, "-c", "http.extraHeader="+header)
	}
	return append(prefix, args...)
}

func (g *GitClient) buildEnv() []string {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	if g.opts.SSHKey != "" {
		sshCmd := fmt.Sprintf("ssh -i %s -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=5", g.opts.SSHKey)
		env = append(env, "GIT_SSH_COMMAND="+sshCmd)
	}
	if len(g.opts.ExtraEnv) > 0 {
		env = append(env, g.opts.ExtraEnv...)
	}
	return env
}

func formatAuthHeader(token string) string {
	if strings.Contains(token, ":") {
		encoded := base64.StdEncoding.EncodeToString([]byte(token))
		return "Authorization: Basic " + encoded
	}
	return "Authorization: Bearer " + token
}

func extractHTTPStatus(stderr string) int {
	for _, re := range []*regexp.Regexp{reReturnedError, reHTTPStatus} {
		if m := re.FindStringSubmatch(stderr); len(m) == 2 {
			if code, err := strconv.Atoi(m[1]); err == nil {
				return code
			}
		}
	}
	return 0
}

func redactSecret(s, secret string) string {
	if secret == "" {
		return s
	}
	redacted := strings.ReplaceAll(s, secret, "[REDACTED]")
	if strings.Contains(secret, ":") {
		encoded := base64.StdEncoding.EncodeToString([]byte(secret))
		redacted = strings.ReplaceAll(redacted, encoded, "[REDACTED]")
	}
	return redacted
}
