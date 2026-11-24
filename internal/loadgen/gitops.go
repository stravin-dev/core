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
	"time"
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

// GitOptions configures auth, SSH, and timeout settings for GitClient.
type GitOptions struct {
	AuthToken  string
	SSHKey     string
	CmdTimeout time.Duration
	ExtraEnv   []string
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

// WithAuthor returns a shallow copy of GitClient with git author/committer
// environment variables set in ExtraEnv (replacing any prior values for those keys).
func (g *GitClient) WithAuthor(name, email string) *GitClient {
	extra := make([]string, 0, len(g.opts.ExtraEnv)+4)
	for _, kv := range g.opts.ExtraEnv {
		if isAuthorEnvVar(kv) {
			continue
		}
		extra = append(extra, kv)
	}
	extra = append(extra,
		"GIT_AUTHOR_NAME="+name,
		"GIT_AUTHOR_EMAIL="+email,
		"GIT_COMMITTER_NAME="+name,
		"GIT_COMMITTER_EMAIL="+email,
	)
	opts := g.opts
	opts.ExtraEnv = extra
	return &GitClient{
		workDir: g.workDir,
		opts:    opts,
	}
}

func isAuthorEnvVar(kv string) bool {
	return strings.HasPrefix(kv, "GIT_AUTHOR_NAME=") ||
		strings.HasPrefix(kv, "GIT_AUTHOR_EMAIL=") ||
		strings.HasPrefix(kv, "GIT_COMMITTER_NAME=") ||
		strings.HasPrefix(kv, "GIT_COMMITTER_EMAIL=")
}

// Run executes a git command with context cancellation and structured error capture.
func (g *GitClient) Run(ctx context.Context, args ...string) (string, error) {
	op := "git"
	if len(args) > 0 {
		op = args[0]
	}

	cmdCtx := ctx
	if g.opts.CmdTimeout > 0 {
		var cancel context.CancelFunc
		cmdCtx, cancel = context.WithTimeout(ctx, g.opts.CmdTimeout)
		defer cancel()
	}

	fullArgs := g.buildArgs(args)
	cmd := exec.CommandContext(cmdCtx, "git", fullArgs...) //#nosec G204 -- loadgen invokes local git CLI with controlled arguments
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
		if ctx.Err() == nil && errors.Is(cmdCtx.Err(), context.DeadlineExceeded) {
			err = fmt.Errorf("git %s timed out after %s: %w", op, g.opts.CmdTimeout, context.DeadlineExceeded)
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

// WorktreeAddDetach creates a new detached linked worktree at destDir checked out at ref.
func (g *GitClient) WorktreeAddDetach(ctx context.Context, destDir, ref string) error {
	if err := os.MkdirAll(filepath.Dir(destDir), 0o750); err != nil {
		return err
	}
	_, err := g.Run(ctx, "worktree", "add", "--detach", destDir, ref)
	return err
}

// WorktreeRemove forcibly removes a linked worktree at destDir.
func (g *GitClient) WorktreeRemove(ctx context.Context, destDir string) error {
	_, err := g.Run(ctx, "worktree", "remove", "--force", destDir)
	return err
}

// WorktreePrune removes stale linked worktree administrative metadata.
func (g *GitClient) WorktreePrune(ctx context.Context) error {
	_, err := g.Run(ctx, "worktree", "prune")
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
	if rev == "HEAD" && ctx.Err() == nil {
		if sha, ok := resolveHeadSHA(g.workDir); ok {
			return sha, nil
		}
	}
	out, err := g.Run(ctx, "rev-parse", rev)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// resolveHeadSHA resolves HEAD directly from .git/HEAD (including linked worktree
// .git gitdir pointers) and loose ref files without spawning a git subprocess.
// Returns ("", false) for unborn refs, packed-refs, reftables, or any non-standard
// layout so callers fall back cleanly to `git rev-parse HEAD`.
func resolveHeadSHA(workDir string) (string, bool) {
	if workDir == "" {
		return "", false
	}
	gitDir, commonDir, ok := resolveGitDirs(workDir)
	if !ok {
		return "", false
	}
	headPath := filepath.Join(gitDir, "HEAD")
	headBytes, err := os.ReadFile(headPath) //#nosec G304 -- gitDir is resolved from a loadgen-managed local repository directory
	if err != nil {
		return "", false
	}
	head := strings.TrimSpace(string(headBytes))
	if strings.HasPrefix(head, "ref: ") {
		refRel := strings.TrimSpace(strings.TrimPrefix(head, "ref: "))
		cleanRef := filepath.Clean(filepath.FromSlash(refRel))
		if refRel == "" || filepath.IsAbs(cleanRef) || strings.HasPrefix(cleanRef, "..") {
			return "", false
		}
		refPath := filepath.Clean(filepath.Join(commonDir, cleanRef))
		if !strings.HasPrefix(refPath, commonDir+string(filepath.Separator)) {
			return "", false
		}
		refBytes, refErr := os.ReadFile(refPath) //#nosec G304,G703 -- refPath is verified to stay within commonDir
		if refErr != nil {
			return "", false
		}
		oid := strings.TrimSpace(string(refBytes))
		if isValidHexOID(oid) {
			return oid, true
		}
		return "", false
	}
	if isValidHexOID(head) {
		return head, true
	}
	return "", false
}

// resolveGitDirs returns the per-worktree gitDir and shared commonDir for workDir.
func resolveGitDirs(workDir string) (gitDir, commonDir string, ok bool) {
	dotGit := filepath.Clean(filepath.Join(workDir, ".git"))
	info, err := os.Stat(dotGit)
	if err != nil {
		return "", "", false
	}
	if info.IsDir() {
		return dotGit, dotGit, true
	}

	// Linked git worktree: .git is a file containing "gitdir: <path>".
	content, err := os.ReadFile(dotGit) //#nosec G304 -- dotGit is inside a loadgen-managed local worktree directory
	if err != nil {
		return "", "", false
	}
	line := strings.TrimSpace(string(content))
	if !strings.HasPrefix(line, "gitdir:") {
		return "", "", false
	}
	target := strings.TrimSpace(strings.TrimPrefix(line, "gitdir:"))
	if target == "" {
		return "", "", false
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(workDir, target)
	}
	gitDir = filepath.Clean(target)
	commonDir = gitDir

	if commonBytes, err := os.ReadFile(filepath.Join(gitDir, "commondir")); err == nil { //#nosec G304,G703 -- commondir is a standard git worktree metadata file inside gitDir
		rel := strings.TrimSpace(string(commonBytes))
		if rel != "" {
			if filepath.IsAbs(rel) {
				commonDir = filepath.Clean(rel)
			} else {
				commonDir = filepath.Clean(filepath.Join(gitDir, rel))
			}
		}
	}
	return gitDir, commonDir, true
}

func isValidHexOID(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		isDigit := c >= '0' && c <= '9'
		isHexLower := c >= 'a' && c <= 'f'
		if !isDigit && !isHexLower {
			return false
		}
	}
	return true
}

// Fsck runs git fsck --full --strict in workDir.
func (g *GitClient) Fsck(ctx context.Context) error {
	_, err := g.Run(ctx, "fsck", "--full", "--strict")
	return err
}

func (g *GitClient) buildArgs(args []string) []string {
	prefix := []string{
		"-c", "gc.auto=0",
		"-c", "maintenance.auto=false",
	}
	if g.opts.AuthToken != "" {
		header := formatAuthHeader(g.opts.AuthToken)
		prefix = append(prefix, "-c", "http.extraHeader="+header)
	}
	return append(prefix, args...)
}

func (g *GitClient) buildEnv() []string {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	sshCmd := "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o BatchMode=yes -o ConnectTimeout=5"
	if g.opts.SSHKey != "" {
		sshCmd += " -i " + g.opts.SSHKey
	}
	env = append(env, "GIT_SSH_COMMAND="+sshCmd)
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
