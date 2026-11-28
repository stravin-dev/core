package gitexec

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"stravin.dev/core/internal/tenant"
)

// ObjectMeta holds the Git object type and uncompressed byte size returned by cat-file --batch-check.
type ObjectMeta struct {
	OID    string
	Type   string
	Size   uint64
	Exists bool
}

// GitDataExecutor is the narrow read-only interface for inspecting commits,
// diffs, blobs, and trees on a materialized bare repository.
type GitDataExecutor interface {
	ExecRevParse(ctx context.Context, req RevParseRequest) (string, error)
	ExecMergeBase(ctx context.Context, req MergeBaseRequest) (string, error)
	ExecDiffTreeRaw(ctx context.Context, req DiffTreeRawRequest) error
	ExecDiffTreePatch(ctx context.Context, req DiffTreePatchRequest) error
	ExecCatFileBatchCheck(ctx context.Context, req CatFileBatchCheckRequest) (map[string]ObjectMeta, error)
	ExecCatFileBlob(ctx context.Context, req CatFileBlobRequest) error
	ExecCatFileBlobHead(ctx context.Context, req CatFileBlobHeadRequest) ([]byte, error)
	ExecLsTree(ctx context.Context, req LsTreeRequest) error
	ExecLog(ctx context.Context, req LogRequest) error
	ExecRevListCount(ctx context.Context, req RevListCountRequest) (uint64, error)
	ExecCatFileBatch(ctx context.Context, req CatFileBatchRequest) (map[string][]byte, error)
}

// RevParseRequest configures `git rev-parse --verify`.
type RevParseRequest struct {
	Repo    tenant.RepoKey
	RepoDir string
	Rev     string
	Timeout time.Duration
}

// MergeBaseRequest configures `git merge-base`.
type MergeBaseRequest struct {
	Repo    tenant.RepoKey
	RepoDir string
	BaseOID string
	HeadOID string
	Timeout time.Duration
}

// DiffTreeRawRequest configures `git diff-tree -r -M -z --no-abbrev --raw`.
type DiffTreeRawRequest struct {
	Repo    tenant.RepoKey
	RepoDir string
	BaseOID string
	HeadOID string
	Stdout  io.Writer
	Timeout time.Duration
}

// DiffTreePatchRequest configures `git diff-tree -r -M -p` for specific paths.
type DiffTreePatchRequest struct {
	Repo         tenant.RepoKey
	RepoDir      string
	BaseOID      string
	HeadOID      string
	ContextLines uint32
	Paths        []string
	Stdout       io.Writer
	Timeout      time.Duration
}

// CatFileBatchCheckRequest configures `git cat-file --batch-check` for a list of object OIDs.
type CatFileBatchCheckRequest struct {
	Repo    tenant.RepoKey
	RepoDir string
	OIDs    []string
	Timeout time.Duration
}

// CatFileBlobRequest configures streaming `git cat-file blob <oid>`.
type CatFileBlobRequest struct {
	Repo    tenant.RepoKey
	RepoDir string
	BlobOID string
	Stdout  io.Writer
	Timeout time.Duration
}

// CatFileBlobHeadRequest reads at most MaxBytes from the start of a blob.
type CatFileBlobHeadRequest struct {
	Repo     tenant.RepoKey
	RepoDir  string
	BlobOID  string
	MaxBytes int
	Timeout  time.Duration
}

// LsTreeRequest configures `git ls-tree -l -z`.
type LsTreeRequest struct {
	Repo      tenant.RepoKey
	RepoDir   string
	TreeOID   string
	Recursive bool
	Stdout    io.Writer
	Timeout   time.Duration
}

// LogRequest configures `git log -z --max-count=<N> --format=<fmt>`.
type LogRequest struct {
	Repo      tenant.RepoKey
	RepoDir   string
	CommitOID string
	Path      string
	MaxCount  uint32
	Format    string
	NameOnly  bool
	Stdout    io.Writer
	Timeout   time.Duration
}

// RevListCountRequest configures `git rev-list --count`.
type RevListCountRequest struct {
	Repo      tenant.RepoKey
	RepoDir   string
	CommitOID string
	Path      string
	MaxCount  uint64
	Timeout   time.Duration
}

// CatFileBatchRequest configures `git cat-file --batch` for bounded raw object reads.
type CatFileBatchRequest struct {
	Repo           tenant.RepoKey
	RepoDir        string
	OIDs           []string
	MaxObjectBytes uint64
	Timeout        time.Duration
}

// ExecRevParse resolves a revision expression to a full object ID.
func (e *SubprocessExecutor) ExecRevParse(ctx context.Context, req RevParseRequest) (string, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return "", ErrEmptyRepoDir
	}
	if !isSafeRevSpec(req.Rev) {
		return "", fmt.Errorf("%w: invalid revision %q", ErrInvalidRequest, req.Rev)
	}
	var out bytes.Buffer
	args := []string{"--git-dir", req.RepoDir, "rev-parse", "--verify", "--end-of-options", req.Rev}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	if err := e.run(ctx, "rev-parse", spec, req.RepoDir, args, nil, nil, &out, nil, req.Timeout); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

// ExecMergeBase computes the merge-base commit OID between BaseOID and HeadOID.
func (e *SubprocessExecutor) ExecMergeBase(ctx context.Context, req MergeBaseRequest) (string, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return "", ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.BaseOID) || !IsValidHexOID(req.HeadOID) {
		return "", fmt.Errorf("%w: invalid merge-base OIDs", ErrInvalidRequest)
	}
	var out bytes.Buffer
	args := []string{"--git-dir", req.RepoDir, "merge-base", "--", req.BaseOID, req.HeadOID}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	if err := e.run(ctx, "merge-base", spec, req.RepoDir, args, nil, nil, &out, nil, req.Timeout); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

// ExecDiffTreeRaw outputs machine-readable NUL-delimited diff-tree --raw entries to req.Stdout.
func (e *SubprocessExecutor) ExecDiffTreeRaw(ctx context.Context, req DiffTreeRawRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.BaseOID) || !IsValidHexOID(req.HeadOID) {
		return fmt.Errorf("%w: invalid diff-tree OIDs", ErrInvalidRequest)
	}
	args := []string{
		"--git-dir", req.RepoDir,
		"diff-tree", "-r", "-M", "-z", "--no-abbrev", "--raw",
		req.BaseOID, req.HeadOID,
	}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	return e.run(ctx, "diff-tree", spec, req.RepoDir, args, nil, nil, req.Stdout, nil, req.Timeout)
}

// ExecDiffTreePatch outputs unified diff patches for the specified paths to req.Stdout.
func (e *SubprocessExecutor) ExecDiffTreePatch(ctx context.Context, req DiffTreePatchRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.BaseOID) || !IsValidHexOID(req.HeadOID) {
		return fmt.Errorf("%w: invalid diff-tree OIDs", ErrInvalidRequest)
	}
	args := []string{
		"--git-dir", req.RepoDir,
		"diff-tree", "-r", "-M", "-p",
		"--no-color", "--no-ext-diff", "--no-textconv",
		"-U" + strconv.FormatUint(uint64(req.ContextLines), 10),
		req.BaseOID, req.HeadOID,
	}
	if len(req.Paths) > 0 {
		args = append(args, "--")
		for _, p := range req.Paths {
			if p == "" || strings.ContainsRune(p, '\x00') {
				return fmt.Errorf("%w: invalid diff path %q", ErrInvalidRequest, p)
			}
			args = append(args, p)
		}
	}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	return e.run(ctx, "diff-tree", spec, req.RepoDir, args, nil, nil, req.Stdout, nil, req.Timeout)
}

// ExecCatFileBatchCheck resolves object types and sizes for all req.OIDs in a single git subprocess.
func (e *SubprocessExecutor) ExecCatFileBatchCheck(ctx context.Context, req CatFileBatchCheckRequest) (map[string]ObjectMeta, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return nil, ErrEmptyRepoDir
	}
	result := make(map[string]ObjectMeta, len(req.OIDs))
	if len(req.OIDs) == 0 {
		return result, nil
	}
	var stdinBuf bytes.Buffer
	seen := make(map[string]struct{}, len(req.OIDs))
	for _, oid := range req.OIDs {
		if !IsValidHexOID(oid) {
			return nil, fmt.Errorf("%w: invalid object OID %q", ErrInvalidRequest, oid)
		}
		if _, dup := seen[oid]; dup {
			continue
		}
		seen[oid] = struct{}{}
		stdinBuf.WriteString(oid)
		stdinBuf.WriteByte('\n')
	}

	var out bytes.Buffer
	args := []string{"--git-dir", req.RepoDir, "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	if err := e.run(ctx, "cat-file", spec, req.RepoDir, args, nil, &stdinBuf, &out, nil, req.Timeout); err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(&out)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[1] == "missing" {
			result[fields[0]] = ObjectMeta{OID: fields[0], Exists: false}
			continue
		}
		if len(fields) >= 3 {
			sz, err := strconv.ParseUint(fields[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("gitexec: parse cat-file size %q: %w", fields[2], err)
			}
			result[fields[0]] = ObjectMeta{
				OID:    fields[0],
				Type:   fields[1],
				Size:   sz,
				Exists: true,
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("gitexec: scan cat-file output: %w", err)
	}
	return result, nil
}

// ExecCatFileBlob streams raw blob bytes directly into req.Stdout without buffering in memory.
func (e *SubprocessExecutor) ExecCatFileBlob(ctx context.Context, req CatFileBlobRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.BlobOID) {
		return fmt.Errorf("%w: invalid blob OID %q", ErrInvalidRequest, req.BlobOID)
	}
	args := []string{"--git-dir", req.RepoDir, "cat-file", "blob", req.BlobOID}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	return e.run(ctx, "cat-file", spec, req.RepoDir, args, nil, nil, req.Stdout, nil, req.Timeout)
}

// ExecCatFileBlobHead reads at most req.MaxBytes from the beginning of a blob
// and cancels the git subprocess once the prefix cap is reached.
func (e *SubprocessExecutor) ExecCatFileBlobHead(ctx context.Context, req CatFileBlobHeadRequest) ([]byte, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return nil, ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.BlobOID) {
		return nil, fmt.Errorf("%w: invalid blob OID %q", ErrInvalidRequest, req.BlobOID)
	}
	maxBytes := req.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 8192
	}

	headCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	cw := &prefixCaptureWriter{
		limit:  maxBytes,
		cancel: cancel,
	}
	args := []string{"--git-dir", req.RepoDir, "cat-file", "blob", req.BlobOID}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	err := e.run(headCtx, "cat-file", spec, req.RepoDir, args, nil, nil, cw, nil, req.Timeout)
	if err != nil {
		if cw.reachedLimit && ctx.Err() == nil {
			return cw.buf.Bytes(), nil
		}
		return nil, err
	}
	return cw.buf.Bytes(), nil
}

// ExecLsTree outputs NUL-delimited `git ls-tree -l -z` entries to req.Stdout.
func (e *SubprocessExecutor) ExecLsTree(ctx context.Context, req LsTreeRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.TreeOID) {
		return fmt.Errorf("%w: invalid tree OID %q", ErrInvalidRequest, req.TreeOID)
	}
	args := []string{"--git-dir", req.RepoDir, "ls-tree", "-l", "-z"}
	if req.Recursive {
		args = append(args, "-r")
	}
	args = append(args, req.TreeOID)
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	return e.run(ctx, "ls-tree", spec, req.RepoDir, args, nil, nil, req.Stdout, nil, req.Timeout)
}

const defaultCatFileBatchMaxBytes uint64 = 1 << 20 // 1 MiB

// ExecLog outputs bounded, NUL-delimited `git log` records to req.Stdout.
func (e *SubprocessExecutor) ExecLog(ctx context.Context, req LogRequest) error {
	if strings.TrimSpace(req.RepoDir) == "" {
		return ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.CommitOID) {
		return fmt.Errorf("%w: invalid log commit OID %q", ErrInvalidRequest, req.CommitOID)
	}
	if req.MaxCount == 0 {
		return fmt.Errorf("%w: log max_count must be positive", ErrInvalidRequest)
	}
	if strings.ContainsRune(req.Format, '\x00') {
		return fmt.Errorf("%w: invalid log format", ErrInvalidRequest)
	}
	if req.Path != "" && strings.ContainsRune(req.Path, '\x00') {
		return fmt.Errorf("%w: invalid log path %q", ErrInvalidRequest, req.Path)
	}
	args := []string{
		"--git-dir", req.RepoDir,
		"log", "-z",
		"--max-count=" + strconv.FormatUint(uint64(req.MaxCount), 10),
	}
	if req.Format != "" {
		args = append(args, "--format="+req.Format)
	}
	if req.NameOnly {
		args = append(args, "-m", "--first-parent", "--name-only")
	}
	args = append(args, req.CommitOID)
	if req.Path != "" {
		args = append(args, "--", req.Path)
	}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	return e.run(ctx, "log", spec, req.RepoDir, args, nil, nil, req.Stdout, nil, req.Timeout)
}

// ExecRevListCount returns a bounded commit count reachable from req.CommitOID (optionally filtered by req.Path).
func (e *SubprocessExecutor) ExecRevListCount(ctx context.Context, req RevListCountRequest) (uint64, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return 0, ErrEmptyRepoDir
	}
	if !IsValidHexOID(req.CommitOID) {
		return 0, fmt.Errorf("%w: invalid rev-list commit OID %q", ErrInvalidRequest, req.CommitOID)
	}
	if req.Path != "" && strings.ContainsRune(req.Path, '\x00') {
		return 0, fmt.Errorf("%w: invalid rev-list path %q", ErrInvalidRequest, req.Path)
	}
	args := []string{"--git-dir", req.RepoDir, "rev-list", "--count"}
	if req.MaxCount > 0 {
		args = append(args, "--max-count="+strconv.FormatUint(req.MaxCount, 10))
	}
	args = append(args, req.CommitOID)
	if req.Path != "" {
		args = append(args, "--", req.Path)
	}
	var out bytes.Buffer
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	if err := e.run(ctx, "rev-list", spec, req.RepoDir, args, nil, nil, &out, nil, req.Timeout); err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimSpace(out.String()), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("gitexec: parse rev-list count %q: %w", strings.TrimSpace(out.String()), err)
	}
	return n, nil
}

// ExecCatFileBatch reads raw Git object payloads for req.OIDs in a single `git cat-file --batch` subprocess.
// Objects exceeding MaxObjectBytes (default 1 MiB) are skipped to keep memory bounded.
func (e *SubprocessExecutor) ExecCatFileBatch(ctx context.Context, req CatFileBatchRequest) (map[string][]byte, error) {
	if strings.TrimSpace(req.RepoDir) == "" {
		return nil, ErrEmptyRepoDir
	}
	result := make(map[string][]byte, len(req.OIDs))
	if len(req.OIDs) == 0 {
		return result, nil
	}
	maxBytes := req.MaxObjectBytes
	if maxBytes == 0 {
		maxBytes = defaultCatFileBatchMaxBytes
	}

	var stdinBuf bytes.Buffer
	seen := make(map[string]struct{}, len(req.OIDs))
	for _, oid := range req.OIDs {
		if !IsValidHexOID(oid) {
			return nil, fmt.Errorf("%w: invalid object OID %q", ErrInvalidRequest, oid)
		}
		if _, dup := seen[oid]; dup {
			continue
		}
		seen[oid] = struct{}{}
		stdinBuf.WriteString(oid)
		stdinBuf.WriteByte('\n')
	}

	var out bytes.Buffer
	args := []string{"--git-dir", req.RepoDir, "cat-file", "--batch"}
	spec := SandboxSpec{Repo: req.Repo, RepoDir: req.RepoDir, ReadOnly: true}
	if err := e.run(ctx, "cat-file", spec, req.RepoDir, args, nil, &stdinBuf, &out, nil, req.Timeout); err != nil {
		return nil, err
	}

	reader := bufio.NewReader(&out)
	for {
		header, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) && header == "" {
			break
		}
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("gitexec: read cat-file batch header: %w", err)
		}
		fields := strings.Fields(header)
		if len(fields) == 2 && fields[1] == "missing" {
			continue
		}
		if len(fields) < 3 {
			return nil, fmt.Errorf("gitexec: malformed cat-file batch header %q", strings.TrimSpace(header))
		}
		oid := fields[0]
		sz, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gitexec: parse cat-file batch size %q: %w", fields[2], err)
		}
		if sz < 0 || sz == math.MaxInt64 {
			return nil, fmt.Errorf("gitexec: invalid cat-file batch size %d", sz)
		}
		if uint64(sz) > maxBytes {
			if _, err := io.CopyN(io.Discard, reader, sz+1); err != nil {
				return nil, fmt.Errorf("gitexec: discard oversized object %s: %w", oid, err)
			}
			continue
		}
		payload := make([]byte, sz)
		if _, err := io.ReadFull(reader, payload); err != nil {
			return nil, fmt.Errorf("gitexec: read cat-file batch body %s: %w", oid, err)
		}
		// Consume trailing LF separator emitted by git cat-file --batch.
		if _, err := reader.ReadByte(); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("gitexec: read cat-file batch delimiter %s: %w", oid, err)
		}
		result[oid] = payload
	}
	return result, nil
}

// IsValidHexOID reports whether oid is a valid 40-char (SHA-1) or 64-char (SHA-256) lowercase hex string.
func IsValidHexOID(oid string) bool {
	if len(oid) != 40 && len(oid) != 64 {
		return false
	}
	for i := 0; i < len(oid); i++ {
		c := oid[i]
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !isHex {
			return false
		}
	}
	return true
}

func isSafeRevSpec(rev string) bool {
	if rev == "" || len(rev) > 1024 || strings.HasPrefix(rev, "-") {
		return false
	}
	if strings.ContainsAny(rev, "\r\n\x00") {
		return false
	}
	return true
}

type prefixCaptureWriter struct {
	buf          bytes.Buffer
	limit        int
	cancel       context.CancelFunc
	reachedLimit bool
}

func (w *prefixCaptureWriter) Write(p []byte) (int, error) {
	rem := w.limit - w.buf.Len()
	if rem <= 0 {
		w.reachedLimit = true
		w.cancel()
		return 0, errors.New("prefix limit reached")
	}
	if len(p) >= rem {
		w.buf.Write(p[:rem])
		w.reachedLimit = true
		w.cancel()
		return rem, errors.New("prefix limit reached")
	}
	w.buf.Write(p)
	return len(p), nil
}

var _ GitDataExecutor = (*SubprocessExecutor)(nil)
