package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

// ErrorClass categorizes client-observed failures for retry vs oracle failure accounting.
type ErrorClass string

const (
	// ErrorRetryableConflict indicates expected Ref-CAS / non-fast-forward contention on shared branches.
	ErrorRetryableConflict ErrorClass = "retryable_conflict"
	// ErrorRetryableTransient indicates expected transient transport/5xx errors during chaos.
	ErrorRetryableTransient ErrorClass = "retryable_transient"
	// ErrorExpectedReject indicates an expected rejection in negative/hook-reject scenarios.
	ErrorExpectedReject ErrorClass = "expected_reject"
	// ErrorUnexpected indicates a hard integrity, protocol, or server failure.
	ErrorUnexpected ErrorClass = "unexpected"
)

// ClassifyOptions provides scenario context for error classification.
type ClassifyOptions struct {
	ExclusiveBranch bool
	ExpectReject    bool
}

// ClassifyError classifies a Git or scenario error according to client-side oracle rules.
func ClassifyError(err error, opts ClassifyOptions) ErrorClass {
	if err == nil {
		return ""
	}

	var gitErr *GitError
	hasGitErr := errors.As(err, &gitErr)

	combined := strings.ToLower(err.Error())
	httpStatus := 0
	op := ""
	if hasGitErr {
		op = strings.ToLower(gitErr.Op)
		httpStatus = gitErr.HTTPStatus
		combined = strings.ToLower(gitErr.Stderr + "\n" + gitErr.Stdout + "\n" + gitErr.Error())
	}

	// Integrity / fsck failures are ALWAYS unexpected hard failures.
	if op == "fsck" || isCorruptionMessage(combined) {
		return ErrorUnexpected
	}

	// Expected rejection in negative test scenarios (e.g. Wasm pre-receive reject).
	if opts.ExpectReject && isCleanReject(httpStatus, combined) {
		return ErrorExpectedReject
	}

	// Explicit HTTP status classification.
	switch httpStatus {
	case 502, 503, 504:
		return ErrorRetryableTransient
	case 409, 412:
		if opts.ExclusiveBranch {
			return ErrorUnexpected
		}
		return ErrorRetryableConflict
	case 400, 401, 403, 404, 500:
		return ErrorUnexpected
	}

	// Ref-CAS / non-fast-forward push contention.
	if isConflictMessage(combined) {
		if opts.ExclusiveBranch {
			return ErrorUnexpected
		}
		return ErrorRetryableConflict
	}

	// Transient transport / chaos errors.
	if isTransientMessage(combined) {
		return ErrorRetryableTransient
	}

	return ErrorUnexpected
}

func isCorruptionMessage(msg string) bool {
	markers := []string{
		"missing blob",
		"missing tree",
		"missing commit",
		"bad object",
		"corrupt",
		"hash mismatch",
		"invalid sha1",
		"broken link",
		"dangling",
		"fsck",
	}
	for _, m := range markers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

func isCleanReject(httpStatus int, msg string) bool {
	if httpStatus == 400 || httpStatus == 403 || httpStatus == 422 {
		return true
	}
	markers := []string{
		"pre-receive hook declined",
		"remote rejected",
		"hook declined",
		"wasm",
		"rejected by policy",
		"invalid refspec",
		"src refspec",
		"invalid ref",
		"funny refname",
		"not a valid ref",
		"bad ref",
		"refusing to",
	}
	for _, m := range markers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

func isConflictMessage(msg string) bool {
	markers := []string{
		"non-fast-forward",
		"fetch first",
		"stale info",
		"cannot lock ref",
		"reference already exists",
		"atomic push failed",
		"ref-cas",
		"ref cas",
	}
	for _, m := range markers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

func isTransientMessage(msg string) bool {
	markers := []string{
		"502",
		"503",
		"504",
		"early eof",
		"unexpected eof",
		"connection refused",
		"connection reset",
		"connection closed by",
		"broken pipe",
		"i/o timeout",
		"operation timed out",
		"timed out",
		"remote end hung up unexpectedly",
		"the remote end hung up",
		"unexpected disconnect",
		"empty reply from server",
		"failed to connect",
		"could not connect",
		"recv failure",
		"send failure",
		"rpc failed; curl",
		"transfer closed with outstanding read data",
		"kex_exchange_identification",
		"banner exchange",
		"client_loop: send disconnect",
		"expected flush after ref listing",
		"temporary failure",
		"tls handshake timeout",
		"service unavailable",
		"bad gateway",
		"gateway timeout",
		"context deadline exceeded",
	}
	for _, m := range markers {
		if strings.Contains(msg, m) {
			return true
		}
	}
	return false
}

// UnexpectedErrorRecord captures a single hard failure with trace correlation.
type UnexpectedErrorRecord struct {
	Timestamp time.Time `json:"timestamp"`
	TraceID   string    `json:"trace_id,omitempty"`
	Persona   Persona   `json:"persona"`
	Scenario  string    `json:"scenario"`
	Op        string    `json:"op"`
	Detail    string    `json:"detail"`
}

// OracleSummary aggregates run-wide operation and failure counts.
type OracleSummary struct {
	TotalOps         int64 `json:"total_ops"`
	Retries          int64 `json:"retries"`
	UnexpectedErrors int64 `json:"unexpected_errors"`
	FsckFailures     int64 `json:"fsck_failures"`
}

// HasFailures reports whether any unexpected error or git fsck failure occurred.
func (s OracleSummary) HasFailures() bool {
	return s.UnexpectedErrors > 0 || s.FsckFailures > 0
}

const maxStoredRecords = 500

// Oracle is the thread-safe client-side integrity verifier and failure recorder.
type Oracle struct {
	mu               sync.Mutex
	totalOps         int64
	retries          int64
	unexpectedErrors int64
	fsckFailures     int64
	records          []UnexpectedErrorRecord
}

// NewOracle constructs an empty Oracle.
func NewOracle() *Oracle {
	return &Oracle{}
}

// RecordOp increments the total operations counter.
func (o *Oracle) RecordOp() {
	o.mu.Lock()
	o.totalOps++
	o.mu.Unlock()
}

// RecordRetry increments the total retries counter.
func (o *Oracle) RecordRetry() {
	o.mu.Lock()
	o.retries++
	o.mu.Unlock()
}

// RecordUnexpected records an unexpected error correlated with ctx's active trace_id.
func (o *Oracle) RecordUnexpected(ctx context.Context, persona Persona, scenario, op, detail string) {
	traceID := traceIDFromContext(ctx)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.unexpectedErrors++
	if len(o.records) < maxStoredRecords {
		o.records = append(o.records, UnexpectedErrorRecord{
			Timestamp: time.Now().UTC(),
			TraceID:   traceID,
			Persona:   persona,
			Scenario:  scenario,
			Op:        op,
			Detail:    detail,
		})
	}
}

// RecordFsckFailure records a git fsck --full --strict integrity failure.
func (o *Oracle) RecordFsckFailure(ctx context.Context, persona Persona, scenario, detail string) {
	traceID := traceIDFromContext(ctx)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fsckFailures++
	if len(o.records) < maxStoredRecords {
		o.records = append(o.records, UnexpectedErrorRecord{
			Timestamp: time.Now().UTC(),
			TraceID:   traceID,
			Persona:   persona,
			Scenario:  scenario,
			Op:        "fsck",
			Detail:    detail,
		})
	}
}

// VerifySHA asserts that a previously committed SHA is present and matches actualSHA.
func (o *Oracle) VerifySHA(ctx context.Context, persona Persona, scenario, expectedSHA, actualSHA string) error {
	if expectedSHA == actualSHA && expectedSHA != "" {
		return nil
	}
	detail := fmt.Sprintf("lost committed SHA: expected %q, got %q", expectedSHA, actualSHA)
	o.RecordUnexpected(ctx, persona, scenario, "verify_sha", detail)
	return errors.New(detail)
}

// HasFailures returns true if any unexpected error or fsck failure was recorded.
func (o *Oracle) HasFailures() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.unexpectedErrors > 0 || o.fsckFailures > 0
}

// Summary returns an immutable snapshot of Oracle counts.
func (o *Oracle) Summary() OracleSummary {
	o.mu.Lock()
	defer o.mu.Unlock()
	return OracleSummary{
		TotalOps:         o.totalOps,
		Retries:          o.retries,
		UnexpectedErrors: o.unexpectedErrors,
		FsckFailures:     o.fsckFailures,
	}
}

// Records returns a copy of recorded unexpected error and fsck failure entries.
func (o *Oracle) Records() []UnexpectedErrorRecord {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]UnexpectedErrorRecord, len(o.records))
	copy(out, o.records)
	return out
}

func traceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// RetryPolicy computes bounded exponential backoff with jitter.
type RetryPolicy struct {
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
}

// DefaultRetryPolicy returns a bounded exponential backoff policy for transient chaos errors.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseBackoff: 100 * time.Millisecond,
		MaxBackoff:  5 * time.Second,
	}
}

// BackoffDuration returns a jittered exponential backoff for zero-based attempt.
func (p RetryPolicy) BackoffDuration(attempt int, rng *rand.Rand) time.Duration {
	base := p.BaseBackoff
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	maxD := p.MaxBackoff
	if maxD < base {
		maxD = base
	}
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 10 {
		attempt = 10
	}

	ceil := base << uint(attempt)
	if ceil > maxD || ceil <= 0 {
		ceil = maxD
	}
	if rng == nil {
		return ceil
	}
	// Half-to-full jitter in [ceil/2, ceil].
	half := ceil / 2
	if half <= 0 {
		return ceil
	}
	jitter := time.Duration(rng.Int64N(int64(half) + 1))
	return half + jitter
}
