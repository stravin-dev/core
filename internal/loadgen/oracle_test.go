package loadgen

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		opts ClassifyOptions
		want ErrorClass
	}{
		{
			name: "non-fast-forward on shared branch is retryable conflict",
			err:  &GitError{Op: "push", ExitCode: 1, Stderr: "! [rejected] main -> main (non-fast-forward)\nerror: failed to push some refs"},
			opts: ClassifyOptions{ExclusiveBranch: false},
			want: ErrorRetryableConflict,
		},
		{
			name: "fetch first on shared branch is retryable conflict",
			err:  &GitError{Op: "push", ExitCode: 1, Stderr: "! [rejected] main -> main (fetch first)"},
			opts: ClassifyOptions{ExclusiveBranch: false},
			want: ErrorRetryableConflict,
		},
		{
			name: "non-fast-forward on exclusive branch is unexpected",
			err:  &GitError{Op: "push", ExitCode: 1, Stderr: "! [rejected] agent-1/feat -> agent-1/feat (non-fast-forward)"},
			opts: ClassifyOptions{ExclusiveBranch: true},
			want: ErrorUnexpected,
		},
		{
			name: "http 503 during chaos is retryable transient",
			err:  &GitError{Op: "fetch", ExitCode: 128, HTTPStatus: 503, Stderr: "fatal: unable to access '...': The requested URL returned error: 503"},
			want: ErrorRetryableTransient,
		},
		{
			name: "http 502 during chaos is retryable transient",
			err:  &GitError{Op: "push", ExitCode: 128, HTTPStatus: 502, Stderr: "The requested URL returned error: 502"},
			want: ErrorRetryableTransient,
		},
		{
			name: "connection reset is retryable transient",
			err:  &GitError{Op: "clone", ExitCode: 128, Stderr: "fatal: read error: connection reset by peer"},
			want: ErrorRetryableTransient,
		},
		{
			name: "remote end hung up unexpectedly is retryable transient",
			err:  &GitError{Op: "fetch", ExitCode: 128, Stderr: "fatal: the remote end hung up unexpectedly\nfatal: early EOF"},
			want: ErrorRetryableTransient,
		},
		{
			name: "http 500 is unexpected",
			err:  &GitError{Op: "push", ExitCode: 128, HTTPStatus: 500, Stderr: "The requested URL returned error: 500"},
			want: ErrorUnexpected,
		},
		{
			name: "http 401 is unexpected",
			err:  &GitError{Op: "clone", ExitCode: 128, HTTPStatus: 401, Stderr: "The requested URL returned error: 401"},
			want: ErrorUnexpected,
		},
		{
			name: "http 404 is unexpected",
			err:  &GitError{Op: "clone", ExitCode: 128, HTTPStatus: 404, Stderr: "The requested URL returned error: 404"},
			want: ErrorUnexpected,
		},
		{
			name: "git fsck corruption is always unexpected",
			err:  &GitError{Op: "fsck", ExitCode: 1, Stderr: "missing blob 1234567890abcdef1234567890abcdef12345678"},
			want: ErrorUnexpected,
		},
		{
			name: "expected wasm hook reject is expected reject",
			err:  &GitError{Op: "push", ExitCode: 1, Stderr: "! [remote rejected] main -> main (pre-receive hook declined)"},
			opts: ClassifyOptions{ExpectReject: true},
			want: ErrorExpectedReject,
		},
		{
			name: "unexpected wasm hook reject without ExpectReject is unexpected",
			err:  &GitError{Op: "push", ExitCode: 1, Stderr: "! [remote rejected] main -> main (pre-receive hook declined)"},
			opts: ClassifyOptions{ExpectReject: false},
			want: ErrorUnexpected,
		},
		{
			name: "generic error defaults to unexpected",
			err:  errors.New("boom"),
			want: ErrorUnexpected,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyError(tc.err, tc.opts)
			if got != tc.want {
				t.Errorf("ClassifyError() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOracleTracksUnexpectedAndFsckWithTraceID(t *testing.T) {
	o := NewOracle()
	if o.HasFailures() {
		t.Fatal("fresh oracle should not have failures")
	}

	traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	spanID, _ := trace.SpanIDFromHex("0102030405060708")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	}))

	o.RecordOp()
	o.RecordRetry()
	o.RecordUnexpected(ctx, PersonaDev, "dev-push", "push", "HTTP 500 internal error")
	o.RecordFsckFailure(ctx, PersonaAgent, "agent-clone", "missing blob abc")

	if !o.HasFailures() {
		t.Fatal("expected HasFailures() == true")
	}
	s := o.Summary()
	if s.TotalOps != 1 || s.Retries != 1 || s.UnexpectedErrors != 1 || s.FsckFailures != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	recs := o.Records()
	if len(recs) != 2 {
		t.Fatalf("len(Records) = %d, want 2", len(recs))
	}
	if recs[0].TraceID != traceID.String() {
		t.Errorf("Record[0].TraceID = %q, want %q", recs[0].TraceID, traceID.String())
	}
	if recs[0].Scenario != "dev-push" || recs[0].Op != "push" {
		t.Errorf("Record[0] = %+v", recs[0])
	}
	if recs[1].Op != "fsck" || recs[1].Persona != PersonaAgent {
		t.Errorf("Record[1] = %+v", recs[1])
	}
}

func TestOracleVerifySHA(t *testing.T) {
	o := NewOracle()
	if err := o.VerifySHA(context.Background(), PersonaDev, "dev-push", "abc123", "abc123"); err != nil {
		t.Fatalf("matching SHAs returned error: %v", err)
	}
	if o.HasFailures() {
		t.Fatal("matching SHAs should not record failure")
	}

	if err := o.VerifySHA(context.Background(), PersonaDev, "dev-push", "abc123", "def456"); err == nil {
		t.Fatal("mismatched SHAs should return error")
	}
	if !o.HasFailures() {
		t.Fatal("mismatched SHAs should record unexpected error")
	}
}

func TestBackoffDurationBoundedAndDeterministic(t *testing.T) {
	rng1 := rand.New(rand.NewPCG(123, 456))
	rng2 := rand.New(rand.NewPCG(123, 456))

	p := DefaultRetryPolicy()
	for attempt := 0; attempt < 6; attempt++ {
		d1 := p.BackoffDuration(attempt, rng1)
		d2 := p.BackoffDuration(attempt, rng2)
		if d1 != d2 {
			t.Fatalf("attempt %d: non-deterministic backoff %v vs %v", attempt, d1, d2)
		}
		if d1 <= 0 || d1 > p.MaxBackoff {
			t.Fatalf("attempt %d: backoff %v out of bounds (0, %v]", attempt, d1, p.MaxBackoff)
		}
	}
	_ = time.Millisecond
}
