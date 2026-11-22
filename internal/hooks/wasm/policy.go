package wasm

import (
	"fmt"
	"log/slog"
)

// failPolicy implements GoF Strategy for hook failure handling (fail_mode: closed | open).
type failPolicy interface {
	handleFailure(hookID string, err error, logger *slog.Logger) HookOutcome
}

type closedFailPolicy struct{}

func (closedFailPolicy) handleFailure(hookID string, err error, _ *slog.Logger) HookOutcome {
	return HookOutcome{
		HookID:   hookID,
		Decision: VerdictReject,
		Message:  fmt.Sprintf("hook %q failed (fail_mode=closed): %v", hookID, err),
	}
}

type openFailPolicy struct{}

func (openFailPolicy) handleFailure(hookID string, err error, logger *slog.Logger) HookOutcome {
	msg := fmt.Sprintf("warning: hook %q failed (fail_mode=open): %v", hookID, err)
	if logger != nil {
		logger.Warn("tier1 wasm hook failed open", "hook_id", hookID, "error", err)
	}
	return HookOutcome{
		HookID:   hookID,
		Decision: VerdictAllow,
		Warning:  msg,
	}
}

func policyFor(mode FailMode) failPolicy {
	if mode == FailModeOpen {
		return openFailPolicy{}
	}
	return closedFailPolicy{}
}
