package ddb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	ErrNotFound            = errors.New("ddb: item not found")
	ErrAlreadyExists       = errors.New("ddb: item already exists")
	ErrRefConflict         = errors.New("ddb: ref compare-and-swap conflict")
	ErrRefCASConflict      = ErrRefConflict
	ErrTransactionConflict = errors.New("ddb: transient transaction conflict")
	ErrTooManyRefs         = errors.New("ddb: ref count exceeds atomic transaction limit")
	ErrEpochConflict       = errors.New("ddb: failover epoch conflict")
	ErrLeaseHeld           = errors.New("ddb: lease is held by another owner")
	ErrLeaseLost           = errors.New("ddb: lease ownership lost or expired")
	ErrInvalidInput        = errors.New("ddb: invalid input")
	ErrInvalidOID          = errors.New("ddb: invalid git object id")
	ErrInvalidShardCount   = errors.New("ddb: shard count out of range")
)

// RefConflictError reports one or more references that failed CAS validation
// inside an atomic CommitPush transaction.
type RefConflictError struct {
	ConflictingRefs []string
	Cause           error
}

func (e *RefConflictError) Error() string {
	if len(e.ConflictingRefs) == 0 {
		return ErrRefConflict.Error()
	}
	return fmt.Sprintf("%s: refs=[%s]", ErrRefConflict.Error(), strings.Join(e.ConflictingRefs, ","))
}

// Unwrap allows errors.Is(err, ErrRefConflict) and errors.Is(err, ErrRefCASConflict).
func (e *RefConflictError) Unwrap() error {
	return ErrRefConflict
}

func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

func classifyTransactionError(err error, refIndex map[int]string) error {
	var tce *types.TransactionCanceledException
	if !errors.As(err, &tce) {
		return err
	}

	var conflictingRefs []string
	var hasConditionalCheckFailed bool
	var hasTransactionConflict bool

	for idx, reason := range tce.CancellationReasons {
		if reason.Code == nil {
			continue
		}
		switch *reason.Code {
		case "ConditionalCheckFailed":
			hasConditionalCheckFailed = true
			if refName, ok := refIndex[idx]; ok {
				conflictingRefs = append(conflictingRefs, refName)
			}
		case "TransactionConflict":
			hasTransactionConflict = true
		}
	}

	if hasConditionalCheckFailed {
		return &RefConflictError{
			ConflictingRefs: conflictingRefs,
			Cause:           err,
		}
	}
	if hasTransactionConflict {
		return fmt.Errorf("%w: %v", ErrTransactionConflict, err)
	}
	return err
}
