package wire

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// OutcomeFromCore maps runtime.Outcome to the wire enum.
func OutcomeFromCore(o runtime.Outcome) cpv1.Outcome {
	switch o {
	case runtime.MaybeApplied:
		return cpv1.Outcome_OUTCOME_MAYBE_APPLIED
	case runtime.Applied:
		return cpv1.Outcome_OUTCOME_APPLIED
	default:
		return cpv1.Outcome_OUTCOME_NOT_APPLIED
	}
}

// ToRPCError is the gRPC error of a failed call: a status with the gRPC code of
// the error code, and an ErrorDetail with the wire error code and the outcome. An
// OK code gives nil.
func ToRPCError(code runtime.ErrCode, outcome runtime.Outcome) error {
	st := codes.Internal
	switch code {
	case runtime.ErrUnavailable:
		st = codes.Unavailable
	case runtime.ErrInvalid:
		st = codes.InvalidArgument
	case runtime.ErrNotFound:
		st = codes.NotFound
	case runtime.ErrConflict, runtime.ErrTopicExists:
		st = codes.AlreadyExists
	case runtime.ErrTimeout:
		st = codes.DeadlineExceeded
	case runtime.ErrTooLarge:
		st = codes.ResourceExhausted
	case runtime.ErrUnknownMember, runtime.ErrUnknownPartition:
		st = codes.NotFound
	case runtime.ErrIllegalGeneration, runtime.ErrRebalanceInProgress, runtime.ErrInconsistentGroupProtocol:
		st = codes.FailedPrecondition
	case runtime.OK:
		return nil
	}
	detail := &cpv1.ErrorDetail{
		Code:    ErrCodeFromCore(code),
		Outcome: OutcomeFromCore(outcome),
	}
	s, err := status.New(st, code.String()).WithDetails(detail)
	if err != nil {
		return status.Error(st, code.String())
	}
	return s.Err()
}
