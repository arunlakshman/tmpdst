package server

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/apiconvert"
	"manifester/internal/core"
)

func toRPCError(code core.ErrCode, outcome core.Outcome) error {
	st := codes.Internal
	switch code {
	case core.ErrUnavailable:
		st = codes.Unavailable
	case core.ErrInvalid:
		st = codes.InvalidArgument
	case core.ErrNotFound:
		st = codes.NotFound
	case core.ErrConflict:
		st = codes.AlreadyExists
	case core.ErrTimeout:
		st = codes.DeadlineExceeded
	case core.OK:
		return nil
	}
	detail := &cpv1.ErrorDetail{
		Code:    apiconvert.FromCoreErr(code),
		Outcome: apiconvert.FromCoreOutcome(outcome),
	}
	s, err := status.New(st, code.String()).WithDetails(detail)
	if err != nil {
		return status.Error(st, code.String())
	}
	return s.Err()
}
