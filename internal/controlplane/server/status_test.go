package server

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/core"
)

// TestToRPCErrorTable: every core error code maps to one gRPC code and carries
// an ErrorDetail with the matching wire code and outcome.
func TestToRPCErrorTable(t *testing.T) {
	cases := []struct {
		code   core.ErrCode
		want   codes.Code
		detail cpv1.ErrCode
	}{
		{core.ErrTimeout, codes.DeadlineExceeded, cpv1.ErrCode_ERR_CODE_TIMEOUT},
		{core.ErrUnavailable, codes.Unavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
		{core.ErrNotFound, codes.NotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
		{core.ErrConflict, codes.AlreadyExists, cpv1.ErrCode_ERR_CODE_CONFLICT},
		{core.ErrConnClosed, codes.Internal, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
		{core.ErrTooLarge, codes.ResourceExhausted, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
		{core.ErrCorrupt, codes.Internal, cpv1.ErrCode_ERR_CODE_CORRUPT},
		{core.ErrInvalid, codes.InvalidArgument, cpv1.ErrCode_ERR_CODE_INVALID},
		{core.ErrIO, codes.Internal, cpv1.ErrCode_ERR_CODE_IO},
		{core.ErrUnknownMember, codes.NotFound, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
		{core.ErrIllegalGeneration, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
		{core.ErrUnknownPartition, codes.NotFound, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
		{core.ErrRebalanceInProgress, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
		{core.ErrInconsistentGroupProtocol, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
		{core.ErrTopicExists, codes.AlreadyExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
	}
	for _, c := range cases {
		err := toRPCError(c.code, core.MaybeApplied)
		st, ok := status.FromError(err)
		if !ok {
			t.Fatalf("%v: not a status error: %v", c.code, err)
		}
		if st.Code() != c.want {
			t.Fatalf("%v: code = %v, want %v", c.code, st.Code(), c.want)
		}
		if st.Message() != c.code.String() {
			t.Fatalf("%v: message = %q, want %q", c.code, st.Message(), c.code.String())
		}
		var detail *cpv1.ErrorDetail
		for _, d := range st.Details() {
			if e, ok := d.(*cpv1.ErrorDetail); ok {
				detail = e
			}
		}
		if detail == nil {
			t.Fatalf("%v: no ErrorDetail", c.code)
		}
		if detail.Code != c.detail {
			t.Fatalf("%v: detail code = %v, want %v", c.code, detail.Code, c.detail)
		}
		if detail.Outcome != cpv1.Outcome_OUTCOME_MAYBE_APPLIED {
			t.Fatalf("%v: detail outcome = %v, want MAYBE_APPLIED", c.code, detail.Outcome)
		}
	}
}

// TestToRPCErrorOKIsNil: an OK code is not an error.
func TestToRPCErrorOKIsNil(t *testing.T) {
	if err := toRPCError(core.OK, core.Applied); err != nil {
		t.Fatalf("OK: err = %v, want nil", err)
	}
}
