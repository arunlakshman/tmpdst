package wire

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// TestToRPCErrorTable: every runtime error code maps to one gRPC code and carries
// an ErrorDetail with the matching wire code and outcome.
func TestToRPCErrorTable(t *testing.T) {
	cases := []struct {
		code   runtime.ErrCode
		want   codes.Code
		detail cpv1.ErrCode
	}{
		{runtime.ErrTimeout, codes.DeadlineExceeded, cpv1.ErrCode_ERR_CODE_TIMEOUT},
		{runtime.ErrUnavailable, codes.Unavailable, cpv1.ErrCode_ERR_CODE_UNAVAILABLE},
		{runtime.ErrNotFound, codes.NotFound, cpv1.ErrCode_ERR_CODE_NOT_FOUND},
		{runtime.ErrConflict, codes.AlreadyExists, cpv1.ErrCode_ERR_CODE_CONFLICT},
		{runtime.ErrConnClosed, codes.Internal, cpv1.ErrCode_ERR_CODE_CONN_CLOSED},
		{runtime.ErrTooLarge, codes.ResourceExhausted, cpv1.ErrCode_ERR_CODE_TOO_LARGE},
		{runtime.ErrCorrupt, codes.Internal, cpv1.ErrCode_ERR_CODE_CORRUPT},
		{runtime.ErrInvalid, codes.InvalidArgument, cpv1.ErrCode_ERR_CODE_INVALID},
		{runtime.ErrIO, codes.Internal, cpv1.ErrCode_ERR_CODE_IO},
		{runtime.ErrUnknownMember, codes.NotFound, cpv1.ErrCode_ERR_CODE_UNKNOWN_MEMBER},
		{runtime.ErrIllegalGeneration, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_ILLEGAL_GENERATION},
		{runtime.ErrUnknownPartition, codes.NotFound, cpv1.ErrCode_ERR_CODE_UNKNOWN_PARTITION},
		{runtime.ErrRebalanceInProgress, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_REBALANCE_IN_PROGRESS},
		{runtime.ErrInconsistentGroupProtocol, codes.FailedPrecondition, cpv1.ErrCode_ERR_CODE_INCONSISTENT_GROUP_PROTOCOL},
		{runtime.ErrTopicExists, codes.AlreadyExists, cpv1.ErrCode_ERR_CODE_TOPIC_EXISTS},
	}
	for _, c := range cases {
		err := ToRPCError(c.code, runtime.MaybeApplied)
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
	if err := ToRPCError(runtime.OK, runtime.Applied); err != nil {
		t.Fatalf("OK: err = %v, want nil", err)
	}
}
