package server_test

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "manifester/internal/controlplane/api/v1"
	"manifester/internal/controlplane/statemachine"
)

func TestCommitTooLargeForOneLogEntry(t *testing.T) {
	cli, _, stop := startNode(t, statemachine.Config{MaxEntryBytes: 2000})
	defer stop()
	req := &cpv1.CommitSegmentRequest{SegmentKey: "seg/big"}
	for i := range 300 {
		req.Batches = append(req.Batches, &cpv1.PendingBatch{
			Tp: &cpv1.TopicPartition{Topic: "orders", Partition: int32(i)}, Records: 1,
			Loc: &cpv1.SegmentRef{Key: "seg/big", Off: int64(i), Len: 1},
		})
	}
	_, err := cli.CommitSegment(t.Context(), req)
	st, _ := status.FromError(err)
	if st.Code() != codes.ResourceExhausted {
		t.Fatalf("code = %v (%v), want ResourceExhausted", st.Code(), err)
	}
	var detail *cpv1.ErrorDetail
	for _, d := range st.Details() {
		detail, _ = d.(*cpv1.ErrorDetail)
	}
	if detail == nil || detail.Code != cpv1.ErrCode_ERR_CODE_TOO_LARGE || detail.Outcome != cpv1.Outcome_OUTCOME_NOT_APPLIED {
		t.Fatalf("detail = %v, want TOO_LARGE with NOT_APPLIED", detail)
	}
	// A small commit still works afterwards.
	ok := &cpv1.CommitSegmentRequest{SegmentKey: "seg/small", Batches: []*cpv1.PendingBatch{{
		Tp: &cpv1.TopicPartition{Topic: "orders", Partition: 0}, Records: 1, Loc: &cpv1.SegmentRef{Key: "seg/small", Len: 1}}}}
	if _, err := cli.CommitSegment(t.Context(), ok); err != nil {
		t.Fatal(fmt.Errorf("small commit after a refusal: %w", err))
	}
}
