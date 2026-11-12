package server_test

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	sharedv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds/controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
)

func TestCommitTooLargeForOneLogEntry(t *testing.T) {
	cli, _, stop := startNode(t, replica.Config{MaxEntryBytes: 2000})
	defer stop()
	request := &cpv1.CommitSegmentRequest{SegmentKey: "seg/big"}
	for i := range 300 {
		request.Batches = append(request.Batches, &sharedv1.PendingBatch{
			Tp: &sharedv1.TopicPartition{Topic: "orders", Partition: int32(i)}, Records: 1,
			Loc: &sharedv1.SegmentRef{Key: "seg/big", Off: int64(i), Len: 1},
		})
	}
	_, err := cli.CommitSegment(t.Context(), request)
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
	ok := &cpv1.CommitSegmentRequest{SegmentKey: "seg/small", Batches: []*sharedv1.PendingBatch{{
		Tp: &sharedv1.TopicPartition{Topic: "orders", Partition: 0}, Records: 1, Loc: &sharedv1.SegmentRef{Key: "seg/small", Len: 1}}}}
	if _, err := cli.CommitSegment(t.Context(), ok); err != nil {
		t.Fatal(fmt.Errorf("small commit after a refusal: %w", err))
	}
}
