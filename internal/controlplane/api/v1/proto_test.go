package controlplanev1

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestCommitSegmentProtoRoundTrip(t *testing.T) {
	in := &CommitSegmentRequest{
		SegmentKey: "seg/aabb",
		Batches: []*PendingBatch{{
			Tp:      &TopicPartition{Topic: "t", Partition: 1},
			Records: 2,
			Loc:     &SegmentRef{Key: "seg/aabb", Off: 0, Len: 8},
		}},
	}
	b, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := &CommitSegmentRequest{}
	if err := proto.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round-trip mismatch\nin=%v\nout=%v", in, out)
	}
	detail := &ErrorDetail{Code: ErrCode_ERR_CODE_TIMEOUT, Outcome: Outcome_OUTCOME_MAYBE_APPLIED}
	db, err := proto.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	got := &ErrorDetail{}
	if err := proto.Unmarshal(db, got); err != nil || !proto.Equal(detail, got) {
		t.Fatalf("detail %v %v", err, got)
	}
	if bytes.Equal(b, db) {
		t.Fatal("unrelated messages marshaled identically")
	}
}
