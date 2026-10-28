package apiconv

import (
	"testing"

	"manifester/internal/core"
)

func TestCommitSegmentRoundTrip(t *testing.T) {
	op := core.CommitSegment{
		SegmentKey: "seg/abc",
		Batches: []core.PendingBatch{{
			TP: core.TopicPartitionKey("t", 1), Records: 3,
			Loc: core.SegmentRef{Key: "seg/abc", Off: 0, Len: 10},
		}},
	}
	got := CommitSegmentToCore(CommitSegmentFromCore(op))
	if got.SegmentKey != op.SegmentKey || len(got.Batches) != 1 {
		t.Fatalf("got %+v", got)
	}
	b := got.Batches[0]
	if b.TP != op.Batches[0].TP || b.Records != 3 || b.Loc != op.Batches[0].Loc {
		t.Fatalf("batch %+v", b)
	}
}

func TestLookupDescribeRoundTrip(t *testing.T) {
	lk := core.FindSegments{TP: core.TopicPartitionKey("t", 2), From: 5, MaxBytes: 100}
	if got := LookupToCore(LookupFromCore(lk)); got != lk {
		t.Fatalf("lookup %+v", got)
	}
	dt := core.DescribeTopics{Topics: []string{"a", "b"}}
	got := DescribeTopicsToCore(DescribeTopicsFromCore(dt))
	if len(got.Topics) != 2 || got.Topics[0] != "a" || got.Topics[1] != "b" {
		t.Fatalf("describe %+v", got)
	}
}

func TestRespRoundTrip(t *testing.T) {
	cr := core.CommitSegmentResp{Assigned: []core.BatchRange{{
		TP: core.TopicPartitionKey("t", 0), Base: 10, Count: 2,
		Loc: core.SegmentRef{Key: "k", Off: 1, Len: 2},
	}}}
	got := CommitSegmentRespToCore(CommitSegmentRespFromCore(cr))
	if len(got.Assigned) != 1 || got.Assigned[0] != cr.Assigned[0] {
		t.Fatalf("commit resp %+v", got)
	}
	lr := core.FindSegmentsResp{Ranges: cr.Assigned, End: 12}
	lgot := LookupRespToCore(LookupRespFromCore(lr))
	if lgot.End != 12 || len(lgot.Ranges) != 1 {
		t.Fatalf("lookup resp %+v", lgot)
	}
	dr := core.DescribeTopicsResp{Topics: []core.TopicInfo{{Name: "t", Partitions: 3}}}
	dgot := DescribeTopicsRespToCore(DescribeTopicsRespFromCore(dr))
	if len(dgot.Topics) != 1 || dgot.Topics[0] != dr.Topics[0] {
		t.Fatalf("describe resp %+v", dgot)
	}
}

func TestErrOutcomeMaps(t *testing.T) {
	for _, c := range []core.ErrCode{
		core.OK, core.ErrTimeout, core.ErrUnavailable, core.ErrNotFound,
		core.ErrConflict, core.ErrConnClosed, core.ErrTooLarge, core.ErrCorrupt,
		core.ErrInvalid, core.ErrIO,
	} {
		if ToCoreErr(FromCoreErr(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []core.Outcome{core.NotApplied, core.MaybeApplied, core.Applied} {
		if ToCoreOutcome(FromCoreOutcome(o)) != o {
			t.Fatalf("outcome %v", o)
		}
	}
}
