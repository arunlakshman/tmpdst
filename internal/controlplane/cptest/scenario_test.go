package cptest

import (
	"errors"
	"testing"

	"manifester/internal/controlplane/cpclient"
	"manifester/internal/controlplane/grpctest"
	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest/generator"
	"manifester/internal/verify/veritas/veritastest/observation"
	"manifester/internal/verify/veritas/veritastest/suites"
)

// compareSteps runs the operations one by one on the subject and on a fresh
// spec. It fails at the first difference.
func compareSteps(t *testing.T, subject suites.ControlPlaneSubject, ops []generator.ControlPlaneOp) {
	t.Helper()
	spec := suites.NewSpecControlPlaneSubject()
	for i, op := range ops {
		expected, err := spec.Do(op)
		if err != nil {
			t.Fatalf("The spec could not run operation %d (%s): %v", i, op.Describe(), err)
		}
		actual, err := subject.Do(op)
		if err != nil {
			t.Fatalf("The subject could not run operation %d (%s): %v", i, op.Describe(), err)
		}
		if op.CreateTopic != nil {
			expected, actual = observation.ControlPlaneResult{Code: expected.Code}, observation.ControlPlaneResult{Code: actual.Code}
		}
		if diff := observation.DiffControlPlane(expected, actual); diff != "" {
			t.Fatalf("For operation %d (%s): %s", i, op.Describe(), diff)
		}
	}
}

// call wraps a control-plane operation. The request number is not used by the
// stores, so it is always 1.
func call(op core.ControlPlaneOperation) generator.ControlPlaneOp {
	return generator.NewCall(core.ControlPlaneCall{Req: 1, Op: op})
}

// replayOps commits a three-batch, two-partition segment, replays it after the
// ends moved, sends a different request under its key, an invalid request, and
// a retry of the same batches under a fresh key (new offsets).
func replayOps() []generator.ControlPlaneOp {
	a, b := core.NewTopicPartition("w", 0), core.NewTopicPartition("w", 1)
	batches := func(key string) []core.PendingBatch {
		return []core.PendingBatch{
			{TP: a, Records: 2, Loc: core.SegmentRef{Key: key, Off: 0, Len: 20}},
			{TP: b, Records: 5, Loc: core.SegmentRef{Key: key, Off: 20, Len: 50}},
			{TP: a, Records: 3, Loc: core.SegmentRef{Key: key, Off: 70, Len: 30}},
		}
	}
	commit := func(key string, bs []core.PendingBatch) generator.ControlPlaneOp {
		return call(core.CommitSegment{SegmentKey: key, Batches: bs})
	}
	return []generator.ControlPlaneOp{
		commit("s1", batches("s1")),
		commit("s2", batches("s2")),
		commit("s1", batches("s1")),                // replay: the original ranges
		commit("s1", batches("s2")),                // other request under s1: conflict
		commit("s1", nil),                          // malformed under a committed key: conflict
		commit("s3", []core.PendingBatch{{TP: a}}), // invalid: no records
		commit("s3", batches("s3")),                // the failed key is reusable
		commit("re", batches("s3")),                // the same batches, a fresh key: new offsets
		call(core.FindSegments{TP: a}), call(core.FindSegments{TP: b, From: 6, MaxBytes: 25}),
		call(core.DescribeTopics{}),
	}
}

func TestReplayMemstore(t *testing.T) { compareSteps(t, implSubject{storeImpl{newMem()}}, replayOps()) }

// manyBatchesOps commits segments of many small batches (one DynamoDB range
// item per partition and segment), with a replay
// of a segment, a segment spanning two runs of one partition (two commits), and
// a Lookup from every offset with and without a byte limit.
func manyBatchesOps() []generator.ControlPlaneOp {
	var ops []generator.ControlPlaneOp
	a := core.NewTopicPartition("m", 0)
	b := core.NewTopicPartition("m", 1)
	var s1, s2 []core.PendingBatch
	for i := range 200 { // 200 batches for a, interleaved with b
		for _, tp := range []core.TopicPartition{a, b} {
			if tp == b && i%4 != 0 {
				continue
			}
			s1 = append(s1, core.PendingBatch{TP: tp, Records: int64(1 + i%3),
				Loc: core.SegmentRef{Key: "big1", Off: int64(i) * 70, Len: int64(10 + i%5)}})
		}
	}
	// Several batches of one partition around one of another.
	s2 = []core.PendingBatch{
		{TP: a, Records: 2, Loc: core.SegmentRef{Key: "big2", Off: 0, Len: 9}},
		{TP: a, Records: 2, Loc: core.SegmentRef{Key: "big2", Off: 9, Len: 9}},
		{TP: a, Records: 3, Loc: core.SegmentRef{Key: "big2", Off: 18, Len: 9}},
		{TP: b, Records: 1, Loc: core.SegmentRef{Key: "big2", Off: 27, Len: 9}},
		{TP: a, Records: 1, Loc: core.SegmentRef{Key: "big2", Off: 36, Len: 9}},
	}
	commit := func(key string, bs []core.PendingBatch) generator.ControlPlaneOp {
		return call(core.CommitSegment{SegmentKey: key, Batches: bs})
	}
	ops = append(ops, commit("big1", s1), commit("big1", s1), commit("big2", s2), commit("big2", s2),
		commit("big3", s1), // a second run on each partition, right after the first
		commit("big1", s2)) // same key, other batches: conflict
	for _, tp := range []core.TopicPartition{a, b, core.NewTopicPartition("m", 2)} {
		for from := int64(0); from <= 520; from += 7 {
			for _, mb := range []int32{0, 1, 25, 300} {
				ops = append(ops, call(core.FindSegments{TP: tp, From: core.Offset(from), MaxBytes: mb}))
			}
		}
	}
	return append(ops, call(core.DescribeTopics{}))
}

func TestManyBatchesMemstore(t *testing.T) {
	compareSteps(t, implSubject{storeImpl{newMem()}}, manyBatchesOps())
}

func TestRoundTrip(t *testing.T) {
	ctx := t.Context()
	n := grpctest.StartMem(t)
	c := n.Client
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	commit := core.CommitSegment{SegmentKey: "k", Batches: []core.PendingBatch{
		{TP: core.NewTopicPartition("t", 1), Records: 4, Loc: core.SegmentRef{Key: "k", Off: 8, Len: 40}},
	}}
	cr, err := c.Call(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.(core.CommitSegmentResp).Assigned; len(got) != 1 || got[0].Base != 0 || got[0].Count != 4 || got[0].Loc != commit.Batches[0].Loc {
		t.Fatalf("commit: %+v", got)
	}
	l, err := c.Call(ctx, core.FindSegments{TP: commit.Batches[0].TP})
	if err != nil || l.(core.FindSegmentsResp).End != 4 || len(l.(core.FindSegmentsResp).Ranges) != 1 {
		t.Fatalf("lookup: %+v %v", l, err)
	}
	d, err := c.Call(ctx, core.DescribeTopics{})
	if err != nil || d.(core.DescribeTopicsResp).Topics[0] != (core.TopicInfo{Name: "t", Partitions: 3}) {
		t.Fatalf("describe: %+v %v", d, err)
	}
	commit.Batches[0].Records = 9
	_, err = c.Call(ctx, commit)
	var be *cpclient.Error
	if !errors.As(err, &be) || be.Code != core.ErrConflict {
		t.Fatalf("want ErrConflict, got %v", err)
	}
	if code, out := (cpclient.Classifier{}).Classify(core.ControlPlaneCall{Op: commit}, err); code != core.ErrConflict || out != core.NotApplied {
		t.Fatalf("classify: %v %v", code, out)
	}
}
