package speccheck

import (
	"testing"

	"mds/internal/core"
)

// otherOp is an operation that Exec does not know. The embedded interface
// gives it the marker method, and Exec never calls any method on it.
type otherOp struct{ core.ControlPlaneOperation }

// TestExecDispatch checks that Exec routes every operation kind to its rule.
func TestExecDispatch(t *testing.T) {
	c := newGroupControl(t)
	cases := []struct {
		name      string
		operation core.ControlPlaneOperation
		want      core.ErrCode
		outcome   core.Outcome
	}{
		{"commit", core.CommitSegment{SegmentKey: "s", Batches: []core.PendingBatch{pb("orders", 0, 2)}}, core.OK, core.Applied},
		{"lookup", core.FindSegments{TopicPartition: tp("orders", 0), From: -1}, core.ErrInvalid, core.NotApplied},
		{"describe topics", core.DescribeTopics{}, core.OK, core.NotApplied},
		{"create topic", core.CreateTopic{Name: "n", Partitions: 1}, core.OK, core.Applied},
		{"group install", gInstall("g", 1, "m1", "m1"), core.OK, core.Applied},
		{"group assign with no group", gAssign("nope", 0, "m1", "m1", "a"), core.ErrUnknownMember, core.NotApplied},
		{"group remove", gRemove("g", "leave", "m1"), core.OK, core.Applied},
		{"offset commit with no group", gCommit("nope", "m1", 1, gPair("orders", 0, 1)), core.ErrUnknownMember, core.NotApplied},
		{"offset fetch", core.OffsetFetch{Group: "g"}, core.OK, core.NotApplied},
		{"describe group", core.DescribeGroup{Group: "g"}, core.OK, core.NotApplied},
		{"list groups", core.ListGroups{}, core.OK, core.NotApplied},
		{"unknown operation", otherOp{}, core.ErrInvalid, core.NotApplied},
	}
	for _, tc := range cases {
		d := call(c, tc.operation)
		if d.Err != tc.want || d.Outcome != tc.outcome {
			t.Fatalf("%s: err %v outcome %v, want %v %v", tc.name, d.Err, d.Outcome, tc.want, tc.outcome)
		}
	}
	if d := call(c, core.DescribeTopics{}); d.Resp.(core.DescribeTopicsResp).Topics == nil {
		t.Fatal("describe topics lost its answer")
	}
}

// TestExecCreateTopicFailIfExists: with FailIfExists an existing topic is
// ErrTopicExists and nothing changes. Without the flag a create of an existing
// topic is OK and only grows the topic.
func TestExecCreateTopicFailIfExists(t *testing.T) {
	c := NewControlPlane()
	if d := call(c, core.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}); d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatalf("new topic: %+v", d)
	}
	h := c.StateHash()
	if d := call(c, core.CreateTopic{Name: "t", Partitions: 4, FailIfExists: true}); d.Err != core.ErrTopicExists || d.Outcome != core.NotApplied {
		t.Fatalf("exists: %+v", d)
	}
	if c.StateHash() != h {
		t.Fatal("a refused exclusive create changed the state")
	}
	if d := call(c, core.CreateTopic{Name: "t", Partitions: 4}); d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatalf("plain create of an existing topic: %+v", d)
	}
	if d := call(c, core.CreateTopic{Name: "t", Partitions: 3}); d.Err != core.OK {
		t.Fatalf("a smaller plain create: %+v", d)
	}
	if d := call(c, core.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}); d.Err != core.ErrInvalid || d.Outcome != core.NotApplied {
		t.Fatalf("empty name with the flag: %+v", d)
	}
	if d := call(c, core.CreateTopic{Name: "t", Partitions: 0}); d.Err != core.ErrInvalid {
		t.Fatalf("zero partitions: %+v", d)
	}
	got := call(c, core.DescribeTopics{Topics: []string{"t"}}).Resp.(core.DescribeTopicsResp).Topics
	if len(got) != 1 || got[0] != (core.TopicInfo{Name: "t", Partitions: 4}) {
		t.Fatalf("topics %+v, want t with 4 partitions", got)
	}
}

// notAControlPlaneCall is an IOCall that is not a ControlPlaneCall.
type notAControlPlaneCall struct{ core.IOCall }

func TestExecPanicsOnOtherCalls(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Exec did not panic on a non-control-plane call")
		}
	}()
	NewControlPlane().Exec(notAControlPlaneCall{})
}
