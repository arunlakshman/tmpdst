package statemachine_test

import (
	"testing"

	"manifester/internal/controlplane/statemachine"
	"manifester/internal/core"
)

var brokerFrom = core.NodeInstance{Node: "b1"}

// writeOp sends one write op from a broker, lets its log slot apply, and
// returns the reply the broker gets.
func writeOp(t *testing.T, m *statemachine.Machine, req core.RequestID, op core.ControlPlaneOperation) core.ControlPlaneDone {
	t.Helper()
	ap := onlyAppend(t, m.Step(core.Event{Body: core.ControlPlaneDeliver{From: brokerFrom, Req: req, Op: op}}))
	acts := step(m, core.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: core.OK, Outcome: core.Applied})
	for _, a := range acts {
		if r, ok := a.(core.ControlPlaneReply); ok && r.To == brokerFrom {
			if r.Done.Req != req {
				t.Fatalf("reply req = %d, want %d", r.Done.Req, req)
			}
			return r.Done
		}
	}
	t.Fatalf("no reply to broker after apply: %v", acts)
	return core.ControlPlaneDone{}
}

// readOp sends one read op from a broker on a ready machine. A read waits for
// a tail check that starts after it, so this runs catch-up rounds until the
// reply comes out. It returns the reply and the catch-up timer to use next.
func readOp(t *testing.T, m *statemachine.Machine, timer core.SetTimer, req core.RequestID, op core.ControlPlaneOperation) (core.ControlPlaneDone, core.SetTimer) {
	t.Helper()
	if acts := m.Step(core.Event{Body: core.ControlPlaneDeliver{From: brokerFrom, Req: req, Op: op}}); len(acts) != 0 {
		t.Fatalf("read answered at once: %v", acts)
	}
	for range 3 {
		tail := m.Step(core.Event{Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})[0].(core.LogGetEnd)
		acts := m.Step(core.Event{Body: core.LogGetEndDone{Req: tail.Req, Tail: 0, Err: core.OK, Outcome: core.Applied}})
		next, hasNext := findTimer(acts, 1)
		if hasNext {
			timer = next
		}
		for _, a := range acts {
			if r, ok := a.(core.ControlPlaneReply); ok && r.To == brokerFrom && r.Done.Req == req {
				return r.Done, timer
			}
		}
		if !hasNext {
			t.Fatalf("no catch-up timer after tail check: %v", acts)
		}
	}
	t.Fatalf("read %d was never answered", req)
	return core.ControlPlaneDone{}, timer
}

// TestOnControlPlaneDeliverWritesAnswerBroker: each write op goes through the log and the
// broker gets its reply after the slot applies.
func TestOnControlPlaneDeliverWritesAnswerBroker(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 4, MaxCmdsPerSlot: 1, Writer: "w"})

	// CommitSegment: the reply carries one assignment per batch.
	done := writeOp(t, m, 1, core.CommitSegment{
		SegmentKey: "s1",
		Batches: []core.PendingBatch{
			{TopicPartition: core.NewTopicPartition("t", 0), Records: 2, Loc: core.SegmentRef{Key: "s1", Off: 0, Len: 2}},
		},
	})
	if done.Err != core.OK {
		t.Fatalf("commit err = %v", done.Err)
	}
	if resp, ok := done.Resp.(core.CommitSegmentResp); !ok || len(resp.Assigned) != 1 || resp.Assigned[0].Count != 2 {
		t.Fatalf("commit resp = %+v", done.Resp)
	}

	// CreateTopic without FailIfExists succeeds.
	if done := writeOp(t, m, 2, core.CreateTopic{Name: "orders", Partitions: 2}); done.Err != core.OK {
		t.Fatalf("create err = %v", done.Err)
	}
	// CreateTopic with FailIfExists on the same name is refused.
	if done := writeOp(t, m, 3, core.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true}); done.Err != core.ErrTopicExists {
		t.Fatalf("exclusive create err = %v, want ErrTopicExists", done.Err)
	}

	// Group ops: install a group with one member, assign at the same
	// generation as its leader, commit an offset, and remove the member.
	member := core.GroupMember{MemberID: "m1", ClientID: "c1", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000}
	if done := writeOp(t, m, 4, core.GroupInstall{
		Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m1",
		Members: []core.GroupMember{member},
	}); done.Err != core.OK {
		t.Fatalf("install err = %v", done.Err)
	}
	if done := writeOp(t, m, 5, core.GroupAssign{
		Group: "g", Generation: 1, Leader: "m1",
		Assignments: []core.GroupAssignment{{Member: "m1", Bytes: []byte("a")}},
	}); done.Err != core.OK {
		t.Fatalf("assign err = %v", done.Err)
	}
	if done := writeOp(t, m, 6, core.OffsetCommit{
		Group: "g", Member: "m1", Generation: 1,
		Offsets: []core.PartitionOffset{{TopicPartition: core.NewTopicPartition("orders", 0), Offset: 7}},
	}); done.Err != core.OK {
		t.Fatalf("offset commit err = %v", done.Err)
	}
	if done := writeOp(t, m, 7, core.GroupRemove{Group: "g", Members: []string{"m1"}, Reason: "leave"}); done.Err != core.OK {
		t.Fatalf("remove err = %v", done.Err)
	}
}

// TestOnControlPlaneDeliverReadsAnswerBroker: each read op gets its answer from the fold
// once the node is ready and a fresh tail has applied.
func TestOnControlPlaneDeliverReadsAnswerBroker(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: 10 * core.Second,
	})
	acts := bootEmpty(t, m, 0)
	timer, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}

	done, timer := readOp(t, m, timer, 10, core.FindSegments{TopicPartition: core.NewTopicPartition("t", 0), From: 0, MaxBytes: 100})
	if _, ok := done.Resp.(core.FindSegmentsResp); !ok {
		t.Fatalf("find resp = %#v", done.Resp)
	}
	done, timer = readOp(t, m, timer, 11, core.DescribeTopics{})
	if _, ok := done.Resp.(core.DescribeTopicsResp); !ok || done.Err != core.OK {
		t.Fatalf("describe topics = %+v", done)
	}
	done, timer = readOp(t, m, timer, 12, core.OffsetFetch{Group: "g"})
	if _, ok := done.Resp.(core.OffsetFetchResp); !ok {
		t.Fatalf("offset fetch resp = %#v", done.Resp)
	}
	done, timer = readOp(t, m, timer, 13, core.DescribeGroup{Group: "g"})
	if resp, ok := done.Resp.(core.DescribeGroupResp); !ok || resp.Exists {
		t.Fatalf("describe group = %#v, want a group that does not exist", done.Resp)
	}
	done, _ = readOp(t, m, timer, 14, core.ListGroups{})
	if _, ok := done.Resp.(core.ListGroupsResp); !ok || done.Err != core.OK {
		t.Fatalf("list groups = %+v", done)
	}
}

// TestOnControlPlaneDeliverUnknownOpInvalid: an op the machine does not know is refused
// with ErrInvalid and nothing reaches the log.
func TestOnControlPlaneDeliverUnknownOpInvalid(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 4, Writer: "w"})
	acts := m.Step(core.Event{Body: core.ControlPlaneDeliver{From: brokerFrom, Req: 9, Op: nil}})
	if len(acts) != 1 {
		t.Fatalf("actions = %v, want one reply", acts)
	}
	r, ok := acts[0].(core.ControlPlaneReply)
	if !ok || r.To != brokerFrom || r.Done.Err != core.ErrInvalid || r.Done.Req != 9 {
		t.Fatalf("reply = %v, want ErrInvalid to broker", acts[0])
	}
}
