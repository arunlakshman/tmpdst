package statemachine_test

import (
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/core"
)

// A read that arrives while a tail check is in flight waits for the NEXT check.
func TestReadWaitsForFreshTail(t *testing.T) {
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
	tailA := m.Step(core.Event{At: 1, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})[0].(core.LogGetEnd)
	got := m.Step(core.Event{At: 2, Body: core.ControlPlaneDeliver{
		From: core.NodeInstance{Node: "b"}, Req: 7, Op: core.DescribeTopics{},
	}})
	if len(got) != 0 {
		t.Fatalf("read answered at once: %v", got)
	}
	acts = m.Step(core.Event{At: 3, Body: core.LogGetEndDone{Req: tailA.Req, Err: core.OK, Outcome: core.Applied}})
	for _, a := range acts {
		if _, isReply := a.(core.ControlPlaneReply); isReply {
			t.Fatalf("read answered from a tail check that started before it: %v", acts)
		}
	}
	timer, ok = findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	tailB := m.Step(core.Event{At: 4, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: 5, Body: core.LogGetEndDone{Req: tailB.Req, Err: core.OK, Outcome: core.Applied}})
	for _, a := range acts {
		if r, isReply := a.(core.ControlPlaneReply); isReply && r.Done.Req == 7 && r.Done.Err == core.OK {
			return
		}
	}
	t.Fatalf("no reply after a fresh tail: %v", acts)
}

// A node that is not Ready refuses a read at once, so the bridge can fail over.
func TestReadRefusedWhenNotReady(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, Writer: "s1", CatchUpEvery: core.Millisecond})
	m.Step(core.Event{Body: core.NodeStarted{}})
	acts := m.Step(core.Event{At: 1, Body: core.ControlPlaneDeliver{
		From: core.NodeInstance{Node: "b"}, Req: 8, Op: core.DescribeTopics{},
	}})
	if len(acts) != 1 {
		t.Fatalf("want one reply, got %v", acts)
	}
	if r, ok := acts[0].(core.ControlPlaneReply); !ok || r.Done.Err != core.ErrUnavailable {
		t.Fatalf("want ErrUnavailable reply, got %v", acts[0])
	}
}
