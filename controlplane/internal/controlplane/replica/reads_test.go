package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// A read that arrives while a tail check is in flight waits for the NEXT check.
func TestReadWaitsForFreshTail(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  machine.Millisecond,
		TakeoverAfter: 10 * machine.Second,
	})
	acts := bootEmpty(t, m, 0)
	timer, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	tailA := stepAt(m, 1, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})[0].(runtime.LogGetEnd)
	got := stepAt(m, 2, runtime.ControlPlaneDeliver{
		From: machine.NodeInstance{Node: "b"}, Req: 7, Op: contract.DescribeTopics{},
	})
	if len(got) != 0 {
		t.Fatalf("read answered at once: %v", got)
	}
	acts = stepAt(m, 3, runtime.LogGetEndDone{Req: tailA.Req, Err: runtime.OK, Outcome: runtime.Applied})
	for _, a := range acts {
		if _, isReply := a.(runtime.ControlPlaneReply); isReply {
			t.Fatalf("read answered from a tail check that started before it: %v", acts)
		}
	}
	timer, ok = findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	tailB := stepAt(m, 4, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})[0].(runtime.LogGetEnd)
	acts = stepAt(m, 5, runtime.LogGetEndDone{Req: tailB.Req, Err: runtime.OK, Outcome: runtime.Applied})
	for _, a := range acts {
		if r, isReply := a.(runtime.ControlPlaneReply); isReply && r.Done.Req == 7 && r.Done.Err == runtime.OK {
			return
		}
	}
	t.Fatalf("no reply after a fresh tail: %v", acts)
}

// A node that is not Ready refuses a read at once, so the bridge can fail over.
func TestReadRefusedWhenNotReady(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "s1", CatchUpEvery: machine.Millisecond})
	step(m, runtime.NodeStarted{})
	acts := stepAt(m, 1, runtime.ControlPlaneDeliver{
		From: machine.NodeInstance{Node: "b"}, Req: 8, Op: contract.DescribeTopics{},
	})
	if len(acts) != 1 {
		t.Fatalf("want one reply, got %v", acts)
	}
	if r, ok := acts[0].(runtime.ControlPlaneReply); !ok || r.Done.Err != runtime.ErrUnavailable {
		t.Fatalf("want ErrUnavailable reply, got %v", acts[0])
	}
}
