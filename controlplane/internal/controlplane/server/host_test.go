package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// These tests drive the host of the rebalance machine by hand: no loop runs,
// so what the host delivers stays in the inbox of the machine for the test to read.

func popGroups(t *testing.T, n *Node) any {
	t.Helper()
	for end := time.Now().Add(5 * time.Second); time.Now().Before(end); time.Sleep(time.Millisecond) {
		if v, ok := n.groups.Pop(); ok {
			return v
		}
	}
	t.Fatal("nothing was delivered to the machine")
	return nil
}

// A call whose context ends is abandoned: the machine hears of it, the
// caller gets a timeout that may have been applied, and a reply that comes
// later is dropped.
func TestAbandonedCallDropsLateReply(t *testing.T) {
	n := New(Config{})
	ctx, cancel := context.WithCancel(t.Context())
	errs := make(chan error, 1)
	go func() {
		_, err := n.call(ctx, false, func(call rebalance.CallID) any { return rebalance.Heartbeat{Call: call, Group: "g"} })
		errs <- err
	}()
	in, ok := popGroups(t, n).(rebalance.Heartbeat)
	if !ok {
		t.Fatalf("first input %T, want the heartbeat", in)
	}
	cancel()
	if got, ok := popGroups(t, n).(rebalance.CallAbandoned); !ok || got.Call != in.Call {
		t.Fatalf("second input %+v, want CallAbandoned of call %d", got, in.Call)
	}
	err := <-errs
	if st, _ := status.FromError(err); st.Code() != codes.DeadlineExceeded {
		t.Fatalf("abandoned call: %v, want DeadlineExceeded (ErrTimeout)", err)
	}
	n.answer(rebalance.Reply{Call: in.Call}) // late: dropped, and it must not block
	n.answer(rebalance.Reply{Call: 99})      // never asked: dropped
	if len(n.calls) != 0 {
		t.Fatalf("calls still waiting: %v", n.calls)
	}
}

// A reply of the machine reaches its call.
func TestReplyReachesItsCall(t *testing.T) {
	n := New(Config{})
	type result struct {
		r   rebalance.Reply
		err error
	}
	done := make(chan result, 1)
	go func() {
		r, err := n.call(t.Context(), true, func(call rebalance.CallID) any { return rebalance.List{Call: call} })
		done <- result{r, err}
	}()
	in := popGroups(t, n).(rebalance.List)
	n.answer(rebalance.Reply{Call: in.Call, Body: rebalance.ListAnswer{}})
	if got := <-done; got.err != nil || got.r.Call != in.Call {
		t.Fatalf("reply %+v, %v; want the reply of call %d", got.r, got.err, in.Call)
	}
}

// The server and the machine count request ids apart, so one id can wait in
// both. A reply for the machine goes to the machine and leaves the server's
// waiter alone; the server's reply still reaches its waiter.
func TestReplyLooksAtToFirst(t *testing.T) {
	n := New(Config{})
	ch := make(chan runtime.ControlPlaneDone, 1)
	n.waiters[5] = ch
	n.reply(runtime.ControlPlaneReply{To: rebalanceFrom, Done: runtime.ControlPlaneDone{Req: 5, Err: runtime.ErrTooLarge}})
	if got, ok := popGroups(t, n).(rebalance.ReplicaReply); !ok || got.Req != 5 || got.Done.Err != runtime.ErrTooLarge {
		t.Fatalf("the machine got %+v, want the ReplicaReply of request 5", got)
	}
	if n.waiters[5] == nil {
		t.Fatal("a reply for the machine removed the server's waiter of the same id")
	}
	n.reply(runtime.ControlPlaneReply{To: serverFrom, Done: runtime.ControlPlaneDone{Req: 5}})
	select {
	case d := <-ch:
		if d.Err != runtime.OK {
			t.Fatalf("server reply %+v", d)
		}
	default:
		t.Fatal("the server's reply did not reach its waiter")
	}
}

// The role bit changes on an edge only, and each change is one RoleChanged.
func TestRoleChangedOnEdgesOnly(t *testing.T) {
	n := New(Config{})
	for _, active := range []bool{false, true, true, false, false, true} {
		n.setRole(active)
	}
	var got []bool
	for {
		v, ok := n.groups.Pop()
		if !ok {
			break
		}
		got = append(got, v.(rebalance.RoleChanged).Active)
	}
	if len(got) != 3 || !got[0] || got[1] || !got[2] {
		t.Fatalf("RoleChanged %v, want [true false true]", got)
	}
}
