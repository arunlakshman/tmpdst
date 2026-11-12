package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// brokerFrom is the sender of the requests in these tests.
var brokerFrom = machine.NodeInstance{Node: "b1"}

func readyMachine(t *testing.T) *replica.Replica {
	t.Helper()
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  machine.Millisecond,
		TakeoverAfter: 10 * machine.Second,
	})
	bootEmpty(t, m, 0)
	if !m.Ready() {
		t.Fatal("machine is not Ready")
	}
	return m
}

func onlyReply(t *testing.T, acts []any) runtime.ControlPlaneReply {
	t.Helper()
	if len(acts) != 1 {
		t.Fatalf("want one reply, got %v", acts)
	}
	r, ok := acts[0].(runtime.ControlPlaneReply)
	if !ok {
		t.Fatalf("want a ControlPlaneReply, got %T", acts[0])
	}
	return r
}

// A LocalRead on a Ready node is answered in the same step, with no wait for
// a tail check, and the reply carries the request id.
func TestLocalReadAnswersAtOnce(t *testing.T) {
	m := readyMachine(t)
	r := onlyReply(t, step(m, runtime.LocalRead{From: brokerFrom, Req: 21, Op: contract.DescribeTopics{}}))
	if r.To != brokerFrom || r.Done.Req != 21 || r.Done.Err != runtime.OK {
		t.Fatalf("reply = %+v", r)
	}
	if _, ok := r.Done.Resp.(contract.DescribeTopicsResp); !ok {
		t.Fatalf("resp = %#v", r.Done.Resp)
	}
}

// A node that is not Ready refuses at once.
func TestLocalReadRefusedWhenNotReady(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "s1", CatchUpEvery: machine.Millisecond})
	step(m, runtime.NodeStarted{})
	r := onlyReply(t, step(m, runtime.LocalRead{From: brokerFrom, Req: 3, Op: contract.DescribeTopics{}}))
	if r.Done.Err != runtime.ErrUnavailable || r.Done.Req != 3 {
		t.Fatalf("reply = %+v, want ErrUnavailable", r)
	}
}

// A command is not a read: it is refused and nothing reaches the log.
func TestLocalReadRefusesACommand(t *testing.T) {
	m := readyMachine(t)
	r := onlyReply(t, step(m, runtime.LocalRead{From: brokerFrom, Req: 4, Op: contract.CreateTopic{Name: "x", Partitions: 1}}))
	if r.Done.Err != runtime.ErrInvalid || r.Done.Req != 4 {
		t.Fatalf("reply = %+v, want ErrInvalid", r)
	}
}

// An operation that the machine does not know is refused with ErrInvalid, and
// nothing reaches the log.
func TestUnknownOperationIsInvalid(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "w"})
	r := onlyReply(t, step(m, runtime.ControlPlaneDeliver{From: brokerFrom, Req: 9, Op: nil}))
	if r.To != brokerFrom || r.Done.Err != runtime.ErrInvalid || r.Done.Req != 9 {
		t.Fatalf("reply = %+v, want ErrInvalid to the sender", r)
	}
}
