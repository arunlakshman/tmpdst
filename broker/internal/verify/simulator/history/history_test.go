package history

import (
	"testing"

	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

func id(b byte) machine.ID { return machine.ID{b} }

func TestInvokeAndReturnStampSequenceAndTime(t *testing.T) {
	now := kernel.Time(10)
	var bad []string
	h := New[string, int](func() kernel.Time { return now }, func(m string) { bad = append(bad, m) })
	client := machine.NodeInstance{Node: "w"}
	h.Invoke(client, id(1), id(1), "put")
	now = 20
	h.Invoke(client, id(2), id(2), "get")
	now = 30
	if !h.Return(id(2), 7, ResultKnown) || !h.Return(id(1), 0, ResultUnknown) {
		t.Fatal("Return of a known op failed")
	}
	ops := h.Ops()
	if len(ops) != 2 || ops[0].Op != "put" || ops[1].Op != "get" {
		t.Fatalf("ops = %+v", ops)
	}
	if ops[0].InvSeq != 1 || ops[1].InvSeq != 2 || ops[1].RetSeq != 3 || ops[0].RetSeq != 4 {
		t.Errorf("sequence numbers: %+v", ops)
	}
	if ops[0].Invoke != 10 || ops[1].Invoke != 20 || ops[1].Return != 30 {
		t.Errorf("times: %+v", ops)
	}
	if ops[1].Result != 7 || ops[1].Outcome != ResultKnown || ops[0].Outcome != ResultUnknown {
		t.Errorf("results: %+v", ops)
	}
	if len(bad) != 0 {
		t.Errorf("unexpected protocol messages: %v", bad)
	}
}

func TestPendingUntilReturned(t *testing.T) {
	h := New[string, int](func() kernel.Time { return 0 }, func(string) {})
	h.Invoke(machine.NodeInstance{}, id(1), id(1), "put")
	if h.Ops()[0].Outcome != Pending {
		t.Fatal("a new operation is not Pending")
	}
}

func TestProtocolBreaksAreReported(t *testing.T) {
	var bad []string
	h := New[string, int](func() kernel.Time { return 0 }, func(m string) { bad = append(bad, m) })
	h.Invoke(machine.NodeInstance{}, id(1), id(1), "put")
	h.Invoke(machine.NodeInstance{}, id(1), id(1), "put again") // the same id
	if h.Return(id(9), 0, GivenUp) {
		t.Error("Return of an unknown id succeeded")
	}
	if !h.Return(id(1), 1, ResultKnown) {
		t.Fatal("Return failed")
	}
	if h.Return(id(1), 2, GivenUp) {
		t.Error("a second Return succeeded")
	}
	if len(bad) != 3 || len(h.Ops()) != 1 {
		t.Errorf("messages %v, ops %d", bad, len(h.Ops()))
	}
}
