package statemachine_test

import (
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/core"
)

// A node that fell behind a trim reads from its next slot. LogRead skips the
// deleted slots, so the first entry it returns is later than the slot asked
// for. The log has no empty slot below its tail, so the node must restore from
// the checkpoint instead of waiting or writing.
func TestMissingNextSlotRestoresFromCheckpoint(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		CatchUpEvery: core.Millisecond, Writer: "w",
		CheckpointPrefix: "checkpoint/t/", CheckpointEvery: core.Second, Rank: 1,
	})
	acts := step(m, core.NodeStarted{})
	list := acts[0].(core.ObjectList)
	acts = step(m, core.ObjectListDone{Req: list.Req})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("late")}},
	})
	if !m.Booting() {
		t.Fatal("want booting after a missing next slot")
	}
	if m.IsActive() {
		t.Fatal("a missing next slot must demote")
	}
	if len(acts) != 1 {
		t.Fatalf("acts = %v, want one ObjectList", acts)
	}
	if list, ok := acts[0].(core.ObjectList); !ok || list.Prefix != "checkpoint/t/" {
		t.Fatalf("act = %#v, want ObjectList checkpoint/t/", acts[0])
	}
}

// A put that the log refuses because the slot is at or below the floor is not a
// commit. The node must not apply or answer it: it refuses the clients and
// restores from the checkpoint.
func TestBelowFloorAppendRestoresFromCheckpoint(t *testing.T) {
	m := newMachine(t, statemachine.Config{MaxCmdsPerSlot: 1, Writer: "w", CheckpointPrefix: "checkpoint/t/"})
	ap := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("a", 1)}))
	acts := step(m, core.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: nil,
		Err: core.ErrConflict, Outcome: core.NotApplied,
	})
	if !m.Booting() {
		t.Fatal("want booting after an append was refused below the floor")
	}
	if m.Applied() != 0 {
		t.Fatalf("Applied = %d, the refused put must not be applied", m.Applied())
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 1 || ans[0].Done.Err != core.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 1", ans)
	}
	if list, ok := acts[len(acts)-1].(core.ObjectList); !ok || list.Prefix != "checkpoint/t/" {
		t.Fatalf("acts = %#v, want a final ObjectList checkpoint/t/", acts)
	}
}
