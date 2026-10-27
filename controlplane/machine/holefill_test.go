package machine_test

import (
	"testing"

	"manifester/controlplane/applier"
	"manifester/controlplane/machine"
	"manifester/core"
)

func driveToHole(t *testing.T, m *machine.Machine) core.LogAppend {
	t.Helper()
	acts := step(m, core.Started{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogCheckTail)
	acts = step(m, core.LogCheckTailDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("late")}},
	})
	if len(acts) != 1 {
		t.Fatalf("hole actions = %v, want one action", acts)
	}
	ap, ok := acts[0].(core.LogAppend)
	if !ok || ap.Slot != 0 {
		t.Fatalf("hole action = %#v, want LogAppend slot 0", acts[0])
	}
	return ap
}

func TestHoleFillEmitsNoopAppend(t *testing.T) {
	m := newMachine(t, machine.Config{CatchUpEvery: core.Millisecond, HoleAfter: 0, Writer: "w"})
	ap := driveToHole(t, m)
	got, err := applier.Decode(ap.Data)
	if err != nil || got.Kind != applier.KindNoop {
		t.Fatalf("append payload = %+v %v, want KindNoop", got, err)
	}

	acts := step(m, core.LogAppendDone{
		Req: ap.Req, Slot: 0, Taken: false, Err: core.OK, Outcome: core.Applied,
	})
	if m.Applied() != 2 {
		t.Fatalf("after fill Applied = %d, want 2", m.Applied())
	}
	if len(acts) != 1 {
		t.Fatalf("after fill actions = %v, want [SetTimer]", acts)
	}
	if _, ok := acts[0].(core.SetTimer); !ok {
		t.Fatalf("after fill action = %T, want SetTimer", acts[0])
	}
}

func TestHoleFillUsesWinnerWhenTaken(t *testing.T) {
	m := newMachine(t, machine.Config{CatchUpEvery: core.Millisecond, HoleAfter: 0})
	ap := driveToHole(t, m)
	winner := applier.Encode(applier.Entry{
		ID: "other", Kind: applier.KindCommit,
		Commit: core.CommitSegment{
			SegmentKey: "win",
			Batches:    []core.PendingBatch{{TP: core.TP("t", 0), Records: 1, Loc: core.SegmentRef{Key: "o", Off: 0, Len: 1}}},
		},
	})
	step(m, core.LogAppendDone{
		Req: ap.Req, Slot: 0, Taken: true, Existing: winner, Err: core.OK, Outcome: core.Applied,
	})
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}
	if m.Fold().End(core.TP("t", 0)) != 1 {
		t.Fatalf("end = %d, want 1 from winning commit", m.Fold().End(core.TP("t", 0)))
	}
}

func TestHoleFillTimerWaits(t *testing.T) {
	m := newMachine(t, machine.Config{
		CatchUpEvery: core.Millisecond,
		HoleAfter:    5 * core.Millisecond,
	})
	acts := step(m, core.Started{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogCheckTail)
	acts = step(m, core.LogCheckTailDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("y")}},
	})
	if len(acts) != 1 {
		t.Fatalf("actions = %v, want [SetTimer hole]", acts)
	}
	holeTimer, ok := acts[0].(core.SetTimer)
	if !ok || holeTimer.After != 5*core.Millisecond {
		t.Fatalf("action = %#v, want hole SetTimer", acts[0])
	}
	acts = step(m, core.TimerFired{ID: holeTimer.ID, Gen: holeTimer.Gen})
	if len(acts) != 1 {
		t.Fatalf("hole timer actions = %v, want [LogAppend]", acts)
	}
	if _, ok := acts[0].(core.LogAppend); !ok {
		t.Fatalf("hole timer action = %T, want LogAppend", acts[0])
	}
}
