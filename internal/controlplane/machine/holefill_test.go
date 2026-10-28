package machine_test

import (
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/controlplane/machine"
	"manifester/internal/core"
)

func driveToHole(t *testing.T, m *machine.Machine) core.LogAppend {
	t.Helper()
	acts := step(m, core.NodeStarted{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
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
	payload, ok := machine.UnwrapPayload(ap.Data)
	if !ok {
		t.Fatal("hole-fill envelope truncated")
	}
	got, err := applier.Decode(payload)
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

func TestHoleFillTombRestoresFromCheckpoint(t *testing.T) {
	// Lagging replica: LogRead skips a trimmed slot, hole-fill hits the tomb,
	// machine must restore instead of looping armCatchUp (seed-2 campaign hang).
	m := newMachine(t, machine.Config{
		CatchUpEvery: core.Millisecond, HoleAfter: 0, Writer: "w",
		CkptKey: "ckpt/t", CkptEvery: core.Second, Rank: 1,
	})
	acts := step(m, core.NodeStarted{})
	get := acts[0].(core.ObjectGet)
	acts = step(m, core.ObjectGetDone{Req: get.Req, Err: core.ErrNotFound})
	// after empty restore: catch-up timer then driveToHole shape
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("late")}},
	})
	ap := acts[0].(core.LogAppend)
	acts = step(m, core.LogAppendDone{
		Req: ap.Req, Slot: 0, Taken: true, Existing: nil,
		Err: core.ErrConflict, Outcome: core.NotApplied,
	})
	if !m.Booting() {
		t.Fatal("want booting after tomb hit")
	}
	if m.IsActive() {
		t.Fatal("tomb hit must demote")
	}
	if len(acts) != 1 {
		t.Fatalf("acts = %v, want ObjGet restore", acts)
	}
	if get, ok := acts[0].(core.ObjectGet); !ok || get.Ref.Key != "ckpt/t" {
		t.Fatalf("act = %#v, want ObjGet ckpt/t", acts[0])
	}
}

func TestHoleFillTimerWaits(t *testing.T) {
	m := newMachine(t, machine.Config{
		CatchUpEvery: core.Millisecond,
		HoleAfter:    5 * core.Millisecond,
	})
	acts := step(m, core.NodeStarted{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
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

func TestHoleFillStaysQuietWhileArmed(t *testing.T) {
	m := newMachine(t, machine.Config{
		CatchUpEvery: core.Millisecond,
		HoleAfter:    5 * core.Millisecond,
	})
	acts := step(m, core.NodeStarted{})
	catch := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: catch.ID, Gen: catch.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("y")}},
	})
	holeTimer, ok := acts[0].(core.SetTimer)
	if !ok || holeTimer.After != 5*core.Millisecond {
		t.Fatalf("action = %#v, want hole SetTimer", acts[0])
	}

	acts = step(m, core.TimerFired{ID: catch.ID, Gen: catch.Gen})
	tail = acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	if len(acts) != 0 {
		t.Fatalf("while hole timer armed, catch-up returned %v, want none", acts)
	}

	acts = step(m, core.TimerFired{ID: holeTimer.ID, Gen: holeTimer.Gen})
	if len(acts) != 1 {
		t.Fatalf("original hole timer actions = %v, want [LogAppend]", acts)
	}
	if _, ok := acts[0].(core.LogAppend); !ok {
		t.Fatalf("original hole timer action = %T, want LogAppend", acts[0])
	}
}

func TestHoleFillSkipMutantAdvancesWithoutAppend(t *testing.T) {
	const prop = core.PropertyID("logcp-sim: a replica's state equals the replay of the log at its slot")
	m, env := newMachineEnv(t, machine.Config{
		CatchUpEvery: core.Millisecond,
		HoleAfter:    0,
		Bug:          machine.BugHoleSkip,
		Writer:       "w",
	})
	acts := step(m, core.NodeStarted{})
	timer := acts[0].(core.SetTimer)
	acts = step(m, core.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(core.LogGetEnd)
	acts = step(m, core.LogGetEndDone{Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied})
	read := acts[0].(core.LogRead)
	acts = step(m, core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 1, Data: noop("late")}},
	})
	if _, ok := acts[0].(core.LogAppend); ok {
		t.Fatalf("skip mutant appended %#v", acts[0])
	}
	rd, ok := acts[0].(core.LogRead)
	if !ok || rd.From != 1 {
		t.Fatalf("after skip actions = %#v, want LogRead from 1", acts)
	}
	if m.Applied() != 1 {
		t.Fatalf("Applied = %d, want 1", m.Applied())
	}
	if len(env.always) != 1 || env.always[0].cond || env.always[0].prop != prop {
		t.Fatalf("Always = %+v, want one failing %q", env.always, prop)
	}
	d := env.always[0].details
	if len(d) != 1 || d[0].Key != "slot" || d[0].Value != core.LogSlot(1) {
		t.Fatalf("Always details = %+v, want slot 1 after the advance", d)
	}
}
