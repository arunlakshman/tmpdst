package statemachine_test

import (
	"errors"
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

// trackedStore is an in-memory store that remembers whether it was closed.
type trackedStore struct {
	*stateupdater.MemoryKeyValueStore
	closed bool
}

func (s *trackedStore) Close() error {
	s.closed = true
	return nil
}

// storeSource is a Config.OpenStore that counts its calls and can fail on one.
type storeSource struct {
	opened []*trackedStore
	failOn int // 1-based call number that fails; 0 never fails
}

func (s *storeSource) open() (stateupdater.KeyValueStore, error) {
	if s.failOn == len(s.opened)+1 {
		return nil, errors.New("disk is full")
	}
	st := &trackedStore{MemoryKeyValueStore: stateupdater.NewMemoryKeyValueStore()}
	s.opened = append(s.opened, st)
	return st, nil
}

// driveToTomb runs a lagging replica into a trimmed slot, which makes it drop
// its state and restore from a checkpoint. It returns the actions of that step.
func driveToTomb(t *testing.T, m *statemachine.Machine) []core.Action {
	t.Helper()
	acts := step(m, core.NodeStarted{})
	get := acts[0].(core.ObjectGet)
	acts = step(m, core.ObjectGetDone{Req: get.Req, Err: core.ErrNotFound})
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
	return step(m, core.LogAppendDone{
		Req: ap.Req, Slot: 0, Taken: true, Existing: nil,
		Err: core.ErrConflict, Outcome: core.NotApplied,
	})
}

func tombConfig(src *storeSource) statemachine.Config {
	return statemachine.Config{
		CatchUpEvery: core.Millisecond, GapAfter: 0, Writer: "w",
		CheckpointKey: "ckpt/t", CheckpointEvery: core.Second, Rank: 1,
		OpenStore: src.open,
	}
}

func TestOpenStoreRunsAtBuildAndAfterRestore(t *testing.T) {
	src := &storeSource{}
	m := newMachine(t, tombConfig(src))
	if len(src.opened) != 1 {
		t.Fatalf("stores opened after build = %d, want 1", len(src.opened))
	}
	acts := driveToTomb(t, m)
	if len(src.opened) != 2 {
		t.Fatalf("stores opened after a restore = %d, want 2", len(src.opened))
	}
	if !src.opened[0].closed {
		t.Error("the old store must be closed before the new one opens")
	}
	if src.opened[1].closed {
		t.Error("the new store must stay open")
	}
	if _, ok := acts[0].(core.ObjectGet); !ok {
		t.Fatalf("act = %#v, want the checkpoint read", acts[0])
	}
}

func TestOpenStoreFailureAfterRestoreStopsTheNode(t *testing.T) {
	src := &storeSource{failOn: 2}
	m := newMachine(t, tombConfig(src))
	acts := driveToTomb(t, m)
	stop, ok := acts[len(acts)-1].(core.StopForRestart)
	if !ok {
		t.Fatalf("acts = %#v, want StopForRestart last", acts)
	}
	if stop.Cause != "disk is full" {
		t.Errorf("cause = %q, want the open error", stop.Cause)
	}
}

func TestOpenStoreFailureAtBuildStopsOnStart(t *testing.T) {
	m := newMachine(t, tombConfig(&storeSource{failOn: 1}))
	acts := step(m, core.NodeStarted{})
	stop, ok := acts[0].(core.StopForRestart)
	if !ok || stop.Cause != "disk is full" {
		t.Fatalf("acts = %#v, want StopForRestart with the open error", acts)
	}
}
