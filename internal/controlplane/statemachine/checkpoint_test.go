package statemachine_test

import (
	"bytes"
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

func TestRestoreOnStartedThenReady(t *testing.T) {
	// Build a snapshot at slot 2 with one commit applied.
	src := newMachine(t, statemachine.Config{K: 1, Writer: "w"})
	ap := onlyAppend(t, src.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("seg", 3)}))
	step(src, core.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: core.OK, Outcome: core.Applied})
	if src.Applied() != 1 {
		t.Fatalf("Applied = %d, want 1", src.Applied())
	}
	// Force applied to 2 by applying a second noop via fold snapshot tag.
	var buf bytes.Buffer
	if err := src.Fold().Snapshot(&buf, 2); err != nil {
		t.Fatal(err)
	}
	snap := buf.Bytes()

	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "w",
		CatchUpEvery:  core.Millisecond,
		CheckpointKey: "ckpt/latest",
	})
	acts := m.Step(core.Event{At: 0, Body: core.NodeStarted{}})
	if len(acts) != 1 {
		t.Fatalf("Started = %v, want ObjGet", acts)
	}
	get, ok := acts[0].(core.ObjectGet)
	if !ok || get.Ref.Key != "ckpt/latest" {
		t.Fatalf("Started action = %#v, want ObjGet ckpt/latest", acts[0])
	}
	acts = m.Step(core.Event{At: 0, Body: core.ObjectGetDone{Req: get.Req, Data: snap, Err: core.OK}})
	if m.Applied() != 2 {
		t.Fatalf("Applied after restore = %d, want 2", m.Applied())
	}
	if m.Ready() {
		t.Fatal("Ready before catch-up")
	}
	timer := acts[0].(core.SetTimer)
	acts = m.Step(core.Event{At: 0, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	tail := acts[0].(core.LogGetEnd)
	m.Step(core.Event{At: 0, Body: core.LogGetEndDone{
		Req: tail.Req, Tail: 2, Err: core.OK, Outcome: core.Applied,
	}})
	if !m.Ready() {
		t.Fatal("Ready after restore catch-up")
	}
	if m.Fold().End(core.NewTopicPartition("t", 0)) != 3 {
		t.Fatalf("restored end = %d, want 3", m.Fold().End(core.NewTopicPartition("t", 0)))
	}
}

func TestCheckpointPublishAndTrim(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "w", Rank: 0,
		CatchUpEvery:       core.Millisecond,
		Heartbeat:          core.Second,
		TakeoverAfter:      core.Second,
		CheckpointEvery:    10 * core.Millisecond,
		CheckpointMinSlots: 1,
		KeepSlots:          1,
		CheckpointKey:      "ckpt/latest",
	})
	// NodeStarted, ObjectGet miss, catch-up, Ready, active (rank 0).
	acts := m.Step(core.Event{At: 0, Body: core.NodeStarted{}})
	get := acts[0].(core.ObjectGet)
	acts = m.Step(core.Event{At: 0, Body: core.ObjectGetDone{Req: get.Req, Err: core.ErrNotFound}})
	timer := acts[0].(core.SetTimer)
	acts = m.Step(core.Event{At: 0, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	tail := acts[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: 0, Body: core.LogGetEndDone{
		Req: tail.Req, Tail: 0, Err: core.OK, Outcome: core.Applied,
	}})
	if !m.Ready() || !m.IsActive() {
		t.Fatalf("ready=%v active=%v", m.Ready(), m.IsActive())
	}
	// Commit one slot so applied=1.
	ap := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("c", 1)}))
	step(m, core.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 1 {
		t.Fatalf("Applied = %d, want 1", m.Applied())
	}
	checkpoint, ok := findTimer(acts, statemachine.TimerCheckpoint)
	if !ok {
		t.Fatalf("checkpoint timer missing in %v", acts)
	}
	acts = m.Step(core.Event{At: core.NodeTime(20 * core.Millisecond), Body: core.TimerFired{
		ID: checkpoint.ID, Gen: checkpoint.Gen,
	}})
	put, ok := acts[0].(core.ObjectPut)
	if !ok || put.Key != "ckpt/latest" || len(put.Data) == 0 {
		t.Fatalf("checkpoint action = %#v, want ObjPut", acts[0])
	}
	slot, err := stateupdater.NewMem().Restore(bytes.NewReader(put.Data))
	if err != nil || slot != 1 {
		t.Fatalf("published snapshot slot=%d err=%v, want 1", slot, err)
	}
	acts = m.Step(core.Event{At: 0, Body: core.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: core.OK, Outcome: core.Applied,
	}})
	// KeepSlots=1, checkpointSlot=1: below=0, no trim.
	if len(acts) != 0 {
		if _, isTrim := acts[0].(core.LogDeleteOld); isTrim {
			t.Fatalf("unexpected trim with nothing below keep window: %v", acts)
		}
	}

	// Apply more slots and checkpoint again so trim fires.
	ap2 := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 2, Cmd: commitCmd("d", 1)}))
	step(m, core.LogAppendDone{Req: ap2.Req, Slot: ap2.Slot, Err: core.OK, Outcome: core.Applied})
	ap3 := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 3, Cmd: commitCmd("e", 1)}))
	step(m, core.LogAppendDone{Req: ap3.Req, Slot: ap3.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 3 {
		t.Fatalf("Applied = %d, want 3", m.Applied())
	}
	// Re-arm checkpoint by firing a fresh gen (timer was re-armed in onCheckpointTimer).
	acts = m.Step(core.Event{At: core.NodeTime(40 * core.Millisecond), Body: core.TimerFired{
		ID: statemachine.TimerCheckpoint, Gen: checkpoint.Gen + 1,
	}})
	put = acts[0].(core.ObjectPut)
	acts = m.Step(core.Event{At: 0, Body: core.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: core.OK, Outcome: core.Applied,
	}})
	trim, ok := acts[0].(core.LogDeleteOld)
	if !ok {
		t.Fatalf("after put actions = %v, want LogTrim", acts)
	}
	// checkpointSlot=3, KeepSlots=1: below=2, Through=1.
	if trim.Through != 1 {
		t.Fatalf("LogTrim Through = %d, want 1", trim.Through)
	}
	m.Step(core.Event{At: 0, Body: core.LogDeleteOldDone{Req: trim.Req, Err: core.OK, Outcome: core.Applied}})
}

func TestRestoreMissFallsThroughToCatchUp(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		CatchUpEvery:  core.Millisecond,
		CheckpointKey: "ckpt/latest",
	})
	acts := m.Step(core.Event{At: 0, Body: core.NodeStarted{}})
	get := acts[0].(core.ObjectGet)
	acts = m.Step(core.Event{At: 0, Body: core.ObjectGetDone{Req: get.Req, Err: core.ErrNotFound}})
	if _, ok := acts[0].(core.SetTimer); !ok {
		t.Fatalf("after miss = %v, want catch-up SetTimer", acts)
	}
	if m.Applied() != 0 {
		t.Fatalf("Applied = %d, want 0", m.Applied())
	}
}

// A checkpoint read with a changed byte is read again, never restored.
func TestRestoreReadsAgainOnBadCRC(t *testing.T) {
	src := newMachine(t, statemachine.Config{K: 1, Writer: "w"})
	var buf bytes.Buffer
	if err := src.Fold().Snapshot(&buf, 5); err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(buf.Bytes())
	bad[len(bad)/2] ^= 1

	m := newMachine(t, statemachine.Config{K: 1, Writer: "w", CatchUpEvery: core.Millisecond, CheckpointKey: "ckpt/latest"})
	get := m.Step(core.Event{Body: core.NodeStarted{}})[0].(core.ObjectGet)
	acts := m.Step(core.Event{Body: core.ObjectGetDone{Req: get.Req, Data: bad, Err: core.OK}})
	again, ok := acts[0].(core.ObjectGet)
	if len(acts) != 1 || !ok || m.Applied() != 0 {
		t.Fatalf("want one new ObjectGet and nothing restored, got %v, applied %d", acts, m.Applied())
	}
	m.Step(core.Event{Body: core.ObjectGetDone{Req: again.Req, Data: buf.Bytes(), Err: core.OK}})
	if m.Applied() != 5 {
		t.Fatalf("Applied after a good read = %d, want 5", m.Applied())
	}
}

// A failed checkpoint read must not start catch-up from slot 0: the log may be trimmed.
func TestFailedCheckpointReadIsRetried(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, Writer: "w", CatchUpEvery: core.Millisecond, CheckpointKey: "ckpt/latest"})
	get := m.Step(core.Event{Body: core.NodeStarted{}})[0].(core.ObjectGet)
	acts := m.Step(core.Event{Body: core.ObjectGetDone{Req: get.Req, Err: core.ErrUnavailable}})
	timer, ok := findTimer(acts, statemachine.TimerCheckpoint)
	if !ok || len(acts) != 1 {
		t.Fatalf("want one checkpoint retry timer, got %v", acts)
	}
	acts = m.Step(core.Event{At: 1, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	if _, ok := acts[0].(core.ObjectGet); !ok || m.Ready() {
		t.Fatalf("want a new ObjectGet and not Ready, got %v", acts)
	}
}
