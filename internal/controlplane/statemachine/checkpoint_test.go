package statemachine_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

// ckptKey is the object key of the checkpoint taken at slot, under the test prefix.
func ckptKey(slot core.LogPosition) string { return fmt.Sprintf("checkpoint/%020d", slot) }

// startRestore runs NodeStarted and returns the first action, which must be
// the list of checkpoint objects.
func startRestore(t *testing.T, m *statemachine.Machine) core.ObjectList {
	t.Helper()
	acts := m.Step(core.Event{At: 0, Body: core.NodeStarted{}})
	list, ok := acts[0].(core.ObjectList)
	if len(acts) != 1 || !ok || list.Prefix != "checkpoint/" {
		t.Fatalf("Started = %v, want one ObjectList checkpoint/", acts)
	}
	return list
}

func TestRestoreOnStartedThenReady(t *testing.T) {
	// Build a snapshot at slot 2 with one commit applied.
	src := newMachine(t, statemachine.Config{Writer: "w"})
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
		Writer:           "w",
		CatchUpEvery:     core.Millisecond,
		CheckpointPrefix: "checkpoint/",
	})
	list := startRestore(t, m)
	acts := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req, Keys: []string{ckptKey(1), ckptKey(2)}}})
	get, ok := acts[0].(core.ObjectGet)
	if !ok || get.Ref.Key != ckptKey(2) {
		t.Fatalf("action = %#v, want ObjectGet of the newest checkpoint %s", acts[0], ckptKey(2))
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
		Writer: "w", Rank: 0,
		CatchUpEvery:       core.Millisecond,
		Heartbeat:          core.Second,
		TakeoverAfter:      core.Second,
		CheckpointEvery:    10 * core.Millisecond,
		CheckpointMinSlots: 1,
		KeepSlots:          2,
		CheckpointPrefix:   "checkpoint/",
	})
	// NodeStarted, empty list, catch-up, Ready, claim, fence entry, active (rank 0).
	list := startRestore(t, m)
	acts := m.Step(core.Event{At: 0, Body: core.ObjectListDone{Req: list.Req}})
	timer := acts[0].(core.SetTimer)
	acts = m.Step(core.Event{At: 0, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	tail := acts[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: 0, Body: core.LogGetEndDone{
		Req: tail.Req, Tail: 0, Err: core.OK, Outcome: core.Applied,
	}})
	if !m.Ready() {
		t.Fatal("not Ready after catch-up")
	}
	winClaim(t, m, acts, 1, 0) // the fence entry takes slot 0
	// Commit one slot so applied=2.
	ap := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("c", 1)}))
	step(m, core.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}
	checkpoint, ok := findTimer(acts, statemachine.TimerCheckpoint)
	if !ok {
		t.Fatalf("checkpoint timer missing in %v", acts)
	}
	acts = m.Step(core.Event{At: core.NodeTime(20 * core.Millisecond), Body: core.TimerFired{
		ID: checkpoint.ID, Gen: checkpoint.Gen,
	}})
	put, ok := acts[0].(core.ObjectPut)
	if !ok || put.Key != ckptKey(2) || len(put.Data) == 0 {
		t.Fatalf("checkpoint action = %#v, want ObjPut", acts[0])
	}
	slot, err := stateupdater.NewMem().Restore(bytes.NewReader(put.Data))
	if err != nil || slot != 2 {
		t.Fatalf("published snapshot slot=%d err=%v, want 2", slot, err)
	}
	acts = m.Step(core.Event{At: 0, Body: core.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: core.OK, Outcome: core.Applied,
	}})
	// KeepSlots=2, checkpointSlot=2: below=0, no trim. No older checkpoint to delete.
	if len(acts) != 0 {
		t.Fatalf("unexpected actions after the first checkpoint: %v", acts)
	}

	// Apply more slots and checkpoint again so trim fires.
	ap2 := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 2, Cmd: commitCmd("d", 1)}))
	step(m, core.LogAppendDone{Req: ap2.Req, Slot: ap2.Slot, Err: core.OK, Outcome: core.Applied})
	ap3 := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 3, Cmd: commitCmd("e", 1)}))
	step(m, core.LogAppendDone{Req: ap3.Req, Slot: ap3.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 4 {
		t.Fatalf("Applied = %d, want 4", m.Applied())
	}
	// Re-arm checkpoint by firing a fresh gen (timer was re-armed in onCheckpointTimer).
	acts = m.Step(core.Event{At: core.NodeTime(40 * core.Millisecond), Body: core.TimerFired{
		ID: statemachine.TimerCheckpoint, Gen: checkpoint.Gen + 1,
	}})
	put = acts[0].(core.ObjectPut)
	if put.Key != ckptKey(4) {
		t.Fatalf("second checkpoint key = %s, want %s", put.Key, ckptKey(4))
	}
	acts = m.Step(core.Event{At: 0, Body: core.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: core.OK, Outcome: core.Applied,
	}})
	if del, ok := acts[len(acts)-1].(core.ObjectDelete); !ok || del.Key != ckptKey(2) {
		t.Fatalf("after put actions = %v, want a final ObjectDelete of the older checkpoint %s", acts, ckptKey(2))
	}
	trim, ok := acts[0].(core.LogDeleteOld)
	if !ok {
		t.Fatalf("after put actions = %v, want LogTrim", acts)
	}
	// checkpointSlot=4, KeepSlots=2: below=2, Through=1.
	if trim.Through != 1 {
		t.Fatalf("LogTrim Through = %d, want 1", trim.Through)
	}
	m.Step(core.Event{At: 0, Body: core.LogDeleteOldDone{Req: trim.Req, Err: core.OK, Outcome: core.Applied}})
}

func TestRestoreMissFallsThroughToCatchUp(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		CatchUpEvery:     core.Millisecond,
		CheckpointPrefix: "checkpoint/",
	})
	list := startRestore(t, m)
	acts := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req}})
	if _, ok := acts[0].(core.SetTimer); !ok {
		t.Fatalf("after an empty list = %v, want catch-up SetTimer", acts)
	}
	if m.Applied() != 0 {
		t.Fatalf("Applied = %d, want 0", m.Applied())
	}
}

// A checkpoint read with a changed byte is read again (same key), never restored.
func TestRestoreReadsAgainOnBadCRC(t *testing.T) {
	src := newMachine(t, statemachine.Config{Writer: "w"})
	var buf bytes.Buffer
	if err := src.Fold().Snapshot(&buf, 5); err != nil {
		t.Fatal(err)
	}
	bad := bytes.Clone(buf.Bytes())
	bad[len(bad)/2] ^= 1

	m := newMachine(t, statemachine.Config{Writer: "w", CatchUpEvery: core.Millisecond, CheckpointPrefix: "checkpoint/"})
	list := startRestore(t, m)
	get := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req, Keys: []string{ckptKey(5)}}})[0].(core.ObjectGet)
	acts := m.Step(core.Event{Body: core.ObjectGetDone{Req: get.Req, Data: bad, Err: core.OK}})
	again, ok := acts[0].(core.ObjectGet)
	if len(acts) != 1 || !ok || again.Ref.Key != ckptKey(5) || m.Applied() != 0 {
		t.Fatalf("want one new ObjectGet of the same key and nothing restored, got %v, applied %d", acts, m.Applied())
	}
	m.Step(core.Event{Body: core.ObjectGetDone{Req: again.Req, Data: buf.Bytes(), Err: core.OK}})
	if m.Applied() != 5 {
		t.Fatalf("Applied after a good read = %d, want 5", m.Applied())
	}
}

// A failed checkpoint list must not start catch-up from slot 0: the log may be trimmed.
func TestFailedCheckpointListIsRetried(t *testing.T) {
	m := newMachine(t, statemachine.Config{Writer: "w", CatchUpEvery: core.Millisecond, CheckpointPrefix: "checkpoint/"})
	list := startRestore(t, m)
	acts := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req, Err: core.ErrUnavailable}})
	timer, ok := findTimer(acts, statemachine.TimerCheckpoint)
	if !ok || len(acts) != 1 {
		t.Fatalf("want one checkpoint retry timer, got %v", acts)
	}
	acts = m.Step(core.Event{At: 1, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	if _, ok := acts[0].(core.ObjectList); !ok || m.Ready() {
		t.Fatalf("want a new ObjectList and not Ready, got %v", acts)
	}
}

// A listed checkpoint can be deleted by the time it is read, because a newer
// one replaced it. The node lists again instead of starting from slot 0.
func TestVanishedCheckpointIsListedAgain(t *testing.T) {
	m := newMachine(t, statemachine.Config{Writer: "w", CatchUpEvery: core.Millisecond, CheckpointPrefix: "checkpoint/"})
	list := startRestore(t, m)
	get := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req, Keys: []string{ckptKey(5)}}})[0].(core.ObjectGet)
	acts := m.Step(core.Event{Body: core.ObjectGetDone{Req: get.Req, Err: core.ErrNotFound}})
	if _, ok := acts[0].(core.ObjectList); !ok || len(acts) != 1 || m.Ready() {
		t.Fatalf("want one new ObjectList and not Ready, got %v", acts)
	}
}
