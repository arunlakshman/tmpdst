package replica_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// ckptKey is the object key of the checkpoint taken at slot, under the test prefix.
func ckptKey(slot runtime.LogPosition) string { return fmt.Sprintf("checkpoint/%020d", slot) }

// startRestore runs NodeStarted and returns the first action, which must be
// the list of checkpoint objects.
func startRestore(t *testing.T, m *replica.Replica) runtime.ObjectList {
	t.Helper()
	acts := stepAt(m, 0, runtime.NodeStarted{})
	list, ok := acts[0].(runtime.ObjectList)
	if len(acts) != 1 || !ok || list.Prefix != "checkpoint/" {
		t.Fatalf("Started = %v, want one ObjectList checkpoint/", acts)
	}
	return list
}

func TestCheckpointPublishAndTrim(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0,
		CatchUpEvery:       runtime.Millisecond,
		Heartbeat:          runtime.Second,
		TakeoverAfter:      runtime.Second,
		CheckpointEvery:    10 * runtime.Millisecond,
		CheckpointMinSlots: 1,
		KeepSlots:          2,
		CheckpointPrefix:   "checkpoint/",
	})
	// NodeStarted, empty list, catch-up, Ready, claim, fence entry, active (rank 0).
	list := startRestore(t, m)
	acts := stepAt(m, 0, runtime.ObjectListDone{Req: list.Req})
	timer := mustTimer(t, acts[0])
	acts = stepAt(m, 0, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(runtime.LogGetEnd)
	acts = stepAt(m, 0, runtime.LogGetEndDone{
		Req: tail.Req, Tail: 0, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if !m.Ready() {
		t.Fatal("not Ready after catch-up")
	}
	winClaim(t, m, acts, 1, 0) // the fence entry takes slot 0
	// Commit one slot so applied=2.
	ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("c", 1)}))
	step(m, runtime.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}
	checkpoint, ok := findTimer(acts, replica.TimerCheckpoint)
	if !ok {
		t.Fatalf("checkpoint timer missing in %v", acts)
	}
	acts = stepAt(m, runtime.NodeTime(20*runtime.Millisecond), runtime.TimerFired{
		ID: checkpoint.ID, Gen: checkpoint.Gen,
	})
	put, ok := acts[0].(runtime.ObjectPut)
	if !ok || put.Key != ckptKey(2) || len(put.Data) == 0 {
		t.Fatalf("checkpoint action = %#v, want ObjPut", acts[0])
	}
	slot, err := metadb.NewMem().Restore(bytes.NewReader(put.Data))
	if err != nil || slot != 2 {
		t.Fatalf("published snapshot slot=%d err=%v, want 2", slot, err)
	}
	acts = stepAt(m, 0, runtime.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: runtime.OK, Outcome: runtime.Applied,
	})
	// KeepSlots=2, checkpointSlot=2: below=0, no trim. No older checkpoint to delete.
	if len(acts) != 0 {
		t.Fatalf("unexpected actions after the first checkpoint: %v", acts)
	}

	// Apply more slots and checkpoint again so trim fires.
	ap2 := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 2, Cmd: commitCmd("d", 1)}))
	step(m, runtime.LogAppendDone{Req: ap2.Req, Slot: ap2.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	ap3 := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 3, Cmd: commitCmd("e", 1)}))
	step(m, runtime.LogAppendDone{Req: ap3.Req, Slot: ap3.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	if m.Applied() != 4 {
		t.Fatalf("Applied = %d, want 4", m.Applied())
	}
	// Re-arm checkpoint by firing a fresh gen (timer was re-armed in onCheckpointTimer).
	acts = stepAt(m, runtime.NodeTime(40*runtime.Millisecond), runtime.TimerFired{
		ID: replica.TimerCheckpoint, Gen: checkpoint.Gen + 1,
	})
	put = acts[0].(runtime.ObjectPut)
	if put.Key != ckptKey(4) {
		t.Fatalf("second checkpoint key = %s, want %s", put.Key, ckptKey(4))
	}
	acts = stepAt(m, 0, runtime.ObjectPutDone{
		Req: put.Req, Key: put.Key, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if del, ok := acts[len(acts)-1].(runtime.ObjectDelete); !ok || del.Key != ckptKey(2) {
		t.Fatalf("after put actions = %v, want a final ObjectDelete of the older checkpoint %s", acts, ckptKey(2))
	}
	trim, ok := acts[0].(runtime.LogDeleteOld)
	if !ok {
		t.Fatalf("after put actions = %v, want LogTrim", acts)
	}
	// checkpointSlot=4, KeepSlots=2: below=2, Through=1.
	if trim.Through != 1 {
		t.Fatalf("LogTrim Through = %d, want 1", trim.Through)
	}
	stepAt(m, 0, runtime.LogDeleteOldDone{Req: trim.Req, Err: runtime.OK, Outcome: runtime.Applied})
}

// A listed checkpoint can be deleted by the time it is read, because a newer
// one replaced it. The node lists again instead of starting from slot 0.
func TestVanishedCheckpointIsListedAgain(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "w", CatchUpEvery: runtime.Millisecond, CheckpointPrefix: "checkpoint/"})
	list := startRestore(t, m)
	get := step(m, runtime.ObjectListDone{Req: list.Req, Keys: []string{ckptKey(5)}})[0].(runtime.ObjectGet)
	acts := step(m, runtime.ObjectGetDone{Req: get.Req, Err: runtime.ErrNotFound})
	if _, ok := acts[0].(runtime.ObjectList); !ok || len(acts) != 1 || m.Ready() {
		t.Fatalf("want one new ObjectList and not Ready, got %v", acts)
	}
}
