package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// A put that the log refuses because the slot is at or below the floor is not a
// commit. The node must not apply or answer it: it refuses the clients and
// restores from the checkpoint.
func TestBelowFloorAppendRestoresFromCheckpoint(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w", CheckpointPrefix: "checkpoint/t/"})
	ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("a", 1)}))
	acts := step(m, runtime.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: nil,
		Err: runtime.ErrConflict, Outcome: runtime.NotApplied,
	})
	if !m.Booting() {
		t.Fatal("want booting after an append was refused below the floor")
	}
	if m.Applied() != 0 {
		t.Fatalf("Applied = %d, the refused put must not be applied", m.Applied())
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 1 || ans[0].Done.Err != runtime.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 1", ans)
	}
	if list, ok := acts[len(acts)-1].(runtime.ObjectList); !ok || list.Prefix != "checkpoint/t/" {
		t.Fatalf("acts = %#v, want a final ObjectList checkpoint/t/", acts)
	}
}
