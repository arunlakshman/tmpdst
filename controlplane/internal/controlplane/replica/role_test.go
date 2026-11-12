package replica_test

import (
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func findTimer(acts []any, id runtime.TimerID) (timerView, bool) {
	for _, a := range acts {
		if st, ok := asTimer(a); ok && st.ID == id {
			return st, true
		}
	}
	return timerView{}, false
}

func findAppend(acts []any) (runtime.LogAppend, bool) {
	for _, a := range acts {
		if ap, ok := a.(runtime.LogAppend); ok {
			return ap, true
		}
	}
	return runtime.LogAppend{}, false
}

func findClaim(acts []any) (runtime.LogClaimEpoch, bool) {
	for _, a := range acts {
		if c, ok := a.(runtime.LogClaimEpoch); ok {
			return c, true
		}
	}
	return runtime.LogClaimEpoch{}, false
}

// bootEmpty boots a machine on an empty log and returns the actions of the
// last step (the tail check that makes the node Ready).
func bootEmpty(t *testing.T, m *replica.Replica, at runtime.NodeTime) []any {
	t.Helper()
	acts := stepAt(m, at, runtime.NodeStarted{})
	timer, ok := asTimer(acts[0])
	if !ok {
		t.Fatalf("Started = %v, want SetTimer", acts)
	}
	acts = stepAt(m, at, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(runtime.LogGetEnd)
	acts = stepAt(m, at, runtime.LogGetEndDone{
		Req: tail.Req, Tail: 0, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if !m.Ready() {
		t.Fatal("not Ready after empty catch-up")
	}
	return acts
}

// winClaim answers the claim in acts as won with the given epoch, checks that
// the node writes a fence entry before it is the writer, and applies the fence.
func winClaim(t *testing.T, m *replica.Replica, acts []any, epoch uint64, at runtime.NodeTime) (runtime.LogAppend, []any) {
	t.Helper()
	claim, ok := findClaim(acts)
	if !ok {
		t.Fatalf("no LogClaimEpoch in %v", acts)
	}
	acts = stepAt(m, at, runtime.LogClaimEpochDone{
		Req: claim.Req, Epoch: epoch, Owner: claim.Owner, Won: true, Err: runtime.OK, Outcome: runtime.Applied,
	})
	fence, ok := findAppend(acts)
	if !ok {
		t.Fatalf("won claim emitted %v, want the fence LogAppend", acts)
	}
	if m.IsActive() {
		t.Fatal("writer before the fence entry is applied")
	}
	got, _, ok := replica.EntryWriter(fence.Data)
	if !ok || got != epoch {
		t.Fatalf("fence epoch = %d (ok=%v), want %d", got, ok, epoch)
	}
	last := stepAt(m, at, runtime.LogAppendDone{Req: fence.Req, Slot: fence.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	if !m.IsActive() || m.Epoch() != epoch {
		t.Fatalf("after the fence: active=%v epoch=%d, want active epoch %d", m.IsActive(), m.Epoch(), epoch)
	}
	return fence, last
}

func TestFencedByNewerEpochEntry(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: runtime.Millisecond, Heartbeat: runtime.Second, MaxCmdsPerSlot: 1,
	})
	acts := bootEmpty(t, m, 0)
	winClaim(t, m, acts, 1, 0)
	ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 7, Cmd: commitCmd("k", 1)}))

	// The commit's slot is taken by an entry written in epoch 2 by another node.
	foreign := replica.EncodeEnvelopedForTest(2, "other", metadb.Encode(metadb.Entry{
		ID: "other/e2/1", Kind: metadb.KindNoop,
	}))
	acts = step(m, runtime.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: foreign, Err: runtime.OK, Outcome: runtime.Applied,
	})
	if m.IsActive() || m.Epoch() != 0 {
		t.Fatalf("fenced writer still active: active=%v epoch=%d", m.IsActive(), m.Epoch())
	}
	if _, ok := findAppend(acts); ok {
		t.Fatalf("fenced writer retried the append: %v", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 7 || ans[0].Done.Err != runtime.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 7", ans)
	}
	// It stays a standby: new commits are refused, reads still work.
	if got := m.Offer(replica.ClientOperation{Req: 8, Cmd: commitCmd("k2", 1)}); len(got) != 0 {
		t.Fatalf("read-only node accepted a commit: %v", got)
	}
}
