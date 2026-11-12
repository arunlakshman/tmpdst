package replica_test

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

func commitCmd(key string, records int64) metadb.Entry {
	return metadb.Entry{
		Kind: metadb.KindCommit,
		Op: contract.CommitSegment{
			SegmentKey: key,
			Batches: []contract.PendingBatch{
				{TopicPartition: contract.NewTopicPartition("t", 0), Records: records, Loc: runtime.SegmentRef{Key: key, Off: 0, Len: records}},
			},
		},
	}
}

func onlyAppend(t *testing.T, acts []any) runtime.LogAppend {
	t.Helper()
	if len(acts) != 1 {
		t.Fatalf("actions = %v, want one LogAppend", acts)
	}
	ap, ok := acts[0].(runtime.LogAppend)
	if !ok {
		t.Fatalf("action = %T, want LogAppend", acts[0])
	}
	return ap
}

func TestGroupPackingCapsAtMaxCmdsPerSlot(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 3, Writer: "w"})
	// First Offer flushes alone (low load). Further Offers queue while in flight;
	// the next flush packs up to MaxCmdsPerSlot, matching logcp flusher take().
	first := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("g0", 1)}))
	for i := 2; i <= 6; i++ {
		if acts := m.Offer(replica.ClientOperation{
			Req: machine.RequestID(i),
			Cmd: commitCmd("g"+string(rune('0'+i-1)), 1),
		}); len(acts) != 0 {
			t.Fatalf("Offer while in-flight returned %v", acts)
		}
	}
	if m.QueueDepth() != 5 {
		t.Fatalf("queue depth = %d, want 5", m.QueueDepth())
	}
	acts := step(m, runtime.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	_ = m.TakeAnswers() // answer for g0
	packed := onlyAppend(t, acts)
	id, cmds, ok := replica.DecodeGroup(packed.Data)
	if !ok || len(cmds) != 3 {
		t.Fatalf("packed group id=%q cmds=%d ok=%v, want 3 cmds", id, len(cmds), ok)
	}
	if m.QueueDepth() != 2 {
		t.Fatalf("queue depth = %d, want 2 left after packing 3", m.QueueDepth())
	}
	step(m, runtime.LogAppendDone{Req: packed.Req, Slot: packed.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 3 {
		t.Fatalf("packed answers = %d, want 3", len(ans))
	}
	for i, a := range ans {
		if a.Req != machine.RequestID(i+2) || a.Done.Err != runtime.OK {
			t.Fatalf("answer[%d] = %+v", i, a)
		}
	}
	d, err := m.Metadata().Read(contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 0)})
	if end := runtime.ResponseAs[contract.FindSegmentsResp](d).End; err != nil || end != 4 { // g0 + 3 packed
		t.Fatalf("end = %d (err %v), want 4", end, err)
	}
}

func TestOfferKnownCommitSkipsAppend(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w"})
	first := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("known", 2)}))
	step(m, runtime.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	if ans := m.TakeAnswers(); len(ans) != 1 || ans[0].Done.Err != runtime.OK {
		t.Fatalf("first answers = %+v", ans)
	}
	acts := m.Offer(replica.ClientOperation{Req: 2, Cmd: commitCmd("known", 2)})
	if len(acts) != 0 {
		t.Fatalf("known Offer returned %v, want immediate answer", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 2 || ans[0].Done.Err != runtime.OK {
		t.Fatalf("known answers = %+v", ans)
	}
	response, ok := ans[0].Done.Resp.(contract.CommitSegmentResp)
	if !ok || len(response.Assigned) != 1 || response.Assigned[0].Count != 2 {
		t.Fatalf("known resp = %+v", ans[0].Done.Resp)
	}
}

func TestOfferRetargetsInFlightCommit(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w"})
	first := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("retry", 1)}))
	acts := m.Offer(replica.ClientOperation{Req: 2, Cmd: commitCmd("retry", 1)})
	if len(acts) != 0 {
		t.Fatalf("retarget Offer returned %v", acts)
	}
	if m.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d, want 0 (coalesced)", m.QueueDepth())
	}
	step(m, runtime.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 2 || ans[0].Done.Err != runtime.OK {
		t.Fatalf("answers = %+v, want Req 2", ans)
	}
}

// Keys leave the idempotency window only at slots that are a multiple of
// TrimStride, so every replica forgets the same keys at the same slot. A replica
// that forgets on its own clock lets a replay
// commit again as a new segment, with new offsets.
func TestWindowForgetsKeysOnlyOnLogStride(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w", WindowSlots: 4, TrimStride: 8})
	var last runtime.LogPosition
	req := machine.RequestID(0)
	commit := func(key string) {
		req++
		ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: req, Cmd: commitCmd(key, 1)}))
		step(m, runtime.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: runtime.OK, Outcome: runtime.Applied})
		m.TakeAnswers()
		last = ap.Slot
	}
	commit("first") // slot 0
	for last < 7 {
		commit(fmt.Sprintf("filler%d", last))
	}
	req++
	if acts := m.Offer(replica.ClientOperation{Req: req, Cmd: commitCmd("first", 1)}); len(acts) != 0 {
		t.Fatalf("at slot %d the key is forgotten, but the window keeps it until slot 8: %v", last, acts)
	}
	m.TakeAnswers()
	commit("filler8") // slot 8 is a stride slot, so the keys below slot 4 go
	req++
	onlyAppend(t, m.Offer(replica.ClientOperation{Req: req, Cmd: commitCmd("first", 1)}))
}
