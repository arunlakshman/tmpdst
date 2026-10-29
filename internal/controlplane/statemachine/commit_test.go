package statemachine_test

import (
	"strings"
	"testing"

	"manifester/internal/controlplane/statemachine"
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

func commitCmd(key string, records int64) stateupdater.Entry {
	return stateupdater.Entry{
		Kind: stateupdater.KindCommit,
		Commit: core.CommitSegment{
			SegmentKey: key,
			Batches: []core.PendingBatch{
				{TP: core.NewTopicPartition("t", 0), Records: records, Loc: core.SegmentRef{Key: key, Off: 0, Len: records}},
			},
		},
	}
}

func onlyAppend(t *testing.T, acts []core.Action) core.LogAppend {
	t.Helper()
	if len(acts) != 1 {
		t.Fatalf("actions = %v, want one LogAppend", acts)
	}
	ap, ok := acts[0].(core.LogAppend)
	if !ok {
		t.Fatalf("action = %T, want LogAppend", acts[0])
	}
	return ap
}

func TestKConcurrentLogAppend(t *testing.T) {
	const k = 3
	m := newMachine(t, statemachine.Config{K: k, MaxCmdsPerSlot: 1, Writer: "w"})
	var first []core.LogAppend
	for i := range k {
		key := "s" + string(rune('a'+i))
		acts := m.Offer(statemachine.ClientOperation{Req: core.RequestID(i + 1), Cmd: commitCmd(key, 1)})
		first = append(first, onlyAppend(t, acts))
	}
	if m.InFlightCommits() != k {
		t.Fatalf("in-flight = %d, want %d", m.InFlightCommits(), k)
	}
	slots := map[core.LogPosition]bool{}
	for _, ap := range first {
		if slots[ap.Slot] {
			t.Fatalf("duplicate slot %d among concurrent appends", ap.Slot)
		}
		slots[ap.Slot] = true
	}
	acts := m.Offer(statemachine.ClientOperation{Req: 99, Cmd: commitCmd("extra", 1)})
	if len(acts) != 0 {
		t.Fatalf("over-K Offer returned %v", acts)
	}
	if m.QueueDepth() != 1 {
		t.Fatalf("queue depth = %d, want 1", m.QueueDepth())
	}
	if ans := m.TakeAnswers(); len(ans) != 0 {
		t.Fatalf("answers before apply: %v", ans)
	}

	acts = step(m, core.LogAppendDone{
		Req: first[0].Req, Slot: first[0].Slot, Err: core.OK, Outcome: core.Applied,
	})
	if m.Applied() != 1 {
		t.Fatalf("Applied = %d, want 1", m.Applied())
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 1 || ans[0].Done.Err != core.OK {
		t.Fatalf("answers after first apply = %+v", ans)
	}
	if len(acts) != 1 {
		t.Fatalf("after first done actions = %v, want one LogAppend for queued op", acts)
	}
	if _, ok := acts[0].(core.LogAppend); !ok {
		t.Fatalf("action = %T, want LogAppend", acts[0])
	}
	if m.InFlightCommits() != k {
		t.Fatalf("in-flight after refill = %d, want %d", m.InFlightCommits(), k)
	}
}

func TestGroupPackingCapsAtMaxCmdsPerSlot(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, MaxCmdsPerSlot: 3, Writer: "w"})
	// First Offer flushes alone (low load). Further Offers queue while in flight;
	// the next flush packs up to MaxCmdsPerSlot, matching logcp flusher take().
	first := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("g0", 1)}))
	for i := 2; i <= 6; i++ {
		if acts := m.Offer(statemachine.ClientOperation{
			Req: core.RequestID(i),
			Cmd: commitCmd("g"+string(rune('0'+i-1)), 1),
		}); len(acts) != 0 {
			t.Fatalf("Offer while in-flight returned %v", acts)
		}
	}
	if m.QueueDepth() != 5 {
		t.Fatalf("queue depth = %d, want 5", m.QueueDepth())
	}
	acts := step(m, core.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: core.OK, Outcome: core.Applied})
	_ = m.TakeAnswers() // answer for g0
	packed := onlyAppend(t, acts)
	id, cmds, ok := statemachine.DecodeGroup(packed.Data)
	if !ok || len(cmds) != 3 {
		t.Fatalf("packed group id=%q cmds=%d ok=%v, want 3 cmds", id, len(cmds), ok)
	}
	if m.QueueDepth() != 2 {
		t.Fatalf("queue depth = %d, want 2 left after packing 3", m.QueueDepth())
	}
	step(m, core.LogAppendDone{Req: packed.Req, Slot: packed.Slot, Err: core.OK, Outcome: core.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 3 {
		t.Fatalf("packed answers = %d, want 3", len(ans))
	}
	for i, a := range ans {
		if a.Req != core.RequestID(i+2) || a.Done.Err != core.OK {
			t.Fatalf("answer[%d] = %+v", i, a)
		}
	}
	if m.Fold().End(core.NewTopicPartition("t", 0)) != 4 { // g0 + 3 packed
		t.Fatalf("end = %d, want 4", m.Fold().End(core.NewTopicPartition("t", 0)))
	}
}

func TestMaybeAppliedRetriesSameSlot(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 2, MaxCmdsPerSlot: 1, Writer: "w"})
	a := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("a", 1)}))
	b := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 2, Cmd: commitCmd("b", 1)}))
	// b lands first; a times out. Same-slot retry must not leave a hole that
	// wedges drain past b.
	step(m, core.LogAppendDone{Req: b.Req, Slot: b.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 0 {
		t.Fatalf("Applied = %d before hole filled, want 0", m.Applied())
	}
	acts := step(m, core.LogAppendDone{
		Req: a.Req, Slot: a.Slot, Err: core.ErrTimeout, Outcome: core.MaybeApplied,
	})
	retry := onlyAppend(t, acts)
	if retry.Slot != a.Slot {
		t.Fatalf("MaybeApplied retry slot %d, want same slot %d", retry.Slot, a.Slot)
	}
	step(m, core.LogAppendDone{Req: retry.Req, Slot: retry.Slot, Err: core.OK, Outcome: core.Applied})
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}
	ans := m.TakeAnswers()
	if len(ans) != 2 {
		t.Fatalf("answers = %d, want 2 (drain unwedged)", len(ans))
	}
}

func TestLostAppendRetriesSameEntryLaterSlot(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, MaxCmdsPerSlot: 1, Writer: "w"})
	ap := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 7, Cmd: commitCmd("lost-key", 2)}))
	other := statemachine.EncodeGroupForTest("other/1", []stateupdater.Entry{commitCmd("other-key", 1)})

	acts := step(m, core.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: other,
		Err: core.OK, Outcome: core.Applied,
	})
	retry := onlyAppend(t, acts)
	if retry.Slot <= ap.Slot {
		t.Fatalf("retry slot %d, want > %d", retry.Slot, ap.Slot)
	}
	if string(retry.Data) != string(ap.Data) {
		t.Fatal("retry changed entry bytes; want same entry id and cmds")
	}
	if ans := m.TakeAnswers(); len(ans) != 0 {
		t.Fatalf("answered on lost slot: %v", ans)
	}
	if m.Applied() != 1 {
		t.Fatalf("Applied = %d after observing foreign winner, want 1", m.Applied())
	}
	step(m, core.LogAppendDone{Req: retry.Req, Slot: retry.Slot, Err: core.OK, Outcome: core.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 7 || ans[0].Done.Err != core.OK {
		t.Fatalf("answers after retry apply = %+v", ans)
	}
	if m.Fold().End(core.NewTopicPartition("t", 0)) != 3 {
		t.Fatalf("end = %d, want 3", m.Fold().End(core.NewTopicPartition("t", 0)))
	}
}

func TestStickySingleAppenderAtMachineLevel(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 2, MaxCmdsPerSlot: 1, Writer: "sticky"})
	var apps []core.LogAppend
	for i := range 2 {
		apps = append(apps, onlyAppend(t, m.Offer(statemachine.ClientOperation{
			Req: core.RequestID(i + 1),
			Cmd: commitCmd("st"+string(rune('a'+i)), 1),
		})))
	}
	if apps[0].Slot == apps[1].Slot {
		t.Fatal("sticky appender allocated the same slot twice")
	}
	for _, ap := range apps {
		id, cmds, ok := statemachine.DecodeGroup(ap.Data)
		if !ok || len(cmds) != 1 {
			t.Fatalf("group decode failed for %#v", ap)
		}
		if !strings.HasPrefix(id, "sticky/") {
			t.Fatalf("entry id %q, want sticky/ prefix (single writer)", id)
		}
	}
	step(m, core.LogAppendDone{Req: apps[0].Req, Slot: apps[0].Slot, Err: core.OK, Outcome: core.Applied})
	step(m, core.LogAppendDone{Req: apps[1].Req, Slot: apps[1].Slot, Err: core.OK, Outcome: core.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 2 {
		t.Fatalf("answers = %d, want 2", len(ans))
	}
}

func TestOfferRefusedWhenCommitQueueDisabled(t *testing.T) {
	m := newMachine(t, statemachine.Config{})
	if acts := m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("x", 1)}); acts != nil {
		t.Fatalf("disabled Offer returned %v", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Done.Err != core.ErrUnavailable {
		t.Fatalf("answers = %+v, want ErrUnavailable", ans)
	}
}

func TestOfferKnownCommitSkipsAppend(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, MaxCmdsPerSlot: 1, Writer: "w"})
	first := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("known", 2)}))
	step(m, core.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: core.OK, Outcome: core.Applied})
	if ans := m.TakeAnswers(); len(ans) != 1 || ans[0].Done.Err != core.OK {
		t.Fatalf("first answers = %+v", ans)
	}
	acts := m.Offer(statemachine.ClientOperation{Req: 2, Cmd: commitCmd("known", 2)})
	if len(acts) != 0 {
		t.Fatalf("known Offer returned %v, want immediate answer", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 2 || ans[0].Done.Err != core.OK {
		t.Fatalf("known answers = %+v", ans)
	}
	resp, ok := ans[0].Done.Resp.(core.CommitSegmentResp)
	if !ok || len(resp.Assigned) != 1 || resp.Assigned[0].Count != 2 {
		t.Fatalf("known resp = %+v", ans[0].Done.Resp)
	}
}

func TestOfferRetargetsInFlightCommit(t *testing.T) {
	m := newMachine(t, statemachine.Config{K: 1, MaxCmdsPerSlot: 1, Writer: "w"})
	first := onlyAppend(t, m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("retry", 1)}))
	acts := m.Offer(statemachine.ClientOperation{Req: 2, Cmd: commitCmd("retry", 1)})
	if len(acts) != 0 {
		t.Fatalf("retarget Offer returned %v", acts)
	}
	if m.QueueDepth() != 0 {
		t.Fatalf("queue depth = %d, want 0 (coalesced)", m.QueueDepth())
	}
	step(m, core.LogAppendDone{Req: first.Req, Slot: first.Slot, Err: core.OK, Outcome: core.Applied})
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 2 || ans[0].Done.Err != core.OK {
		t.Fatalf("answers = %+v, want Req 2", ans)
	}
}
