package replica_test

import (
	"strings"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/metadata"
	"github.com/manifesterio/mds/internal/controlplane/replica"
	"github.com/manifesterio/mds/internal/core"
)

func findTimer(acts []core.Action, id core.TimerID) (core.SetTimer, bool) {
	for _, a := range acts {
		if st, ok := a.(core.SetTimer); ok && st.ID == id {
			return st, true
		}
	}
	return core.SetTimer{}, false
}

func findAppend(acts []core.Action) (core.LogAppend, bool) {
	for _, a := range acts {
		if ap, ok := a.(core.LogAppend); ok {
			return ap, true
		}
	}
	return core.LogAppend{}, false
}

func findClaim(acts []core.Action) (core.LogClaimEpoch, bool) {
	for _, a := range acts {
		if c, ok := a.(core.LogClaimEpoch); ok {
			return c, true
		}
	}
	return core.LogClaimEpoch{}, false
}

// bootEmpty boots a machine on an empty log and returns the actions of the
// last step (the tail check that makes the node Ready).
func bootEmpty(t *testing.T, m *replica.Replica, at core.NodeTime) []core.Action {
	t.Helper()
	acts := m.Step(core.Event{At: at, Body: core.NodeStarted{}})
	timer, ok := acts[0].(core.SetTimer)
	if !ok {
		t.Fatalf("Started = %v, want SetTimer", acts)
	}
	acts = m.Step(core.Event{At: at, Body: core.TimerFired{ID: timer.ID, Gen: timer.Gen}})
	tail := acts[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: at, Body: core.LogGetEndDone{
		Req: tail.Req, Tail: 0, Err: core.OK, Outcome: core.Applied,
	}})
	if !m.Ready() {
		t.Fatal("not Ready after empty catch-up")
	}
	return acts
}

// winClaim answers the claim in acts as won with the given epoch, checks that
// the node writes a fence entry before it is the writer, and applies the fence.
func winClaim(t *testing.T, m *replica.Replica, acts []core.Action, epoch uint64, at core.NodeTime) (core.LogAppend, []core.Action) {
	t.Helper()
	claim, ok := findClaim(acts)
	if !ok {
		t.Fatalf("no LogClaimEpoch in %v", acts)
	}
	acts = m.Step(core.Event{At: at, Body: core.LogClaimEpochDone{
		Req: claim.Req, Epoch: epoch, Owner: claim.Owner, Won: true, Err: core.OK, Outcome: core.Applied,
	}})
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
	last := m.Step(core.Event{At: at, Body: core.LogAppendDone{Req: fence.Req, Slot: fence.Slot, Err: core.OK, Outcome: core.Applied}})
	if !m.IsActive() || m.Epoch() != epoch {
		t.Fatalf("after the fence: active=%v epoch=%d, want active epoch %d", m.IsActive(), m.Epoch(), epoch)
	}
	return fence, last
}

// tickAfterSilence refreshes the last good pull at time at, then fires the role
// timer from the boot actions. It returns the actions of the role tick.
func tickAfterSilence(t *testing.T, m *replica.Replica, boot []core.Action, at core.NodeTime) []core.Action {
	t.Helper()
	role, ok := findTimer(boot, replica.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", boot)
	}
	catch, ok := findTimer(boot, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", boot)
	}
	acts := m.Step(core.Event{At: at, Body: core.TimerFired{ID: catch.ID, Gen: catch.Gen}})
	check := acts[0].(core.LogGetEnd)
	m.Step(core.Event{At: at, Body: core.LogGetEndDone{Req: check.Req, Tail: 0, Err: core.OK, Outcome: core.Applied}})
	return m.Step(core.Event{At: at, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
}

func TestStandbyRefuseOfferNoAppend(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: 10 * core.Second,
	})
	bootEmpty(t, m, 0)
	if m.IsActive() {
		t.Fatal("rank 1 became active at boot")
	}
	acts := m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("k", 1)})
	if len(acts) != 0 {
		t.Fatalf("standby Offer returned %v, want no LogAppend", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Done.Err != core.ErrUnavailable {
		t.Fatalf("answers = %+v, want ErrUnavailable", ans)
	}
}

func TestRank0ClaimsAtBootOnEmptyLog(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: core.Millisecond, Heartbeat: core.Second,
	})
	acts := bootEmpty(t, m, 0)
	claim, ok := findClaim(acts)
	if !ok || claim.Expect != 0 || claim.Owner != "w" {
		t.Fatalf("boot actions = %v, want LogClaimEpoch{Expect 0, Owner w}", acts)
	}
	// No commit is accepted until the fence entry is applied.
	if got := m.Offer(replica.ClientOperation{Req: 1, Cmd: commitCmd("k", 1)}); len(got) != 0 {
		t.Fatalf("Offer before the fence returned %v", got)
	}
	m.TakeAnswers()
	fence, _ := winClaim(t, m, acts, 1, 0)
	payload, ok := replica.UnwrapPayload(fence.Data)
	if !ok {
		t.Fatal("fence envelope truncated")
	}
	if e, err := metadata.Decode(payload); err != nil || e.Kind != metadata.KindNoop {
		t.Fatalf("fence payload = %+v %v, want KindNoop", e, err)
	}
	if !strings.HasPrefix(entryID(t, payload), "w/e1/") {
		t.Fatalf("fence id = %q, want the epoch in the id (w/e1/...)", entryID(t, payload))
	}
	// Now a commit is accepted.
	if got := m.Offer(replica.ClientOperation{Req: 2, Cmd: commitCmd("k", 1)}); len(got) != 1 {
		t.Fatalf("Offer after the fence returned %v, want one LogAppend", got)
	}
}

func entryID(t *testing.T, payload []byte) string {
	t.Helper()
	e, err := metadata.Decode(payload)
	if err != nil {
		t.Fatal(err)
	}
	return e.ID
}

func TestTakeoverAfterSilence(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: take,
		Heartbeat:     take,
	})
	boot := bootEmpty(t, m, 0)
	if m.IsActive() {
		t.Fatal("active before silence")
	}
	if _, ok := findClaim(boot); ok {
		t.Fatal("rank 1 claimed at boot")
	}
	later := core.NodeTime(take + core.Millisecond)
	acts := tickAfterSilence(t, m, boot, later)
	claim, ok := findClaim(acts)
	if !ok || claim.Expect != 0 {
		t.Fatalf("role tick after silence = %v, want LogClaimEpoch{Expect 0}", acts)
	}
	winClaim(t, m, acts, 1, later)
}

func TestLostClaimLearnsEpochAndWaits(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: take,
		Heartbeat:     take,
	})
	boot := bootEmpty(t, m, 0)
	later := core.NodeTime(take + core.Millisecond)
	acts := tickAfterSilence(t, m, boot, later)
	claim, ok := findClaim(acts)
	if !ok {
		t.Fatalf("no claim in %v", acts)
	}
	// Another node won epoch 5 first.
	acts = m.Step(core.Event{At: later, Body: core.LogClaimEpochDone{
		Req: claim.Req, Epoch: 5, Owner: "other", Won: false, Err: core.OK, Outcome: core.Applied,
	}})
	if m.IsActive() || m.Epoch() != 0 {
		t.Fatalf("lost claim made the node a writer: active=%v epoch=%d", m.IsActive(), m.Epoch())
	}
	if _, ok := findAppend(acts); ok {
		t.Fatalf("lost claim wrote to the log: %v", acts)
	}
	// Right after the loss the node does not ask again: it waits a full silence period.
	role, ok := findTimer(boot, replica.TimerRole)
	if !ok {
		t.Fatal("role timer missing")
	}
	if got, ok := findClaim(m.Step(core.Event{At: later + core.NodeTime(core.Millisecond), Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})); ok {
		t.Fatalf("claimed again right after a loss: %+v", got)
	}
}

func TestClaimThatTimedOutButLandedIsAccepted(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: core.Millisecond, TakeoverAfter: take, Heartbeat: take,
	})
	boot := bootEmpty(t, m, 0)
	claim, ok := findClaim(boot)
	if !ok {
		t.Fatalf("no claim at boot in %v", boot)
	}
	// The answer to the first claim is lost: the node does not know the result.
	m.Step(core.Event{Body: core.LogClaimEpochDone{Req: claim.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}})
	if m.IsActive() || m.Epoch() != 0 {
		t.Fatal("unknown claim result made the node a writer")
	}
	// After the silence period it asks again with the same expectation. The log
	// says the stored epoch is 1 and the owner is this node: the first claim had landed.
	later := core.NodeTime(2 * take)
	acts := tickAfterSilence(t, m, boot, later)
	again, ok := findClaim(acts)
	if !ok || again.Expect != 0 {
		t.Fatalf("second claim = %v, want LogClaimEpoch{Expect 0}", acts)
	}
	acts = m.Step(core.Event{At: later, Body: core.LogClaimEpochDone{
		Req: again.Req, Epoch: 1, Owner: "w", Won: false, Err: core.OK, Outcome: core.Applied,
	}})
	fence, ok := findAppend(acts)
	if !ok {
		t.Fatalf("accepted claim emitted %v, want the fence LogAppend", acts)
	}
	m.Step(core.Event{At: later, Body: core.LogAppendDone{Req: fence.Req, Slot: fence.Slot, Err: core.OK, Outcome: core.Applied}})
	if !m.IsActive() || m.Epoch() != 1 {
		t.Fatalf("active=%v epoch=%d, want active epoch 1", m.IsActive(), m.Epoch())
	}
}

func TestFencedByNewerEpochEntry(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: core.Millisecond, Heartbeat: core.Second, MaxCmdsPerSlot: 1,
	})
	acts := bootEmpty(t, m, 0)
	winClaim(t, m, acts, 1, 0)
	ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 7, Cmd: commitCmd("k", 1)}))

	// The commit's slot is taken by an entry written in epoch 2 by another node.
	foreign := replica.EncodeEnvelopedForTest(2, "other", metadata.Encode(metadata.Entry{
		ID: "other/e2/1", Kind: metadata.KindNoop,
	}))
	acts = m.Step(core.Event{Body: core.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: foreign, Err: core.OK, Outcome: core.Applied,
	}})
	if m.IsActive() || m.Epoch() != 0 {
		t.Fatalf("fenced writer still active: active=%v epoch=%d", m.IsActive(), m.Epoch())
	}
	if _, ok := findAppend(acts); ok {
		t.Fatalf("fenced writer retried the append: %v", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Req != 7 || ans[0].Done.Err != core.ErrUnavailable {
		t.Fatalf("answers = %+v, want one ErrUnavailable for request 7", ans)
	}
	// It stays a standby: new commits are refused, reads still work.
	if got := m.Offer(replica.ClientOperation{Req: 8, Cmd: commitCmd("k2", 1)}); len(got) != 0 {
		t.Fatalf("read-only node accepted a commit: %v", got)
	}
}

func TestLostSlotToOlderEpochIsRetried(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: core.Millisecond, Heartbeat: core.Second, MaxCmdsPerSlot: 1,
	})
	acts := bootEmpty(t, m, 0)
	winClaim(t, m, acts, 2, 0)
	ap := onlyAppend(t, m.Offer(replica.ClientOperation{Req: 7, Cmd: commitCmd("k", 1)}))
	// A deposed writer of epoch 1 took the slot: we keep the role and retry later.
	old := replica.EncodeEnvelopedForTest(1, "old", metadata.Encode(metadata.Entry{
		ID: "old/e1/9", Kind: metadata.KindNoop,
	}))
	acts = m.Step(core.Event{Body: core.LogAppendDone{
		Req: ap.Req, Slot: ap.Slot, Taken: true, Existing: old, Err: core.OK, Outcome: core.Applied,
	}})
	if !m.IsActive() {
		t.Fatal("writer stepped down because of an older epoch")
	}
	retry, ok := findAppend(acts)
	if !ok || retry.Slot <= ap.Slot {
		t.Fatalf("retry = %+v (ok=%v), want a LogAppend at a later slot than %d", retry, ok, ap.Slot)
	}
}

// A node that is restoring must not claim, send a heartbeat, or become ready.
// This is the shape of simulator seed 2318.
func TestRestoringNodeNeverClaimsOrWrites(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0, CatchUpEvery: core.Millisecond,
		TakeoverAfter: take, Heartbeat: take, CheckpointPrefix: "checkpoint/",
	})
	list := startRestore(t, m)
	acts := m.Step(core.Event{Body: core.ObjectListDone{Req: list.Req}})
	catch, _ := findTimer(acts, 1)
	acts = m.Step(core.Event{Body: core.TimerFired{ID: catch.ID, Gen: catch.Gen}})
	tail := acts[0].(core.LogGetEnd)
	boot := m.Step(core.Event{Body: core.LogGetEndDone{Req: tail.Req, Tail: 0, Err: core.OK, Outcome: core.Applied}})
	role, ok := findTimer(boot, replica.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", boot)
	}
	_, last := winClaim(t, m, boot, 1, 0) // the node is the writer, fence at slot 0

	// The node falls behind a trim: its next slot (1) is missing, slot 3 exists.
	catch, ok = findTimer(last, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", last)
	}
	acts = m.Step(core.Event{At: 1, Body: core.TimerFired{ID: catch.ID, Gen: catch.Gen}})
	tail = acts[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: 1, Body: core.LogGetEndDone{Req: tail.Req, Tail: 5, Err: core.OK, Outcome: core.Applied}})
	read := acts[0].(core.LogRead)
	acts = m.Step(core.Event{At: 1, Body: core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 3, Data: noop("late")}},
	}})
	relist, ok := acts[len(acts)-1].(core.ObjectList)
	if !ok || m.IsActive() || m.Ready() {
		t.Fatalf("want a restore (ObjectList), not active, not ready; got %v active=%v ready=%v", acts, m.IsActive(), m.Ready())
	}
	// The list fails, so the node still does not know whether a checkpoint exists.
	m.Step(core.Event{At: 2, Body: core.ObjectListDone{Req: relist.Req, Err: core.ErrTimeout}})

	// While it restores, the role timer must not make it claim or write.
	for _, at := range []core.NodeTime{core.NodeTime(take) + 1, core.NodeTime(2 * take), core.NodeTime(10 * take)} {
		acts = m.Step(core.Event{At: at, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
		if c, ok := findClaim(acts); ok {
			t.Fatalf("restoring node claimed at %d: %+v", at, c)
		}
		if ap, ok := findAppend(acts); ok {
			t.Fatalf("restoring node wrote at %d: %+v", at, ap)
		}
		if next, ok := findTimer(acts, replica.TimerRole); ok {
			role = next
		}
	}
	if m.Ready() || m.IsActive() {
		t.Fatalf("restoring node: ready=%v active=%v", m.Ready(), m.IsActive())
	}
}

func TestHeartbeatEmitsNoopWhenIdle(t *testing.T) {
	const hb = 5 * core.Millisecond
	m := newMachine(t, replica.Config{
		Writer: "w", Rank: 0,
		CatchUpEvery: core.Millisecond,
		Heartbeat:    hb,
	})
	ready := bootEmpty(t, m, 0)
	winClaim(t, m, ready, 1, 0)
	role, ok := findTimer(ready, replica.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", ready)
	}

	acts := m.Step(core.Event{At: 0, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if ap, ok := findAppend(acts); ok {
		t.Fatalf("role tick at boot appended %#v, want silence until heartbeat", ap)
	}
	role, ok = findTimer(acts, replica.TimerRole)
	if !ok {
		t.Fatalf("role rearm missing in %v", acts)
	}

	due := core.NodeTime(hb + core.Millisecond)
	acts = m.Step(core.Event{At: due, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	ap, ok := findAppend(acts)
	if !ok {
		t.Fatalf("idle heartbeat actions = %v, want LogAppend", acts)
	}
	payload, ok := replica.UnwrapPayload(ap.Data)
	if !ok {
		t.Fatal("heartbeat envelope truncated")
	}
	got, err := metadata.Decode(payload)
	if err != nil || got.Kind != metadata.KindNoop {
		t.Fatalf("heartbeat payload = %+v %v, want KindNoop", got, err)
	}
	if !m.IsActive() {
		t.Fatal("heartbeat changed role")
	}
	role, ok = findTimer(acts, replica.TimerRole)
	if !ok {
		t.Fatalf("role rearm after heartbeat missing in %v", acts)
	}

	acts = m.Step(core.Event{At: due + 1, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if ap, ok := findAppend(acts); ok {
		t.Fatalf("in-flight heartbeat appended again: %#v", ap)
	}
}
