package statemachine_test

import (
	"testing"

	"manifester/internal/controlplane/statemachine"
	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

func findTimer(acts []core.Action, id core.TimerID) (core.SetTimer, bool) {
	for _, a := range acts {
		if st, ok := a.(core.SetTimer); ok && st.ID == id {
			return st, true
		}
	}
	return core.SetTimer{}, false
}

func bootEmpty(t *testing.T, m *statemachine.Machine, at core.NodeTime) []core.Action {
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

func TestStandbyRefuseOfferNoAppend(t *testing.T) {
	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: 10 * core.Second,
	})
	bootEmpty(t, m, 0)
	if m.IsActive() {
		t.Fatal("rank 1 became active at boot")
	}
	acts := m.Offer(statemachine.ClientOperation{Req: 1, Cmd: commitCmd("k", 1)})
	if len(acts) != 0 {
		t.Fatalf("standby Offer returned %v, want no LogAppend", acts)
	}
	ans := m.TakeAnswers()
	if len(ans) != 1 || ans[0].Done.Err != core.ErrUnavailable {
		t.Fatalf("answers = %+v, want ErrUnavailable", ans)
	}
}

func TestTakeoverAfterSilence(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: take,
		Heartbeat:     take,
	})
	acts := bootEmpty(t, m, 0)
	if m.IsActive() {
		t.Fatal("active before silence")
	}
	role, ok := findTimer(acts, statemachine.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", acts)
	}
	catch, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	// Other writers silent since boot (t=0). Refresh pullOK at later so the
	// logcp rule (recent successful pull) holds, then fire the role timer.
	later := core.NodeTime(take + core.Millisecond)
	acts = m.Step(core.Event{At: later, Body: core.TimerFired{ID: catch.ID, Gen: catch.Gen}})
	check := acts[0].(core.LogGetEnd)
	m.Step(core.Event{At: later, Body: core.LogGetEndDone{
		Req: check.Req, Tail: 0, Err: core.OK, Outcome: core.Applied,
	}})
	m.Step(core.Event{At: later, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if !m.IsActive() {
		t.Fatal("standby did not take over after silence")
	}
}

func TestFailbackOnLowerRank(t *testing.T) {
	const take = 5 * core.Millisecond
	m := newMachine(t, statemachine.Config{
		K: 1, Writer: "s1", Rank: 1,
		CatchUpEvery:  core.Millisecond,
		TakeoverAfter: take,
		Heartbeat:     take,
	})
	acts := bootEmpty(t, m, 0)
	role, ok := findTimer(acts, statemachine.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", acts)
	}
	m.Promote()
	if !m.IsActive() {
		t.Fatal("Promote left standby")
	}
	foreign := statemachine.EncodeEnvelopedForTest(0, "preferred", stateupdater.Encode(stateupdater.Entry{
		ID: "preferred/1", Kind: stateupdater.KindNoop,
	}))
	catch, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}
	at := core.NodeTime(take)
	acts = m.Step(core.Event{At: at, Body: core.TimerFired{ID: catch.ID, Gen: catch.Gen}})
	check := acts[0].(core.LogGetEnd)
	acts = m.Step(core.Event{At: at, Body: core.LogGetEndDone{
		Req: check.Req, Tail: 1, Err: core.OK, Outcome: core.Applied,
	}})
	read := acts[0].(core.LogRead)
	m.Step(core.Event{At: at, Body: core.LogReadDone{
		Req: read.Req, Err: core.OK, Outcome: core.Applied,
		Entries: []core.LogEntry{{Slot: 0, Data: foreign}},
	}})
	m.Step(core.Event{At: at + 1, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if m.IsActive() {
		t.Fatal("active did not fail back after lower-rank writer")
	}
}

func TestHeartbeatEmitsNoopWhenIdle(t *testing.T) {
	const hb = 5 * core.Millisecond
	m := newMachine(t, statemachine.Config{
		Writer: "w", Rank: 0,
		CatchUpEvery: core.Millisecond,
		Heartbeat:    hb,
	})
	ready := bootEmpty(t, m, 0)
	if !m.IsActive() {
		t.Fatal("rank 0 stayed standby after boot")
	}
	role, ok := findTimer(ready, statemachine.TimerRole)
	if !ok {
		t.Fatalf("role timer missing in %v", ready)
	}

	acts := m.Step(core.Event{At: 0, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if ap, ok := findAppend(acts); ok {
		t.Fatalf("role tick at boot appended %#v, want silence until heartbeat", ap)
	}
	role, ok = findTimer(acts, statemachine.TimerRole)
	if !ok {
		t.Fatalf("role rearm missing in %v", acts)
	}

	due := core.NodeTime(hb + core.Millisecond)
	acts = m.Step(core.Event{At: due, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	ap, ok := findAppend(acts)
	if !ok {
		t.Fatalf("idle heartbeat actions = %v, want LogAppend", acts)
	}
	payload, ok := statemachine.UnwrapPayload(ap.Data)
	if !ok {
		t.Fatal("heartbeat envelope truncated")
	}
	got, err := stateupdater.Decode(payload)
	if err != nil || got.Kind != stateupdater.KindNoop {
		t.Fatalf("heartbeat payload = %+v %v, want KindNoop", got, err)
	}
	if !m.IsActive() {
		t.Fatal("heartbeat changed role")
	}
	role, ok = findTimer(acts, statemachine.TimerRole)
	if !ok {
		t.Fatalf("role rearm after heartbeat missing in %v", acts)
	}

	acts = m.Step(core.Event{At: due + 1, Body: core.TimerFired{ID: role.ID, Gen: role.Gen}})
	if ap, ok := findAppend(acts); ok {
		t.Fatalf("in-flight heartbeat appended again: %#v", ap)
	}
}

func findAppend(acts []core.Action) (core.LogAppend, bool) {
	for _, a := range acts {
		if ap, ok := a.(core.LogAppend); ok {
			return ap, true
		}
	}
	return core.LogAppend{}, false
}
