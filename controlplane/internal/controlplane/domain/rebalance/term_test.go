package rebalance

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/internal/spec"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sim"
)

// Tests of log writes, the queue, the term and the load (sections 8 and 9).

// Rule 38: a write with no answer after 10 s ends the term. The late answer
// is ignored, and the fresh reads see the write that applied late.
func TestWriteTimerEndsTerm(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, nil},
		{delay + 10*sec - 1, nil, nil},
		{delay + 10*sec, nil, []string{"1 ErrUnavailable"}},
	})
	h.check("loading", []any{h.phase("g"), heldOps(h)}, []any{"loading", "[GroupInstall ListGroups fresh]"})
	h.check("late answer", h.answer(0, runtime.OK), []string(nil))
	h.hold = false
	h.answer(0, runtime.OK)
	h.check("after the fresh load", []any{h.phase("g"), h.group("g").generation, h.members("g")}, []any{stateJoining, 1, []string{"m1"}})
}

// Rule 39: a load read refused with ErrUnavailable is sent again after 100 ms.
func TestLoadRetry(t *testing.T) {
	h := &harness{t: t, m: New(Config{}), cx: &testCx{rand: rng.New(seed)}, log: spec.New(), session: 10000, hold: true}
	h.step(RoleChanged{Active: true})
	h.check("list read", heldOps(h), "[ListGroups]")
	h.answer(0, runtime.ErrUnavailable)
	h.check("waits", []any{heldOps(h), h.until(99 * msec)}, []any{"[]", []string(nil)})
	h.until(100 * msec)
	h.check("sent again", heldOps(h), "[ListGroups]")
	h.check("a call while loading", h.step(List{Call: 1}), []string{"1 ErrUnavailable"})
	h.answer(0, runtime.OK)
	h.check("serving", h.phase("g"), stateDead)
}

// Section 8: while a write is in flight, the inputs of its group wait in a
// queue and are taken in order after the answer; other calls do not wait.
func TestQueueOrder(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.hold = true
	h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, nil}})
	h.check("install in flight", heldOps(h), "[GroupInstall]")
	h.run([]step{
		{delay, hb(2, "g", a, 1), nil},
		{delay, syncReq(3, "g", a, 1, as(a, "x")), nil},
		{delay, leave(4, "g", "zz"), nil},
		{delay, List{Call: 5}, []string{"5 OK g:consumer:PreparingRebalance"}},
		{delay, hb(6, "other", a, 1), []string{"6 ErrUnknownMember"}},
	})
	// The install applies, then the queue runs until the sync starts its own write.
	got := h.answer(0, runtime.OK)
	h.check("after install", got, []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]", "2 OK"})
	h.check("assign in flight", heldOps(h), "[GroupAssign]")
	h.check("after assign", h.answer(0, runtime.OK), []string{"3 OK assign=x", "4 OK [ErrUnknownMember]"})
}

// Rule 33 with the queue: a queued call that is abandoned is taken out; a
// call whose write is in flight gets no reply, but the write is applied.
func TestAbandonedQueuedAndWritingCalls(t *testing.T) {
	h := newHarness(t, Config{})
	a := h.id(1)
	h.hold = true
	h.run([]step{
		{0, h.join(1, "g", ""), nil},
		{delay, nil, nil},
		{delay, hb(2, "g", a, 1), nil},
		{delay, CallAbandoned{Call: 2}, nil},
	})
	h.check("install", h.answer(0, runtime.OK), []string{"1 OK gen=1 proto=range leader=m1 id=m1 members=[m1:meta-range]"})
	h.run([]step{
		{delay, syncReq(3, "g", a, 1, as(a, "x")), nil},
		{delay, CallAbandoned{Call: 3}, nil},
	})
	h.check("assign", h.answer(0, runtime.OK), []string(nil))
	h.check("applied", []any{h.phase("g"), h.group("g").phase.(*stable).assign[a]}, []any{stateStable, []byte("x")})
	h.check("nothing waits", len(h.m.state.(*serving).where), 0)
}

// Rule 34 and 35: losing the role drops the memory; taking it again reads
// the log. A group in its first round was never logged, so it is gone.
func TestTakeoverDropsState(t *testing.T) {
	h := newHarness(t, Config{})
	a, b := h.stableTwo("g", 0, 1, 1)
	h.run([]step{
		{delay, h.join(10, "new", ""), nil},
		{delay, h.join(11, "g", a), nil},
		{delay, RoleChanged{Active: false}, []string{"10 ErrUnavailable", "11 ErrUnavailable"}},
		{delay, RoleChanged{Active: true}, nil},
		{delay, List{Call: 12}, []string{"12 OK g:consumer:Stable"}},
	})
	g := h.group("g")
	h.check("loaded", []any{h.phase("new"), g.generation, h.members("g"), g.find(a).synced, g.find(b).isNew}, []any{stateDead, 2, []string{"m1", "m2"}, true, false})
	h.run([]step{{delay, syncReq(13, "g", b, 2), []string{"13 OK assign=b2"}}})
}

// Section 8: a business code from the log shows that memory and log differ:
// the term ends. ErrTooLarge and ErrInvalid wrote nothing: the term goes on.
func TestWriteAnswers(t *testing.T) {
	cases := []struct {
		code  runtime.ErrCode
		want  []string
		phase string
	}{
		{runtime.ErrIllegalGeneration, []string{"1 ErrUnavailable"}, "loading"},
		{runtime.ErrTimeout, []string{"1 ErrUnavailable"}, "loading"},
		{runtime.ErrTooLarge, []string{"1 ErrTooLarge"}, stateJoining},
		{runtime.ErrInvalid, []string{"1 ErrInvalid"}, stateJoining},
	}
	for _, tc := range cases {
		h := newHarness(t, Config{})
		h.hold = true
		h.run([]step{{0, h.join(1, "g", ""), nil}, {delay, nil, nil}})
		h.check(tc.code.String(), []any{h.answer(0, tc.code), h.phase("g")}, []any{tc.want, tc.phase})
	}
}

// Rule 31: a failed commit write is ErrUnavailable and the term goes on;
// so is a commit whose write timer fires.
func TestCommitFailedWrite(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	op := contract.OffsetCommit{Group: "x", Generation: -1}
	h.step(OffsetCommit{Call: 1, Op: op})
	h.check("failed", []any{h.answer(0, runtime.ErrTimeout), h.phase("x")}, []any{[]string{"1 ErrUnavailable"}, stateDead})
	h.step(OffsetCommit{Call: 2, Op: op})
	h.check("timeout", h.until(10*sec), []string{"2 ErrUnavailable"})
	h.check("late answer", h.answer(0, runtime.OK), []string(nil))
	h.step(OffsetCommit{Call: 3, Op: op})
	h.check("abandoned", h.step(CallAbandoned{Call: 3}), []string(nil))
	h.check("its answer", h.answer(0, runtime.OK), []string(nil))
	h.check("still serving", h.phase("x"), stateDead)
}

// Section 7: a TimerFired of an older arm, and a second copy of a
// ReplicaReply, are ignored.
func TestStaleTimerAndDuplicateReply(t *testing.T) {
	h := newHarness(t, Config{})
	h.hold = true
	h.run([]step{{0, h.join(1, "g", ""), nil}, {0, h.join(2, "g", ""), nil}})
	var delays []runtime.TimerFired
	for _, o := range h.outs {
		if st, ok := o.(sim.SetTimer); ok && st.ID == uint64(h.group("g").delay.id) {
			delays = append(delays, st.Body.(runtime.TimerFired))
		}
	}
	h.check("delay set twice", len(delays), 2)
	h.check("stale delay", h.step(delays[0]), []string(nil))
	h.check("no write yet", heldOps(h), "[]")
	h.until(delay)
	install := h.held[0]
	r := h.apply(install)
	h.held = nil
	h.check("answer", len(h.step(r)), 2)
	h.check("second copy", h.step(r), []string(nil))
	h.check("phase", h.phase("g"), stateSyncing)
}

// The machine is deterministic: the same inputs give the same outputs and hash.
func TestSameInputsSameRun(t *testing.T) {
	run := func() ([]any, uint64) {
		h := newHarness(t, Config{})
		h.stableTwo("g", 0, 1, 1)
		h.step(h.join(20, "g", ""))
		h.until(30 * sec)
		return h.outs, h.m.StateHash()
	}
	o1, h1 := run()
	o2, h2 := run()
	if h1 != h2 || len(o1) != len(o2) {
		t.Fatalf("runs differ: %x %x, %d %d outputs", h1, h2, len(o1), len(o2))
	}
	for i := range o1 {
		if fmt.Sprintf("%#v", o1[i]) != fmt.Sprintf("%#v", o2[i]) {
			t.Fatalf("output %d differs: %v / %v", i, o1[i], o2[i])
		}
	}
}
