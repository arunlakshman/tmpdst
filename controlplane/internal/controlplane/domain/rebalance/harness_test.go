package rebalance

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// The tests drive the machine with inputs at explicit times and look at its
// replies. The harness plays the host: it keeps the timers the machine sets,
// fires them when the test moves the clock, and answers each submit from a
// fake log (the metadata model of spec), at once unless hold is set.

const (
	seed  = 7
	wall0 = 1000 * machine.Second
	sec   = machine.Second
	msec  = machine.Millisecond
	delay = 3 * machine.Second // the default initial delay
)

// testCx is the clock of the tests: the time that the next step sees.
type testCx struct {
	now machine.NodeTime
}

// at is the time of the next step. The wall clock is the node clock plus wall0.
func (c *testCx) at() machine.Now {
	return machine.Now{Mono: c.now, Wall: machine.RealTime(wall0) + machine.RealTime(c.now)}
}

// testEnv is the machine.Env of the tests. Random numbers come from the seeded stream;
// a buggify site fires only if faults says so (nil: never). Assertions are not recorded.
type testEnv struct {
	rand   *rng.Rand
	faults machine.Faults
}

func newTestEnv(faults machine.Faults) *testEnv {
	return &testEnv{rand: rng.New(seed), faults: faults}
}

func (e *testEnv) Uint64() uint64                      { return e.rand.Uint64() }
func (e *testEnv) Intn(n int) int                      { return e.rand.Intn(n) }
func (e *testEnv) Between(lo, hi int64) int64          { return e.rand.Between(lo, hi) }
func (e *testEnv) Chance(p float64) bool               { return e.rand.Chance(p) }
func (e *testEnv) NewID() machine.ID                   { return newID(e.rand) }
func (e *testEnv) Maybe(s fault.NamedSite) bool        { return e.faults != nil && e.faults.Maybe(s) }
func (e *testEnv) Fault(fault.NamedSite, float64) bool { return false }
func (e *testEnv) MaybeP(s fault.NamedSite, p float64) bool {
	return e.faults != nil && e.faults.MaybeP(s, p)
}
func (*testEnv) Always(bool, assert.Always, ...assert.Pair)                           {}
func (*testEnv) AlwaysOrUnreachable(bool, assert.AlwaysOrUnreachable, ...assert.Pair) {}
func (*testEnv) Sometimes(bool, assert.Sometimes)                                     {}
func (*testEnv) Reachable(assert.Reachable)                                           {}
func (*testEnv) Unreachable(assert.Unreachable, ...assert.Pair)                       {}

// newID draws an id from r the way the simulator does: 16 bytes of the stream.
func newID(r *rng.Rand) machine.ID {
	var id machine.ID
	r.Bytes(id[:])
	return id
}

// armed is one timer the machine set.
type armed struct {
	id   machine.TimerID
	at   machine.NodeTime
	seq  int
	body Input
}

type harness struct {
	t         *testing.T
	m         *Machine
	cx        *testCx
	log       *spec.ControlPlane
	env       *testEnv
	timers    []armed
	seq       int
	inbox     []Input
	hold      bool     // keep submits in held for the test to answer
	held      []Output // SubmitCommand and SubmitRead not answered yet
	got       []Reply
	outs      []Output // every output of every step
	suffix    []string
	session   int32 // session timeout of the joins the harness makes
	rebalance int32 // rebalance timeout; 0 means the session timeout
}

// newHarness starts a machine over an empty log and makes it serve.
func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	return newHarnessFaults(t, cfg, nil)
}

// newHarnessFaults is newHarness with a fault source for the buggify sites.
func newHarnessFaults(t *testing.T, cfg Config, faults machine.Faults) *harness {
	t.Helper()
	h := newBareHarness(t, cfg, faults)
	h.session = 10000
	h.step(RoleChanged{Active: true})
	if h.phase("x") != stateDead {
		t.Fatal("the machine does not serve after the load")
	}
	return h
}

// newBareHarness makes a harness and its machine. The machine has not taken any input yet.
func newBareHarness(t *testing.T, cfg Config, faults machine.Faults) *harness {
	t.Helper()
	env := newTestEnv(faults)
	h := &harness{t: t, m: New(cfg, env), env: env, cx: &testCx{}, log: spec.New()}
	shadow := rng.New(seed)
	for range 64 {
		h.suffix = append(h.suffix, newMemberID("", newID(shadow)))
	}
	return h
}

// id is the id of the k-th new member (counted from 1) with no client id.
func (h *harness) id(k int) string { return h.suffix[k-1] }

// step delivers one input and every input that follows from it (answers of
// the log, sends to itself), and returns the replies, shown short.
func (h *harness) step(in Input) []string {
	h.inbox = append(h.inbox, in)
	for len(h.inbox) > 0 {
		next := h.inbox[0]
		h.inbox = h.inbox[1:]
		for _, o := range h.m.Step(h.cx.at(), next) {
			h.output(o)
		}
	}
	return h.take()
}

func (h *harness) output(o Output) {
	h.outs = append(h.outs, o)
	switch o := o.(type) {
	case Reply:
		h.got = append(h.got, o)
	case SetTimer:
		h.cancel(o.ID)
		h.seq++
		h.timers = append(h.timers, armed{id: o.ID, at: h.cx.now + machine.NodeTime(o.After), seq: h.seq, body: TimerFired{ID: o.ID, Gen: o.Gen}})
	case CancelTimer:
		h.cancel(o.ID)
	case SendSelf:
		h.inbox = append(h.inbox, o.Msg)
	case SubmitCommand, SubmitRead:
		if h.hold {
			h.held = append(h.held, o)
			return
		}
		h.inbox = append(h.inbox, h.apply(o))
	}
}

func (h *harness) cancel(id machine.TimerID) {
	h.timers = slices.DeleteFunc(h.timers, func(a armed) bool { return a.id == id })
}

// apply runs a submit on the log and returns the answer as an input.
func (h *harness) apply(o Output) ReplicaReply {
	var req machine.RequestID
	var op runtime.ControlPlaneOperation
	switch o := o.(type) {
	case SubmitCommand:
		req, op = o.Req, o.Op
	case SubmitRead:
		req, op = o.Req, o.Op
	case Reply, SetTimer, CancelTimer, SendSelf:
		h.t.Fatalf("%T is not a submit", o)
	}
	done := h.log.Apply(op)
	return ReplicaReply{Req: req, Done: runtime.ControlPlaneDone{Req: req, Resp: done.Resp, Err: done.Err, Outcome: done.Outcome}}
}

// answer answers the i-th held submit and removes it. With err OK the log
// runs it; with another code the log does not, and err is the answer.
func (h *harness) answer(i int, err runtime.ErrCode) []string {
	o := h.held[i]
	h.held = slices.Delete(h.held, i, i+1)
	if err == runtime.OK {
		return h.step(h.apply(o))
	}
	req := reqOf(o)
	return h.step(ReplicaReply{Req: req, Done: runtime.ControlPlaneDone{Req: req, Err: err, Outcome: runtime.MaybeApplied}})
}

func reqOf(o Output) machine.RequestID {
	switch o := o.(type) {
	case SubmitCommand:
		return o.Req
	case SubmitRead:
		return o.Req
	case Reply, SetTimer, CancelTimer, SendSelf:
		return 0
	}
	return 0
}

// until moves the clock to at (from the start), firing every due timer in
// time order, and returns the replies.
func (h *harness) until(at machine.Duration) []string {
	h.t.Helper()
	target := machine.NodeTime(at)
	if target < h.cx.now {
		h.t.Fatalf("time goes back: %d < %d", target, h.cx.now)
	}
	var out []string
	for {
		i := h.due(target)
		if i < 0 {
			break
		}
		a := h.timers[i]
		h.timers = slices.Delete(h.timers, i, i+1)
		h.cx.now = a.at
		out = append(out, h.step(a.body)...)
	}
	h.cx.now = target
	return out
}

func (h *harness) due(target machine.NodeTime) int {
	best := -1
	for i, a := range h.timers {
		if a.at <= target && (best < 0 || a.at < h.timers[best].at || (a.at == h.timers[best].at && a.seq < h.timers[best].seq)) {
			best = i
		}
	}
	return best
}

// take returns the replies so far, shown short, and forgets them.
func (h *harness) take() []string {
	out := make([]string, 0, len(h.got))
	for _, r := range h.got {
		out = append(out, h.show(r))
	}
	h.got = nil
	return out
}

// step is one line of a table: move the clock to at, then deliver in (nil:
// none), and expect exactly want (in any order) from both.
type step struct {
	at   machine.Duration
	in   Input
	want []string
}

func (h *harness) run(steps []step) {
	h.t.Helper()
	for i, st := range steps {
		got := h.until(st.at)
		if st.in != nil {
			got = append(got, h.step(st.in)...)
		}
		if !sameSet(got, st.want) {
			h.t.Fatalf("step %d (at %v, %T):\n got  %q\n want %q", i, st.at, st.in, got, st.want)
		}
	}
}

func sameSet(a, b []string) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}

// show prints a reply short: "call code body", with member ids as m1, m2, ...
func (h *harness) show(r Reply) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d %v", r.Call, r.Err)
	switch a := r.Body.(type) {
	case JoinAnswer:
		fmt.Fprintf(&b, " gen=%d proto=%s leader=%s id=%s", a.Generation, a.Protocol, a.Leader, a.MemberID)
		if a.Members != nil {
			var ms []string
			for _, mm := range a.Members {
				ms = append(ms, h.short(mm.ID)+":"+string(mm.Metadata))
			}
			fmt.Fprintf(&b, " members=%v", sortedShort(ms))
		}
	case SyncAnswer:
		fmt.Fprintf(&b, " assign=%s", a.Assignment)
	case LeaveAnswer:
		fmt.Fprintf(&b, " %v", a.Errs)
	case CommitAnswer:
		fmt.Fprintf(&b, " %v", a.Errs)
	case DescribeAnswer:
		for _, d := range a.Groups {
			var ms []string
			for _, mm := range d.Members {
				ms = append(ms, mm.MemberID)
			}
			fmt.Fprintf(&b, " %s:%s gen=%d assigned=%d members=%v", d.Name, d.State, d.Generation, d.AssignedGeneration, ms)
		}
	case ListAnswer:
		for _, g := range a.Groups {
			fmt.Fprintf(&b, " %s:%s:%s", g.Name, g.ProtocolType, g.State)
		}
	}
	return h.short(b.String())
}

func sortedShort(s []string) []string {
	slices.Sort(s)
	return s
}

// short replaces each random member id part by m1, m2, ...
func (h *harness) short(s string) string {
	for k, x := range h.suffix {
		s = strings.ReplaceAll(s, x, fmt.Sprintf("m%d", k+1))
	}
	return s
}

// The inputs, with the harness's session and rebalance timeouts.

func (h *harness) join(call CallID, group, member string, protos ...string) Join {
	j := Join{Call: call, Group: group, MemberID: member, SessionTimeoutMs: h.session, RebalanceTimeoutMs: h.session, ProtocolType: "consumer"}
	if h.rebalance > 0 {
		j.RebalanceTimeoutMs = h.rebalance
	}
	if len(protos) == 0 {
		protos = []string{"range"}
	}
	for _, p := range protos {
		j.Protocols = append(j.Protocols, Protocol{Name: p, Metadata: []byte("meta-" + p)})
	}
	return j
}

func syncReq(call CallID, group, member string, gen int32, assigns ...Assignment) Sync {
	return Sync{Call: call, Group: group, MemberID: member, Generation: gen, Assignments: assigns}
}

func hb(call CallID, group, member string, gen int32) Heartbeat {
	return Heartbeat{Call: call, Group: group, MemberID: member, Generation: gen}
}

func leave(call CallID, group string, members ...string) Leave {
	return Leave{Call: call, Group: group, Members: members}
}

func as(member, bytes string) Assignment { return Assignment{Member: member, Bytes: []byte(bytes)} }

// What the memory and the log hold.

func (h *harness) phase(group string) string {
	switch s := h.m.state.(type) {
	case *serving:
		return stateOf(s, group)
	case *loading:
		return "loading"
	}
	return "standby"
}

func (h *harness) group(group string) *group {
	s, ok := h.m.state.(*serving)
	if !ok || s.groups[group] == nil {
		h.t.Fatalf("no group %q in memory", group)
	}
	return s.groups[group]
}

// members lists the members in memory, shown short and sorted.
func (h *harness) members(group string) []string {
	var out []string
	for _, mm := range h.group(group).members {
		out = append(out, h.short(mm.id))
	}
	return sortedShort(out)
}

// parked lists the members with a parked call, shown short and sorted.
func (h *harness) parked(group string) []string {
	g := h.group(group)
	var out []string
	for _, mm := range g.members {
		if _, ok := g.parked(mm); ok {
			out = append(out, h.short(mm.id))
		}
	}
	return sortedShort(out)
}

// stored lists the members in the log, shown short and sorted.
func (h *harness) stored(group string) []string {
	d := contract.ResponseAs[contract.DescribeGroupResp](h.log.Apply(contract.DescribeGroup{Group: group}))
	var out []string
	for _, mm := range d.Members {
		out = append(out, h.short(mm.MemberID))
	}
	return sortedShort(out)
}

func (h *harness) check(what string, got, want any) {
	h.t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		h.t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// stableTwo makes group Stable with two members at generation 2, from t0.
// m<k> is the leader and m<k+1> the follower; it returns their ids. The
// clock ends at t0 + 3 s.
func (h *harness) stableTwo(group string, t0 machine.Duration, k int, call CallID) (string, string) {
	h.t.Helper()
	a, b := h.id(k), h.id(k+1)
	sa, sb := fmt.Sprintf("m%d", k), fmt.Sprintf("m%d", k+1)
	at := t0 + delay
	h.run([]step{
		{t0, h.join(call, group, ""), nil},
		{at, nil, []string{fmt.Sprintf("%d OK gen=1 proto=range leader=%s id=%s members=[%s:meta-range]", call, sa, sa, sa)}},
		{at, syncReq(call+1, group, a, 1, as(a, "a1")), []string{fmt.Sprintf("%d OK assign=a1", call+1)}},
		{at, h.join(call+2, group, ""), nil},
		{at, h.join(call+3, group, a), []string{
			fmt.Sprintf("%d OK gen=2 proto=range leader=%s id=%s", call+2, sa, sb),
			fmt.Sprintf("%d OK gen=2 proto=range leader=%s id=%s members=[%s:meta-range %s:meta-range]", call+3, sa, sa, sa, sb),
		}},
		{at, syncReq(call+4, group, b, 2), nil},
		{at, syncReq(call+5, group, a, 2, as(a, "a2"), as(b, "b2")), []string{
			fmt.Sprintf("%d OK assign=b2", call+4), fmt.Sprintf("%d OK assign=a2", call+5),
		}},
	})
	return a, b
}
