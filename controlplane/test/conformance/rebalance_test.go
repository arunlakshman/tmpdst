package conformance

import (
	"bytes"
	"encoding/base32"
	"flag"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/sched"
	"github.com/manifesterio/mds/dst/sim"
)

// The rebalance machine (domain/rebalance) against the rebalance model
// (spec.Rebalance). The test plays the host of the machine: it keeps the
// timers the machine sets and fires them in time order, and it answers each
// submit from a fake log, a spec.ControlPlane. The model gets the same inputs
// at the same times, and the answers must be equal call by call.
//
// Time. Each operation starts 1 ns after the one before, and every timeout is
// a whole number of ms. So two deadlines that come from different operations
// never fall on the same instant. The timeouts are also far apart in their low
// bits, so sums of them rarely meet. When two timers still fall on the same
// instant, the order of the machine (the order it set them) and of the model
// (a fixed order of kinds) may differ, and docs/rebalance.md section 12 says
// not to compare that: the check stops there and passes.
//
// Failures. A write can be refused (ErrUnavailable, not applied), applied with
// its answer lost (ErrTimeout), or lost (never answered: the write timer of the
// machine fires, and the write may be applied late, after the term). The model
// has no failures. In an operation where a failure happened, an answer of the
// machine may be ErrUnavailable where the model says something else; a wrong
// answer is never allowed. When the machine ends its term (a fresh load), the
// model drops its memory and loads again from a copy of the fake log.

const (
	rbDelay     = 1532 * machine.Millisecond
	rbNewMember = 20064 * machine.Millisecond
	rbWrite     = 2128 * machine.Millisecond
)

// The kinds of a write failure, for the next SubmitCommand.
const (
	writeOK         = iota
	writeRefused    // ErrUnavailable, not applied
	writeLostAnswer // applied, then answered ErrTimeout
	writeLost       // never answered
)

var (
	rbGroups = []string{"a", "b"}
	// The rebalance timeouts of group a; group b adds 64 ms, so that the rounds
	// of the two groups do not end at the same instant after a load. 0 (the
	// session timeout) is not drawn: it makes the round end meet a session.
	rbRebalances = []int32{3008, 4016}
	rbBig        = bytes.Repeat([]byte{'x'}, 257<<10)
	// rbWant are the answers and events that the draws must reach (see Run).
	rbWant = []string{"join/OK", "join/gen2", "join/ErrInconsistentGroupProtocol", "join/ErrTooLarge",
		"sync/OK", "sync/parked", "heartbeat/OK", "heartbeat/ErrRebalanceInProgress", "leave/OK",
		"commit/OK", "commit/ErrRebalanceInProgress", "describe/OK", "remove/expire", "abandon",
		"roleLoss/parked", "termBreak", "late"}
)

// rbNext is the next call of a member: "join", "sync" (of generation gen), or
// "" for a heartbeat.
type rbNext struct {
	call string
	gen  int32
}

type rbTimer struct {
	id   uint64
	at   machine.NodeTime
	seq  int
	body any
}

type rbWorld struct {
	t     *rapid.T
	m     *rebalance.Machine
	model *spec.Rebalance
	log   *spec.ControlPlane              // the fake log
	cmds  []runtime.ControlPlaneOperation // what the fake log applied, to copy it
	now   machine.NodeTime
	rand  *rng.Rand
	step  int
	seen  map[string]int

	timers []rbTimer
	seq    int
	inbox  []any
	active bool // the last role sent

	fault     int                       // the failure of the next write
	lost      []rebalance.SubmitCommand // writes never answered
	lostUntil machine.NodeTime          // the write timer of the last lost write fires by then
	loose     bool                      // a failure happened in this operation
	late      bool                      // a lost write was applied since the last load
	broke     bool                      // the machine started a fresh load
	tied      bool                      // two timers fell on one instant: stop

	nextCall rebalance.CallID
	inputs   map[rebalance.CallID]any
	sent     map[rebalance.CallID]int  // the operation that sent each call
	open     map[rebalance.CallID]bool // calls with no answer from the machine yet
	got      map[rebalance.CallID]string
	want     map[rebalance.CallID]string
	pool     map[string][]string // member ids per group, as the test made them
	next     map[string]rbNext   // what each member does next, from its last answer
	sessions map[string]int32    // the session timeout of each member
	leaders  map[string]string   // the leader answered for each group and generation
	gens     map[string]int32    // the last generation answered, per group
	logGens  map[string]int32    // the last generation in the log, per group
}

// rbCx is the step context: Wall is Now, so TimeMs is the same as in the model.
type rbCx struct{ w *rbWorld }

func (c rbCx) Now() sched.TimeNs       { return sched.TimeNs(c.w.now) }
func (c rbCx) Wall() sched.TimeNs      { return sched.TimeNs(c.w.now) }
func (c rbCx) Rand() *rng.Rand         { return c.w.rand }
func (c rbCx) Faults() *fault.Injector { return nil }
func (c rbCx) Assert() *assert.Log     { return nil }

func newRBWorld(t *rapid.T, seen map[string]int) *rbWorld {
	w := &rbWorld{
		t: t, seen: seen, rand: rng.New(0), log: spec.New(),
		m:      rebalance.New(rebalance.Config{InitialDelay: rbDelay, NewMemberTimeout: rbNewMember, WriteTimeout: rbWrite}),
		model:  spec.NewRebalance(rbDelay),
		inputs: map[rebalance.CallID]any{}, sent: map[rebalance.CallID]int{}, open: map[rebalance.CallID]bool{},
		got: map[rebalance.CallID]string{}, want: map[rebalance.CallID]string{}, pool: map[string][]string{}, next: map[string]rbNext{}, sessions: map[string]int32{"ghost": 5000},
		leaders: map[string]string{}, gens: map[string]int32{}, logGens: map[string]int32{},
	}
	w.model.NewMemberTimeout = rbNewMember
	w.apply(contract.CreateTopic{Name: "t", Partitions: 2})
	w.model.Store = w.copyLog()
	w.role(true)
	return w
}

// TestRebalanceMachine checks the machine against the model, with timers,
// failed writes, role changes and abandoned calls. Like Run, it makes more
// rounds of checks until every answer in rbWant was seen.
func TestRebalanceMachine(t *testing.T) {
	fewerChecks(t, 100, 80) // more steps than rapid's 30: a group needs a few to become Stable
	seen := map[string]int{}
	check := func() {
		rapid.Check(t, func(rt *rapid.T) {
			w := newRBWorld(rt, seen)
			rt.Repeat(map[string]func(*rapid.T){"step": func(*rapid.T) {
				o := rbOps[rbDraw[rapid.IntRange(0, len(rbDraw)-1).Draw(rt, "op")]]
				rt.Logf("step %d: %s", w.step, o.name)
				w.op(o.run)
			}})
			w.finish()
		})
	}
	check()
	asked := userFlags()
	if n, _ := strconv.Atoi(flag.Lookup("rapid.checks").Value.String()); n < 10 || asked["rapid.seed"] || asked["rapid.failfile"] {
		return
	}
	for range maxExtraRounds {
		if t.Failed() || !slices.ContainsFunc(rbWant, func(k string) bool { return seen[k] == 0 }) {
			break
		}
		check()
	}
	t.Logf("seen: %v", seen)
	for _, k := range rbWant {
		if seen[k] == 0 && !t.Failed() {
			t.Errorf("no %q in any check: the draws do not reach it", k)
		}
	}
}

// rbOps are the operations, each with how often it is drawn. The shrinker
// moves the drawn index toward 0, so the plain ones come first.
var rbOps = []struct {
	name   string
	weight int
	run    func(w *rbWorld)
}{
	{"advance", 8, func(w *rbWorld) {
		// Mostly short, so that members live to sync; sometimes past every session.
		ms := rapid.SampledFrom([]int{100, 300, 600, 900, 1700, 100, 300, 600, 900, 1700, 3300, 100, 300, 600, 900, 1700,
			100, 300, 600, 900, 1700, 3300, 100, 300, 600, 900, 1700, 5800, 100, 300, 9000, 26000}).Draw(w.t, "ms")
		w.advance(w.now + machine.NodeTime(ms)*machine.NodeTime(machine.Millisecond))
	}},
	// A member does what its last answer asks: sync after a join, join again
	// after ErrRebalanceInProgress, else heartbeat. A new one joins. This
	// keeps groups alive.
	{"client", 14, func(w *rbWorld) {
		g := w.group()
		m := w.member(g)
		switch next := w.next[m]; {
		case m == "ghost":
			rbJoin(w, g, "")
		case next.call == "join":
			rbJoin(w, g, m)
		case next.call == "sync":
			rbSync(w, g, m, next.gen)
		default:
			rbHeartbeat(w, g, m, w.describe(g).Generation)
		}
	}},
	{"join", 5, func(w *rbWorld) {
		g, m := w.group(), ""
		if len(w.describe(g).Members) > 0 && rapid.Bool().Draw(w.t, "rejoin") {
			m = w.member(g)
		}
		rbJoin(w, g, m)
	}},
	{"sync", 4, func(w *rbWorld) {
		g := w.group()
		m := w.describe(g).Leader // the leader half of the time: its sync moves the group on
		if m == "" || rapid.Bool().Draw(w.t, "follower") {
			m = w.member(g)
		}
		rbSync(w, g, m, w.generation(g))
	}},
	{"heartbeat", 4, func(w *rbWorld) {
		g := w.group()
		rbHeartbeat(w, g, w.member(g), w.generation(g))
	}},
	{"commit", 3, func(w *rbWorld) {
		op := contract.OffsetCommit{Group: w.group()}
		op.Member, op.Generation = w.member(op.Group), w.generation(op.Group)
		if rapid.IntRange(0, 3).Draw(w.t, "admin") == 3 {
			op.Member, op.Generation = "", -1
		}
		for range rapid.IntRange(0, 2).Draw(w.t, "offsets") {
			p := int32(rapid.IntRange(0, 2).Draw(w.t, "partition"))
			op.Offsets = append(op.Offsets, contract.PartitionOffset{TopicPartition: contract.NewTopicPartition("t", p), Offset: int64(rapid.IntRange(-1, 9).Draw(w.t, "offset"))})
		}
		w.call(rebalance.OffsetCommit{Call: w.newCall(), Op: op}, spec.Commit{Call: spec.CallID(w.nextCall), Op: op})
	}},
	{"leave", 2, func(w *rbWorld) {
		g := w.group()
		ids := rapid.SliceOfN(rapid.SampledFrom(w.members(g)), 1, 2).Draw(w.t, "members")
		w.call(rebalance.Leave{Call: w.newCall(), Group: g, Members: ids}, spec.Leave{Call: spec.CallID(w.nextCall), Group: g, Members: ids})
	}},
	{"describe", 1, func(w *rbWorld) { w.describeCall(rapid.SampledFrom(append(rbGroups, "zz")).Draw(w.t, "group")) }},
	{"list", 1, func(w *rbWorld) { w.call(rebalance.List{Call: w.newCall()}, spec.List{Call: spec.CallID(w.nextCall)}) }},
	{"abandon", 1, func(w *rbWorld) {
		call := w.nextCall + 1 // a call that does not exist
		if calls := slices.Sorted(maps.Keys(w.open)); len(calls) > 0 {
			call = rapid.SampledFrom(calls).Draw(w.t, "call")
			w.seen["abandon"]++
		}
		delete(w.open, call)
		w.modelStep(spec.Abandon{Call: spec.CallID(call)})
		w.deliver(rebalance.CallAbandoned{Call: call})
	}},
	{"failNextWrite", 1, func(w *rbWorld) { w.fault = rapid.IntRange(writeRefused, writeLost).Draw(w.t, "failure") }},
	{"lateApply", 1, func(w *rbWorld) {
		if len(w.lost) == 0 {
			return
		}
		i := rapid.IntRange(0, len(w.lost)-1).Draw(w.t, "write")
		o := w.lost[i]
		w.lost = slices.Delete(w.lost, i, i+1)
		w.late = true
		w.seen["late"]++
		w.model.Store.Apply(o.Op)
		w.deliver(w.answerOf(o.Req, w.apply(o.Op))) // its request id is old: no answer
	}},
	// The role is lost: every call is answered and every timer is stopped.
	// Mostly the role comes back in the same operation.
	{"roleLoss", 1, func(w *rbWorld) {
		if len(w.open) > 0 {
			w.seen["roleLoss/parked"]++
		}
		w.role(false)
		if len(w.timers) > 0 {
			w.t.Fatalf("step %d: %d timers still armed after the role was lost", w.step, len(w.timers))
		}
		if len(w.open) > 0 {
			w.t.Fatalf("step %d: calls %v not answered when the role was lost", w.step, slices.Sorted(maps.Keys(w.open)))
		}
		w.call(rebalance.List{Call: w.newCall()}, spec.List{Call: spec.CallID(w.nextCall)})
		if rapid.IntRange(0, 9).Draw(w.t, "stay in standby") < 9 {
			w.role(true)
			w.sweep()
		}
	}},
	{"takeover", 1, func(w *rbWorld) { w.role(true); w.sweep() }},
}

// rbDraw lists the index of each operation as often as its weight.
var rbDraw = func() []int {
	var out []int
	for i, o := range rbOps {
		out = append(out, slices.Repeat([]int{i}, o.weight)...)
	}
	return out
}()

// rbJoin sends a join of member m ("" for a new member) to group g.
func rbJoin(w *rbWorld, g, m string) {
	in := rebalance.Join{Group: g, MemberID: m, ClientID: rapid.SampledFrom([]string{"", "c"}).Draw(w.t, "client"), ClientHost: "h",
		SessionTimeoutMs: w.sessions[m], RebalanceTimeoutMs: rapid.SampledFrom(rbRebalances).Draw(w.t, "rebalance") + 64*int32(slices.Index(rbGroups, g)),
		ProtocolType: "consumer"}
	if rapid.IntRange(0, 9).Draw(w.t, "type") == 9 {
		in.ProtocolType = "connect"
	}
	names := [][]string{{"range"}, {"range", "rr"}, {"rr", "range"}, {"range"}, {"range"}, {"range"}, {"range"}, {"range"}, {"range"}, {"range"}, {"rr"}, nil}[rapid.IntRange(0, 11).Draw(w.t, "protocols")]
	for _, n := range names {
		in.Protocols = append(in.Protocols, rebalance.Protocol{Name: n, Metadata: []byte("m-" + n)})
	}
	if len(in.Protocols) > 0 && rapid.IntRange(0, 49).Draw(w.t, "big") == 49 {
		in.Protocols[0].Metadata = rbBig
	}
	in.Call = w.newCall()
	model := spec.Join{Call: spec.CallID(in.Call), Group: g, MemberID: in.MemberID, ClientID: in.ClientID, ClientHost: in.ClientHost,
		ProtocolType: in.ProtocolType, SessionMs: in.SessionTimeoutMs, RebalanceMs: in.RebalanceTimeoutMs}
	for _, p := range in.Protocols {
		model.Protocols = append(model.Protocols, spec.Protocol{Name: p.Name, Metadata: p.Metadata})
	}
	if in.MemberID == "" {
		// The machine draws the id from cx.Rand: give it a stream the test can predict.
		w.rand = rng.New(uint64(in.Call))
		var raw [16]byte
		rng.New(uint64(in.Call)).Bytes(raw[:])
		model.NewID = base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw[:])
		id := model.NewID
		if in.ClientID != "" {
			id = in.ClientID + "-" + id
		}
		w.pool[g] = append(w.pool[g], id)
		// Each member has its own session timeout, 5 to 8 s, so that no two
		// sessions end at the same instant.
		in.SessionTimeoutMs = 5000 + 37*int32(len(w.sessions)%80)
		model.SessionMs, w.sessions[id] = in.SessionTimeoutMs, in.SessionTimeoutMs
	}
	w.call(in, model)
}

// rbSync sends a sync of member m of group g. The leader assigns to the
// members in the log, mostly with no mistake.
func rbSync(w *rbWorld, g, m string, gen int32) {
	d := w.describe(g)
	in := rebalance.Sync{Call: w.newCall(), Group: g, MemberID: m, Generation: gen}
	if m == d.Leader || rapid.IntRange(0, 3).Draw(w.t, "follower assigns") == 3 {
		for _, mm := range d.Members {
			if rapid.IntRange(0, 3).Draw(w.t, "no assignment") < 3 {
				in.Assignments = append(in.Assignments, rebalance.Assignment{Member: mm.MemberID, Bytes: fmt.Appendf(nil, "x%d", in.Call)})
			}
		}
		switch rapid.IntRange(0, 15).Draw(w.t, "bad assignment") {
		case 13:
			in.Assignments = append(in.Assignments, rebalance.Assignment{Member: "ghost"})
		case 14:
			in.Assignments = append(in.Assignments, in.Assignments...)
		case 15: // alone: the model checks the size before rule 21, the machine after
			in.Assignments = []rebalance.Assignment{{Member: m, Bytes: rbBig}}
		}
	}
	model := spec.Sync{Call: spec.CallID(in.Call), Group: g, MemberID: m, Generation: in.Generation}
	for _, a := range in.Assignments {
		model.Assignments = append(model.Assignments, contract.GroupAssignment{Member: a.Member, Bytes: a.Bytes})
	}
	w.call(in, model)
}

func rbHeartbeat(w *rbWorld, g, m string, gen int32) {
	w.call(rebalance.Heartbeat{Call: w.newCall(), Group: g, MemberID: m, Generation: gen},
		spec.Heartbeat{Call: spec.CallID(w.nextCall), Group: g, MemberID: m, Generation: gen})
}

func (w *rbWorld) group() string { return rapid.SampledFrom(rbGroups).Draw(w.t, "group") }

// members are the ids to draw for group: the members in the log, the newest
// ids the test made, and one that never was.
func (w *rbWorld) members(group string) []string {
	var ids []string
	for _, m := range w.describe(group).Members {
		ids = append(ids, m.MemberID)
	}
	pool := w.pool[group]
	return append(append(ids, pool[max(0, len(pool)-3):]...), "ghost")
}

// member is mostly a member in the log, which a right call uses.
func (w *rbWorld) member(group string) string {
	ids := w.members(group)
	if n := len(w.describe(group).Members); n > 0 && rapid.IntRange(0, 3).Draw(w.t, "any member") < 3 {
		ids = ids[:n]
	}
	return rapid.SampledFrom(ids).Draw(w.t, "member")
}

// generation is mostly the generation in the log, which a right call uses.
func (w *rbWorld) generation(group string) int32 {
	return w.describe(group).Generation + int32(rapid.SampledFrom([]int{0, 0, 0, 0, -1, 1}).Draw(w.t, "generation"))
}

func (w *rbWorld) describe(group string) contract.DescribeGroupResp {
	return contract.ResponseAs[contract.DescribeGroupResp](w.log.Apply(contract.DescribeGroup{Group: group}))
}

func (w *rbWorld) describeCall(group string) {
	w.call(rebalance.Describe{Call: w.newCall(), Groups: []string{group}}, spec.Describe{Call: spec.CallID(w.nextCall), Group: group})
}

func (w *rbWorld) newCall() rebalance.CallID { w.nextCall++; return w.nextCall }

// op runs one operation at its own instant, then compares the answers.
func (w *rbWorld) op(run func(w *rbWorld)) {
	if w.tied {
		return
	}
	if w.advance(w.now + 1); !w.tied {
		run(w)
	}
	if w.lostUntil > w.now && !w.tied {
		w.advance(w.lostUntil) // the write timer of a lost write fires
	}
	if w.tied {
		return
	}
	w.compare()
	w.step++
}

// call sends one call to the model and to the machine.
func (w *rbWorld) call(in, model any) {
	call, _, _ := rbOf(in)
	w.inputs[call], w.sent[call], w.open[call] = in, w.step, true
	w.modelStep(model)
	w.deliver(in)
}

func (w *rbWorld) role(active bool) {
	w.modelStep(spec.RoleChanged{Active: active})
	w.deliver(rebalance.RoleChanged{Active: active})
	if active && !w.active {
		w.late = false // the machine loaded the log again
	}
	w.active = active
}

// sweep asks for the whole state: every group, and the list.
func (w *rbWorld) sweep() {
	w.call(rebalance.List{Call: w.newCall()}, spec.List{Call: spec.CallID(w.nextCall)})
	for _, g := range rbGroups {
		w.describeCall(g)
	}
}

// finish lets every timer run out, then checks that every call got an answer.
func (w *rbWorld) finish() {
	w.op(func(w *rbWorld) { w.advance(w.now + 120*machine.NodeTime(machine.Second)) })
	if !w.tied && len(w.open) > 0 {
		w.t.Fatalf("calls never answered: %v", slices.Sorted(maps.Keys(w.open)))
	}
	w.op(func(w *rbWorld) { w.sweep() })
}

// advance moves the clock to at, firing the timers of the machine in time
// order. The model runs to the time of each timer first.
func (w *rbWorld) advance(at machine.NodeTime) {
	for {
		i := -1
		for j, a := range w.timers {
			if a.at <= at && (i < 0 || a.at < w.timers[i].at || (a.at == w.timers[i].at && a.seq < w.timers[i].seq)) {
				i = j
			}
		}
		if i < 0 {
			break
		}
		if slices.ContainsFunc(w.timers, func(a rbTimer) bool { return a.id != w.timers[i].id && a.at == w.timers[i].at }) {
			w.tied = true
			w.seen["tie"]++
			return
		}
		a := w.timers[i]
		w.timers = slices.Delete(w.timers, i, i+1)
		w.now = a.at
		w.modelStep(nil)
		w.deliver(a.body)
	}
	w.now = at
	w.modelStep(nil)
}

func (w *rbWorld) modelStep(in any) {
	for _, r := range w.model.Step(w.now, in) {
		call := rebalance.CallID(r.Call)
		if _, twice := w.want[call]; twice {
			w.t.Fatalf("step %d: the model answered call %d twice", w.step, call)
		}
		w.want[call] = rbShowModel(r)
	}
}

// deliver gives in to the machine, with every input that follows from it.
func (w *rbWorld) deliver(in any) {
	w.inbox = append(w.inbox, in)
	for len(w.inbox) > 0 {
		next := w.inbox[0]
		w.inbox = w.inbox[1:]
		for _, o := range w.m.Step(rbCx{w}, next) {
			w.output(o)
		}
	}
	if !w.broke {
		return
	}
	// The term ended. The model loads again too, from a copy of the fake log.
	if !w.loose && !w.late {
		w.t.Fatalf("step %d: the machine ended its term with no failure", w.step)
	}
	w.broke, w.late, w.loose = false, false, true
	w.seen["termBreak"]++
	w.modelStep(spec.RoleChanged{Active: false})
	w.model.Store = w.copyLog()
	w.modelStep(spec.RoleChanged{Active: true})
}

func (w *rbWorld) output(o any) {
	switch o := o.(type) {
	case rebalance.Reply:
		w.answered(o)
	case sim.SetTimer:
		w.cancel(o.ID)
		w.seq++
		w.timers = append(w.timers, rbTimer{id: o.ID, at: w.now + machine.NodeTime(o.After), seq: w.seq, body: o.Body})
	case sim.CancelTimer:
		w.cancel(o.ID)
	case rebalance.SubmitRead:
		w.broke = w.broke || o.Fresh
		w.inbox = append(w.inbox, w.answerOf(o.Req, w.log.Apply(o.Op)))
	case rebalance.SubmitCommand:
		w.write(o)
	default:
		w.t.Fatalf("unknown output %T", o)
	}
}

func (w *rbWorld) cancel(id uint64) {
	w.timers = slices.DeleteFunc(w.timers, func(a rbTimer) bool { return a.id == id })
}

// write answers a SubmitCommand from the fake log, or fails it.
func (w *rbWorld) write(o rebalance.SubmitCommand) {
	kind := w.fault
	w.fault = writeOK
	if kind != writeOK {
		w.loose = true
	}
	switch kind {
	case writeOK:
		w.inbox = append(w.inbox, w.answerOf(o.Req, w.apply(o.Op)))
	case writeRefused:
		w.inbox = append(w.inbox, w.answer(o.Req, runtime.ControlPlaneDone{Err: runtime.ErrUnavailable}))
	case writeLostAnswer:
		w.apply(o.Op)
		w.inbox = append(w.inbox, w.answer(o.Req, runtime.ControlPlaneDone{Err: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}))
	case writeLost:
		w.lost = append(w.lost, o)
		w.lostUntil = w.now + machine.NodeTime(rbWrite)
	}
}

func (w *rbWorld) answer(req machine.RequestID, d runtime.ControlPlaneDone) rebalance.ReplicaReply {
	d.Req = req
	return rebalance.ReplicaReply{Req: req, Done: d}
}

// answerOf turns the answer of the model into the reply of the replica.
func (w *rbWorld) answerOf(req machine.RequestID, d contract.Done) rebalance.ReplicaReply {
	return w.answer(req, runtime.ControlPlaneDone{Resp: d.Resp, Err: d.Err, Outcome: d.Outcome})
}

func (w *rbWorld) apply(op runtime.ControlPlaneOperation) contract.Done {
	if r, ok := op.(contract.GroupRemove); ok {
		w.seen["remove/"+r.Reason]++
	}
	w.cmds = append(w.cmds, op)
	return w.log.Apply(op)
}

// copyLog is a new log with every command of the fake log, for the model.
func (w *rbWorld) copyLog() *spec.ControlPlane {
	c := spec.New()
	for _, op := range w.cmds {
		c.Apply(op)
	}
	return c
}

// answered takes one answer of the machine and checks it against the fake log
// as it is now: an answer may only claim what is logged.
func (w *rbWorld) answered(r rebalance.Reply) {
	if !w.open[r.Call] {
		w.t.Fatalf("step %d: call %d answered twice, or after it was abandoned: %s", w.step, r.Call, rbShowMachine(r))
	}
	delete(w.open, r.Call)
	w.got[r.Call] = rbShowMachine(r)
	in := w.inputs[r.Call]
	_, kind, m := rbOf(in)
	w.seen[kind+"/"+r.Err.String()]++
	if r.Err == runtime.ErrRebalanceInProgress {
		w.next[m] = rbNext{call: "join"}
	} else if r.Err == runtime.OK && kind == "sync" {
		w.next[m] = rbNext{}
	}
	if r.Err != runtime.OK {
		return
	}
	switch a := r.Body.(type) {
	case rebalance.JoinAnswer:
		w.next[a.MemberID] = rbNext{call: "sync", gen: a.Generation}
		g := in.(rebalance.Join).Group
		key := fmt.Sprintf("%s/%d", g, a.Generation)
		if l, ok := w.leaders[key]; ok && l != a.Leader {
			w.t.Fatalf("step %d: generation %s has two leaders: %s and %s", w.step, key, l, a.Leader)
		}
		w.leaders[key] = a.Leader
		if a.Generation < w.gens[g] {
			w.t.Fatalf("step %d: group %s: generation %d answered after %d", w.step, g, a.Generation, w.gens[g])
		}
		w.gens[g] = a.Generation
		if a.MemberID == a.Leader && !slices.ContainsFunc(a.Members, func(m rebalance.Member) bool { return m.ID == a.Leader }) {
			w.t.Fatalf("step %d: the leader %s is not in the member list %v", w.step, a.Leader, a.Members)
		}
		if a.Generation >= 2 {
			w.seen["join/gen2"]++
		}
		if d := w.describe(g); !w.late && (d.Generation != a.Generation || d.Leader != a.Leader || d.Protocol != a.Protocol || !rbHas(d, a.MemberID)) {
			w.t.Fatalf("step %d: join answer %s is not in the log: %+v", w.step, w.got[r.Call], d)
		}
	case rebalance.SyncAnswer:
		s := in.(rebalance.Sync)
		if w.sent[r.Call] != w.step {
			w.seen["sync/parked"]++
		}
		d := w.describe(s.Group)
		i := slices.IndexFunc(d.Members, func(m contract.GroupMemberInfo) bool { return m.MemberID == s.MemberID })
		if !w.late && (d.AssignedGeneration != s.Generation || i < 0 || !bytes.Equal(d.Members[i].Assignment, a.Assignment)) {
			w.t.Fatalf("step %d: sync answer %s of %s is not in the log: %+v", w.step, w.got[r.Call], s.MemberID, d)
		}
	}
}

// compare matches the answers of this operation by call, then checks the log.
func (w *rbWorld) compare() {
	for _, call := range slices.Sorted(maps.Keys(w.got)) {
		got, want := w.got[call], w.want[call]
		delete(w.want, call)
		failed := w.loose && got == runtime.ErrUnavailable.String() // allowed after a failure
		if got != want && !failed {
			w.t.Fatalf("step %d: call %d %s\n  model:   %s\n  machine: %s", w.step, call, rbInput(w.inputs[call]), want, got)
		}
	}
	for _, call := range slices.Sorted(maps.Keys(w.want)) {
		if w.open[call] || !w.loose {
			w.t.Fatalf("step %d: call %d %s\n  model:   %s\n  machine: no answer", w.step, call, rbInput(w.inputs[call]), w.want[call])
		}
	}
	clear(w.got)
	clear(w.want)
	if w.loose {
		w.model.Store = w.copyLog() // a refused commit is not in the fake log
		w.loose = false
	}
	for _, g := range rbGroups {
		d := w.describe(g)
		if d.Generation < w.logGens[g] {
			w.t.Fatalf("step %d: group %s: the generation in the log went down to %d from %d", w.step, g, d.Generation, w.logGens[g])
		}
		w.logGens[g] = d.Generation
		if d.Leader != "" && !rbHas(d, d.Leader) {
			w.t.Fatalf("step %d: group %s: the leader %s is not a member: %+v", w.step, g, d.Leader, d)
		}
	}
}

func rbHas(d contract.DescribeGroupResp, id string) bool {
	return slices.ContainsFunc(d.Members, func(m contract.GroupMemberInfo) bool { return m.MemberID == id })
}

// rbOf gives the call id, the kind and the member (for a join, sync or
// heartbeat) of a call.
func rbOf(in any) (rebalance.CallID, string, string) {
	switch in := in.(type) {
	case rebalance.Join:
		return in.Call, "join", in.MemberID
	case rebalance.Sync:
		return in.Call, "sync", in.MemberID
	case rebalance.Heartbeat:
		return in.Call, "heartbeat", in.MemberID
	case rebalance.Leave:
		return in.Call, "leave", ""
	case rebalance.OffsetCommit:
		return in.Call, "commit", ""
	case rebalance.Describe:
		return in.Call, "describe", ""
	case rebalance.List:
		return in.Call, "list", ""
	}
	panic(fmt.Sprintf("not a call: %T", in))
}

// The answers of the machine and of the model, shown the same way.

func rbShowMachine(r rebalance.Reply) string {
	switch b := r.Body.(type) {
	case rebalance.JoinAnswer:
		var ms []string
		for _, m := range b.Members {
			ms = append(ms, rbShowMember(m.ID, m.InstanceID, m.Metadata))
		}
		return rbShowJoin(r.Err, b.Generation, b.Protocol, b.Leader, b.MemberID, ms)
	case rebalance.SyncAnswer:
		return rbShow(r.Err, rbBytes(b.Assignment))
	case rebalance.LeaveAnswer:
		return rbShow(r.Err, b.Errs)
	case rebalance.CommitAnswer:
		return rbShow(r.Err, b.Errs)
	case rebalance.DescribeAnswer:
		return rbShow(r.Err, fmt.Sprintf("%+v", b.Groups[0].DescribeGroupResp))
	case rebalance.ListAnswer:
		return rbShow(r.Err, fmt.Sprintf("%+v", b.Groups))
	}
	return rbShow(r.Err, nil)
}

func rbShowModel(r spec.Reply) string {
	if r.Err != runtime.OK {
		return rbShow(r.Err, nil)
	}
	switch b := r.Body.(type) {
	case spec.JoinResult:
		var ms []string
		for _, m := range b.Members {
			ms = append(ms, rbShowMember(m.MemberID, m.GroupInstanceID, m.Metadata))
		}
		return rbShowJoin(r.Err, b.Generation, b.Protocol, b.Leader, b.MemberID, ms)
	case []byte:
		return rbShow(r.Err, rbBytes(b))
	case []runtime.ErrCode:
		return rbShow(r.Err, b)
	case contract.OffsetCommitResp:
		return rbShow(r.Err, b.Errs)
	case contract.DescribeGroupResp:
		return rbShow(r.Err, fmt.Sprintf("%+v", b))
	case contract.ListGroupsResp:
		return rbShow(r.Err, fmt.Sprintf("%+v", b.Groups))
	}
	return rbShow(r.Err, nil)
}

func rbShow(err runtime.ErrCode, body any) string {
	if body == nil {
		return err.String()
	}
	return fmt.Sprintf("%v %v", err, body)
}

func rbShowJoin(err runtime.ErrCode, gen int32, protocol, leader, id string, members []string) string {
	return rbShow(err, fmt.Sprintf("gen=%d protocol=%s leader=%s id=%s members=%v", gen, protocol, leader, id, members))
}

func rbShowMember(id, instance string, metadata []byte) string {
	return id + "/" + instance + "/" + rbBytes(metadata)
}

// rbInput shows a call short: a large metadata or assignment would fill the screen.
func rbInput(in any) string {
	s := fmt.Sprintf("%T%+v", in, in)
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

func rbBytes(b []byte) string {
	if len(b) > 64 {
		return fmt.Sprintf("<%d bytes>", len(b))
	}
	return fmt.Sprintf("%q", b)
}
