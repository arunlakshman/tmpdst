package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
	dstsim "github.com/manifesterio/mds/dst/sim"
)

// The host of the rebalance machine (docs/rebalance.md section 11). In
// production server.Node runs the machine in a second loop beside the replica.
// Here each node gets a second dst process ("rb-0" beside "cp-0") that crashes
// and restarts with the replica; a restart takes a new machine, so it starts
// with no memory. The host does what server.Node does, and nothing else:
//
//   - SubmitCommand goes to the replica as a ControlPlaneDeliver from
//     rebalanceFrom; SubmitRead as a LocalRead, or as a ControlPlaneDeliver
//     when Fresh.
//   - A ControlPlaneReply to rebalanceFrom comes back as a ReplicaReply
//     (simNode.outbound looks at To first, as Node.reply does).
//   - After each replica step, a change of "active appender and Ready" is
//     sent as RoleChanged (the AfterStep hook of the replica loop).
//   - A group call gets an id and waits for its Reply. When its deadline
//     passes first, the call is forgotten, CallAbandoned goes to the machine,
//     and a later reply is dropped.
//   - OffsetFetch is not a machine call: behind the role bit it is a
//     LocalRead of the replica.
//
// A real inbox keeps the order in which values were put in it. The dst
// scheduler breaks ties of the same instant at random, so the host keeps two
// queues, one for the machine and one for the replica, and sends a wake token
// for each value: a token takes the value at the head of its queue, whatever
// token comes first.

// rebalanceFrom is who the machine is when it talks to the replica, as in
// server/groups_handler.go.
func rebalanceFrom() machine.NodeInstance { return machine.NodeInstance{Node: "rebalance"} }

// groupsID is the process id of the rebalance machine of node idx.
func groupsID(idx int) dstsim.ProcessID { return dstsim.ProcessID(fmt.Sprintf("rb-%d", idx)) }

// machineConfig is the machine config of node idx, with the times scaled down
// like replicaConfig. faults is the fault injector of the run, so the buggify
// sites of the machine fire and are counted (production passes nil).
func machineConfig(idx int, faults runtime.FaultInjector) rebalance.Config {
	return rebalance.Config{
		ID:               groupsID(idx),
		InitialDelay:     300 * machine.Millisecond,
		NewMemberTimeout: 4 * machine.Second,
		WriteTimeout:     2 * machine.Second,
		Faults:           faults,
	}
}

type (
	machineWake struct{} // take the next input of the machine from its queue
	replicaWake struct{} // take the next value for the replica from the queue of the host
)

// queue is a first in, first out list of values.
type queue struct{ items []any }

func (q *queue) push(v any) { q.items = append(q.items, v) }

func (q *queue) pop() (any, bool) {
	if len(q.items) == 0 {
		return nil, false
	}
	v := q.items[0]
	q.items = q.items[1:]
	return v, true
}

// groupReply is a Reply of the machine. The reply engine hands it to the host.
type groupReply struct {
	node  int
	reply rebalance.Reply
}

// groupHost hosts the rebalance machine of one node and the group calls that
// wait for it. It lives for the whole run; each boot of the node gives it a
// new process with a new machine.
type groupHost struct {
	s *sim
	n *simNode

	proc      *groupProc // the process of this boot; nil while the node is down
	frozen    bool       // the final phase stopped the machine: it takes no more input
	role      bool       // the role bit: the replica is the active appender and Ready
	told      bool       // the role the machine was told last (the last RoleChanged it took)
	in        queue      // inputs of the machine, in the order they were delivered
	toReplica queue      // values for the replica inbox, in order
	held      []any      // inputs that wait while the node is paused, in order
	ran       bool       // the last Step ran the machine (for onStep)
	last      any        // the last input the machine took (for the trace)

	lastToReplica any // the last value the replica took from toReplica (for the trace)

	writes   map[machine.RequestID]sentWrite // commands sent to the replica and not answered yet
	terms    int                             // the terms of the machine that ended: a break, or a role loss
	calls    map[rebalance.CallID]*groupCall // calls that wait for a reply
	gone     map[rebalance.CallID]bool       // calls that were abandoned
	nextCall rebalance.CallID
	reads    map[machine.RequestID]readNote // reads of the machine not answered yet
	sent     map[machine.RequestID]bool     // every request id the machine of this boot used
	timers   map[uint64]bool                // write timers: the timer that the machine sets with each command
}

// sentWrite is a command of the machine that the replica has not answered yet.
type sentWrite struct {
	group string // the group of a write of the membership; "" for an offset commit
	term  int    // the value of groupHost.terms when it was sent
}

// groupCall is one call that waits for the machine.
type groupCall struct {
	try *callTry
}

// readNote is what the host knows about one read of the machine, for the
// coverage notes.
type readNote struct {
	list  bool // a ListGroups read
	load  bool // a read of the load (not of a Describe call)
	fresh bool
}

// groupProc is the dst process of one boot of the machine.
type groupProc struct {
	h *groupHost
	m *rebalance.Machine
}

func (p *groupProc) ID() dstsim.ProcessID { return groupsID(p.h.n.idx) }

func (p *groupProc) StateHash() uint64 { return p.m.StateHash() }

func (p *groupProc) Step(cx dstsim.Context, inbox any) []any { return p.h.step(p, cx, inbox) }

func newGroupHost(s *sim, n *simNode) *groupHost { return &groupHost{s: s, n: n} }

// start makes the machine of a new boot. It has no memory: it is in
// Standby until the replica becomes active and Ready.
func (h *groupHost) start() {
	h.proc = &groupProc{h: h, m: rebalance.New(machineConfig(h.n.idx, h.s.env))}
	h.role, h.told = false, false
	h.sent = map[machine.RequestID]bool{}
	h.timers = map[uint64]bool{}
	h.calls = map[rebalance.CallID]*groupCall{}
	h.gone = map[rebalance.CallID]bool{}
	h.reads = map[machine.RequestID]readNote{}
	h.writes = map[machine.RequestID]sentWrite{}
	h.terms = 0
	h.nextCall = 0
}

// stop ends the boot with the node. Every call that waits gets ErrUnavailable,
// as a client does when the connection closes.
func (h *groupHost) stop() {
	for call := range h.nextCall + 1 { // in call order: the ids of a boot count up from 1
		if gc := h.calls[call]; gc != nil {
			h.s.after(h.s.store.netDelay(h.s.rand), "group-call-closed", func() { gc.try.answered(runtime.ErrUnavailable, nil) })
		}
	}
	h.proc, h.role, h.told = nil, false, false
	h.in, h.toReplica, h.held = queue{}, queue{}, nil
	h.calls, h.gone, h.reads, h.writes, h.sent, h.timers = nil, nil, nil, nil, nil, nil
}

// writing reports that a command of the machine still waits for the replica.
func (h *groupHost) writing() bool { return len(h.writes) > 0 }

// replied takes the replica's answer to the machine: a command is no longer
// in flight. It returns the token that hands the answer to the machine. A
// write of the membership that applied after the term that sent it ended
// makes its group late (groupChecks.late).
func (h *groupHost) replied(d runtime.ControlPlaneDone) dstsim.Send {
	if w, ok := h.writes[d.Req]; ok {
		delete(h.writes, d.Req)
		if w.group != "" && w.term < h.terms && d.Err == runtime.OK {
			h.s.oracle.groups.late[w.group] = true
			h.s.tracef("%s: a write of group %s that the machine gave up applied late", h.n.name(), w.group)
		}
	}
	return h.toMachine(rebalance.ReplicaReply{Req: d.Req, Done: d})
}

// endTerm notes that the machine ended its term (or lost its role): it will
// not take the answers of the writes in flight.
func (h *groupHost) endTerm() { h.terms++ }

// freeze stops the machine for good: the final phase needs a log that no
// longer changes. A frozen machine takes no input and writes nothing.
func (h *groupHost) freeze() { h.frozen = true }

// step is one Step of the machine. A machineWake takes the next input from
// the queue. While the node is paused, inputs wait in held, in order.
func (h *groupHost) step(p *groupProc, cx dstsim.Context, inbox any) []any {
	h.ran = false
	if !h.n.up || h.proc != p || h.frozen {
		return nil
	}
	if _, ok := inbox.(machineWake); ok {
		next, ok := h.in.pop()
		if !ok {
			return nil
		}
		inbox = next
	}
	if h.n.paused {
		h.held = append(h.held, inbox)
		return nil
	}
	h.ran, h.last = true, inbox
	if r, ok := inbox.(rebalance.RoleChanged); ok {
		h.told = r.Active
		if !r.Active {
			h.endTerm()
		}
	}
	outs := p.m.Step(cx, inbox)
	h.note(inbox, outs)
	return h.outbound(outs)
}

// resume gives the inputs held during a pause back to the machine, in order,
// before the inputs that came later.
func (h *groupHost) resume() {
	held := h.held
	h.held = nil
	h.in.items = append(held, h.in.items...)
	for range held {
		h.s.runner.Inject(groupsID(h.n.idx), 0, machineWake{})
	}
}

// toMachine queues an input of the machine and returns the token to send.
func (h *groupHost) toMachine(v any) dstsim.Send {
	h.in.push(v)
	return dstsim.Send{To: groupsID(h.n.idx), Body: machineWake{}}
}

// deliver queues an input of the machine from outside the processes (a call
// that came in from the network, or an abandoned call).
func (h *groupHost) deliver(v any) {
	h.in.push(v)
	h.s.runner.Inject(groupsID(h.n.idx), 0, machineWake{})
}

// replicaSend queues a value for the replica and returns the token to send.
func (h *groupHost) replicaSend(v runtime.EventData) dstsim.Send {
	h.toReplica.push(v)
	return dstsim.Send{To: nodeID(h.n.idx), Body: replicaWake{}}
}

// afterStep is the AfterStep hook of the replica loop: when "active appender
// and Ready" changes, the machine is told, after the other outputs of the step.
func (h *groupHost) afterStep(out []any) []any {
	r := h.n.replica()
	active := r.IsActive() && r.Ready()
	if active == h.role {
		return out
	}
	h.role = active
	return append(out, h.toMachine(rebalance.RoleChanged{Active: active}))
}

// outbound turns the outputs of the machine into what the runner takes. It
// also checks two promises of the machine to its host (docs/rebalance.md
// sections 3 and 9): a request id is never used twice in one run of the
// process (Node.reply depends on it), and no call is answered OK while the
// machine was last told that it does not have the role.
func (h *groupHost) outbound(outs []any) []any {
	var out []any
	for _, o := range outs {
		switch o := o.(type) {
		case dstsim.SetTimer, dstsim.CancelTimer, dstsim.Send:
			out = append(out, o) // timers, and sends of the machine to itself
		case rebalance.SubmitCommand:
			h.newRequest(o.Req)
			h.writes[o.Req] = sentWrite{group: groupOf(o.Op), term: h.terms}
			out = append(out, h.replicaSend(runtime.ControlPlaneDeliver{From: rebalanceFrom(), Req: o.Req, Op: o.Op}))
		case rebalance.SubmitRead:
			h.newRequest(o.Req)
			if _, list := o.Op.(contract.ListGroups); list && o.Fresh {
				h.endTerm() // a fresh load: the term ended
			}
			if o.Fresh {
				out = append(out, h.replicaSend(runtime.ControlPlaneDeliver{From: rebalanceFrom(), Req: o.Req, Op: o.Op}))
			} else {
				out = append(out, h.replicaSend(runtime.LocalRead{From: rebalanceFrom(), Req: o.Req, Op: o.Op}))
			}
		case rebalance.Reply:
			if o.Err == runtime.OK && !h.told {
				h.s.violation("a machine answers only with the role", "%s: the machine answered call %d OK after it was told that it lost the role", h.n.name(), o.Call)
			}
			out = append(out, groupReply{node: h.n.idx, reply: o})
		default:
			h.s.violation("known action", "the rebalance machine of %s returned %T", h.n.name(), o)
		}
	}
	return out
}

// newRequest notes a request id of the machine. It must be new in this boot.
func (h *groupHost) newRequest(req machine.RequestID) {
	if h.sent[req] {
		h.s.violation("request ids never repeat", "%s: the machine sent request %d twice in one run", h.n.name(), req)
	}
	h.sent[req] = true
}

// groupOf is the group of a write of the membership, "" for an offset commit.
func groupOf(op runtime.ControlPlaneOperation) string {
	switch w := op.(type) {
	case contract.GroupInstall:
		return w.Group
	case contract.GroupAssign:
		return w.Group
	case contract.GroupRemove:
		return w.Group
	}
	return ""
}

// startCall is a group call that reached the node. It gets an id and goes
// to the machine. If it has no reply when its deadline passes, the call is
// abandoned: the machine is told, and the client sees a timeout.
func (h *groupHost) startCall(t *callTry) {
	if !h.n.up {
		h.refuse(t)
		return
	}
	h.nextCall++
	call := h.nextCall
	gc := &groupCall{try: t}
	h.calls[call] = gc
	t.quiet = h.s.quiet
	h.deliver(t.input(call))
	h.s.after(t.deadline, "group-call-deadline", func() { h.deadline(call, gc) })
}

// refuse answers a call to a node that is down, as a refused connection.
func (h *groupHost) refuse(t *callTry) {
	h.s.after(h.s.store.netDelay(h.s.rand), "group-call-refused", func() { t.answered(runtime.ErrUnavailable, nil) })
}

func (h *groupHost) deadline(call rebalance.CallID, gc *groupCall) {
	if h.calls == nil || h.calls[call] != gc {
		return // answered, or the node went down
	}
	delete(h.calls, call)
	h.gone[call] = true
	h.s.env.Reachable("rebalance-sim: a call is abandoned")
	h.deliver(rebalance.CallAbandoned{Call: call})
	if !gc.try.onPurpose && gc.try.quiet {
		h.s.violation("every call gets an answer", "%s: %s sent call %d in the quiet tail and had no answer after %dms",
			h.n.name(), gc.try.c.name, call, gc.try.deadline/machine.Millisecond)
	}
	gc.try.answered(runtime.ErrTimeout, nil)
}

// answer gives a reply of the machine to the call that waits for it. A reply
// to an abandoned call is dropped, as in production. A reply to a call that
// never waited, or a second reply, is a failure.
func (h *groupHost) answer(r rebalance.Reply) {
	if h.calls == nil {
		return
	}
	gc, ok := h.calls[r.Call]
	if !ok {
		if h.gone[r.Call] {
			h.s.env.Reachable("rebalance-sim: a late reply to an abandoned call is dropped")
			return
		}
		h.s.violation("every call gets one answer", "%s: the machine answered call %d, which does not wait for an answer", h.n.name(), r.Call)
		return
	}
	delete(h.calls, r.Call)
	h.s.after(h.s.store.netDelay(h.s.rand), "group-reply", func() { gc.try.answered(r.Err, r.Body) })
}

// fetch is the OffsetFetch handler: behind the role bit, a LocalRead of the
// replica. The reply comes back to the consumer as a client reply.
func (h *groupHost) fetch(t *callTry, req machine.RequestID) {
	if !h.n.up || !h.role {
		h.refuse(t)
		return
	}
	t.req = req
	h.toReplica.push(runtime.LocalRead{From: t.c.from, Req: req, Op: t.op.fetch})
	h.s.runner.Inject(nodeID(h.n.idx), 0, replicaWake{})
}

// note marks the paths of the machine that the coverage report lists. The
// machine has no assertions of its own, so the host reads them from its
// inputs and outputs.
func (h *groupHost) note(in any, outs []any) {
	o := h.noteOutputs(in, outs)
	env := h.s.env
	switch in := in.(type) {
	case runtime.TimerFired:
		switch {
		case o.freshList && h.timers[uint64(in.ID)]:
			env.Reachable("rebalance-sim: the write timer ends the term")
		case o.sentRead:
			env.Reachable("rebalance-sim: a refused load read is sent again")
		}
	case rebalance.ReplicaReply:
		if o.freshList {
			env.Reachable("rebalance-sim: a failed write ends the term")
		}
		h.noteRead(in)
	case rebalance.RoleChanged:
		if !in.Active && o.unavailable {
			env.Reachable("rebalance-sim: the role is lost while calls wait")
		}
	}
}

// stepOutputs is what note found in the outputs of one step.
type stepOutputs struct {
	freshList   bool // a fresh ListGroups read: the term ended
	sentRead    bool
	unavailable bool // a call was answered ErrUnavailable
}

// noteOutputs remembers the reads and the write timers of one step.
func (h *groupHost) noteOutputs(in any, outs []any) stepOutputs {
	var r stepOutputs
	afterSubmit := false
	for _, o := range outs {
		if st, ok := o.(dstsim.SetTimer); ok && afterSubmit {
			h.timers[st.ID] = true // the machine arms the timer of a write right after it
		}
		_, afterSubmit = o.(rebalance.SubmitCommand)
		switch o := o.(type) {
		case rebalance.SubmitRead:
			_, list := o.Op.(contract.ListGroups)
			_, describeCall := in.(rebalance.Describe)
			h.reads[o.Req] = readNote{list: list, load: !describeCall, fresh: o.Fresh}
			r.sentRead = true
			r.freshList = r.freshList || (list && o.Fresh)
		case rebalance.SubmitCommand:
			if rm, ok := o.Op.(contract.GroupRemove); ok && rm.Reason == "expire" {
				h.s.consumers.expiring(rm)
			}
		case rebalance.Reply:
			r.unavailable = r.unavailable || o.Err == runtime.ErrUnavailable
		}
	}
	return r
}

// noteRead marks the paths of a read that came back.
func (h *groupHost) noteRead(in rebalance.ReplicaReply) {
	r, ok := h.reads[in.Req]
	if !ok {
		return
	}
	delete(h.reads, in.Req)
	if in.Done.Err != runtime.OK {
		return
	}
	if r.list && r.fresh {
		h.s.env.Reachable("rebalance-sim: groups are read again from a fresh tail")
	}
	if d, ok := in.Done.Resp.(contract.DescribeGroupResp); ok && r.load && !r.fresh && len(d.Members) > 0 {
		h.s.env.Reachable("rebalance-sim: a takeover loads a group with members")
	}
}
