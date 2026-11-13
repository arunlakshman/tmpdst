package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// The host of the rebalance machine (docs/rebalance.md section 11). In
// production server.Node runs the machine in a second loop beside the replica.
// Here each node gets a second kernel node ("rb-0" beside "cp-0") that crashes
// and restarts with the replica; a restart takes a new machine, so it starts
// with no memory. The host does what server.Node does, and nothing else:
//
//   - SubmitCommand goes to the replica as a ControlPlaneDeliver from
//     rebalanceFrom; SubmitRead as a LocalRead, or as a ControlPlaneDeliver
//     when Fresh.
//   - A ControlPlaneReply to rebalanceFrom comes back as a ReplicaReply
//     (simNode.reply looks at To first, as Node.reply does).
//   - After each replica step, a change of "active appender and Ready" is
//     sent as RoleChanged (the AfterStep hook of the replica loop).
//   - A group call gets an id and waits for its Reply. When its deadline
//     passes first, the call is forgotten, CallAbandoned goes to the machine,
//     and a later reply is dropped.
//   - OffsetFetch is not a machine call: behind the role bit it is a
//     LocalRead of the replica.
//
// A real inbox keeps the order in which values were put in it. The kernel
// breaks ties of the same instant at random, so the host sends the values
// that come from outside the machine on two lanes of each node, one for the
// machine and one for the replica. On one lane, values of one instant keep
// their order.

// rebalanceFrom is who the machine is when it talks to the replica, as in
// server/groups_handler.go.
func rebalanceFrom() machine.NodeInstance { return machine.NodeInstance{Node: "rebalance"} }

// groupsID is the node name of the rebalance machine of node idx.
func groupsID(idx int) machine.NodeID { return machine.NodeID(fmt.Sprintf("rb-%d", idx)) }

// machineConfig is the machine config, with the times scaled down like replicaConfig.
// The buggify sites of the machine come from the env of the run, so they fire and are
// counted (production has an env where none fires).
func machineConfig() rebalance.Config {
	return rebalance.Config{
		InitialDelay:     300 * machine.Millisecond,
		NewMemberTimeout: 4 * machine.Second,
		WriteTimeout:     2 * machine.Second,
	}
}

// groupLane is the key of a lane: the values for the machine of a node, or the
// values for the replica of a node.
type groupLane struct {
	node      int
	toReplica bool
}

// groupHost hosts the rebalance machine of one node and the group calls that
// wait for it. It lives for the whole run; each boot of the node gives it a
// new machine.
type groupHost struct {
	s *sim
	n *simNode

	addr          kernel.Addr[rebalance.Input]
	toMachineLane kernel.Lane // values for the machine, in the order they were sent
	toReplicaLane kernel.Lane // values for the replica, in the order they were sent

	role bool // the role bit: the replica is the active appender and Ready
	told bool // the role the machine was told last (the last RoleChanged it took)

	writes   map[machine.RequestID]sentWrite // commands sent to the replica and not answered yet
	terms    int                             // the terms of the machine that ended: a break, or a role loss
	calls    map[rebalance.CallID]*groupCall // calls that wait for a reply
	gone     map[rebalance.CallID]bool       // calls that were abandoned
	nextCall rebalance.CallID
	reads    map[machine.RequestID]readNote // reads of the machine not answered yet
	sent     map[machine.RequestID]bool     // every request id the machine of this boot used
	timers   map[machine.TimerID]bool       // write timers: the timer that the machine sets with each command
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

// groupMachine is the rebalance machine as the kernel node runs it. Around
// each Step it does the bookkeeping of the host: before it, the role the
// machine is told; after it, the coverage notes.
type groupMachine struct {
	h *groupHost
	m *rebalance.Machine
}

func (g *groupMachine) StateHash() uint64 { return g.m.StateHash() }

func (g *groupMachine) Step(now machine.Now, in rebalance.Input) []rebalance.Output {
	h := g.h
	if r, ok := in.(rebalance.RoleChanged); ok {
		h.told = r.Active
		if !r.Active {
			h.endTerm()
		}
	}
	outs := g.m.Step(now, in)
	h.note(in, outs)
	return outs
}

// newGroupHost spawns the node of the rebalance machine of n. It is down until
// n boots it.
func newGroupHost(s *sim, n *simNode) *groupHost {
	h := &groupHost{
		s: s, n: n,
		toMachineLane: s.lanes.Get(groupLane{node: n.idx}),
		toReplicaLane: s.lanes.Get(groupLane{node: n.idx, toReplica: true}),
	}
	h.addr = kernel.Spawn(s.w, kernel.NodeSpec[rebalance.Input, rebalance.Output]{
		Name:  groupsID(n.idx),
		New:   h.newMachine,
		Route: h.route,
	})
	return h
}

// newMachine is the factory of the kernel node: it runs at each boot. The
// machine has no memory: it is in Standby until the replica becomes active
// and Ready.
func (h *groupHost) newMachine(env machine.Env, _ machine.NodeInstance) machine.Machine[rebalance.Input, rebalance.Output] {
	h.role, h.told = false, false
	h.sent = map[machine.RequestID]bool{}
	h.timers = map[machine.TimerID]bool{}
	h.calls = map[rebalance.CallID]*groupCall{}
	h.gone = map[rebalance.CallID]bool{}
	h.reads = map[machine.RequestID]readNote{}
	h.writes = map[machine.RequestID]sentWrite{}
	h.terms = 0
	h.nextCall = 0
	return &groupMachine{h: h, m: rebalance.New(machineConfig(), env)}
}

// stop ends the boot with the node. Every call that waits gets ErrUnavailable,
// as a client does when the connection closes.
func (h *groupHost) stop() {
	for call := range h.nextCall + 1 { // in call order: the ids of a boot count up from 1
		if gc := h.calls[call]; gc != nil {
			h.s.after(h.s.store.netDelay(h.s.rand), func() { gc.try.answered(runtime.ErrUnavailable, nil) })
		}
	}
	h.role, h.told = false, false
	h.calls, h.gone, h.reads, h.writes, h.sent, h.timers = nil, nil, nil, nil, nil, nil
}

// writing reports that a command of the machine still waits for the replica.
func (h *groupHost) writing() bool { return len(h.writes) > 0 }

// replied takes the replica's answer to the machine: a command is no longer
// in flight. It sends the answer to the machine. A write of the membership that
// applied after the term that sent it ended makes its group late
// (groupChecks.late).
func (h *groupHost) replied(r *kernel.Router[runtime.EventData], d runtime.ControlPlaneDone) {
	if w, ok := h.writes[d.Req]; ok {
		delete(h.writes, d.Req)
		if w.group != "" && w.term < h.terms && d.Err == runtime.OK {
			h.s.oracle.groups.late[w.group] = true
			h.s.tracef("%s: a write of group %s that the machine gave up applied late", h.n.name(), w.group)
		}
	}
	kernel.SendOn(r, h.toMachineLane, h.addr, rebalance.Input(rebalance.ReplicaReply{Req: d.Req, Done: d}))
}

// endTerm notes that the machine ended its term (or lost its role): it will
// not take the answers of the writes in flight.
func (h *groupHost) endTerm() { h.terms++ }

// freeze stops the machine for good: the final phase needs a log that no
// longer changes. A frozen machine takes no step: its inputs wait in its inbox.
func (h *groupHost) freeze() { h.s.w.Pause(h.addr) }

// deliver sends an input to the machine from outside the nodes (a call that
// came in from the network, or an abandoned call), on the lane of the machine.
func (h *groupHost) deliver(v rebalance.Input) {
	kernel.InjectOn(h.s.w, h.addr, h.toMachineLane, v)
}

// afterStep is the AfterStep hook of the replica loop: when "active appender
// and Ready" changes, the machine is told.
func (h *groupHost) afterStep() {
	r := h.n.replica()
	active := r.IsActive() && r.Ready()
	if active == h.role {
		return
	}
	h.role = active
	h.deliver(rebalance.RoleChanged{Active: active})
}

// route handles one output of the machine. It is a copy of routeGroups of the
// server (controlplane/internal/controlplane/server). It also checks two
// promises of the machine to its host (docs/rebalance.md sections 3 and 9): a
// request id is never used twice in one run of the process (Node.reply depends
// on it), and no call is answered OK while the machine was last told that it
// does not have the role. Every case of rebalance.Output is listed, so a new
// output without a case does not compile.
func (h *groupHost) route(r *kernel.Router[rebalance.Input], out rebalance.Output) {
	switch o := out.(type) {
	case rebalance.SetTimer:
		r.SetTimer(o.ID, o.After, rebalance.TimerFired{ID: o.ID, Gen: o.Gen})
	case rebalance.CancelTimer:
		r.CancelTimer(o.ID)
	case rebalance.SendSelf:
		r.SendSelf(o.Msg)
	case rebalance.SubmitCommand:
		h.newRequest(o.Req)
		h.writes[o.Req] = sentWrite{group: groupOf(o.Op), term: h.terms}
		h.toReplica(r, runtime.ControlPlaneDeliver{From: rebalanceFrom(), Req: o.Req, Op: o.Op})
	case rebalance.SubmitRead:
		h.newRequest(o.Req)
		if _, list := o.Op.(contract.ListGroups); list && o.Fresh {
			h.endTerm() // a fresh load: the term ended
		}
		if o.Fresh {
			h.toReplica(r, runtime.ControlPlaneDeliver{From: rebalanceFrom(), Req: o.Req, Op: o.Op})
		} else {
			h.toReplica(r, runtime.LocalRead{From: rebalanceFrom(), Req: o.Req, Op: o.Op})
		}
	case rebalance.Reply:
		if o.Err == runtime.OK && !h.told {
			h.s.violation(machineAnswersOnlyWithRole, "%s: the machine answered call %d OK after it was told that it lost the role", h.n.name(), o.Call)
		}
		h.answer(o)
	}
}

// toReplica sends a value to the replica of the node, on the lane of the replica.
func (h *groupHost) toReplica(r *kernel.Router[rebalance.Input], v runtime.EventData) {
	kernel.SendOn(r, h.toReplicaLane, h.n.addr, v)
}

// newRequest notes a request id of the machine. It must be new in this boot.
func (h *groupHost) newRequest(req machine.RequestID) {
	if h.sent[req] {
		h.s.violation(requestIDsNeverRepeat, "%s: the machine sent request %d twice in one run", h.n.name(), req)
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
	if !h.n.up() {
		h.refuse(t)
		return
	}
	h.nextCall++
	call := h.nextCall
	gc := &groupCall{try: t}
	h.calls[call] = gc
	t.quiet = h.s.quiet
	h.deliver(t.input(call))
	h.s.after(t.deadline, func() { h.deadline(call, gc) })
}

// refuse answers a call to a node that is down, as a refused connection.
func (h *groupHost) refuse(t *callTry) {
	h.s.after(h.s.store.netDelay(h.s.rand), func() { t.answered(runtime.ErrUnavailable, nil) })
}

func (h *groupHost) deadline(call rebalance.CallID, gc *groupCall) {
	if h.calls == nil || h.calls[call] != gc {
		return // answered, or the node went down
	}
	delete(h.calls, call)
	h.gone[call] = true
	h.s.env.Reachable(callAbandoned)
	h.deliver(rebalance.CallAbandoned{Call: call})
	if !gc.try.onPurpose && gc.try.quiet {
		h.s.violation(everyCallGetsAnswer, "%s: %s sent call %d in the quiet tail and had no answer after %dms",
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
			h.s.env.Reachable(lateReplyAbandonedCallDropped)
			return
		}
		h.s.violation(everyCallGetsOneAnswer, "%s: the machine answered call %d, which does not wait for an answer", h.n.name(), r.Call)
		return
	}
	delete(h.calls, r.Call)
	h.s.after(h.s.store.netDelay(h.s.rand), func() { gc.try.answered(r.Err, r.Body) })
}

// fetch is the OffsetFetch handler: behind the role bit, a LocalRead of the
// replica. The reply comes back to the consumer as a client reply.
func (h *groupHost) fetch(t *callTry, req machine.RequestID) {
	if !h.n.up() || !h.role {
		h.refuse(t)
		return
	}
	t.req = req
	kernel.InjectOn[runtime.EventData](h.s.w, h.n.addr, h.toReplicaLane, runtime.LocalRead{From: t.c.from, Req: req, Op: t.op.fetch})
}

// note marks the paths of the machine that the coverage report lists. The
// machine has no assertions of its own, so the host reads them from its
// inputs and outputs.
func (h *groupHost) note(in rebalance.Input, outs []rebalance.Output) {
	o := h.noteOutputs(in, outs)
	env := h.s.env
	// Not a switch: a sum type switch must list every case, and the machine
	// has a private input that code outside its package cannot name.
	if t, ok := in.(rebalance.TimerFired); ok {
		switch {
		case o.freshList && h.timers[t.ID]:
			env.Reachable(writeTimerEndsTerm)
		case o.sentRead:
			env.Reachable(refusedLoadReadSentAgain)
		}
	}
	if r, ok := in.(rebalance.ReplicaReply); ok {
		if o.freshList {
			env.Reachable(failedWriteEndsTerm)
		}
		h.noteRead(r)
	}
	if r, ok := in.(rebalance.RoleChanged); ok && !r.Active && o.unavailable {
		env.Reachable(roleLostWhileCallsWait)
	}
}

// stepOutputs is what note found in the outputs of one step.
type stepOutputs struct {
	freshList   bool // a fresh ListGroups read: the term ended
	sentRead    bool
	unavailable bool // a call was answered ErrUnavailable
}

// noteOutputs remembers the reads and the write timers of one step.
func (h *groupHost) noteOutputs(in rebalance.Input, outs []rebalance.Output) stepOutputs {
	var r stepOutputs
	afterSubmit := false
	for _, o := range outs {
		if st, ok := o.(rebalance.SetTimer); ok && afterSubmit {
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
		case rebalance.SetTimer, rebalance.CancelTimer, rebalance.SendSelf:
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
		h.s.env.Reachable(groupsReadAgainFromFreshTail)
	}
	if d, ok := in.Done.Resp.(contract.DescribeGroupResp); ok && r.load && !r.fresh && len(d.Members) > 0 {
		h.s.env.Reachable(takeoverLoadsGroupMembers)
	}
}
