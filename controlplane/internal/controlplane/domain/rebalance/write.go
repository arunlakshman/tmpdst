package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// write is the one log write a group has in flight. The group is busy until
// its ReplicaReply, or until its write timer ends the term (section 8).
type write struct {
	req     machine.RequestID
	timer   timer
	kind    any  // installWrite, assignWrite or removeWrite: what to do with the answer
	dropped bool // the caller abandoned the call: apply the answer, but send no reply
}

type (
	// installWrite ends a join round (rule 16).
	installWrite struct {
		op contract.GroupInstall
	}
	// assignWrite stores the leader's assignment (rule 22).
	assignWrite struct {
		call CallID
		op   contract.GroupAssign
	}
	// removeWrite removes members (rules 12, 24, 26, 28). call and codes are
	// for a leave only.
	removeWrite struct {
		cause cause
		ids   []string
		call  CallID
		codes []runtime.ErrCode
	}
)

// cause is why members are removed.
type cause uint8

const (
	causeLeave        cause = iota + 1 // a LeaveGroup call
	causeSession                       // a session timer
	causeRoundEnd                      // the round end
	causeSyncDeadline                  // the sync deadline
)

// groupReq marks a request id as the write of a group.
type groupReq struct{ group string }

// caller returns the call that waits for w, if any.
func (w *write) caller() (CallID, bool) {
	switch k := w.kind.(type) {
	case assignWrite:
		return k.call, true
	case removeWrite:
		return k.call, k.cause == causeLeave
	}
	return 0, false
}

// startWrite logs op for g. g is busy until the answer.
func (m *Machine) startWrite(s *serving, g *group, op runtime.ControlPlaneOperation, kind any) {
	w := &write{req: m.submit(op), timer: m.newTimer(), kind: kind}
	s.reqs[w.req] = groupReq{group: g.name}
	s.owners[w.timer.id] = timerRef{kind: timerWrite, group: g.name}
	m.arm(&w.timer, m.cfg.WriteTimeout)
	if call, ok := w.caller(); ok {
		s.where[call] = g.name
	}
	g.write = w
}

// replicaReply takes the answer of a write or a read. An unknown request id
// (a late answer from an older term, or a second copy) is ignored.
func (m *Machine) replicaReply(s *serving, r ReplicaReply) {
	p, ok := s.reqs[r.Req]
	if !ok {
		return
	}
	delete(s.reqs, r.Req)
	if m.maybe(SiteDuplicateReply) {
		m.send(r) // a second copy comes later; its request id is unknown then
	}
	switch p := p.(type) {
	case groupReq:
		g := s.groups[p.group]
		w := g.write
		g.write = nil
		m.stop(&w.timer)
		delete(s.owners, w.timer.id)
		m.settle(s, g, w, r.Done)
		if m.state == s {
			m.drain(s, g)
		}
	case *commit:
		m.commitDone(s, p, r.Done)
	case describeRead:
		m.describeDone(s, p, r.Done)
	}
}

// settle applies the answer of a write. OK: apply. ErrTooLarge or
// ErrInvalid: nothing was written, the caller gets the code. Anything else
// may have changed the log, or shows that memory and log differ: the term ends.
func (m *Machine) settle(s *serving, g *group, w *write, done runtime.ControlPlaneDone) {
	switch done.Err {
	case runtime.OK:
		m.applied(s, g, w)
	case runtime.ErrTooLarge, runtime.ErrInvalid:
		m.refused(s, g, w, done.Err)
	default:
		m.breakTerm(s, w)
	}
}

func (m *Machine) applied(s *serving, g *group, w *write) {
	switch k := w.kind.(type) {
	case installWrite:
		m.installed(g, k.op)
	case assignWrite:
		m.assigned(g, k, w.dropped)
	case removeWrite:
		m.removed(s, g, k, w.dropped)
	}
}

// refused gives the code of a refused write to its callers (rules 17 and 26).
func (m *Machine) refused(s *serving, g *group, w *write, code runtime.ErrCode) {
	switch k := w.kind.(type) {
	case installWrite:
		m.answerParked(g, g.phase.(*joining).joins, code, nil)
	case assignWrite:
		m.answerCaller(s, w, k.call, code, nil)
	case removeWrite:
		m.removeRefused(s, g, w, k, code)
	}
}

// answerCaller replies to the caller of w, unless it was abandoned.
func (m *Machine) answerCaller(s *serving, w *write, call CallID, code runtime.ErrCode, body Answer) {
	if w.dropped {
		delete(s.where, call)
		return
	}
	m.reply(call, code, body)
}

// drain takes the queued inputs of g in order, until one starts a write.
func (m *Machine) drain(s *serving, g *group) {
	if len(g.queue) > 0 && m.maybe(SiteLateDrain) {
		g.draining = true
		m.send(drainLater{group: g.name})
		return
	}
	for m.state == s && !g.busy() && len(g.queue) > 0 {
		in := g.queue[0]
		g.queue = slices.Delete(g.queue, 0, 1)
		m.handle(s, in)
	}
}

// drainLater is sent by the machine to itself when the late drain site fires.
// Until it comes, the group stays busy, so the order of its inputs is kept.
type drainLater struct{ group string }

func (m *Machine) drainNow(s *serving, d drainLater) {
	if g := s.groups[d.group]; g != nil && g.draining {
		g.draining = false
		m.drain(s, g)
	}
}

// breakTerm ends the term after a write that may have changed the log (rule
// 36). w is the write that failed, already taken off its group. Every call
// gets ErrUnavailable, every timer stops, memory is dropped, and every group
// is read again from a fresh tail.
func (m *Machine) breakTerm(s *serving, w *write) {
	m.dropMemory(s, w)
	m.startLoad(true)
}

// dropMemory answers every waiting call with ErrUnavailable and stops every
// timer (rule 34). extra is a write that was already taken off its group.
func (m *Machine) dropMemory(s *serving, extra *write) {
	if extra != nil {
		m.stop(&extra.timer)
		if call, ok := extra.caller(); ok {
			m.answerCaller(s, extra, call, runtime.ErrUnavailable, nil)
		}
	}
	for _, name := range s.names {
		m.dropGroup(s, s.groups[name])
	}
	for _, d := range s.describes {
		m.reply(d.call, runtime.ErrUnavailable, nil)
	}
	for _, c := range s.commits {
		m.stop(&c.timer)
		m.reply(c.call, runtime.ErrUnavailable, nil)
	}
	s.describes, s.commits = nil, nil
}

func (m *Machine) dropGroup(s *serving, g *group) {
	m.stopAll(g)
	if w := g.write; w != nil {
		if call, ok := w.caller(); ok {
			m.answerCaller(s, w, call, runtime.ErrUnavailable, nil)
		}
	}
	for _, mm := range g.members {
		if call, ok := g.parked(mm); ok {
			m.reply(call, runtime.ErrUnavailable, nil)
		}
	}
	for _, in := range g.queue {
		if call, ok := callOf(in); ok {
			m.reply(call, runtime.ErrUnavailable, nil)
		}
	}
}
