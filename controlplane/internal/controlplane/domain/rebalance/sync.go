package rebalance

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// check is rule 18: an unknown group or member is ErrUnknownMember, then
// another generation is ErrIllegalGeneration.
func check(s *serving, group, id string, generation int32) (*group, *member, runtime.ErrCode) {
	g := s.groups[group]
	if g == nil {
		return nil, nil, runtime.ErrUnknownMember
	}
	mm := g.find(id)
	switch {
	case mm == nil:
		return g, nil, runtime.ErrUnknownMember
	case generation != g.generation:
		return g, mm, runtime.ErrIllegalGeneration
	}
	return g, mm, runtime.OK
}

// syncGroup handles a SyncGroup call (rules 19 to 24).
func (m *Machine) syncGroup(s *serving, in Sync) {
	g, mm, code := check(s, in.Group, in.MemberID, in.Generation)
	if code != runtime.OK {
		m.reply(in.Call, code, nil)
		return
	}
	switch p := g.phase.(type) {
	case *stable:
		// The stored assignment, at once. The session does not start again (rule 23).
		mm.synced = true
		m.allSynced(g)
		m.reply(in.Call, runtime.OK, SyncAnswer{Assignment: clone(p.assign[mm.id])})
	case *syncing:
		if mm.id == g.leader {
			m.leaderSync(s, g, in)
			return
		}
		mm.synced = true
		if old, waits := p.syncs[mm.id]; waits {
			delete(p.syncs, mm.id)
			m.reply(old, runtime.ErrRebalanceInProgress, nil)
			m.touch(mm)
		}
		p.syncs[mm.id] = in.Call
		s.where[in.Call] = g.name
	default:
		m.reply(in.Call, runtime.ErrRebalanceInProgress, nil) // rule 19
	}
}

// leaderSync checks the leader's assignment (rule 21) and logs it (rule 22).
func (m *Machine) leaderSync(s *serving, g *group, in Sync) {
	list := make([]contract.GroupAssignment, 0, len(in.Assignments))
	for i, a := range in.Assignments {
		if g.find(a.Member) == nil {
			m.reply(in.Call, runtime.ErrUnknownMember, nil)
			return
		}
		if slices.ContainsFunc(in.Assignments[:i], func(b Assignment) bool { return b.Member == a.Member }) {
			m.reply(in.Call, runtime.ErrInvalid, nil)
			return
		}
		list = append(list, contract.GroupAssignment{Member: a.Member, Bytes: clone(a.Bytes)})
	}
	op := contract.GroupAssign{Group: g.name, Generation: g.generation, Leader: g.leader, TimeMs: m.wallMs(), Assignments: list}
	m.startWrite(s, g, op, assignWrite{call: in.Call, op: op})
}

// assigned applies a logged assignment: the group is Stable, and the leader
// and every parked follower get their own bytes (rule 22).
func (m *Machine) assigned(g *group, k assignWrite, dropped bool) {
	sy := g.phase.(*syncing)
	st := &stable{assign: map[string][]byte{}}
	for _, a := range k.op.Assignments {
		if len(a.Bytes) > 0 {
			st.assign[a.Member] = a.Bytes
		}
	}
	g.phase = st
	leader := g.find(g.leader)
	leader.synced = true
	for _, mm := range g.members {
		if call, ok := sy.syncs[mm.id]; ok {
			m.reply(call, runtime.OK, SyncAnswer{Assignment: clone(st.assign[mm.id])})
			m.touch(mm)
		}
	}
	m.touch(leader)
	m.allSynced(g)
	if !dropped {
		m.reply(k.call, runtime.OK, SyncAnswer{Assignment: clone(st.assign[leader.id])})
	}
}

// allSynced stops the sync deadline when every member has synced (rule 24).
func (m *Machine) allSynced(g *group) {
	if !slices.ContainsFunc(g.members, func(mm *member) bool { return !mm.synced }) {
		m.stop(&g.round)
	}
}

// heartbeat handles a Heartbeat call (rule 25). It starts the session again.
func (m *Machine) heartbeat(s *serving, in Heartbeat) {
	g, mm, code := check(s, in.Group, in.MemberID, in.Generation)
	if code != runtime.OK {
		m.reply(in.Call, code, nil)
		return
	}
	m.touch(mm)
	if _, ok := g.phase.(*joining); ok {
		m.reply(in.Call, runtime.ErrRebalanceInProgress, nil)
		return
	}
	m.reply(in.Call, runtime.OK, nil)
}

// abandon forgets a call whose RPC context ended (rule 33). A queued call is
// taken out; a call whose write is in flight gets no reply, but the write is
// applied; a parked call is forgotten in order with the other inputs.
func (m *Machine) abandon(s *serving, call CallID) {
	if m.abandonRead(s, call) || m.abandonCommit(s, call) {
		return
	}
	name, ok := s.where[call]
	if !ok {
		return
	}
	g := s.groups[name]
	if !g.busy() {
		m.abandonParked(s, call)
		return
	}
	if i := slices.IndexFunc(g.queue, func(in any) bool { c, ok := callOf(in); return ok && c == call }); i >= 0 {
		g.queue = slices.Delete(g.queue, i, i+1)
		delete(s.where, call)
		return
	}
	if w := g.write; w != nil {
		if c, ok := w.caller(); ok && c == call {
			w.dropped = true
			delete(s.where, call)
			return
		}
	}
	g.queue = append(g.queue, CallAbandoned{Call: call})
}

// abandonParked forgets a parked join or sync. The member stays, but no
// longer counts as joined, and its session starts again.
func (m *Machine) abandonParked(s *serving, call CallID) {
	name, ok := s.where[call]
	if !ok {
		return
	}
	delete(s.where, call)
	g := s.groups[name]
	for _, mm := range g.members {
		if c, ok := g.parked(mm); ok && c == call {
			g.unpark(mm)
			m.touch(mm)
			return
		}
	}
}
