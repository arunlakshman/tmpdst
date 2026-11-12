package rebalance

import (
	"cmp"
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// join handles a JoinGroup call (rules 3 to 11). The call parks until its round ends.
func (m *Machine) join(s *serving, in Join) {
	g := s.groups[in.Group]
	if g != nil && len(g.members) > 0 && in.ProtocolType != g.protoType {
		m.reply(in.Call, runtime.ErrInconsistentGroupProtocol, nil)
		return
	}
	var mm *member
	if g != nil {
		mm = g.find(in.MemberID)
	}
	if in.MemberID != "" && mm == nil {
		m.reply(in.Call, runtime.ErrUnknownMember, nil)
		return
	}
	if g == nil {
		g = m.newGroup(s, in.Group)
	}
	if len(g.members) == 0 {
		g.protoType = in.ProtocolType
	}
	isNew := mm == nil
	if isNew {
		mm = m.newMember(s, g, newMemberID(in.ClientID, m.cx.Rand()))
		mm.isNew = true
		m.arm(&mm.session, m.cfg.NewMemberTimeout)
	}
	mm.clientID, mm.clientHost, mm.instanceID = in.ClientID, in.ClientHost, in.GroupInstanceID
	mm.sessionMs, mm.rebalanceMs = in.SessionTimeoutMs, in.RebalanceTimeoutMs
	if mm.rebalanceMs <= 0 {
		mm.rebalanceMs = mm.sessionMs // rule 6
	}
	mm.protocols = copyProtocols(in.Protocols)
	jn, ok := g.phase.(*joining)
	if ok {
		m.joinRound(g, jn, mm, isNew)
	} else {
		jn = m.openRound(g, isEmpty(g.phase))
	}
	if old, waits := jn.joins[mm.id]; waits {
		// A newer join of the same member answers the older one (rule 7).
		delete(jn.joins, mm.id)
		m.reply(old, runtime.ErrRebalanceInProgress, nil)
		m.touch(mm)
	}
	jn.joins[mm.id] = in.Call
	s.where[in.Call] = g.name
	m.maybeComplete(s, g, false)
}

// joinRound updates an open round for a join: a new member starts the
// initial delay again, never past the round end (rule 8), and a longer
// rebalance timeout moves the round end later (rule 10).
func (m *Machine) joinRound(g *group, jn *joining, mm *member, isNew bool) {
	if isNew && jn.delay {
		m.arm(&g.delay, m.after(min(m.now+runtime.NodeTime(m.cfg.InitialDelay), jn.end)))
	}
	if until := jn.start + g.longest(); !jn.ended && until > jn.end {
		jn.end = until
		m.arm(&g.round, m.after(jn.end))
	}
}

func isEmpty(p phase) bool {
	_, ok := p.(empty)
	return ok
}

// openRound starts a join round (rules 8 and 9). The parked syncs of a
// Syncing group get ErrRebalanceInProgress. Only a round that starts from
// Empty waits the initial delay.
func (m *Machine) openRound(g *group, initial bool) *joining {
	old := g.phase
	jn := &joining{start: m.now, end: m.now + g.longest(), joins: map[string]CallID{}}
	g.phase = jn
	m.arm(&g.round, m.after(jn.end))
	if initial {
		jn.delay = true
		m.arm(&g.delay, m.after(min(m.now+runtime.NodeTime(m.cfg.InitialDelay), jn.end)))
	} else {
		m.stop(&g.delay)
	}
	if sy, ok := old.(*syncing); ok {
		m.answerParked(g, sy.syncs, runtime.ErrRebalanceInProgress, nil)
	}
	return jn
}

// answerParked gives every parked call in calls the same answer, in member
// id order. Each answered member's session starts again.
func (m *Machine) answerParked(g *group, calls map[string]CallID, err runtime.ErrCode, body Answer) {
	for _, mm := range g.members {
		if call, ok := calls[mm.id]; ok {
			delete(calls, mm.id)
			m.reply(call, err, body)
			m.touch(mm)
		}
	}
}

// maybeComplete ends the round when every member has a parked join and the
// initial delay passed (rule 11), or at the round end (force, rule 12):
// members with no parked join are removed first.
func (m *Machine) maybeComplete(s *serving, g *group, force bool) {
	jn, ok := g.phase.(*joining)
	if !ok {
		return
	}
	late := g.ids(func(mm *member) bool { _, ok := jn.joins[mm.id]; return !ok })
	if !force && (jn.delay || len(late) > 0) {
		return
	}
	if force {
		jn.ended = true // a round that fails now waits for its members with no end
		jn.delay = false
		m.stop(&g.delay)
	}
	if len(late) == 0 {
		m.finishRound(s, g)
		return
	}
	m.remove(s, g, late, removeWrite{cause: causeRoundEnd})
}

// finishRound picks the leader and the protocol and logs the install
// (rules 14 to 16). Every member has a parked join here.
func (m *Machine) finishRound(s *serving, g *group) {
	jn := g.phase.(*joining)
	leader := g.find(g.leader) // the leader stays if it is still a member
	if leader == nil {
		leader = slices.MinFunc(g.members, func(a, b *member) int { return cmp.Compare(a.order, b.order) })
	}
	install := contract.GroupInstall{
		Group: g.name, ProtocolType: g.protoType, Generation: g.generation + 1,
		Leader: leader.id, TimeMs: m.wallMs(),
	}
	for _, p := range leader.protocols {
		if install.Members = withProtocol(g, p.Name); install.Members != nil {
			install.Protocol = p.Name
			break
		}
	}
	if install.Members == nil {
		m.answerParked(g, jn.joins, runtime.ErrInconsistentGroupProtocol, nil)
		return
	}
	m.startWrite(s, g, install, installWrite{op: install})
}

// withProtocol lists the members, in id order, with their metadata for
// protocol. It is nil when a member does not support it.
func withProtocol(g *group, protocol string) []contract.GroupMember {
	list := make([]contract.GroupMember, 0, len(g.members))
	for _, mm := range g.members {
		i := slices.IndexFunc(mm.protocols, func(p Protocol) bool { return p.Name == protocol })
		if i < 0 {
			return nil
		}
		list = append(list, contract.GroupMember{
			MemberID: mm.id, ClientID: mm.clientID, ClientHost: mm.clientHost, GroupInstanceID: mm.instanceID,
			SessionTimeoutMs: mm.sessionMs, RebalanceTimeoutMs: mm.rebalanceMs, Metadata: clone(mm.protocols[i].Metadata),
		})
	}
	return list
}

// installed applies a logged install: the group goes to Syncing and every
// parked join gets its answer; only the leader gets the member list (rule 16).
func (m *Machine) installed(g *group, op contract.GroupInstall) {
	jn := g.phase.(*joining)
	g.generation, g.protocol, g.leader = op.Generation, op.Protocol, op.Leader
	g.phase = &syncing{syncs: map[string]CallID{}}
	m.stop(&g.delay)
	m.arm(&g.round, runtime.Duration(g.longest())) // the sync deadline (rule 24)
	list := make([]Member, 0, len(op.Members))
	for _, gm := range op.Members {
		list = append(list, Member{ID: gm.MemberID, InstanceID: gm.GroupInstanceID, Metadata: gm.Metadata})
	}
	for _, mm := range g.members {
		mm.isNew, mm.synced = false, false
	}
	for _, mm := range g.members {
		call, ok := jn.joins[mm.id]
		if !ok {
			continue
		}
		answer := JoinAnswer{Generation: g.generation, Protocol: g.protocol, Leader: g.leader, MemberID: mm.id}
		if mm.id == g.leader {
			answer.Members = list
		}
		m.reply(call, runtime.OK, answer)
		m.touch(mm)
	}
}

func copyProtocols(ps []Protocol) []Protocol {
	out := make([]Protocol, len(ps))
	for i, p := range ps {
		out[i] = Protocol{Name: p.Name, Metadata: clone(p.Metadata)}
	}
	return out
}
