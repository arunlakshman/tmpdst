package rebalance

import (
	"cmp"
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// loading reads every group from the log (section 9). It answers every call
// ErrUnavailable. fresh: the reads wait for a fresh log tail (after a broken
// term); else they are LocalReads (after a takeover).
type loading struct {
	fresh  bool
	listed bool     // the ListGroups read is back
	names  []string // the groups to read, from ListGroups
	descs  map[string]contract.DescribeGroupResp
	reqs   map[runtime.RequestID]loadRead
	again  []loadRead // refused reads, sent again when retry fires (rule 39)
	retry  timer
}

// loadRead is one read of the load: ListGroups, or DescribeGroup of name.
type loadRead struct {
	list bool
	name string
}

// startLoad drops into Loading and reads the list of groups.
func (m *Machine) startLoad(fresh bool) {
	l := &loading{fresh: fresh, descs: map[string]contract.DescribeGroupResp{}, reqs: map[runtime.RequestID]loadRead{}, retry: m.newTimer()}
	m.state = l
	m.sendLoadRead(l, loadRead{list: true})
}

func (m *Machine) sendLoadRead(l *loading, r loadRead) {
	var op runtime.ControlPlaneOperation = contract.ListGroups{}
	if !r.list {
		op = describeOp(r.name)
	}
	l.reqs[m.read(op, l.fresh)] = r
}

func (m *Machine) loadStep(l *loading, inbox any) {
	switch in := inbox.(type) {
	case RoleChanged:
		if !in.Active {
			m.stop(&l.retry)
			m.state = &standby{}
		}
	case ReplicaReply:
		m.loadReply(l, in)
	case runtime.TimerFired:
		if in.ID == l.retry.id && l.retry.fired(in) {
			again := l.again
			l.again = nil
			for _, r := range again {
				m.sendLoadRead(l, r)
			}
		}
	default:
		m.refuse(inbox)
	}
}

// loadReply takes one load read. A refused read is sent again after LoadRetry.
func (m *Machine) loadReply(l *loading, r ReplicaReply) {
	lr, ok := l.reqs[r.Req]
	if !ok {
		return
	}
	delete(l.reqs, r.Req)
	if r.Done.Err != runtime.OK || m.maybe(SiteLoadRetry) {
		l.again = append(l.again, lr)
		if !l.retry.on {
			m.arm(&l.retry, m.cfg.LoadRetry)
		}
		return
	}
	if lr.list {
		l.listed = true
		for _, s := range runtime.ResponseAs[contract.ListGroupsResp](r.Done).Groups {
			l.names = insertName(l.names, s.Name)
			m.sendLoadRead(l, loadRead{name: s.Name})
		}
	} else {
		l.descs[lr.name] = runtime.ResponseAs[contract.DescribeGroupResp](r.Done)
	}
	if l.listed && len(l.reqs) == 0 && len(l.again) == 0 {
		s := newServing()
		m.state = s
		for _, name := range l.names {
			m.loadGroup(s, name, l.descs[name])
		}
	}
}

// loadGroup fills a group from the log (rule 35). Each member's session
// starts now; no member is new or owes a sync. The group is Stable when its
// assignment is for its generation and its leader is a member, Empty with
// no member, else in a round from now with no delay.
func (m *Machine) loadGroup(s *serving, name string, d contract.DescribeGroupResp) {
	g := m.newGroup(s, name)
	g.generation, g.protoType, g.protocol, g.leader = d.Generation, d.ProtocolType, d.Protocol, d.Leader
	infos := slices.Clone(d.Members)
	slices.SortStableFunc(infos, func(a, b contract.GroupMemberInfo) int {
		return cmp.Or(cmp.Compare(a.JoinedTimeMs, b.JoinedTimeMs), strings.Compare(a.MemberID, b.MemberID))
	})
	assign := map[string][]byte{}
	for _, info := range infos {
		mm := m.newMember(s, g, info.MemberID)
		mm.clientID, mm.clientHost, mm.instanceID = info.ClientID, info.ClientHost, info.GroupInstanceID
		mm.sessionMs, mm.rebalanceMs = info.SessionTimeoutMs, info.RebalanceTimeoutMs
		mm.synced = true
		m.touch(mm)
		if len(info.Assignment) > 0 {
			assign[info.MemberID] = info.Assignment
		}
	}
	switch {
	case len(g.members) == 0:
		m.toEmpty(g)
	case d.AssignedGeneration == d.Generation && g.find(d.Leader) != nil:
		g.phase = &stable{assign: assign}
	default:
		m.openRound(g, false)
	}
}
