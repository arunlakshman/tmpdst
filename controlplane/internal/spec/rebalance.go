package spec

import (
	"cmp"
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Rebalance is the model of the classic consumer group protocol, with the
// rules of the Apache Kafka classic group coordinator (docs/rebalance.md). It
// takes one input at a time and has no faults. Deadlines up to the time of an
// input fire first, earliest first (a tie: group name, then initial delay,
// round end or sync deadline, sessions by member id). Store, the metadata
// model, keeps the logged half of the state.
type Rebalance struct {
	Store            *ControlPlane
	Delay            runtime.Duration // the initial rebalance delay of a new group
	NewMemberTimeout runtime.Duration // how long a new member may wait for its first round
	now              runtime.NodeTime
	active           bool
	groups           map[string]*rgroup
}

// CallID names one call. Every call gets one Reply, unless it is abandoned.
type CallID uint64

// Protocol is one protocol a member can use, with its metadata.
type Protocol struct {
	Name     string
	Metadata []byte
}

// The inputs. Every call has a Call id.
type (
	// Join is a JoinGroup call. An empty MemberID asks for a new member, named
	// ClientID + "-" + NewID (NewID alone with no ClientID). RebalanceMs 0 means SessionMs.
	Join struct {
		Call                                                     CallID
		Group, MemberID, NewID, ClientID, ClientHost, InstanceID string
		ProtocolType                                             string
		Protocols                                                []Protocol
		SessionMs, RebalanceMs                                   int32
	}
	// Sync is a SyncGroup call. Only the leader's Assignments are used.
	Sync struct {
		Call            CallID
		Group, MemberID string
		Generation      int32
		Assignments     []contract.GroupAssignment
	}
	// Heartbeat is a Heartbeat call.
	Heartbeat struct {
		Call            CallID
		Group, MemberID string
		Generation      int32
	}
	// Leave is a LeaveGroup call. The reply has one code per member.
	Leave struct {
		Call    CallID
		Group   string
		Members []string
	}
	// Commit is an OffsetCommit call. The model sets TimeMs.
	Commit struct {
		Call CallID
		Op   contract.OffsetCommit
	}
	// Describe is a DescribeGroup call for one group.
	Describe struct {
		Call  CallID
		Group string
	}
	// List is a ListGroups call.
	List struct{ Call CallID }
	// Abandon says the caller of Call stopped waiting. It gets no reply.
	Abandon struct{ Call CallID }
	// RoleChanged says whether this node serves groups from now on.
	RoleChanged struct{ Active bool }
)

// Reply answers one call. Body is a JoinResult (Join), the assignment (Sync),
// one code per member (Leave), a contract response (Commit, Describe, List), or nil.
type Reply struct {
	Call CallID
	Err  runtime.ErrCode
	Body any
}

// JoinResult is the body of a Join reply. Only the leader gets Members, in id
// order, with the metadata of the chosen protocol.
type JoinResult struct {
	Generation                 int32
	Protocol, Leader, MemberID string
	Members                    []contract.GroupMember
}

func (in Join) callID() CallID      { return in.Call }
func (in Sync) callID() CallID      { return in.Call }
func (in Heartbeat) callID() CallID { return in.Call }
func (in Leave) callID() CallID     { return in.Call }
func (in Commit) callID() CallID    { return in.Call }
func (in Describe) callID() CallID  { return in.Call }
func (in List) callID() CallID      { return in.Call }

// The Kafka group states (Dead is a group the model does not have), the two
// waiting calls of a member, and a time that never comes.
const (
	PhaseEmpty   = "Empty"
	PhaseJoining = "PreparingRebalance"
	PhaseSyncing = "CompletingRebalance"
	PhaseStable  = "Stable"
	PhaseDead    = "Dead"
	joinCall     = 0
	syncCall     = 1
	never        = runtime.NodeTime(1<<63 - 1)
)

// rgroup is one group in memory. end is the round end (PreparingRebalance) or
// the sync deadline. delayEnd is the end of the initial delay, 0 for none.
type rgroup struct {
	phase                       string
	generation                  int32
	protoType, protocol, leader string
	members                     map[string]*rmember
	joined                      int // members that ever joined, for the join order
	start, end, delayEnd        runtime.NodeTime
}

// rmember is one member, with its waiting join and sync (0 for none). A new
// member has not been in a done round yet. synced: it synced this generation.
type rmember struct {
	contract.GroupMember
	order         int
	protocols     []Protocol
	expires       runtime.NodeTime
	wait          [2]CallID
	assignment    []byte
	isNew, synced bool
}

// NewRebalance returns a model with an empty Store that does not serve yet.
func NewRebalance(delay runtime.Duration) *Rebalance {
	return &Rebalance{Store: New(), Delay: delay, NewMemberTimeout: 300 * runtime.Second, groups: map[string]*rgroup{}}
}

// Phase is the phase of a group in memory, Dead when there is none.
func (r *Rebalance) Phase(group string) string {
	if g := r.groups[group]; g != nil {
		return g.phase
	}
	return PhaseDead
}

// Step fires the deadlines up to now, then handles in (nil for none).
func (r *Rebalance) Step(now runtime.NodeTime, in any) []Reply {
	var out []Reply
	for at, fire := r.nextDeadline(now); fire != nil; at, fire = r.nextDeadline(now) {
		r.now = at
		out = append(out, fire()...)
	}
	r.now = max(r.now, now)
	return append(out, r.handle(in)...)
}

func (r *Rebalance) handle(in any) []Reply {
	if c, ok := in.(interface{ callID() CallID }); ok {
		j, isJoin := in.(Join)
		if isJoin && (j.Group == "" || j.ProtocolType == "" || len(j.Protocols) == 0 || j.SessionMs <= 0) {
			return []Reply{{Call: j.Call, Err: runtime.ErrInvalid}}
		}
		if !r.active {
			return []Reply{{Call: c.callID(), Err: runtime.ErrUnavailable}}
		}
	}
	switch in := in.(type) {
	case RoleChanged:
		return r.roleChanged(in.Active)
	case Abandon:
		r.abandon(in.Call)
	case Join:
		return r.join(in)
	case Sync:
		return r.syncGroup(in)
	case Heartbeat:
		return []Reply{{Call: in.Call, Err: r.heartbeat(in.Group, in.MemberID, in.Generation)}}
	case Leave:
		return r.leave(in)
	case Commit:
		return []Reply{r.commit(in)}
	case Describe: // the log's description, with the state from memory
		d, _ := r.Store.Apply(contract.DescribeGroup{Group: in.Group}).Resp.(contract.DescribeGroupResp)
		d.State = r.Phase(in.Group)
		return []Reply{{Call: in.Call, Body: d}}
	case List: // from memory
		var l contract.ListGroupsResp
		for _, name := range sortedKeys(r.groups) {
			l.Groups = append(l.Groups, contract.GroupSummary{Name: name, ProtocolType: r.groups[name].protoType, State: r.groups[name].phase})
		}
		return []Reply{{Call: in.Call, Body: l}}
	}
	return nil
}

func (r *Rebalance) ms() int64 { return int64(r.now) / int64(runtime.Millisecond) }

func (r *Rebalance) member(group, id string) *rmember {
	if g := r.groups[group]; g != nil {
		return g.members[id]
	}
	return nil
}

// touch starts the session of m again. A new member keeps its new member timeout.
func (r *Rebalance) touch(m *rmember) {
	if m != nil && !m.isNew {
		m.expires = r.now + runtime.NodeTime(m.SessionTimeoutMs)*runtime.NodeTime(runtime.Millisecond)
	}
}

// answer gives the waiting call k of m its reply, and starts the session again.
func (r *Rebalance) answer(m *rmember, k int, reply Reply) Reply {
	reply.Call, m.wait[k] = m.wait[k], 0
	r.touch(m)
	return reply
}

func (r *Rebalance) answerAll(g *rgroup, k int, reply Reply) []Reply {
	var out []Reply
	for _, id := range sortedKeys(g.members) {
		if m := g.members[id]; m.wait[k] != 0 {
			out = append(out, r.answer(m, k, reply))
		}
	}
	return out
}

// park makes call the waiting call k of m. An older one gets ErrRebalanceInProgress.
func (r *Rebalance) park(m *rmember, k int, call CallID) []Reply {
	var out []Reply
	if m.wait[k] != 0 {
		out = append(out, r.answer(m, k, Reply{Err: runtime.ErrRebalanceInProgress}))
	}
	m.wait[k] = call
	return out
}

// idsWhere lists the members of g, in id order, for which keep is true.
func idsWhere(g *rgroup, keep func(*rmember) bool) []string {
	var ids []string
	for _, id := range sortedKeys(g.members) {
		if keep(g.members[id]) {
			ids = append(ids, id)
		}
	}
	return ids
}

// nextDeadline returns the earliest deadline at or before now and what it
// does. An existing member that waits in a join or a sync does not expire
// (Kafka hasSatisfiedHeartbeat); a new member does.
func (r *Rebalance) nextDeadline(now runtime.NodeTime) (runtime.NodeTime, func() []Reply) {
	best, found := now, (func() []Reply)(nil)
	consider := func(at runtime.NodeTime, fire func() []Reply) {
		if at <= now && (found == nil || at < best) {
			best, found = at, fire
		}
	}
	for _, name := range sortedKeys(r.groups) {
		g := r.groups[name]
		if g.phase == PhaseJoining && g.delayEnd > 0 {
			consider(g.delayEnd, func() []Reply { g.delayEnd = 0; return r.complete(name, g, false) })
		}
		if g.phase == PhaseJoining {
			consider(g.end, func() []Reply { return r.complete(name, g, true) })
		}
		late := idsWhere(g, func(m *rmember) bool { return !m.synced })
		if (g.phase == PhaseSyncing || g.phase == PhaseStable) && len(late) > 0 {
			consider(g.end, func() []Reply { out, _ := r.remove(name, g, late, "expire"); return out })
		}
		for _, id := range sortedKeys(g.members) {
			if m := g.members[id]; m.isNew || m.wait == [2]CallID{} {
				consider(m.expires, func() []Reply { out, _ := r.remove(name, g, []string{id}, "expire"); return out })
			}
		}
	}
	return best, found
}

func (r *Rebalance) join(in Join) []Reply {
	g, m := r.groups[in.Group], r.member(in.Group, in.MemberID)
	switch {
	case g != nil && len(g.members) > 0 && in.ProtocolType != g.protoType:
		return []Reply{{Call: in.Call, Err: runtime.ErrInconsistentGroupProtocol}}
	case in.MemberID != "" && m == nil:
		return []Reply{{Call: in.Call, Err: runtime.ErrUnknownMember}}
	case g == nil:
		g = &rgroup{phase: PhaseEmpty, members: map[string]*rmember{}}
		r.groups[in.Group] = g
	}
	if len(g.members) == 0 {
		g.protoType = in.ProtocolType
	}
	id, isNew := in.MemberID, m == nil
	if isNew {
		if id = in.NewID; in.ClientID != "" {
			id = in.ClientID + "-" + in.NewID
		}
		g.joined++
		m = &rmember{order: g.joined, isNew: true, expires: r.now + runtime.NodeTime(r.NewMemberTimeout)}
		g.members[id] = m
	}
	m.GroupMember = contract.GroupMember{MemberID: id, ClientID: in.ClientID, ClientHost: in.ClientHost,
		GroupInstanceID: in.InstanceID, SessionTimeoutMs: in.SessionMs, RebalanceTimeoutMs: in.RebalanceMs}
	if in.RebalanceMs <= 0 {
		m.RebalanceTimeoutMs = in.SessionMs
	}
	m.protocols = slices.Clone(in.Protocols)
	out := r.park(m, joinCall, in.Call)
	if g.phase != PhaseJoining {
		out = append(out, r.openRound(g, g.phase == PhaseEmpty)...)
	} else {
		if isNew && g.delayEnd > 0 {
			g.delayEnd = min(r.now+runtime.NodeTime(r.Delay), g.end)
		}
		g.end = max(g.end, g.start+longestRebalance(g))
	}
	return append(out, r.complete(in.Group, g, false)...)
}

func longestRebalance(g *rgroup) runtime.NodeTime {
	var ms int32
	for _, id := range sortedKeys(g.members) {
		ms = max(ms, g.members[id].RebalanceTimeoutMs)
	}
	return runtime.NodeTime(ms) * runtime.NodeTime(runtime.Millisecond)
}

// openRound starts a join round. Waiting syncs get ErrRebalanceInProgress.
func (r *Rebalance) openRound(g *rgroup, initial bool) []Reply {
	g.phase, g.start, g.end, g.delayEnd = PhaseJoining, r.now, r.now+longestRebalance(g), 0
	if initial && r.Delay > 0 {
		g.delayEnd = min(r.now+runtime.NodeTime(r.Delay), g.end)
	}
	return r.answerAll(g, syncCall, Reply{Err: runtime.ErrRebalanceInProgress})
}

// complete ends a join round when every member waits in a join and no initial
// delay is left, or at the round end (the join timeout): the others are removed.
func (r *Rebalance) complete(name string, g *rgroup, timeout bool) []Reply {
	late := idsWhere(g, func(m *rmember) bool { return m.wait[joinCall] == 0 })
	if g.phase != PhaseJoining || (!timeout && (g.delayEnd > 0 || len(late) > 0)) {
		return nil
	}
	if timeout {
		g.end = never // a round that fails now waits for its members to join again
	}
	var out []Reply
	if len(late) > 0 {
		r.Store.Apply(contract.GroupRemove{Group: name, Members: late, TimeMs: r.ms(), Reason: "expire"})
		if out = drop(g, late); len(g.members) == 0 {
			return out
		}
	}
	leader := g.members[g.leader]
	for _, id := range sortedKeys(g.members) {
		if m := g.members[id]; g.members[g.leader] == nil && (leader == nil || m.order < leader.order) {
			leader = m
		}
	}
	install := contract.GroupInstall{Group: name, ProtocolType: g.protoType, Generation: g.generation + 1, Leader: leader.MemberID, TimeMs: r.ms()}
	for _, p := range leader.protocols {
		if install.Members = metadataFor(g, p.Name); install.Members != nil {
			install.Protocol = p.Name
			break
		}
	}
	code := runtime.ErrInconsistentGroupProtocol
	if install.Members != nil {
		code = r.Store.Apply(install).Err
	}
	if code != runtime.OK {
		return append(out, r.answerAll(g, joinCall, Reply{Err: code})...)
	}
	g.phase, g.generation, g.protocol, g.leader = PhaseSyncing, install.Generation, install.Protocol, leader.MemberID
	g.end = r.now + longestRebalance(g) // the sync deadline
	for _, gm := range install.Members {
		m := g.members[gm.MemberID]
		m.assignment, m.isNew, m.synced = nil, false, false
		result := JoinResult{Generation: g.generation, Protocol: g.protocol, Leader: g.leader, MemberID: gm.MemberID}
		if gm.MemberID == g.leader {
			result.Members = install.Members
		}
		out = append(out, r.answer(m, joinCall, Reply{Body: result}))
	}
	return out
}

// metadataFor lists the members, in id order, with their metadata for
// protocol; nil when a member does not have it.
func metadataFor(g *rgroup, protocol string) []contract.GroupMember {
	var list []contract.GroupMember
	for _, id := range sortedKeys(g.members) {
		m := g.members[id]
		i := slices.IndexFunc(m.protocols, func(p Protocol) bool { return p.Name == protocol })
		if i < 0 {
			return nil
		}
		gm := m.GroupMember
		gm.Metadata = nonEmpty(m.protocols[i].Metadata)
		list = append(list, gm)
	}
	return list
}

// drop removes members from memory; their waiting calls get ErrUnknownMember.
// With no member left the group is Empty; it keeps its generation.
func drop(g *rgroup, ids []string) []Reply {
	var out []Reply
	for _, id := range ids {
		for _, call := range g.members[id].wait {
			if call != 0 {
				out = append(out, Reply{Call: call, Err: runtime.ErrUnknownMember})
			}
		}
		delete(g.members, id)
	}
	if len(g.members) == 0 {
		g.phase, g.leader, g.delayEnd = PhaseEmpty, "", 0
	}
	return out
}

// remove logs that members left and drops them. Then an open round may end,
// or a Stable or CompletingRebalance group opens a round.
func (r *Rebalance) remove(name string, g *rgroup, ids []string, reason string) ([]Reply, runtime.ErrCode) {
	if len(ids) == 0 {
		return nil, runtime.OK
	}
	if code := r.Store.Apply(contract.GroupRemove{Group: name, Members: ids, TimeMs: r.ms(), Reason: reason}).Err; code != runtime.OK {
		return nil, code
	}
	out := drop(g, ids)
	switch g.phase {
	case PhaseJoining:
		out = append(out, r.complete(name, g, false)...)
	case PhaseSyncing, PhaseStable:
		out = append(out, r.openRound(g, false)...)
	}
	return out, runtime.OK
}

func (r *Rebalance) leave(in Leave) []Reply {
	codes := make([]runtime.ErrCode, len(in.Members))
	var gone []string
	for i, id := range in.Members {
		if r.member(in.Group, id) == nil {
			codes[i] = runtime.ErrUnknownMember
		} else if !slices.Contains(gone, id) {
			gone = append(gone, id)
		}
	}
	out, code := r.remove(in.Group, r.groups[in.Group], gone, "leave")
	for i := range codes {
		if codes[i] == runtime.OK {
			codes[i] = code
		}
	}
	return append(out, Reply{Call: in.Call, Body: codes})
}

// check is the member and generation check of sync, heartbeat and commit.
func (r *Rebalance) check(group, id string, generation int32) (*rgroup, *rmember, runtime.ErrCode) {
	g, m := r.groups[group], r.member(group, id)
	switch {
	case m == nil:
		return g, nil, runtime.ErrUnknownMember
	case generation != g.generation:
		return g, m, runtime.ErrIllegalGeneration
	}
	return g, m, runtime.OK
}

// heartbeat is Kafka's: after the check, PreparingRebalance is
// ErrRebalanceInProgress, else OK. Both start the session again.
func (r *Rebalance) heartbeat(group, id string, generation int32) runtime.ErrCode {
	g, m, code := r.check(group, id, generation)
	if code != runtime.OK {
		return code
	}
	r.touch(m)
	if g.phase == PhaseJoining {
		return runtime.ErrRebalanceInProgress
	}
	return runtime.OK
}

// commit checks the commit against memory as Kafka does (a generation below
// 0 is not checked on a group with no members, or one the model does not
// have), then Store checks it again and stores it.
func (r *Rebalance) commit(in Commit) Reply {
	c := in.Op
	if g := r.groups[c.Group]; g != nil && (c.Generation >= 0 || len(g.members) > 0) {
		_, m, code := r.check(c.Group, c.Member, c.Generation)
		if code == runtime.OK && g.phase == PhaseSyncing {
			code = runtime.ErrRebalanceInProgress
		}
		if code != runtime.OK {
			return Reply{Call: in.Call, Err: code}
		}
		r.touch(m) // the group is PreparingRebalance or Stable here
	}
	c.TimeMs = r.ms()
	d := r.Store.Apply(c)
	return Reply{Call: in.Call, Err: d.Err, Body: d.Resp}
}

// syncGroup answers at once, parks a follower, or stores the leader's
// assignment (Store checks it) and answers every waiting follower.
func (r *Rebalance) syncGroup(in Sync) []Reply {
	g, m, code := r.check(in.Group, in.MemberID, in.Generation)
	switch {
	case code != runtime.OK:
		return []Reply{{Call: in.Call, Err: code}}
	case g.phase == PhaseJoining:
		return []Reply{{Call: in.Call, Err: runtime.ErrRebalanceInProgress}}
	case g.phase == PhaseStable: // Kafka does not start the session again here
		m.synced = true
		return []Reply{{Call: in.Call, Body: m.assignment}}
	case in.MemberID != g.leader:
		m.synced = true
		return r.park(m, syncCall, in.Call)
	}
	assign := contract.GroupAssign{Group: in.Group, Generation: g.generation, Leader: g.leader, TimeMs: r.ms(), Assignments: in.Assignments}
	if code := r.Store.Apply(assign).Err; code != runtime.OK {
		return []Reply{{Call: in.Call, Err: code}}
	}
	g.phase, m.synced = PhaseStable, true
	var out []Reply
	for _, id := range sortedKeys(g.members) {
		mm := g.members[id]
		mm.assignment = nil
		if i := slices.IndexFunc(in.Assignments, func(a contract.GroupAssignment) bool { return a.Member == id }); i >= 0 {
			mm.assignment = nonEmpty(in.Assignments[i].Bytes)
		}
		if mm.wait[syncCall] != 0 {
			out = append(out, r.answer(mm, syncCall, Reply{Body: mm.assignment}))
		}
	}
	r.touch(m)
	return append(out, Reply{Call: in.Call, Body: m.assignment})
}

// abandon forgets a waiting call. Its member waits no more; its session starts again.
func (r *Rebalance) abandon(call CallID) {
	for _, name := range sortedKeys(r.groups) {
		for _, id := range sortedKeys(r.groups[name].members) {
			m := r.groups[name].members[id]
			for k := range m.wait {
				if call != 0 && m.wait[k] == call {
					r.answer(m, k, Reply{})
				}
			}
		}
	}
}

// roleChanged starts or stops serving. Stopping answers every waiting call
// with ErrUnavailable and drops the memory. Starting reads every group from
// Store: Stable when its assignment is for its generation and its leader is a
// member, Empty with no member, else in a round. Each session starts now; the
// join order is the join time, then the id. No member is new or owes a sync.
func (r *Rebalance) roleChanged(active bool) []Reply {
	if active == r.active {
		return nil
	}
	r.active = active
	var out []Reply
	for _, name := range sortedKeys(r.groups) {
		out = append(out, r.answerAll(r.groups[name], joinCall, Reply{Err: runtime.ErrUnavailable})...)
		out = append(out, r.answerAll(r.groups[name], syncCall, Reply{Err: runtime.ErrUnavailable})...)
	}
	r.groups = map[string]*rgroup{}
	if !active {
		return out
	}
	listed, _ := r.Store.Apply(contract.ListGroups{}).Resp.(contract.ListGroupsResp)
	for _, s := range listed.Groups {
		d, _ := r.Store.Apply(contract.DescribeGroup{Group: s.Name}).Resp.(contract.DescribeGroupResp)
		g := &rgroup{phase: PhaseEmpty, generation: d.Generation, protoType: d.ProtocolType, protocol: d.Protocol, leader: d.Leader, members: map[string]*rmember{}}
		slices.SortStableFunc(d.Members, func(a, b contract.GroupMemberInfo) int { return cmp.Compare(a.JoinedTimeMs, b.JoinedTimeMs) })
		for i, info := range d.Members {
			g.members[info.MemberID] = &rmember{GroupMember: info.GroupMember, order: i + 1, assignment: info.Assignment, synced: true}
			r.touch(g.members[info.MemberID])
		}
		g.joined = len(d.Members)
		r.groups[s.Name] = g
		switch {
		case len(g.members) > 0 && d.AssignedGeneration == d.Generation && g.members[d.Leader] != nil:
			g.phase = PhaseStable
		case len(g.members) > 0:
			r.openRound(g, false)
		}
	}
	return out
}
