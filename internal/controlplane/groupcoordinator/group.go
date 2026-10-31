package groupcoordinator

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"mds/internal/core"
)

// phase is the state of a group in memory. It is not logged.
type phase uint8

const (
	phaseEmpty      phase = iota
	phasePrep             // PreparingRebalance
	phaseCompleting       // CompletingRebalance
	phaseStable
)

func (p phase) String() string {
	switch p {
	case phaseEmpty:
		return "Empty"
	case phasePrep:
		return "PreparingRebalance"
	case phaseCompleting:
		return "CompletingRebalance"
	case phaseStable:
		return "Stable"
	}
	return "unknown"
}

// slot is one armed timer. disarm moves version on, so a callback that fires
// late sees an old version and does nothing.
type slot struct {
	t       Timer
	version uint64
}

func (s *slot) disarm() {
	if s.t != nil {
		s.t.Stop()
		s.t = nil
	}
	s.version++
}

// arm sets s to call fire(version) after d. The caller holds the group mutex.
func arm(clock Clock, s *slot, d time.Duration, fire func(version uint64)) {
	s.disarm()
	version := s.version
	s.t = clock.AfterFunc(d, func() { fire(version) })
}

// group is the memory of one group. Its mutex guards every field.
type group struct {
	mu         sync.Mutex
	name       string
	phase      phase
	generation int32
	protoType  string
	protocol   string
	leader     string
	members    map[string]*member
	joinSeq    uint64
	initial    bool      // the round started from an empty group
	delayDone  bool      // the initial delay passed
	roundStart time.Time // start of the round or of the sync wait
	deadlineAt time.Time // roundStart plus the largest rebalance timeout
	round      slot      // the deadline of the round or of the sync wait
	delay      slot      // the initial delay
}

// member is one known member. Its join and sync calls, if any, are parked here.
type member struct {
	id          string
	clientID    string
	clientHost  string
	instanceID  string
	sessionMs   int32
	rebalanceMs int32
	joinSeq     uint64
	protocols   []Protocol // from the last join
	meta        []byte     // metadata of the chosen protocol
	assignment  []byte
	joinWait    chan JoinResp // parked JoinGroup, nil when not joined this round
	syncWait    chan SyncResp // parked SyncGroup, nil when none
	sess        slot          // session timer
}

func newGroup(name string) *group {
	return &group{name: name, members: map[string]*member{}}
}

// metaFor returns the metadata of protocol name, if the member supports it.
func (m *member) metaFor(name string) ([]byte, bool) {
	for _, p := range m.protocols {
		if p.Name == name {
			return p.Metadata, true
		}
	}
	return nil, false
}

func copyProtocols(ps []Protocol) []Protocol {
	out := make([]Protocol, len(ps))
	for i, p := range ps {
		out[i] = Protocol{Name: p.Name, Metadata: copyBytes(p.Metadata)}
	}
	return out
}

// stopTimers stops every timer of the group. The caller holds the mutex.
func (g *group) stopTimers() {
	g.round.disarm()
	g.delay.disarm()
	for _, m := range g.members {
		m.sess.disarm()
	}
}

// toEmpty moves a group with no member to Empty. The caller holds the mutex.
func (g *group) toEmpty() {
	g.phase = phaseEmpty
	g.leader = ""
	g.initial = false
	g.round.disarm()
	g.delay.disarm()
}

// load fills a group from the log at Rebuild. The group is Stable when the
// stored assignment is for the current generation and the leader is still a
// member. Otherwise every member must rejoin. Each member gets a session
// timer from now, the grace window of decision Q5.
func (c *Coordinator) load(t *term, g *group, d core.DescribeGroupResp) {
	g.generation = d.Generation
	g.protoType = d.ProtocolType
	g.protocol = d.Protocol
	g.leader = d.Leader
	infos := slices.Clone(d.Members)
	slices.SortStableFunc(infos, func(a, b core.GroupMemberInfo) int {
		return cmp.Or(cmp.Compare(a.JoinedTimeMs, b.JoinedTimeMs), strings.Compare(a.MemberID, b.MemberID))
	})
	for _, info := range infos {
		g.joinSeq++
		m := &member{
			id: info.MemberID, joinSeq: g.joinSeq,
			clientID: info.ClientID, clientHost: info.ClientHost, instanceID: info.GroupInstanceID,
			sessionMs: info.SessionTimeoutMs, rebalanceMs: info.RebalanceTimeoutMs,
			meta: info.Metadata, assignment: info.Assignment,
		}
		g.members[m.id] = m
		c.armSession(t, g, m)
	}
	switch {
	case len(g.members) == 0:
		g.toEmpty()
	case d.AssignedGeneration == d.Generation && g.members[g.leader] != nil:
		g.phase = phaseStable
	default:
		c.openRound(t, g, false)
	}
}

// joinLocked does one join under the group mutex. It returns the answer at
// once when the join fails, or the channel the call waits on.
func (c *Coordinator) joinLocked(t *term, g *group, request JoinReq, id string) (JoinResp, chan JoinResp) {
	if len(g.members) == 0 {
		g.protoType = request.ProtocolType
	} else if request.ProtocolType != g.protoType {
		return JoinResp{Err: core.ErrInconsistentGroupProtocol}, nil
	}
	m, known := g.members[id]
	if !known && request.MemberID != "" {
		return JoinResp{Err: core.ErrUnknownMember}, nil
	}
	if !known {
		g.joinSeq++
		m = &member{id: id, joinSeq: g.joinSeq}
		g.members[id] = m
	}
	m.clientID = request.ClientID
	m.clientHost = request.ClientHost
	m.instanceID = request.GroupInstanceID
	m.sessionMs = request.SessionTimeoutMs
	m.rebalanceMs = request.RebalanceTimeoutMs
	if m.rebalanceMs <= 0 {
		m.rebalanceMs = m.sessionMs
	}
	m.protocols = copyProtocols(request.Protocols)
	ch := make(chan JoinResp, 1)
	if m.joinWait != nil {
		// A newer join from the same member replaces the older call.
		m.joinWait <- JoinResp{Err: core.ErrRebalanceInProgress}
	}
	m.joinWait = ch
	c.armSession(t, g, m)
	switch g.phase {
	case phaseEmpty:
		c.openRound(t, g, true)
	case phaseStable, phaseCompleting:
		c.openRound(t, g, false)
	case phasePrep:
		if !known && g.initial && !g.delayDone {
			c.armDelay(t, g)
		}
		c.extendRound(t, g)
	}
	c.maybeComplete(t, g, false)
	return JoinResp{}, ch
}

// openRound starts a join round. The sync wait of a Completing group ends
// here too: its parked syncs get ErrRebalanceInProgress. An initial round
// also waits for the delay.
func (c *Coordinator) openRound(t *term, g *group, initial bool) {
	now := c.config.Clock.Now()
	g.phase = phasePrep
	g.initial = initial
	g.delayDone = false
	g.roundStart = now
	g.deadlineAt = now.Add(c.maxRebalance(g))
	c.armRound(t, g)
	if initial {
		c.armDelay(t, g)
	} else {
		g.delay.disarm()
	}
	for _, m := range g.members {
		if m.syncWait != nil {
			m.syncWait <- SyncResp{Err: core.ErrRebalanceInProgress}
			m.syncWait = nil
		}
	}
}

// maxRebalance is the largest rebalance timeout of the members.
func (c *Coordinator) maxRebalance(g *group) time.Duration {
	var ms int32
	for _, m := range g.members {
		ms = max(ms, m.rebalanceMs)
	}
	return time.Duration(ms) * time.Millisecond
}

// extendRound moves the deadline when a member with a longer rebalance timeout joins.
func (c *Coordinator) extendRound(t *term, g *group) {
	until := g.roundStart.Add(c.maxRebalance(g))
	if until.After(g.deadlineAt) {
		g.deadlineAt = until
		c.armRound(t, g)
	}
}

func (c *Coordinator) armRound(t *term, g *group) {
	left := g.deadlineAt.Sub(c.config.Clock.Now())
	arm(c.config.Clock, &g.round, left, func(version uint64) { c.onRound(t, g, version) })
}

// armDelay waits the initial delay again. It never passes the round deadline.
func (c *Coordinator) armDelay(t *term, g *group) {
	d := c.config.InitialRebalanceDelay
	if left := g.deadlineAt.Sub(c.config.Clock.Now()); left < d {
		d = left
	}
	arm(c.config.Clock, &g.delay, d, func(version uint64) { c.onDelay(t, g, version) })
}

// maybeComplete ends the join round when every known member has rejoined,
// and the initial delay has passed for a new group. A timeout ends it at
// once. The caller holds the mutex.
func (c *Coordinator) maybeComplete(t *term, g *group, timeout bool) {
	if g.phase != phasePrep {
		return
	}
	if !timeout {
		if g.initial && !g.delayDone {
			return
		}
		for _, m := range g.members {
			if m.joinWait == nil {
				return
			}
		}
	}
	c.completeRound(t, g)
}

// candidate is the member whose protocol list decides the round: the leader
// if it is still a member, else the member that joined first. The leader is
// also the candidate for the new leader.
func candidate(g *group) *member {
	if m := g.members[g.leader]; m != nil {
		return m
	}
	var first *member
	for _, m := range g.members {
		if first == nil || m.joinSeq < first.joinSeq {
			first = m
		}
	}
	return first
}

// chooseProtocol returns the first protocol of cand that every member
// supports, with each member's metadata for it.
func chooseProtocol(g *group, cand *member) (string, map[string][]byte, bool) {
	for _, p := range cand.protocols {
		metas := make(map[string][]byte, len(g.members))
		ok := true
		for id, m := range g.members {
			meta, found := m.metaFor(p.Name)
			if !found {
				ok = false
				break
			}
			metas[id] = meta
		}
		if ok {
			return p.Name, metas, true
		}
	}
	return "", nil, false
}

// completeRound ends a join round. Members that did not rejoin are removed
// (a logged expiry). The rest get the next generation (a logged install).
// The caller holds the mutex. A failed write answers the parked joins with
// the error. The round stays open, so a retry works.
func (c *Coordinator) completeRound(t *term, g *group) {
	var gone []string
	for id, m := range g.members {
		if m.joinWait == nil {
			gone = append(gone, id)
		}
	}
	slices.Sort(gone)
	if len(gone) > 0 {
		ctx, cancel := c.writeCtx(t)
		code, err := c.store.GroupRemove(ctx, core.GroupRemove{Group: g.name, Members: gone, TimeMs: c.nowMs(), Reason: "expire"})
		cancel()
		if code = settle(t, code, err); code != core.OK {
			answerJoins(g, JoinResp{Err: code})
			return
		}
		for _, id := range gone {
			c.dropMember(g, id)
		}
	}
	if len(g.members) == 0 {
		g.toEmpty()
		return
	}
	cand := candidate(g)
	proto, metas, ok := chooseProtocol(g, cand)
	if !ok {
		answerJoins(g, JoinResp{Err: core.ErrInconsistentGroupProtocol})
		return
	}
	ids := slices.Sorted(maps.Keys(g.members))
	install := core.GroupInstall{
		Group: g.name, ProtocolType: g.protoType, Protocol: proto,
		Generation: g.generation + 1, Leader: cand.id, TimeMs: c.nowMs(),
	}
	for _, id := range ids {
		m := g.members[id]
		install.Members = append(install.Members, core.GroupMember{
			MemberID: id, ClientID: m.clientID, ClientHost: m.clientHost, GroupInstanceID: m.instanceID,
			SessionTimeoutMs: m.sessionMs, RebalanceTimeoutMs: m.rebalanceMs, Metadata: metas[id],
		})
	}
	ctx, cancel := c.writeCtx(t)
	code, err := c.store.GroupInstall(ctx, install)
	cancel()
	if code = settle(t, code, err); code != core.OK {
		answerJoins(g, JoinResp{Err: code})
		return
	}
	c.applyInstall(t, g, install.Generation, proto, cand.id, ids, metas)
}

// applyInstall moves the group to CompletingRebalance after a logged install
// and answers the parked joins. Only the leader gets the member list.
func (c *Coordinator) applyInstall(t *term, g *group, generation int32, proto, leader string, ids []string, metas map[string][]byte) {
	g.generation = generation
	g.protocol = proto
	g.leader = leader
	g.phase = phaseCompleting
	g.initial = false
	g.delayDone = false
	g.delay.disarm()
	g.roundStart = c.config.Clock.Now()
	g.deadlineAt = g.roundStart.Add(c.maxRebalance(g))
	c.armRound(t, g)
	list := make([]Member, 0, len(ids))
	for _, id := range ids {
		m := g.members[id]
		m.meta = metas[id]
		m.assignment = nil
		list = append(list, Member{ID: id, InstanceID: m.instanceID, Metadata: metas[id]})
	}
	for _, id := range ids {
		m := g.members[id]
		if m.joinWait == nil {
			continue
		}
		response := JoinResp{Generation: generation, Protocol: proto, Leader: leader, MemberID: id}
		if id == leader {
			response.Members = list
		}
		m.joinWait <- response
		m.joinWait = nil
	}
}

// answerJoins gives every parked join the same answer.
func answerJoins(g *group, response JoinResp) {
	for _, m := range g.members {
		if m.joinWait != nil {
			m.joinWait <- response
			m.joinWait = nil
		}
	}
}

// dropMember removes a member from memory. Its parked calls get ErrUnknownMember.
func (c *Coordinator) dropMember(g *group, id string) {
	m := g.members[id]
	if m == nil {
		return
	}
	m.sess.disarm()
	delete(g.members, id)
	if m.joinWait != nil {
		m.joinWait <- JoinResp{Err: core.ErrUnknownMember}
		m.joinWait = nil
	}
	if m.syncWait != nil {
		m.syncWait <- SyncResp{Err: core.ErrUnknownMember}
		m.syncWait = nil
	}
}

// afterLeave runs after members were removed. No member left means Empty.
// Otherwise the group rebalances, or a round that is open may now complete.
func (c *Coordinator) afterLeave(t *term, g *group) {
	if len(g.members) == 0 {
		g.toEmpty()
		return
	}
	switch g.phase {
	case phasePrep:
		c.maybeComplete(t, g, false)
	case phaseStable, phaseCompleting:
		c.openRound(t, g, false)
	}
}

// leaderSync stores the leader's assignment. The caller holds the mutex, and
// this function releases it. The leader's assignment is checked against the
// members first, so the log does not have to refuse it.
func (c *Coordinator) leaderSync(t *term, g *group, m *member, assigns []Assignment) SyncResp {
	seen := make(map[string]bool, len(assigns))
	list := make([]core.GroupAssignment, 0, len(assigns))
	for _, a := range assigns {
		if g.members[a.Member] == nil {
			g.mu.Unlock()
			return SyncResp{Err: core.ErrUnknownMember}
		}
		if seen[a.Member] {
			g.mu.Unlock()
			return SyncResp{Err: core.ErrInvalid}
		}
		seen[a.Member] = true
		list = append(list, core.GroupAssignment{Member: a.Member, Bytes: a.Bytes})
	}
	ctx, cancel := c.writeCtx(t)
	code, err := c.store.GroupAssign(ctx, core.GroupAssign{
		Group: g.name, Generation: g.generation, Leader: m.id, TimeMs: c.nowMs(), Assignments: list,
	})
	cancel()
	if code = settle(t, code, err); code != core.OK {
		g.mu.Unlock()
		return SyncResp{Err: code}
	}
	bytesOf := make(map[string][]byte, len(list))
	for _, a := range list {
		bytesOf[a.Member] = a.Bytes
	}
	g.phase = phaseStable
	g.round.disarm()
	for _, mm := range g.members {
		mm.assignment = copyBytes(bytesOf[mm.id])
		c.armSession(t, g, mm)
		if mm.syncWait != nil {
			mm.syncWait <- SyncResp{Assignment: copyBytes(mm.assignment)}
			mm.syncWait = nil
		}
	}
	response := SyncResp{Assignment: copyBytes(m.assignment)}
	g.mu.Unlock()
	return response
}

// waitJoin waits for the round to end, the term to end, or ctx to end.
//
//nolint:dupl // same shape as the other wait, with its own answer type
func waitJoin(ctx context.Context, t *term, g *group, id string, ch chan JoinResp) (JoinResp, error) {
	select {
	case r := <-ch:
		return r, nil
	case <-t.ctx.Done():
		return JoinResp{Err: core.ErrUnavailable}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if m := g.members[id]; m != nil && m.joinWait == ch {
			m.joinWait = nil
		}
		g.mu.Unlock()
		select {
		case r := <-ch:
			return r, nil
		default:
		}
		return JoinResp{}, ctx.Err()
	}
}

// waitSync waits for the leader's sync, the term to end, or ctx to end.
//
//nolint:dupl // same shape as the other wait, with its own answer type
func waitSync(ctx context.Context, t *term, g *group, id string, ch chan SyncResp) (SyncResp, error) {
	select {
	case r := <-ch:
		return r, nil
	case <-t.ctx.Done():
		return SyncResp{Err: core.ErrUnavailable}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if m := g.members[id]; m != nil && m.syncWait == ch {
			m.syncWait = nil
		}
		g.mu.Unlock()
		select {
		case r := <-ch:
			return r, nil
		default:
		}
		return SyncResp{}, ctx.Err()
	}
}

// armSession starts or restarts a member's session timer.
func (c *Coordinator) armSession(t *term, g *group, m *member) {
	c.armSessionFor(t, g, m, time.Duration(m.sessionMs)*time.Millisecond)
}

func (c *Coordinator) armSessionFor(t *term, g *group, m *member, d time.Duration) {
	arm(c.config.Clock, &m.sess, d, func(version uint64) { c.onSession(t, g, m, version) })
}

// onSession expires a member that was silent for its whole session timeout.
// The removal is logged first. A failed write tries again later.
func (c *Coordinator) onSession(t *term, g *group, m *member, version uint64) {
	if !t.lockLive(g) {
		return
	}
	defer g.mu.Unlock()
	if version != m.sess.version || g.members[m.id] != m {
		return
	}
	ctx, cancel := c.writeCtx(t)
	code, err := c.store.GroupRemove(ctx, core.GroupRemove{Group: g.name, Members: []string{m.id}, TimeMs: c.nowMs(), Reason: "expire"})
	cancel()
	if code = settle(t, code, err); code != core.OK {
		c.armSessionFor(t, g, m, retryAfter)
		return
	}
	c.dropMember(g, m.id)
	c.afterLeave(t, g)
}

// onRound fires at the round deadline. A round ends with the members that
// joined. A sync wait that timed out sends the group back to a join round.
func (c *Coordinator) onRound(t *term, g *group, version uint64) {
	if !t.lockLive(g) {
		return
	}
	defer g.mu.Unlock()
	if version != g.round.version {
		return
	}
	switch g.phase {
	case phasePrep:
		c.maybeComplete(t, g, true)
	case phaseCompleting:
		c.openRound(t, g, false)
	}
}

// onDelay fires when the initial delay of a new group passed with no new member.
func (c *Coordinator) onDelay(t *term, g *group, version uint64) {
	if !t.lockLive(g) {
		return
	}
	defer g.mu.Unlock()
	if version != g.delay.version || g.phase != phasePrep {
		return
	}
	g.delayDone = true
	c.maybeComplete(t, g, false)
}
