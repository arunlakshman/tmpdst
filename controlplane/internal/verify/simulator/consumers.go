package simulator

import (
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/domain/rebalance"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

// The consumer workload. A consumer is a client that uses the group protocol
// as a Kafka consumer does, through the rebalance machine of a node: it
// joins, syncs (the leader makes a simple assignment), heartbeats, commits
// offsets, fetches them, and sometimes leaves, goes silent for good, or
// abandons a call. It sends each call to the node that answered it last (the
// coordinator), and tries another node when the answer is ErrUnavailable or
// the call times out, like the real client. Every choice comes from the
// random stream of the run.
//
// The answers go to the oracle (oracle.go). In the quiet tail the consumers
// do not leave, go silent or abandon calls, and a watcher reads the groups
// until they settle: each is Stable with its live consumers and no silent
// member (groupsUnsettled). The oracle judges the last read of the watcher.
// In the final phase the consumers stop and the machines are frozen, so that
// the log no longer changes for the final reads of the replicas.

const (
	consumerGroupCount = 3
	consumerType       = "consumer"
	consumerProtocol   = "range"

	groupCallTimeout = 5 * machine.Second // deadline of a heartbeat, commit, leave, describe, list or fetch
	parkedCallSlack  = 6 * machine.Second // a join or sync may wait its rebalance timeout, plus this
	maxGroupAttempts = 8
	maxSilences      = 3    // silent consumers in one run; each gets a new consumer in its place
	silenceChance    = 0.01 // per action of a consumer in its group
	skipSyncChance   = 0.02 // go silent after the join, before the sync
	leaveChance      = 0.02
	abandonChance    = 0.04
	watchEvery       = 300 * machine.Millisecond
)

func consumerGroupName(i int) string { return fmt.Sprintf("cg%d", i) }

// stage is where a consumer is in the protocol.
type stage uint8

const (
	needJoin stage = iota
	needSync
	inGroup
)

// callKind is the kind of a group call.
type callKind uint8

const (
	callJoin callKind = iota + 1
	callSync
	callHeartbeat
	callCommit
	callLeave
	callDescribe
	callList
	callFetch
)

// groupOp is one call of a consumer, with its tries.
type groupOp struct {
	kind     callKind
	input    func(rebalance.CallID) any // the input of the machine; nil for a fetch
	fetch    contract.OffsetFetch
	deadline machine.Duration
	attempts int

	member  string                     // join: the member id sent ("" for a new member)
	gen     int32                      // the generation the call was sent with
	assign  []rebalance.Assignment     // sync: the assignment the leader sent
	offsets []contract.PartitionOffset // commit
}

// callTry is one try of a groupOp at one node.
type callTry struct {
	c         *consumer
	op        *groupOp
	node      int
	deadline  machine.Duration
	onPurpose bool // the consumer abandons the call on purpose: its deadline is short
	quiet     bool // sent in the quiet tail
	req       machine.RequestID
}

func (t *callTry) input(call rebalance.CallID) any { return t.op.input(call) }

// answered is the end of the try: a reply, a timeout or a closed connection.
func (t *callTry) answered(code runtime.ErrCode, body rebalance.Answer) { t.c.result(t, code, body) }

// consumer is one simulated consumer client.
type consumer struct {
	pool                   *consumerPool
	name                   string
	from                   machine.NodeInstance
	group                  string
	sessionMs, rebalanceMs int32

	member  string
	gen     int32
	leader  string
	members []string // the member ids of the last join answer (the leader gets them)
	stage   stage
	node    int // the node that answered last: the coordinator

	silent     bool  // makes no more calls
	silentIn   stage // the stage when it went silent
	stopping   bool
	idle       bool
	cur        *callTry
	watcher    bool   // reads the groups (the watcher of the quiet tail, and the final read)
	lastMember string // the member id when it went silent
	mustBeat   bool   // the last call did not start the session again: the next is a heartbeat
}

// consumerPool is the set of consumers of a run.
type consumerPool struct {
	s        *sim
	all      []*consumer
	byName   map[machine.NodeID]*consumer
	byMember map[string]*consumer // member id -> the consumer that got it
	nextReq  machine.RequestID
	nextOff  int64
	silences int

	last   rebalance.DescribeAnswer // the last read of the groups by the watcher
	lastOK bool
}

func newConsumerPool(s *sim, count int) *consumerPool {
	p := &consumerPool{s: s, byName: map[machine.NodeID]*consumer{}, byMember: map[string]*consumer{}}
	for i := range count {
		p.add(consumerGroupName(i % consumerGroupCount))
	}
	return p
}

// add makes a consumer of group with random session and rebalance timeouts.
func (p *consumerPool) add(group string) *consumer {
	s := p.s
	c := &consumer{
		pool: p, name: fmt.Sprintf("consumer-%d", len(p.all)), group: group, idle: true,
		sessionMs:   int32(s.rand.Between(1500, 2500)),
		rebalanceMs: int32(s.rand.Between(2000, 3000)),
		node:        s.rand.Intn(len(s.nodes)),
	}
	c.from = machine.NodeInstance{Node: machine.NodeID(c.name)}
	p.all = append(p.all, c)
	p.byName[c.from.Node] = c
	return c
}

func (p *consumerPool) start() {
	for _, c := range p.all {
		c.idle = false
		c.later(machine.Duration(p.s.rand.Between(int64(20*machine.Millisecond), int64(300*machine.Millisecond))))
	}
}

// busy reports that a consumer still makes calls.
func (p *consumerPool) busy() bool {
	return slices.ContainsFunc(p.all, func(c *consumer) bool { return !c.idle })
}

// stop asks every consumer to stop after its current call.
func (p *consumerPool) stop() {
	for _, c := range p.all {
		c.stopping = true
	}
}

// watch starts the watcher of the quiet tail: it reads every consumer group
// from the machines, again and again, so settled can tell when the groups
// have settled.
func (p *consumerPool) watch() {
	w := p.add("")
	w.watcher, w.idle = true, false
	w.later(0)
}

// settled reports that the last read of the watcher, made in the quiet tail,
// shows every consumer group settled (groupsUnsettled).
func (p *consumerPool) settled() bool {
	return p.lastOK && groupsUnsettled(p, p.last) == ""
}

// live lists the consumers of group that still run, in order.
func (p *consumerPool) live(group string) []*consumer {
	var out []*consumer
	for _, c := range p.all {
		if c.group == group && !c.silent && !c.watcher {
			out = append(out, c)
		}
	}
	return out
}

// consumerGroups lists the names of the consumer groups.
func consumerGroups() []string {
	names := make([]string, consumerGroupCount)
	for i := range names {
		names[i] = consumerGroupName(i)
	}
	return names
}

func (p *consumerPool) newReq() machine.RequestID {
	p.nextReq++
	return p.nextReq
}

// otherNode is the next node to try after last: the next one in the list, as
// a client walks its list of addresses.
func (p *consumerPool) otherNode(last int) int { return (last + 1) % len(p.s.nodes) }

// expiring is told of a GroupRemove that the machine logs because members
// expired. It marks the coverage paths of silent members.
func (p *consumerPool) expiring(rm contract.GroupRemove) {
	for _, id := range rm.Members {
		c := p.byMember[id]
		if c == nil || !c.silent || c.lastMember != id {
			continue
		}
		if c.silentIn == needSync {
			p.s.env.Reachable(memberNeverSyncedRemoved)
		} else {
			p.s.env.Reachable(silentMemberRemoved)
		}
	}
}

// fetched is the replica's answer to the OffsetFetch of a consumer.
func (p *consumerPool) fetched(n *simNode, a runtime.ControlPlaneReply) bool {
	c := p.byName[a.To.Node]
	if c == nil {
		return false
	}
	prefix := n.replica().Applied()
	p.s.after(p.s.store.netDelay(p.s.rand), "consumer-fetch-reply", func() {
		t := c.cur
		if t == nil || t.op.kind != callFetch || t.req != a.Done.Req {
			return
		}
		if a.Done.Err == runtime.OK {
			p.s.oracle.reads = append(p.s.oracle.reads, readRec{op: t.op.fetch, done: a.Done, prefix: prefix, at: int64(p.s.mono())})
			p.s.stats.ReadsOK++
		}
		code := a.Done.Err
		if code != runtime.OK {
			code = runtime.ErrUnavailable // the handler maps every failure so
		}
		c.result(t, code, nil)
	})
	return true
}

// later runs the next step of c after d.
func (c *consumer) later(d machine.Duration) {
	c.pool.s.after(d, "consumer-think", c.next)
}

// gap is the wait between two calls of a consumer in its group: about a
// quarter of its session timeout.
func (c *consumer) gap() machine.Duration {
	q := int64(c.sessionMs) * int64(machine.Millisecond) / 4
	return machine.Duration(c.pool.s.rand.Between(q*2/3, q*4/3))
}

func (c *consumer) think() machine.Duration {
	return machine.Duration(c.pool.s.rand.Between(int64(10*machine.Millisecond), int64(80*machine.Millisecond)))
}

// next makes the next call.
func (c *consumer) next() {
	s := c.pool.s
	if c.silent || c.stopping {
		c.idle = true
		return
	}
	if c.watcher {
		c.describeAll()
		return
	}
	switch c.stage {
	case needJoin:
		c.join()
	case needSync:
		if !s.quiet && c.pool.silences < maxSilences && s.rand.Chance(skipSyncChance) {
			c.goSilent()
			return
		}
		c.sync()
	default:
		c.act()
	}
}

// act is one call of a consumer in its group. Most are heartbeats.
func (c *consumer) act() {
	s := c.pool.s
	if c.mustBeat {
		c.mustBeat = false
		c.heartbeat()
		return
	}
	faults := !s.quiet
	switch p := s.rand.Float64(); {
	case p < 0.12:
		c.commit()
	case p < 0.16:
		c.fetchOffsets()
	case p < 0.18:
		c.describe()
	case p < 0.19:
		c.list()
	case faults && p < 0.19+leaveChance:
		c.leave()
	case faults && p < 0.19+leaveChance+silenceChance && c.pool.silences < maxSilences:
		c.goSilent()
	default:
		c.heartbeat()
	}
}

// goSilent stops the consumer for good, as a client that hangs. Its member
// must be removed by the machine. A new consumer takes its place a few
// seconds later, so that the silent member is often removed by its session
// in a stable group, not by the round that the new consumer opens.
func (c *consumer) goSilent() {
	p := c.pool
	p.silences++
	c.silent, c.silentIn, c.idle = true, c.stage, true
	c.lastMember = c.member
	p.s.tracef("%s goes silent (member %q of %s)", c.name, c.member, c.group)
	n := p.add(c.group)
	n.idle = false
	n.later(machine.Duration(p.s.rand.Between(int64(2500*machine.Millisecond), int64(5*machine.Second))))
}

func (c *consumer) join() {
	member := c.member
	op := &groupOp{kind: callJoin, member: member, deadline: c.parkedDeadline()}
	op.input = func(call rebalance.CallID) any {
		return rebalance.Join{
			Call: call, Group: c.group, MemberID: member, ClientID: c.name, ClientHost: "/10.0.0.1",
			SessionTimeoutMs: c.sessionMs, RebalanceTimeoutMs: c.rebalanceMs, ProtocolType: consumerType,
			Protocols: []rebalance.Protocol{{Name: consumerProtocol, Metadata: []byte(c.group)}},
		}
	}
	c.send(op)
}

func (c *consumer) parkedDeadline() machine.Duration {
	return machine.Duration(c.rebalanceMs)*machine.Millisecond + parkedCallSlack
}

// sync sends the assignment when the consumer leads: each member gets bytes
// that name the group, the generation and the member's place in the list.
func (c *consumer) sync() {
	op := &groupOp{kind: callSync, member: c.member, gen: c.gen, deadline: c.parkedDeadline()}
	if c.member == c.leader {
		for i, m := range c.members {
			op.assign = append(op.assign, rebalance.Assignment{Member: m, Bytes: fmt.Appendf(nil, "%s/%d/%d", c.group, c.gen, i)})
		}
	}
	member, gen, assign := c.member, c.gen, op.assign
	op.input = func(call rebalance.CallID) any {
		return rebalance.Sync{Call: call, Group: c.group, MemberID: member, Generation: gen, Assignments: assign}
	}
	c.send(op)
}

func (c *consumer) heartbeat() {
	member, gen := c.member, c.gen
	op := &groupOp{kind: callHeartbeat, member: member, gen: gen, deadline: groupCallTimeout}
	op.input = func(call rebalance.CallID) any {
		return rebalance.Heartbeat{Call: call, Group: c.group, MemberID: member, Generation: gen}
	}
	c.send(op)
}

// commit commits one or two offsets. Each offset is new in the run, so the
// oracle can find it in the log.
func (c *consumer) commit() {
	s, p := c.pool.s, c.pool
	op := &groupOp{kind: callCommit, member: c.member, gen: c.gen, deadline: groupCallTimeout}
	for range 1 + s.rand.Intn(2) {
		p.nextOff++
		tp := contract.NewTopicPartition(topicName(s.rand.Intn(topicCount)), int32(s.rand.Intn(partitionCount)))
		op.offsets = append(op.offsets, contract.PartitionOffset{TopicPartition: tp, Offset: p.nextOff, LeaderEpoch: -1})
	}
	commit := contract.OffsetCommit{Group: c.group, Member: c.member, Generation: c.gen, Offsets: op.offsets}
	op.input = func(call rebalance.CallID) any { return rebalance.OffsetCommit{Call: call, Op: commit} }
	c.send(op)
}

func (c *consumer) fetchOffsets() {
	op := &groupOp{kind: callFetch, fetch: contract.OffsetFetch{Group: c.group}, deadline: groupCallTimeout}
	c.send(op)
}

func (c *consumer) describe() {
	op := &groupOp{kind: callDescribe, deadline: groupCallTimeout}
	op.input = func(call rebalance.CallID) any { return rebalance.Describe{Call: call, Groups: []string{c.group}} }
	c.send(op)
}

func (c *consumer) describeAll() {
	op := &groupOp{kind: callDescribe, deadline: groupCallTimeout}
	op.input = func(call rebalance.CallID) any { return rebalance.Describe{Call: call, Groups: consumerGroups()} }
	c.send(op)
}

func (c *consumer) list() {
	op := &groupOp{kind: callList, deadline: groupCallTimeout}
	op.input = func(call rebalance.CallID) any { return rebalance.List{Call: call} }
	c.send(op)
}

func (c *consumer) leave() {
	member := c.member
	op := &groupOp{kind: callLeave, member: member, deadline: groupCallTimeout}
	op.input = func(call rebalance.CallID) any {
		return rebalance.Leave{Call: call, Group: c.group, Members: []string{member}}
	}
	c.send(op)
}

// send makes one try of op at the coordinator. In the fault phase a try is
// sometimes abandoned on purpose: its deadline is short.
func (c *consumer) send(op *groupOp) {
	s := c.pool.s
	op.attempts++
	t := &callTry{c: c, op: op, node: c.node, deadline: op.deadline}
	if !s.quiet && !c.watcher && s.rand.Chance(abandonChance) {
		t.onPurpose = true
		t.deadline = machine.Duration(s.rand.Between(int64(machine.Millisecond), int64(300*machine.Millisecond)))
	}
	c.cur = t
	n := s.nodes[t.node]
	s.after(s.store.netDelay(s.rand), "consumer-call", func() {
		if op.kind == callFetch {
			n.groups.fetch(t, c.pool.newReq())
			return
		}
		n.groups.startCall(t)
	})
	if op.kind == callFetch {
		s.after(t.deadline, "consumer-fetch-timeout", func() {
			if c.cur == t {
				c.result(t, runtime.ErrTimeout, nil)
			}
		})
	}
}

// result is the end of a try. A try that is no longer the current one is
// ignored, as a client ignores the answer of a call it gave up.
func (c *consumer) result(t *callTry, code runtime.ErrCode, body rebalance.Answer) {
	if c.cur != t {
		return
	}
	c.cur = nil
	s, op := c.pool.s, t.op
	s.stats.GroupCalls++
	if t.onPurpose && code == runtime.ErrTimeout {
		c.abandoned(op)
		return
	}
	if retriable(code) {
		c.retry(op, code)
		return
	}
	c.node = t.node // this node answered: it is the coordinator
	if code == runtime.OK {
		s.stats.GroupOK++
	}
	c.handle(op, code, body)
}

// retry sends op to another node, or gives up after a few tries.
func (c *consumer) retry(op *groupOp, code runtime.ErrCode) {
	s := c.pool.s
	c.node = c.pool.otherNode(c.node)
	if op.attempts < maxGroupAttempts && !c.stopping {
		backoff := machine.Duration(op.attempts) * 50 * machine.Millisecond
		s.after(backoff, "consumer-retry", func() { c.send(op) })
		return
	}
	c.later(c.think())
}

// abandoned is a call that the consumer gave up on purpose. Its outcome is
// not known: the consumer goes on as a client does after a timeout.
func (c *consumer) abandoned(op *groupOp) {
	switch op.kind {
	case callJoin:
		c.stage = needJoin
	case callLeave:
		c.member, c.stage = "", needJoin
	}
	c.later(c.think())
}

// handle takes an answer that is not a retry.
func (c *consumer) handle(op *groupOp, code runtime.ErrCode, body rebalance.Answer) {
	o := c.pool.s.oracle
	switch op.kind {
	case callJoin:
		if code == runtime.OK {
			a, _ := body.(rebalance.JoinAnswer)
			o.joinAnswered(c, op, a)
			c.member, c.gen, c.leader, c.stage = a.MemberID, a.Generation, a.Leader, needSync
			c.members = c.members[:0]
			for _, m := range a.Members {
				c.members = append(c.members, m.ID)
			}
			c.pool.byMember[a.MemberID] = c
			c.later(c.think())
			return
		}
	case callSync:
		if code == runtime.OK {
			a, _ := body.(rebalance.SyncAnswer)
			o.syncAnswered(c, op, a)
			c.stage = inGroup
			c.later(c.gap())
			return
		}
	case callHeartbeat:
		if code == runtime.OK {
			c.later(c.gap())
			return
		}
	case callCommit:
		if code == runtime.OK {
			a, _ := body.(rebalance.CommitAnswer)
			o.commitAnswered(c, op, a)
			c.later(c.gap())
			return
		}
	case callLeave:
		if a, ok := body.(rebalance.LeaveAnswer); ok && code == runtime.OK && len(a.Errs) == 1 && a.Errs[0] == runtime.OK {
			c.pool.s.env.Reachable(leaveRemovesMember)
		}
		c.member, c.stage = "", needJoin
		c.later(machine.Duration(c.pool.s.rand.Between(int64(200*machine.Millisecond), int64(machine.Second))))
		return
	case callDescribe:
		if c.watcher {
			if a, ok := body.(rebalance.DescribeAnswer); ok && code == runtime.OK {
				c.pool.last, c.pool.lastOK = a, true
			}
			c.later(watchEvery)
			return
		}
		c.mustBeat = true
		c.later(c.think())
		return
	case callList, callFetch:
		c.mustBeat = true
		c.later(c.think())
		return
	}
	c.rejoin(op.kind, code)
}

// rejoin follows a refused join, sync, heartbeat or commit, as Kafka's
// client does: an unknown member joins again as a new member; any other
// code means the group is in a new round.
func (c *consumer) rejoin(kind callKind, code runtime.ErrCode) {
	if kind == callHeartbeat && code == runtime.ErrRebalanceInProgress {
		c.pool.s.env.Reachable(heartbeatToldJoinAgain)
	}
	if code == runtime.ErrUnknownMember {
		c.member = ""
	}
	c.stage = needJoin
	c.later(c.think())
}
