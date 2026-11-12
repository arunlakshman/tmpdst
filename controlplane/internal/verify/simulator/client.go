package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

const (
	clientTimeout = 2 * machine.Second
	maxAttempts   = 8
	finalAttempts = 40
)

// opState is the operation a client is working on right now.
type opState struct {
	op       runtime.ControlPlaneOperation
	write    bool
	attempts int
	req      machine.RequestID
	node     int // node of the current attempt
	snap     *snapshot
	final    bool   // a read of the final check: one node, retried there
	then     func() // what the client does when the operation is over
}

// client sends one operation at a time to a random node, and retries it with a new
// request id when the node says unavailable or does not answer. The same
// operation is sent every time, so a commit keeps its segment key.
type client struct {
	pool *clientPool
	from machine.NodeInstance
	cur  *opState
	idle bool
}

// clientPool is the set of clients of a run.
type clientPool struct {
	s       *sim
	w       *workload
	all     []*client
	byName  map[machine.NodeID]*client
	nextReq machine.RequestID
}

func newClientPool(s *sim, count int) *clientPool {
	p := &clientPool{s: s, w: &workload{rand: s.rand}, byName: map[machine.NodeID]*client{}}
	for i := range count {
		p.add(fmt.Sprintf("client-%d", i))
	}
	return p
}

func (p *clientPool) add(name string) *client {
	c := &client{pool: p, idle: true, from: machine.NodeInstance{Node: machine.NodeID(name)}}
	p.all = append(p.all, c)
	p.byName[c.from.Node] = c
	return c
}

func (p *clientPool) start() {
	for _, c := range p.all {
		d := machine.Duration(p.s.rand.Between(int64(20*machine.Millisecond), int64(200*machine.Millisecond)))
		p.s.after(d, "client-start", c.issue)
	}
}

// busy reports that a client still works on an operation.
func (p *clientPool) busy() bool {
	for _, c := range p.all {
		if !c.idle {
			return true
		}
	}
	return false
}

// startFinal makes one client per node. Each reads all the state from its node.
func (p *clientPool) startFinal() {
	for _, n := range p.s.nodes {
		c := p.add(fmt.Sprintf("final-%d", n.idx))
		reads := finalReads(p.s.consumers != nil)
		var next func()
		next = func() {
			if len(reads) == 0 {
				c.idle = true
				return
			}
			op := reads[0]
			reads = reads[1:]
			c.begin(&opState{op: op, final: true, node: n.idx, then: next})
		}
		next()
	}
}

func (p *clientPool) pickNode(last int) int {
	n := len(p.s.nodes)
	if last < 0 || n == 1 {
		return p.s.rand.Intn(n)
	}
	return (last + 1 + p.s.rand.Intn(n-1)) % n
}

// reply is called when a node returns a ControlPlaneReply. The answer reaches
// the client after a network delay. prefix is how many log slots the node had
// applied when it computed the answer.
func (p *clientPool) reply(n *simNode, a runtime.ControlPlaneReply) {
	c := p.byName[a.To.Node]
	if c == nil {
		if p.s.consumers != nil {
			p.s.consumers.fetched(n, a)
		}
		return
	}
	prefix := n.replica().Applied()
	p.s.after(p.s.store.netDelay(p.s.rand), "client-reply", func() { c.receive(a.Done, prefix) })
}

// issue starts the next random operation, unless the quiet tail has started.
func (c *client) issue() {
	s := c.pool.s
	if s.quiet {
		c.idle = true
		return
	}
	nowMs := int64(s.mono()) / int64(machine.Millisecond)
	op, write := c.pool.w.next(nowMs)
	c.begin(&opState{op: op, write: write, node: -1, then: func() {
		think := machine.Duration(s.rand.Between(int64(10*machine.Millisecond), int64(120*machine.Millisecond)))
		s.after(think, "client-think", c.issue)
	}})
}

func (c *client) begin(op *opState) {
	c.idle = false
	c.cur = op
	c.pool.s.stats.Ops++
	c.attempt()
}

// attempt sends the operation to a node with a new request id.
func (c *client) attempt() {
	p, op := c.pool, c.cur
	op.attempts++
	p.nextReq++
	req := p.nextReq
	op.req = req
	if !op.final {
		op.node = p.pickNode(op.node)
	}
	op.snap = p.s.oracle.snapshotFor(op.op)
	node := p.s.nodes[op.node]
	deliver := runtime.ControlPlaneDeliver{From: c.from, Req: req, Op: op.op}
	p.s.after(p.s.store.netDelay(p.s.rand), "client-request", func() { node.deliverNow(deliver) })
	p.s.after(clientTimeout, "client-timeout", func() {
		if c.cur == op && op.req == req {
			c.answered(runtime.ControlPlaneDone{Req: req, Err: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}, 0)
		}
	})
}

// receive handles a reply. A reply to an old attempt is ignored, like a real
// client ignores an answer for a request that it has given up on.
func (c *client) receive(d runtime.ControlPlaneDone, prefix runtime.LogPosition) {
	if c.cur != nil && c.cur.req == d.Req {
		c.answered(d, prefix)
	}
}

func retriable(code runtime.ErrCode) bool {
	switch code {
	case runtime.ErrUnavailable, runtime.ErrTimeout, runtime.ErrIO, runtime.ErrConnClosed:
		return true
	}
	return false
}

func (c *client) answered(d runtime.ControlPlaneDone, prefix runtime.LogPosition) {
	s, op := c.pool.s, c.cur
	if !retriable(d.Err) {
		s.oracle.record(op, d, prefix)
		c.finish()
		return
	}
	limit := maxAttempts
	if op.final {
		limit = finalAttempts
	}
	if op.attempts >= limit {
		s.stats.GaveUp++
		if op.final {
			s.violation(finalReadsAnswered, "node cp-%d did not answer %T in the quiet tail (last error %v)", op.node, op.op, d.Err)
		}
		c.finish()
		return
	}
	s.stats.Retries++
	backoff := machine.Duration(op.attempts) * 60 * machine.Millisecond
	s.after(backoff, "client-retry", func() {
		if c.cur == op {
			c.attempt()
		}
	})
}

func (c *client) finish() {
	op := c.cur
	c.cur = nil
	op.then()
}
