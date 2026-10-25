package sim

import "manifester/core"

// inboxQueue is one inbox: a FIFO slice implementing core.Queue.
type inboxQueue struct{ q []core.EventBody }

func (q *inboxQueue) Len() int { return len(q.q) }

func (q *inboxQueue) Pop() (core.EventBody, bool) {
	if len(q.q) == 0 {
		return nil, false
	}
	b := q.q[0]
	q.q[0] = nil
	q.q = q.q[1:]
	return b, true
}

// nodeInbox is the NodeInbox: a node's control and data queues plus the time
// its loop goroutine is busy until.
type nodeInbox struct {
	ctrl, data inboxQueue
	busy       Time
}

// NewNodeInbox returns an empty, idle inbox.
func NewNodeInbox() NodeInbox { return &nodeInbox{} }

func (n *nodeInbox) Control() core.Queue { return &n.ctrl }
func (n *nodeInbox) Data() core.Queue    { return &n.data }
func (n *nodeInbox) BusyUntil() Time     { return n.busy }

// Push routes body with core.ClassOf.
func (n *nodeInbox) Push(body core.EventBody) {
	if core.ClassOf(body) == core.Control {
		n.ctrl.q = append(n.ctrl.q, body)
	} else {
		n.data.q = append(n.data.q, body)
	}
}

func (n *nodeInbox) empty() bool { return len(n.ctrl.q) == 0 && len(n.data.q) == 0 }

// defaultCost is the CostModel of docs/flow-control.md sec. 4.1.
type defaultCost struct{ p CostParams }

// NewCostModel returns the default CostModel. Zero params cost nothing and
// draw nothing, so a step is instantaneous.
func NewCostModel(p CostParams) CostModel { return defaultCost{p} }

func (c defaultCost) Sample(body core.EventBody, r CountedRand) core.Duration {
	d := c.p.PerEvent
	if b, ok := body.(core.BytesReceived); ok {
		d += c.p.PerByte * core.Duration(len(b.Data))
	}
	if r.Chance(c.p.PauseProb) {
		d += c.p.Pause.Sample(r)
	}
	return d
}

var (
	_ NodeInbox = (*nodeInbox)(nil)
	_ CostModel = defaultCost{}
)
