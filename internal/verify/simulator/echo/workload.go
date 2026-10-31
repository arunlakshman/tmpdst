package echo

import (
	"fmt"
	"strconv"
	"strings"

	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/verify/simulator/internal/session"
)

// PutOp and GetOp are the History ops. A PUT result is the reply text, or ""
// when the client lost the answer (OpIndeterminate).
type PutOp struct {
	Client, Seq int
	Payload     string
}

// GetOp is a read request from Client.
type GetOp struct {
	Client int
	Offset int64
}

// Opts configures the workload. Phase behavior is not configured: the workload
// is simulator.PhaseAware (see OnPhase).
type Opts struct {
	Clients int
}

const (
	requestTimeout = 3 * core.Second
	keepAcked      = 16 // GET candidates per client
)

const (
	clientReady = session.Connected + iota // wake timer pending (think time)
	clientBusy                             // request outstanding
	clientDone
)

type acked struct {
	off     int64
	payload string
}

type pending struct {
	id      core.ID
	get     bool
	seq     int
	payload string
	off     int64
}

type client struct {
	session.Client
	fr      framer
	seq     int
	recycle bool
	acked   []acked
	digest  core.Hasher // every ack ever recorded
	nacked  uint64
	pend    *pending
}

// Workload is N closed-loop clients speaking the echo protocol.
type Workload struct {
	env  core.Environment
	self core.NodeInstance
	opts Opts
	hist simulator.History

	stopped bool // PhaseSettle began: no new requests
	loop    session.Loop[*client]
}

// NewWorkload builds the workload machine.
func NewWorkload(env core.Environment, self core.NodeInstance, brokers []core.NodeInstance, o Opts) *Workload {
	w := &Workload{env: env, self: self, opts: o}
	w.loop = session.Loop[*client]{
		Connected: func(c *client) bool { return c.State == clientReady || c.State == clientBusy },
		Busy:      func(c *client) bool { return c.State == clientBusy },
		Ready:     w.onReady,
		Idle:      w.next,
		Bytes:     w.onBytes,
		Lost:      w.lost,
	}
	for _, b := range brokers {
		w.loop.Brokers = append(w.loop.Brokers, b.Node)
	}
	for i := range o.Clients {
		w.loop.Clients = append(w.loop.Clients, &client{Client: session.Client{ID: i}, digest: core.NewHasher()})
	}
	return w
}

// Name implements simulator.Workload.
func (w *Workload) Name() string { return "echo" }

// SetHistory is called by the kernel.
func (w *Workload) SetHistory(h simulator.History) { w.hist = h }

// Quiescent reports that no request is outstanding.
func (w *Workload) Quiescent() bool {
	for _, c := range w.loop.Clients {
		if c.pend != nil {
			return false
		}
	}
	return true
}

// Step implements core.StateMachine.
func (w *Workload) Step(ev core.Event) []core.Action { return w.loop.Step(ev) }

func (w *Workload) onReady(c *client) []core.Action {
	c.Fails, c.fr = 0, framer{}
	return []core.Action{c.Wake(core.Millisecond)}
}

// OnPhase implements simulator.PhaseAware. Entering Settling every client drops its
// connection at its next op boundary (idle ones now), so a connection damaged
// in Chaos (black-holed, clogged) cannot starve it. Entering Settle (the drain)
// clients issue nothing new; outstanding requests finish.
func (w *Workload) OnPhase(p simulator.Phase) []core.Action {
	var acts []core.Action
	switch p {
	case simulator.PhaseRecovering:
		for _, c := range w.loop.Clients {
			c.recycle = true
			if c.State == clientReady { // idle: recycle now
				acts = append(acts, w.next(c)...)
			}
		}
	case simulator.PhaseRecover:
		w.stopped = true
	}
	return acts
}

// next issues the client's next request, or recycles / stops.
func (w *Workload) next(c *client) []core.Action {
	if w.stopped {
		c.State = clientDone
		c.WakeGen++
		return nil
	}
	if c.recycle {
		c.recycle = false
		acts := []core.Action{core.CloseConn{Conn: c.Conn, Abort: true}}
		c.Conn = 0
		return append(acts, w.loop.Dial(c)...)
	}
	p := &pending{id: w.env.NewID()}
	var op any
	var body string
	if w.env.Intn(3) == 0 && len(c.acked) > 0 {
		a := c.acked[w.env.Intn(len(c.acked))]
		p.get, p.off = true, a.off
		op, body = GetOp{c.ID, a.off}, "GET "+strconv.FormatInt(a.off, 10)
	} else {
		c.seq++
		p.seq = c.seq
		p.payload = fmt.Sprintf("c%d-s%d-%s", c.ID, c.seq, strings.Repeat("x", w.env.Intn(48)))
		op, body = PutOp{c.ID, c.seq, p.payload}, "PUT "+p.payload
	}
	c.pend, c.State = p, clientBusy
	if w.hist != nil {
		w.hist.Invoke(w.self, p.id, p.id, op)
	}
	return w.loop.Request(c, appendFrame(nil, body), requestTimeout)
}

func (w *Workload) onBytes(c *client, data []byte) []core.Action {
	frames, bad := c.fr.push(data)
	var acts []core.Action
	for _, f := range frames {
		if c.State != clientBusy || c.pend == nil {
			bad = true // unsolicited reply
			break
		}
		acts = append(acts, w.reply(c, f)...)
	}
	if bad && c.Conn != 0 {
		return append(acts, w.loop.Abort(c, "bad_stream")...)
	}
	return acts
}

// reply completes the outstanding request with the broker's answer.
func (w *Workload) reply(c *client, text string) []core.Action {
	p := c.pend
	c.pend, c.State = nil, clientReady
	out := simulator.OpResultKnown
	if !p.get {
		if rest, ok := strings.CutPrefix(text, "OK "); ok {
			if off, err := strconv.ParseInt(rest, 10, 64); err == nil {
				c.record(off, p.payload)
			}
		} else if text == "ERR maybe" {
			out = simulator.OpResultUnknown
		}
	}
	if w.hist != nil {
		w.hist.Return(p.id, text, out)
	}
	think := core.Duration(200+w.env.Intn(600)) * core.Millisecond
	return []core.Action{c.Answered(), c.Wake(think)}
}

func (c *client) record(off int64, payload string) {
	c.nacked++
	c.digest.Uint64Value(uint64(off))
	c.digest.StringValue(payload)
	c.acked = append(c.acked, acked{off, payload})
	if len(c.acked) > keepAcked {
		c.acked = append([]acked(nil), c.acked[1:]...)
	}
}

// lost settles the outstanding request, whose fate is unknown.
func (w *Workload) lost(c *client, why string) {
	if p := c.pend; p != nil {
		out := simulator.OpResultUnknown
		if p.get {
			out = simulator.OpGivenUp
		}
		if w.hist != nil {
			w.hist.Return(p.id, why, out)
		}
	}
	c.pend, c.fr = nil, framer{}
}

// StateHash folds all state.
func (w *Workload) StateHash() uint64 {
	h := core.NewHasher()
	h.Bool(w.stopped)
	h.Uint64Value(uint64(w.loop.NextReq))
	for _, c := range w.loop.Clients {
		h.Uint64Value(uint64(c.State))
		h.Uint64Value(uint64(c.Conn))
		h.Uint64Value(uint64(c.DialReq))
		h.Uint64Value(uint64(c.SendReq))
		h.Uint64Value(uint64(c.Dials))
		h.Uint64Value(uint64(c.Fails))
		h.Uint64Value(uint64(c.WakeGen))
		h.Uint64Value(uint64(c.ReqGen))
		h.Bytes(c.fr.buf)
		h.Uint64Value(uint64(c.seq))
		h.Bool(c.recycle)
		h.Uint64Value(uint64(len(c.acked)))
		h.Uint64Value(uint64(c.digest))
		h.Uint64Value(c.nacked)
		h.Bool(c.pend != nil)
		if p := c.pend; p != nil {
			h.Bytes(p.id[:])
			h.Bool(p.get)
			h.Uint64Value(uint64(p.seq))
			h.StringValue(p.payload)
			h.Uint64Value(uint64(p.off))
		}
	}
	return uint64(h)
}

var (
	_ simulator.Workload            = (*Workload)(nil)
	_ simulator.ReceivesPhaseChange = (*Workload)(nil)
)
