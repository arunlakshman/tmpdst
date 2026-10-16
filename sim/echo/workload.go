package echo

import (
	"fmt"
	"strconv"
	"strings"

	"manifester/core"
	"manifester/sim"
)

// PutOp and GetOp are the History ops. A PUT result is the reply text, or ""
// when the client lost the answer (OpIndeterminate).
type PutOp struct {
	Client, Seq int
	Payload     string
}
type GetOp struct {
	Client int
	Offset int64
}

// Opts configures the workload.
type Opts struct {
	Clients int
	// StopAfter: clients issue no new request once this much node-local time has
	// passed since Started. The kernel does not tell workloads the phase, so the
	// preset passes Phases.Chaos+Phases.Settling.
	StopAfter core.Duration
	// RecycleAt: every client drops its connection at its next op boundary after
	// this time, so a black-holed pre-Settling connection cannot starve it.
	RecycleAt core.Duration
}

const (
	reqTimeout = 3 * core.Second
	keepAcked  = 16 // GET candidates per client
)

type cstate uint8

const (
	csBackoff cstate = iota // wake timer pending, then dial
	csDial
	csReady // connected, wake timer pending (think time)
	csBusy  // request outstanding
	csDone
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
	id      int
	state   cstate
	conn    core.ConnID
	dialReq core.ReqID
	sendReq core.ReqID
	dials   int
	fails   int // consecutive dial/connection failures
	wakeGen core.TimerGen
	reqGen  core.TimerGen
	fr      framer
	seq     int
	recycle bool
	acked   []acked
	digest  hasher // every ack ever recorded
	nacked  uint64
	pend    *pending
}

// Workload is N closed-loop clients speaking the echo protocol.
type Workload struct {
	env     core.Env
	self    core.NodeRef
	brokers []core.NodeID
	opts    Opts
	hist    sim.History

	startAt core.Time
	nextReq core.ReqID
	clients []*client
}

// NewWorkload builds the workload machine.
func NewWorkload(env core.Env, self core.NodeRef, brokers []core.NodeRef, o Opts) *Workload {
	w := &Workload{env: env, self: self, opts: o}
	for _, b := range brokers {
		w.brokers = append(w.brokers, b.Node)
	}
	for i := 0; i < o.Clients; i++ {
		w.clients = append(w.clients, &client{id: i, digest: newHasher()})
	}
	return w
}

func (w *Workload) Name() string { return "echo" }

// SetHistory is called by the kernel.
func (w *Workload) SetHistory(h sim.History) { w.hist = h }

// Quiescent reports that no request is outstanding.
func (w *Workload) Quiescent() bool {
	for _, c := range w.clients {
		if c.pend != nil {
			return false
		}
	}
	return true
}

func wakeID(c *client) core.TimerID { return core.TimerID(1 + c.id*2) }
func reqID(c *client) core.TimerID  { return core.TimerID(2 + c.id*2) }

const recycleTimer = core.TimerID(0)

func (w *Workload) req() core.ReqID { w.nextReq++; return w.nextReq }

// Step implements core.StateMachine.
func (w *Workload) Step(ev core.Event) []core.Action {
	var acts []core.Action
	switch e := ev.Body.(type) {
	case core.Started:
		w.startAt = ev.At
		acts = append(acts, core.SetTimer{ID: recycleTimer, Gen: 1, After: w.opts.RecycleAt})
		for _, c := range w.clients {
			acts = append(acts, w.dial(c)...)
		}
	case core.TimerFired:
		acts = w.onTimer(ev.At, e)
	case core.DialDone:
		for _, c := range w.clients {
			if c.state == csDial && c.dialReq == e.Req {
				acts = w.onDial(c, e)
			}
		}
	case core.BytesReceived:
		if c := w.byConn(e.Conn); c != nil {
			acts = w.onBytes(ev.At, c, e.Data)
		}
	case core.ConnClosed:
		if c := w.byConn(e.Conn); c != nil {
			acts = w.lost(c, "conn_closed", false)
		}
	case core.SendDone:
		if c := w.byConn(e.Conn); c != nil && e.Err != core.OK && c.sendReq == e.Req {
			acts = w.lost(c, "send_failed", true)
		}
	}
	return acts
}

func (w *Workload) byConn(id core.ConnID) *client {
	for _, c := range w.clients {
		if c.conn == id && id != 0 && (c.state == csReady || c.state == csBusy) {
			return c
		}
	}
	return nil
}

func (w *Workload) wake(c *client, d core.Duration) core.Action {
	c.wakeGen++
	return core.SetTimer{ID: wakeID(c), Gen: c.wakeGen, After: d}
}

func (w *Workload) dial(c *client) []core.Action {
	c.state, c.dialReq = csDial, w.req()
	addr := string(w.brokers[(c.id+c.dials)%len(w.brokers)])
	c.dials++
	return []core.Action{core.Dial{Req: c.dialReq, Addr: addr}}
}

func (w *Workload) onDial(c *client, e core.DialDone) []core.Action {
	c.dialReq = 0
	if e.Err != core.OK {
		c.fails++
		c.state = csBackoff
		return []core.Action{w.wake(c, retryDelay(c.fails))}
	}
	c.conn, c.fails, c.fr = e.Conn, 0, framer{}
	c.state = csReady
	return []core.Action{w.wake(c, core.Millisecond)}
}

func retryDelay(n int) core.Duration {
	if n > 4 {
		n = 4
	}
	return 50 * core.Millisecond << n
}

func (w *Workload) onTimer(now core.Time, e core.TimerFired) []core.Action {
	if e.ID == recycleTimer {
		var acts []core.Action
		for _, c := range w.clients {
			c.recycle = true
			if c.state == csReady { // idle: recycle now
				acts = append(acts, w.next(now, c)...)
			}
		}
		return acts
	}
	for _, c := range w.clients {
		switch {
		case e.ID == wakeID(c):
			if e.Gen != c.wakeGen {
				return nil
			}
			switch c.state {
			case csBackoff:
				return w.dial(c)
			case csReady:
				return w.next(now, c)
			}
			return nil
		case e.ID == reqID(c):
			if e.Gen != c.reqGen || c.state != csBusy {
				return nil
			}
			return w.lost(c, "timeout", true)
		}
	}
	return nil
}

// next issues the client's next request, or recycles / stops.
func (w *Workload) next(now core.Time, c *client) []core.Action {
	if core.Duration(now-w.startAt) >= w.opts.StopAfter {
		c.state = csDone
		c.wakeGen++
		return nil
	}
	if c.recycle {
		c.recycle = false
		acts := []core.Action{core.CloseConn{Conn: c.conn, Abort: true}}
		c.conn, c.state = 0, csBackoff
		return append(acts, w.dial(c)...)
	}
	p := &pending{id: w.env.NewID()}
	var op any
	var body string
	if w.env.Intn(3) == 0 && len(c.acked) > 0 {
		a := c.acked[w.env.Intn(len(c.acked))]
		p.get, p.off = true, a.off
		op, body = GetOp{c.id, a.off}, "GET "+strconv.FormatInt(a.off, 10)
	} else {
		c.seq++
		p.seq = c.seq
		p.payload = fmt.Sprintf("c%d-s%d-%s", c.id, c.seq, strings.Repeat("x", w.env.Intn(48)))
		op, body = PutOp{c.id, c.seq, p.payload}, "PUT "+p.payload
	}
	c.pend, c.state = p, csBusy
	if w.hist != nil {
		w.hist.Invoke(w.self, p.id, p.id, op)
	}
	c.sendReq = w.req()
	c.reqGen++
	return []core.Action{
		core.Send{Req: c.sendReq, Conn: c.conn, Data: appendFrame(nil, body)},
		core.SetTimer{ID: reqID(c), Gen: c.reqGen, After: reqTimeout},
	}
}

func (w *Workload) onBytes(now core.Time, c *client, data []byte) []core.Action {
	frames, bad := c.fr.push(data)
	var acts []core.Action
	for _, f := range frames {
		if c.state != csBusy || c.pend == nil {
			bad = true // unsolicited reply
			break
		}
		acts = append(acts, w.reply(c, f)...)
	}
	if bad && c.conn != 0 {
		return append(acts, w.lost(c, "bad_stream", true)...)
	}
	return acts
}

// reply completes the outstanding request with the broker's answer.
func (w *Workload) reply(c *client, text string) []core.Action {
	p := c.pend
	c.pend, c.state = nil, csReady
	c.reqGen++
	out := sim.OpDefinite
	if !p.get {
		if rest, ok := strings.CutPrefix(text, "OK "); ok {
			if off, err := strconv.ParseInt(rest, 10, 64); err == nil {
				c.record(off, p.payload)
			}
		} else if text == "ERR maybe" {
			out = sim.OpIndeterminate
		}
	}
	if w.hist != nil {
		w.hist.Return(p.id, text, out)
	}
	think := core.Duration(200+w.env.Intn(600)) * core.Millisecond
	return []core.Action{core.CancelTimer{ID: reqID(c)}, w.wake(c, think)}
}

func (c *client) record(off int64, payload string) {
	c.nacked++
	c.digest.u64(uint64(off))
	c.digest.str(payload)
	c.acked = append(c.acked, acked{off, payload})
	if len(c.acked) > keepAcked {
		c.acked = append([]acked(nil), c.acked[1:]...)
	}
}

// lost handles a dead or unusable connection: the outstanding request's fate is
// unknown, and the client reconnects with backoff.
func (w *Workload) lost(c *client, why string, abort bool) []core.Action {
	var acts []core.Action
	if abort {
		acts = append(acts, core.CloseConn{Conn: c.conn, Abort: true})
	}
	if p := c.pend; p != nil {
		out := sim.OpIndeterminate
		if p.get {
			out = sim.OpAbandoned
		}
		if w.hist != nil {
			w.hist.Return(p.id, why, out)
		}
	}
	c.pend, c.conn, c.fr = nil, 0, framer{}
	c.reqGen++
	c.fails++
	c.state = csBackoff
	return append(acts, core.CancelTimer{ID: reqID(c)}, w.wake(c, retryDelay(c.fails)))
}

// StateHash folds all state.
func (w *Workload) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(w.startAt))
	h.u64(uint64(w.nextReq))
	for _, c := range w.clients {
		h.u64(uint64(c.state))
		h.u64(uint64(c.conn))
		h.u64(uint64(c.dialReq))
		h.u64(uint64(c.sendReq))
		h.u64(uint64(c.dials))
		h.u64(uint64(c.fails))
		h.u64(uint64(c.wakeGen))
		h.u64(uint64(c.reqGen))
		h.bytes(c.fr.buf)
		h.u64(uint64(c.seq))
		h.bool(c.recycle)
		h.u64(uint64(len(c.acked)))
		h.u64(uint64(c.digest))
		h.u64(c.nacked)
		h.bool(c.pend != nil)
		if p := c.pend; p != nil {
			h.bytes(p.id[:])
			h.bool(p.get)
			h.u64(uint64(p.seq))
			h.str(p.payload)
			h.u64(uint64(p.off))
		}
	}
	return uint64(h)
}

var _ sim.Workload = (*Workload)(nil)
