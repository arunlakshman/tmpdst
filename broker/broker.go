// Package broker is the Kafka broker state machine: stateless, pure and
// deterministic (core.Machine), with S3 as the data plane and the control
// plane assigning offsets. The simulator and sys.Run drive the same code.
//
// Pipeline of a produce: the batch joins the open segment buffer; a flush
// (BatchWindow timer or MaxSegmentBytes) encodes it with format.Encode,
// uploads it (ObjPut) and commits it (CommitSegment); only then is the client
// answered, with a "produce_acked" fact emitted right before the Send.
// Commits go out strictly in segment order so a producer's sequences reach the
// control plane in order. Every retry reuses the segment key, which both the
// object store and the control plane treat idempotently.
//
// Fail-stop (decision 7): a CommitSegment answered ErrFenced trips the broker;
// afterwards Step returns nothing and nothing is acked.
package broker

import (
	"manifester/core"
	"manifester/kwire"
)

// Fixed timer ids; retry timers use the id of the segment or op they belong to
// (ids >= firstID).
const (
	timerReg   = core.TimerID(1)
	timerFlush = core.TimerID(2)
	firstID    = 16
)

// Broker is the machine. Build it with New.
type Broker struct {
	env   core.Env
	self  core.NodeRef
	cfg   Config
	codec kwire.Codec

	started    bool
	epoch      core.Epoch
	registered bool
	regReq     core.ReqID
	regTries   uint64
	regGen     core.TimerGen

	nextReq  core.ReqID
	nextID   uint64
	timerGen core.TimerGen
	stopping bool
	tripped  bool
	exited   bool

	conns  []*conn
	dirty  []*conn // connections whose queue head may be ready (drained at end of Step)
	parked []parked
	topics []core.TopicInfo // topic cache, refreshed from DescribeTopics on a miss

	now        core.Time // At of the event being stepped (set by Step; not state)
	open       openSegment
	segs       []*segment // sealed segments in commit order
	ops        []*op
	bufBytes   int       // produce bytes admitted and not yet answered
	pressure   bool      // above MaxBufferedBytes, until back at ResumeBufferedBytes
	pressureAt core.Time // when pressure began (fact timing only, not state)
	guess      uint64
}

// parked is a request waiting for registration.
type parked struct {
	s   *slot
	req core.Request
}

// New returns the core.Factory of the broker.
func New(cfg Config) core.Factory {
	cfg = cfg.withDefaults()
	return func(env core.Env, self core.NodeRef) core.Machine {
		return &Broker{env: env, self: self, cfg: cfg, nextID: firstID}
	}
}

func (b *Broker) req() core.ReqID { b.nextReq++; return b.nextReq }
func (b *Broker) id() uint64      { b.nextID++; return b.nextID }

func (b *Broker) backoff(n int) core.Duration {
	d := b.cfg.RetryBase
	for ; n > 1 && d < b.cfg.RetryMax; n-- {
		d *= 2
	}
	return min(d, b.cfg.RetryMax)
}

func (b *Broker) timer(id core.TimerID, n int) (core.SetTimer, core.TimerGen) {
	b.timerGen++
	return core.SetTimer{ID: id, Gen: b.timerGen, After: b.backoff(n)}, b.timerGen
}

// Step implements core.StateMachine.
func (b *Broker) Step(ev core.Event) []core.Action {
	if b.tripped || b.exited {
		return nil
	}
	b.now = ev.At
	var acts []core.Action
	switch e := ev.Body.(type) {
	case core.Started:
		if !b.started {
			b.started = true
			acts = b.register()
		}
	case core.Stop:
		acts = b.stop()
	case core.ConnOpened:
		acts = b.onOpen(e)
	case core.BytesReceived:
		acts = b.onBytes(e)
	case core.ConnClosed:
		acts = b.onClosed(e)
	case core.TimerFired:
		acts = b.onTimer(e)
	case core.ObjPutDone:
		acts = b.onPut(e)
	case core.ObjGetDone:
		acts = b.onGet(e)
	case core.CPDone:
		acts = b.onCP(e)
	}
	return b.finish(acts)
}

// finish drains ready responses, applies admission control and ends a graceful
// stop. After a trip nothing is added: Trip stays the last action.
func (b *Broker) finish(acts []core.Action) []core.Action {
	if b.tripped {
		b.dirty = nil
		return acts
	}
	acts = b.drainDirty(acts)
	acts = b.flow(acts)
	return b.maybeExit(acts)
}

// ---- registration ----

func (b *Broker) register() []core.Action {
	b.regReq = b.req()
	return []core.Action{core.CPCall{Req: b.regReq, Op: core.Register{Self: b.self}}}
}

func (b *Broker) onRegister(e core.CPDone) []core.Action {
	b.regReq = 0
	resp, ok := e.Resp.(core.RegisterResp)
	if e.Err != core.OK || !ok {
		if b.stopping {
			return nil
		}
		b.regTries++
		t, g := b.timer(timerReg, int(b.regTries))
		b.regGen = g
		return []core.Action{t}
	}
	b.epoch, b.registered, b.regTries = resp.Epoch, true, 0
	acts := []core.Action{emit("broker_registered", core.Field{Key: "epoch", Value: core.U64(resp.Epoch)})}
	ps := b.parked
	b.parked = nil
	for _, p := range ps {
		acts = append(acts, b.handle(p.s, p.req)...)
	}
	return acts
}

// ---- stop ----

// stop stops admission, flushes the open segment and lets in-flight work
// finish; maybeExit emits Exit once everything is answered.
func (b *Broker) stop() []core.Action {
	if b.stopping {
		return nil
	}
	b.stopping = true
	// Requests that never got to run (not registered) are dropped with their
	// connections: the client retries elsewhere.
	for _, p := range b.parked {
		p.s.silent = true
		p.s.conn.closing = true
		p.s.pending = 1
		b.finishPart(p.s)
	}
	b.parked = nil
	return b.seal(sealStop)
}

func (b *Broker) idle() bool {
	if len(b.ops) > 0 || len(b.segs) > 0 || len(b.open.batches) > 0 {
		return false
	}
	for _, c := range b.conns {
		if len(c.q) > 0 {
			return false
		}
	}
	return true
}

func (b *Broker) maybeExit(acts []core.Action) []core.Action {
	if !b.stopping || b.exited || !b.idle() {
		return acts
	}
	b.exited = true
	for _, c := range b.conns {
		acts = append(acts, core.CloseConn{Conn: c.id})
	}
	b.conns = nil
	return append(acts, emit("broker_exited"), core.Exit{})
}

// ---- admission control ----

// flow pauses reads on every client connection while the broker holds too many
// unacked produce bytes (or is stopping) and resumes below the low mark.
func (b *Broker) flow(acts []core.Action) []core.Action {
	switch {
	case !b.pressure && b.bufBytes >= b.cfg.MaxBufferedBytes:
		b.pressure, b.pressureAt = true, b.now
		b.env.Reachable("broker: reads paused by backpressure")
		acts = append(acts, emit("backpressure_on",
			core.Field{Key: "buffered", Value: core.I64(b.bufBytes)}))
	case b.pressure && b.bufBytes <= b.cfg.ResumeBufferedBytes:
		b.pressure = false
		b.env.Reachable("broker: reads resumed after backpressure")
		acts = append(acts, emit("backpressure_off",
			core.Field{Key: "buffered", Value: core.I64(b.bufBytes)},
			core.Field{Key: "paused_ns", Value: core.I64(b.now - b.pressureAt)}))
	}
	want := b.pressure || b.stopping
	for _, c := range b.conns {
		if c.closing || c.paused == want {
			continue
		}
		c.paused = want
		if want {
			acts = append(acts, core.PauseReads{Conn: c.id})
		} else {
			acts = append(acts, core.ResumeReads{Conn: c.id})
		}
	}
	return acts
}

// ---- timers ----

func (b *Broker) onTimer(e core.TimerFired) []core.Action {
	switch e.ID {
	case timerReg:
		if e.Gen != b.regGen || b.regGen == 0 || b.registered || b.regReq != 0 || b.stopping {
			return nil
		}
		b.regGen = 0
		return b.register()
	case timerFlush:
		if e.Gen != b.open.gen || b.open.gen == 0 {
			return nil
		}
		return b.seal(sealTimer)
	}
	for _, s := range b.segs {
		if core.TimerID(s.id) == e.ID {
			return b.segTimer(s, e.Gen)
		}
	}
	for _, o := range b.ops {
		if core.TimerID(o.id) == e.ID {
			return b.opTimer(o, e.Gen)
		}
	}
	return nil
}

// ---- completions ----

func (b *Broker) onCP(e core.CPDone) []core.Action {
	if b.regReq != 0 && e.Req == b.regReq {
		return b.onRegister(e)
	}
	for _, s := range b.segs {
		if s.req == e.Req && s.req != 0 && s.state == segCommit {
			return b.onCommit(s, e)
		}
	}
	if o := b.findOp(e.Req); o != nil && o.state == opCP {
		return b.onOpCP(o, e)
	}
	return nil
}

func (b *Broker) onPut(e core.ObjPutDone) []core.Action {
	for _, s := range b.segs {
		if s.req == e.Req && s.req != 0 && s.state == segPut {
			return b.onPutDone(s, e)
		}
	}
	return nil
}

func (b *Broker) onGet(e core.ObjGetDone) []core.Action {
	if o := b.findOp(e.Req); o != nil && o.state == opGet {
		return b.onFetchGet(o, e)
	}
	return nil
}

func emit(name string, fs ...core.Field) core.Emit {
	return core.Emit{Fact: core.Fact{Name: name, Fields: fs}}
}

var _ core.Machine = (*Broker)(nil)
