package echo

import (
	"strconv"
	"strings"

	"manifester/core"
	"manifester/format"
)

// mutation injects a bug for the mutant tests; zero is the real broker.
type mutation uint8

const (
	mutNone       mutation = iota
	mutEarlyAck            // ack once the object is stored, before CommitSegment
	mutAckOnMaybe          // ack (with a guessed offset) when a commit times out
	mutBadRead             // GET returns a mangled payload
)

const (
	maxAttempts = 5
	regTimer    = core.TimerID(0) // op timers use the op id (>= 1)
)

type opKind uint8

const (
	opPut opKind = iota
	opGet
)

type opState uint8

const (
	stWaitReg opState = iota
	stPut
	stCommit
	stLookup
	stGet
	stBackoff
)

type conn struct {
	id core.ConnID
	fr framer
}

// op is one client request in flight.
type op struct {
	id       uint64
	conn     core.ConnID
	kind     opKind
	state    opState
	retry    opState // what a backoff restarts
	req      core.ReqID
	gen      core.TimerGen
	attempts int
	maybe    bool // a commit may have applied
	replied  bool // mutants only: already acked early
	payload  []byte
	seg      []byte
	key      string
	ref      core.SegmentRef
	offset   core.Offset
}

// Broker is the echo broker: PUT stores one batch as a segment and commits it
// to the control plane, GET reads it back.
type Broker struct {
	env  core.Env
	self core.NodeRef
	mut  mutation

	epoch      core.Epoch
	registered bool
	regReq     core.ReqID
	regTries   uint64
	regGen     core.TimerGen

	nextReq  core.ReqID
	nextOp   uint64
	timerGen core.TimerGen
	stopping bool
	acked    uint64

	conns []*conn
	ops   []*op
}

// NewBroker is the core.Factory of the echo broker.
func NewBroker(env core.Env, self core.NodeRef) core.Machine {
	return &Broker{env: env, self: self}
}

func (b *Broker) req() core.ReqID { b.nextReq++; return b.nextReq }

func backoff(n int) core.Duration {
	if n > 5 {
		n = 5
	}
	return 20 * core.Millisecond << n
}

func (b *Broker) timer(id core.TimerID, n int) (core.SetTimer, core.TimerGen) {
	b.timerGen++
	return core.SetTimer{ID: id, Gen: b.timerGen, After: backoff(n)}, b.timerGen
}

func (b *Broker) findConn(id core.ConnID) int {
	for i, c := range b.conns {
		if c.id == id {
			return i
		}
	}
	return -1
}

func (b *Broker) findReq(r core.ReqID) *op {
	for _, o := range b.ops {
		if o.req == r && o.req != 0 {
			return o
		}
	}
	return nil
}

func (b *Broker) drop(o *op) {
	for i, x := range b.ops {
		if x == o {
			b.ops = append(b.ops[:i:i], b.ops[i+1:]...)
			return
		}
	}
}

// Step implements core.StateMachine.
func (b *Broker) Step(ev core.Event) []core.Action {
	var acts []core.Action
	switch e := ev.Body.(type) {
	case core.Started:
		acts = b.register()
	case core.Stop:
		acts = b.stop()
	case core.ConnOpened:
		if b.stopping {
			acts = []core.Action{core.CloseConn{Conn: e.Conn, Abort: true}}
		} else {
			b.conns = append(b.conns, &conn{id: e.Conn})
		}
	case core.BytesReceived:
		acts = b.onBytes(e)
	case core.ConnClosed:
		if i := b.findConn(e.Conn); i >= 0 {
			b.conns = append(b.conns[:i:i], b.conns[i+1:]...)
			if !e.Abort {
				acts = []core.Action{core.CloseConn{Conn: e.Conn}}
			}
		}
	case core.TimerFired:
		acts = b.onTimer(e)
	case core.ObjPutDone:
		if o := b.findReq(e.Req); o != nil && o.state == stPut {
			acts = b.onPut(o, e)
		}
	case core.ObjGetDone:
		if o := b.findReq(e.Req); o != nil && o.state == stGet {
			acts = b.onGet(o, e)
		}
	case core.CPDone:
		acts = b.onCP(e)
	}
	return b.finish(acts)
}

// finish ends a graceful stop once nothing is in flight. Trip stays last.
func (b *Broker) finish(acts []core.Action) []core.Action {
	if n := len(acts); n > 0 {
		if _, trip := acts[n-1].(core.Trip); trip {
			return acts
		}
	}
	if b.stopping && len(b.ops) == 0 {
		for _, c := range b.conns {
			acts = append(acts, core.CloseConn{Conn: c.id})
		}
		b.conns = nil
		acts = append(acts, core.Exit{})
	}
	return acts
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
		t, g := b.timer(regTimer, int(b.regTries))
		b.regGen = g
		return []core.Action{t}
	}
	b.epoch, b.registered, b.regTries = resp.Epoch, true, 0
	acts := []core.Action{emit("echo_registered", core.Field{Key: "epoch", Value: core.U64(resp.Epoch)})}
	for _, o := range append([]*op(nil), b.ops...) {
		if o.state == stWaitReg {
			acts = append(acts, b.start(o)...)
		}
	}
	return acts
}

// ---- requests ----

func (b *Broker) stop() []core.Action {
	b.stopping = true
	var acts []core.Action
	for _, o := range append([]*op(nil), b.ops...) {
		if o.state == stWaitReg || o.state == stBackoff {
			acts = append(acts, b.fail(o)...)
		}
	}
	return acts
}

func (b *Broker) onBytes(e core.BytesReceived) []core.Action {
	i := b.findConn(e.Conn)
	if i < 0 {
		return nil
	}
	c := b.conns[i]
	frames, bad := c.fr.push(e.Data)
	var acts []core.Action
	for _, f := range frames {
		acts = append(acts, b.request(c, f)...)
	}
	if bad {
		b.conns = append(b.conns[:i:i], b.conns[i+1:]...)
		acts = append(acts, core.CloseConn{Conn: c.id, Abort: true})
	}
	return acts
}

func (b *Broker) send(c core.ConnID, text string) []core.Action {
	if b.findConn(c) < 0 {
		return nil
	}
	return []core.Action{core.Send{Req: b.req(), Conn: c, Data: appendFrame(nil, text)}}
}

func (b *Broker) request(c *conn, frame string) []core.Action {
	cmd, arg, _ := strings.Cut(frame, " ")
	o := &op{conn: c.id, state: stWaitReg}
	switch {
	case b.stopping:
		return b.send(c.id, "ERR")
	case cmd == "PUT" && arg != "":
		o.kind, o.payload = opPut, []byte(arg)
	case cmd == "GET":
		off, err := strconv.ParseInt(arg, 10, 64)
		if err != nil || off < 0 {
			return b.send(c.id, "ERR")
		}
		o.kind, o.offset = opGet, core.Offset(off)
	default:
		return b.send(c.id, "ERR")
	}
	b.nextOp++
	o.id = b.nextOp
	b.ops = append(b.ops, o)
	if !b.registered {
		return nil
	}
	return b.start(o)
}

func (b *Broker) start(o *op) []core.Action {
	if o.kind == opGet {
		return b.lookup(o)
	}
	data, entries, err := format.Encode([]format.Batch{{TP: tp(), Records: 1, Data: o.payload}})
	if err != nil {
		return b.reply(o, "ERR")
	}
	o.key = format.SegmentKey(b.self.Node, b.self.Inc, b.env.NewID())
	o.seg, o.ref = data, entries[0].Ref(o.key)
	return b.put(o)
}

// reply answers and retires o. A client that left gets nothing.
func (b *Broker) reply(o *op, text string) []core.Action {
	b.drop(o)
	if o.replied {
		return nil
	}
	return b.send(o.conn, text)
}

// fail ends o after the retry budget or a stop: "ERR maybe" if a commit may
// have applied.
func (b *Broker) fail(o *op) []core.Action {
	if o.maybe {
		return b.reply(o, "ERR maybe")
	}
	return b.reply(o, "ERR")
}

// ack emits the ack fact immediately before the Send that acks (decision 12).
func (b *Broker) ack(o *op, off core.Offset) []core.Action {
	b.acked++
	acts := []core.Action{emit("echo_acked",
		core.Field{Key: "offset", Value: core.I64(off)},
		core.Field{Key: "key", Value: core.Str(o.key)},
		core.Field{Key: "payload", Value: core.Bytes(o.payload)})}
	return append(acts, b.send(o.conn, "OK "+strconv.FormatInt(int64(off), 10))...)
}

func (b *Broker) retryLater(o *op, again opState) []core.Action {
	if o.attempts++; o.attempts >= maxAttempts || b.stopping {
		return b.fail(o)
	}
	o.state, o.retry, o.req = stBackoff, again, 0
	t, g := b.timer(core.TimerID(o.id), o.attempts)
	o.gen = g
	return []core.Action{t}
}

func (b *Broker) onTimer(e core.TimerFired) []core.Action {
	if e.ID == regTimer {
		if e.Gen != b.regGen || b.regGen == 0 || b.registered || b.regReq != 0 || b.stopping {
			return nil
		}
		b.regGen = 0
		return b.register()
	}
	for _, o := range b.ops {
		if core.TimerID(o.id) != e.ID {
			continue
		}
		if o.state != stBackoff || o.gen != e.Gen {
			return nil
		}
		switch o.retry {
		case stPut:
			return b.put(o)
		case stCommit:
			b.env.Reachable("echo: commit retried after MaybeApplied")
			return append([]core.Action{emit("echo_commit_retried", core.Field{Key: "key", Value: core.Str(o.key)})}, b.commit(o)...)
		case stLookup:
			return b.lookup(o)
		default:
			return b.get(o)
		}
	}
	return nil
}

// ---- PUT ----

func (b *Broker) put(o *op) []core.Action {
	o.state, o.req = stPut, b.req()
	return []core.Action{core.ObjPut{Req: o.req, Key: o.key, Data: o.seg}}
}

func (b *Broker) onPut(o *op, e core.ObjPutDone) []core.Action {
	if e.Err != core.OK {
		return b.retryLater(o, stPut)
	}
	if b.mut == mutEarlyAck && !o.replied {
		acts := b.ack(o, core.Offset(b.acked))
		o.replied = true
		return append(acts, b.commit(o)...)
	}
	return b.commit(o)
}

func (b *Broker) commit(o *op) []core.Action {
	o.state, o.req = stCommit, b.req()
	return []core.Action{core.CPCall{Req: o.req, Op: core.CommitSegment{
		SegmentKey: o.key,
		Owner:      b.self,
		Epoch:      b.epoch,
		Batches:    []core.PendingBatch{{TP: tp(), Records: 1, Loc: o.ref}},
	}}}
}

func (b *Broker) onCP(e core.CPDone) []core.Action {
	if b.regReq != 0 && e.Req == b.regReq {
		return b.onRegister(e)
	}
	o := b.findReq(e.Req)
	if o == nil {
		return nil
	}
	switch o.state {
	case stCommit:
		return b.onCommit(o, e)
	case stLookup:
		return b.onLookup(o, e)
	}
	return nil
}

func (b *Broker) onCommit(o *op, e core.CPDone) []core.Action {
	switch {
	case e.Err == core.OK:
		resp, ok := e.Resp.(core.CommitSegmentResp)
		okShape := ok && len(resp.Assigned) == 1 && resp.Assigned[0].TP == tp() && resp.Assigned[0].Loc == o.ref
		b.env.Always(okShape, "echo: commit assigns exactly the one batch")
		if !okShape {
			return b.reply(o, "ERR maybe")
		}
		off := resp.Assigned[0].Base
		b.drop(o)
		if o.replied {
			return nil
		}
		return b.ack(o, off)
	case e.Err == core.ErrFenced:
		b.env.Reachable("echo: broker tripped on fencing")
		return []core.Action{emit("echo_fenced", core.Field{Key: "epoch", Value: core.U64(b.epoch)}), core.Trip{Cause: "fenced"}}
	case e.Outcome == core.MaybeApplied:
		o.maybe = true
		if b.mut == mutAckOnMaybe {
			b.drop(o)
			return b.ack(o, core.Offset(b.acked))
		}
		return b.retryLater(o, stCommit) // idempotent per SegmentKey (decision 3)
	}
	return b.reply(o, "ERR")
}

// ---- GET ----

func (b *Broker) lookup(o *op) []core.Action {
	o.state, o.req = stLookup, b.req()
	return []core.Action{core.CPCall{Req: o.req, Op: core.Lookup{TP: tp(), From: o.offset}}}
}

func (b *Broker) onLookup(o *op, e core.CPDone) []core.Action {
	resp, ok := e.Resp.(core.LookupResp)
	if e.Err != core.OK || !ok {
		return b.retryLater(o, stLookup)
	}
	for _, r := range resp.Ranges {
		if r.Base <= o.offset && o.offset < r.Base+core.Offset(r.Count) {
			o.ref = r.Loc
			return b.get(o)
		}
	}
	return b.reply(o, "ERR notfound")
}

func (b *Broker) get(o *op) []core.Action {
	o.state, o.req = stGet, b.req()
	return []core.Action{core.ObjGet{Req: o.req, Ref: core.SegmentRef{Key: o.ref.Key}}}
}

func (b *Broker) onGet(o *op, e core.ObjGetDone) []core.Action {
	if e.Err != core.OK {
		if e.Err == core.ErrNotFound { // committed segments are never deleted
			b.env.Unreachable("echo: committed segment missing")
			return b.reply(o, "ERR")
		}
		return b.retryLater(o, stGet)
	}
	seg, err := format.Decode(e.Data)
	var data []byte
	if err == nil {
		err = format.ErrCorrupt
		for i, en := range seg.Entries {
			if en.Off == o.ref.Off && en.Len == o.ref.Len {
				if data, err = format.VerifyBatch(en, seg.Batches[i].Data); err != nil {
					data = nil
				}
				break
			}
		}
	}
	if err != nil {
		b.env.Reachable("echo: corruption detected on read")
		acts := []core.Action{emit("corruption_detected", core.Field{Key: "key", Value: core.Str(o.ref.Key)})}
		return append(acts, b.reply(o, "ERR corrupt")...)
	}
	if b.mut == mutBadRead {
		data = append(append([]byte(nil), data...), '!')
	}
	return b.reply(o, "VAL "+string(data))
}

func emit(name string, fs ...core.Field) core.Emit {
	return core.Emit{Fact: core.Fact{Name: name, Fields: fs}}
}

// StateHash folds all state in a fixed order.
func (b *Broker) StateHash() uint64 {
	h := newHasher()
	h.u64(uint64(b.epoch))
	h.bool(b.registered)
	h.u64(uint64(b.regReq))
	h.u64(b.regTries)
	h.u64(uint64(b.regGen))
	h.u64(uint64(b.nextReq))
	h.u64(b.nextOp)
	h.u64(uint64(b.timerGen))
	h.bool(b.stopping)
	h.u64(b.acked)
	h.u64(uint64(b.mut))
	h.u64(uint64(len(b.conns)))
	for _, c := range b.conns {
		h.u64(uint64(c.id))
		h.bytes(c.fr.buf)
	}
	h.u64(uint64(len(b.ops)))
	for _, o := range b.ops {
		h.u64(o.id)
		h.u64(uint64(o.conn))
		h.u64(uint64(o.kind))
		h.u64(uint64(o.state))
		h.u64(uint64(o.retry))
		h.u64(uint64(o.req))
		h.u64(uint64(o.gen))
		h.u64(uint64(o.attempts))
		h.bool(o.maybe)
		h.bool(o.replied)
		h.bytes(o.payload)
		h.u64(uint64(len(o.seg)))
		h.str(o.key)
		h.str(o.ref.Key)
		h.u64(uint64(o.ref.Off))
		h.u64(uint64(o.ref.Len))
		h.u64(uint64(o.offset))
	}
	return uint64(h)
}

var _ core.Machine = (*Broker)(nil)
