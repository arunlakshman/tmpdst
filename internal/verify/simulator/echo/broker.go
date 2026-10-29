package echo

import (
	"strconv"
	"strings"

	"manifester/internal/core"
	"manifester/internal/wire/segmentformat"
)

// mutation injects a bug for the mutant tests; zero is the real broker.
type mutation uint8

const (
	bugNone       mutation = iota
	bugEarlyAck            // ack once the object is stored, before CommitSegment
	bugAckOnMaybe          // ack (with a guessed offset) when a commit times out
	bugBadRead             // GET returns a mangled payload
)

const maxAttempts = 5

type opKind uint8

const (
	opPut opKind = iota
	opGet
)

type opState uint8

const (
	statePut opState = iota
	stateCommit
	stateLookup
	stateGet
	stateRetryWait
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
	req      core.RequestID
	gen      core.TimerVersion
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
	env  core.Environment
	self core.NodeInstance
	mut  mutation

	nextReq  core.RequestID
	nextOp   uint64
	timerGen core.TimerVersion
	stopping bool
	acked    uint64

	conns []*conn
	ops   []*op
}

// NewBroker is the core.MachineFactory of the echo broker.
func NewBroker(env core.Environment, self core.NodeInstance) core.Machine {
	return &Broker{env: env, self: self}
}

func (b *Broker) req() core.RequestID { b.nextReq++; return b.nextReq }

func backoff(n int) core.Duration {
	if n > 5 {
		n = 5
	}
	return 20 * core.Millisecond << n
}

func (b *Broker) timer(id core.TimerID, n int) (core.SetTimer, core.TimerVersion) {
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

func (b *Broker) findReq(r core.RequestID) *op {
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
	case core.StopRequest:
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
	case core.ObjectPutDone:
		if o := b.findReq(e.Req); o != nil && o.state == statePut {
			acts = b.onPut(o, e)
		}
	case core.ObjectGetDone:
		if o := b.findReq(e.Req); o != nil && o.state == stateGet {
			acts = b.onGet(o, e)
		}
	case core.ControlPlaneDone:
		acts = b.onCP(e)
	}
	return b.finish(acts)
}

// finish ends a graceful stop once nothing is in flight.
func (b *Broker) finish(acts []core.Action) []core.Action {
	if b.stopping && len(b.ops) == 0 {
		for _, c := range b.conns {
			acts = append(acts, core.CloseConn{Conn: c.id})
		}
		b.conns = nil
		acts = append(acts, core.ShutdownDone{})
	}
	return acts
}

// ---- requests ----

func (b *Broker) stop() []core.Action {
	b.stopping = true
	var acts []core.Action
	for _, o := range append([]*op(nil), b.ops...) {
		if o.state == stateRetryWait {
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
	o := &op{conn: c.id}
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
	return b.start(o)
}

func (b *Broker) start(o *op) []core.Action {
	if o.kind == opGet {
		return b.lookup(o)
	}
	data, entries, err := segmentformat.Encode([]segmentformat.Batch{{TP: tp(), Records: 1, Data: o.payload}})
	if err != nil {
		return b.reply(o, "ERR")
	}
	o.key = segmentformat.SegmentKey(b.env.NewID())
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
	o.state, o.retry, o.req = stateRetryWait, again, 0
	t, g := b.timer(core.TimerID(o.id), o.attempts)
	o.gen = g
	return []core.Action{t}
}

func (b *Broker) onTimer(e core.TimerFired) []core.Action {
	for _, o := range b.ops {
		if core.TimerID(o.id) != e.ID {
			continue
		}
		if o.state != stateRetryWait || o.gen != e.Gen {
			return nil
		}
		switch o.retry {
		case statePut:
			return b.put(o)
		case stateCommit:
			b.env.Reachable("echo: commit retried after MaybeApplied")
			return append([]core.Action{emit("echo_commit_retried", core.Field{Key: "key", Value: core.Str(o.key)})}, b.commit(o)...)
		case stateLookup:
			return b.lookup(o)
		default:
			return b.get(o)
		}
	}
	return nil
}

// ---- PUT ----

func (b *Broker) put(o *op) []core.Action {
	o.state, o.req = statePut, b.req()
	return []core.Action{core.ObjectPut{Req: o.req, Key: o.key, Data: o.seg}}
}

func (b *Broker) onPut(o *op, e core.ObjectPutDone) []core.Action {
	if e.Err != core.OK {
		return b.retryLater(o, statePut)
	}
	if b.mut == bugEarlyAck && !o.replied {
		acts := b.ack(o, core.Offset(b.acked))
		o.replied = true
		return append(acts, b.commit(o)...)
	}
	return b.commit(o)
}

func (b *Broker) commit(o *op) []core.Action {
	o.state, o.req = stateCommit, b.req()
	return []core.Action{core.ControlPlaneCall{Req: o.req, Op: core.CommitSegment{
		SegmentKey: o.key,
		Batches:    []core.PendingBatch{{TP: tp(), Records: 1, Loc: o.ref}},
	}}}
}

func (b *Broker) onCP(e core.ControlPlaneDone) []core.Action {
	o := b.findReq(e.Req)
	if o == nil {
		return nil
	}
	switch o.state {
	case stateCommit:
		return b.onCommit(o, e)
	case stateLookup:
		return b.onLookup(o, e)
	}
	return nil
}

func (b *Broker) onCommit(o *op, e core.ControlPlaneDone) []core.Action {
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
	case e.Err == core.ErrConflict:
		// The same key with a different request: a broker bug, not a race.
		b.env.Unreachable("echo: commit key reused with a different request")
		return b.reply(o, "ERR")
	case e.Outcome == core.MaybeApplied:
		o.maybe = true
		if b.mut == bugAckOnMaybe {
			b.drop(o)
			return b.ack(o, core.Offset(b.acked))
		}
		return b.retryLater(o, stateCommit) // idempotent per SegmentKey (decision 3)
	}
	return b.reply(o, "ERR")
}

// ---- GET ----

func (b *Broker) lookup(o *op) []core.Action {
	o.state, o.req = stateLookup, b.req()
	return []core.Action{core.ControlPlaneCall{Req: o.req, Op: core.FindSegments{TP: tp(), From: o.offset}}}
}

func (b *Broker) onLookup(o *op, e core.ControlPlaneDone) []core.Action {
	resp, ok := e.Resp.(core.FindSegmentsResp)
	if e.Err != core.OK || !ok {
		return b.retryLater(o, stateLookup)
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
	o.state, o.req = stateGet, b.req()
	return []core.Action{core.ObjectGet{Req: o.req, Ref: core.SegmentRef{Key: o.ref.Key}}}
}

func (b *Broker) onGet(o *op, e core.ObjectGetDone) []core.Action {
	if e.Err != core.OK {
		if e.Err == core.ErrNotFound { // committed segments are never deleted
			b.env.Unreachable("echo: committed segment missing")
			return b.reply(o, "ERR")
		}
		return b.retryLater(o, stateGet)
	}
	seg, err := segmentformat.Decode(e.Data)
	var data []byte
	if err == nil {
		err = segmentformat.ErrCorrupt
		for i, en := range seg.Entries {
			if en.Off == o.ref.Off && en.Len == o.ref.Len {
				if data, err = segmentformat.VerifyBatch(en, seg.Batches[i].Data); err != nil {
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
	if b.mut == bugBadRead {
		data = append(append([]byte(nil), data...), '!')
	}
	return b.reply(o, "VAL "+string(data))
}

func emit(name string, fs ...core.Field) core.RecordFact {
	return core.RecordFact{Fact: core.Fact{Name: name, Fields: fs}}
}

// StateHash folds all state in a fixed order.
func (b *Broker) StateHash() uint64 {
	h := core.NewHasher()
	h.U64(uint64(b.nextReq))
	h.U64(b.nextOp)
	h.U64(uint64(b.timerGen))
	h.Bool(b.stopping)
	h.U64(b.acked)
	h.U64(uint64(b.mut))
	h.U64(uint64(len(b.conns)))
	for _, c := range b.conns {
		h.U64(uint64(c.id))
		h.Bytes(c.fr.buf)
	}
	h.U64(uint64(len(b.ops)))
	for _, o := range b.ops {
		h.U64(o.id)
		h.U64(uint64(o.conn))
		h.U64(uint64(o.kind))
		h.U64(uint64(o.state))
		h.U64(uint64(o.retry))
		h.U64(uint64(o.req))
		h.U64(uint64(o.gen))
		h.U64(uint64(o.attempts))
		h.Bool(o.maybe)
		h.Bool(o.replied)
		h.Bytes(o.payload)
		h.U64(uint64(len(o.seg)))
		h.Str(o.key)
		h.Str(o.ref.Key)
		h.U64(uint64(o.ref.Off))
		h.U64(uint64(o.ref.Len))
		h.U64(uint64(o.offset))
	}
	return uint64(h)
}

var _ core.Machine = (*Broker)(nil)
