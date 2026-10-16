// Package netsim is the simulated network engine: TCP-like byte streams
// between nodes (decision 6). Address = NodeID string.
//
// Model
//   - A connection is a pair of ends (dialer = side 0, listener = side 1) and
//     two directions; direction d carries bytes from side d to side 1-d. Each
//     direction has its own Lane (decision C) and monotone arrival times, so
//     bytes never reorder even when chunks draw different latencies.
//   - Dial: live target -> ConnIDs allocated now (per-node counters, never
//     reused across incarnations), DialDone{OK} to the dialer and
//     ConnOpened to the listener, both after latency. Down target ->
//     DialDone{ErrConnClosed} after latency. A partitioned pair delays the
//     dial until the partition heals.
//   - Send: SendDone{OK} means accepted, not delivered: it arrives one latency
//     sample after issue, independent of delivery, pause or faults. Data is
//     split into chunks of 1..PartialIOMax bytes (drawn at issue). A Send on a
//     closed or aborted conn gets SendDone{ErrConnClosed}.
//   - CloseConn graceful: FIN queued behind the bytes; peer sees EOF
//     (ConnClosed{Abort:false}) after its last bytes. The peer may still send
//     (half-close); bytes to a locally closed end are dropped silently.
//     Abort: queued undelivered bytes are dropped, peer gets
//     ConnClosed{Abort:true}.
//   - PauseReads/ResumeReads: bytes for a paused end are held and released in
//     order on resume. SendDone is NOT held back: the sim does not model the
//     sender's flow-control window, so backpressure is invisible to senders.
//     An RST is not held by pause.
//   - Faults are drawn at issue (Send; Dial draws partition only) and never
//     after EnterRecovery.
//     RandomClose: conn aborted by the network, both ends get Abort:true; the
//     triggering Send gets ErrConnClosed. BlackHole: this direction silently
//     drops everything (incl. FIN) until closed; sends still complete OK.
//     Partition: a node pair is cut for a window; landing bytes (and FIN/RST)
//     freeze and are delivered in order when it heals. An abort during a
//     partition closes the local end now; the peer learns of it at heal.
//     Clog: chunks issued to a direction while clogged arrive after the clog.
//   - OnCrash: every open end at the node is retired; surviving peers get
//     ConnClosed{Abort:true} after latency, behind bytes the dead node had
//     already put on the wire. Bytes to the dead node are dropped.
package netsim

import (
	"hash/crc32"
	"sort"

	"manifester/core"
	"manifester/sim"
)

// Engine is the network engine. Not safe for concurrent use.
type Engine struct {
	f        sim.NetworkFaults
	recovery bool
	nextPair uint64
	nextConn map[core.NodeID]core.ConnID
	pairs    []*pair // ascending id
	ends     map[endKey]endRef
	links    []*link // sorted by (a, b), a <= b
}

// New returns a network engine with the given fault menu.
func New(f sim.NetworkFaults) *Engine {
	return &Engine{f: f, nextConn: map[core.NodeID]core.ConnID{}, ends: map[endKey]endRef{}}
}

type endKey struct {
	node core.NodeID
	id   core.ConnID
}

type endRef struct {
	pair uint64
	side int
}

type end struct {
	ref    core.NodeRef
	id     core.ConnID
	closed bool // closed locally, reset, or crashed: no more sends or delivery
	eof    bool // saw peer FIN
	paused bool
}

type itemKind uint8

const (
	kData itemKind = iota
	kFin
	kRst
)

type item struct {
	kind itemKind
	data []byte
	seq  uint64
	id   sim.EntryID
	net  bool // network-originated RST: passes a black hole
}

type dir struct {
	lane      sim.Lane
	lastArr   sim.Time
	clogUntil sim.Time
	seq       uint64
	flight    []item // scheduled, not yet landed; lands strictly in order
	held      []item // landed, not yet delivered (pause, partition)
	aborted   bool   // no new sends accepted
	blackhole bool
}

type pair struct {
	id   uint64
	end  [2]end
	dirs [2]dir // dirs[d]: side d -> side 1-d
}

// link is per unordered node pair: permanent extra latency and partition state.
type link struct {
	a, b     core.NodeID
	extra    core.Duration
	healAt   sim.Time
	everCut  bool
	hasExtra bool
}

// ---- engine-private entry bodies (plain data) ----

type landBody struct {
	Pair uint64
	Dir  int
	Seq  uint64
}
type rstBody struct {
	Pair uint64
	Dir  int
	Seq  uint64
}
type healBody struct{ A, B core.NodeID }

func (e *Engine) Name() string { return "netsim" }

func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.Send, core.Dial, core.CloseConn, core.PauseReads, core.ResumeReads:
		return true
	}
	return false
}

func (e *Engine) EnterRecovery() { e.recovery = true }

// ---- helpers ----

func laneOf(pair uint64, k uint64) sim.Lane { return sim.Lane(1<<62 | pair<<3 | k) }

const (
	laneAck  = 2 // +side: SendDone to that side
	laneMisc = 4 // dial completions
)

func span(r sim.CountedRand, lo, hi core.Duration) core.Duration {
	if hi <= lo {
		return lo
	}
	return core.Duration(r.Between(int64(lo), int64(hi)))
}

func (e *Engine) sample(r sim.CountedRand) core.Duration {
	l := e.f.Latency
	if r.Chance(l.SlowProb) {
		return span(r, l.SlowMin, l.SlowMax)
	}
	return span(r, l.FastMin, l.FastMax)
}

func (e *Engine) findPair(id uint64) *pair {
	i := sort.Search(len(e.pairs), func(i int) bool { return e.pairs[i].id >= id })
	if i < len(e.pairs) && e.pairs[i].id == id {
		return e.pairs[i]
	}
	return nil
}

func (e *Engine) lookup(n core.NodeID, c core.ConnID) (*pair, int) {
	r, ok := e.ends[endKey{n, c}]
	if !ok {
		return nil, 0
	}
	p := e.findPair(r.pair)
	if p == nil {
		return nil, 0
	}
	return p, r.side
}

func (e *Engine) maybeRemove(p *pair) {
	if !p.end[0].closed || !p.end[1].closed {
		return
	}
	for s := 0; s < 2; s++ {
		delete(e.ends, endKey{p.end[s].ref.Node, p.end[s].id})
	}
	i := sort.Search(len(e.pairs), func(i int) bool { return e.pairs[i].id >= p.id })
	if i < len(e.pairs) && e.pairs[i] == p {
		e.pairs = append(e.pairs[:i], e.pairs[i+1:]...)
	}
}

func (e *Engine) link(a, b core.NodeID) *link {
	if a > b {
		a, b = b, a
	}
	i := sort.Search(len(e.links), func(i int) bool {
		l := e.links[i]
		return l.a > a || (l.a == a && l.b >= b)
	})
	if i < len(e.links) && e.links[i].a == a && e.links[i].b == b {
		return e.links[i]
	}
	l := &link{a: a, b: b}
	e.links = append(e.links, nil)
	copy(e.links[i+1:], e.links[i:])
	e.links[i] = l
	return l
}

// pairExtra is the per-pair permanent extra latency, sampled once on first use.
func (e *Engine) pairExtra(ctx sim.EngineCtx, a, b core.NodeID) core.Duration {
	l := e.link(a, b)
	if !l.hasExtra {
		l.hasExtra = true
		if m := e.f.PairLatencyMax; m > 0 {
			l.extra = span(ctx.Rand(), 0, m+1)
		}
	}
	return l.extra
}

func (e *Engine) partitioned(ctx sim.EngineCtx, a, b core.NodeID) bool {
	l := e.link(a, b)
	return l.everCut && ctx.Now() < l.healAt
}

// clearDir drops undelivered data and FIN on d; pending RSTs stand.
func (e *Engine) clearDir(ctx sim.EngineCtx, d *dir) {
	keep := func(its []item, cancel bool) []item {
		var out []item
		for _, it := range its {
			if it.kind == kRst {
				out = append(out, it)
			} else if cancel {
				ctx.Cancel(it.id)
			}
		}
		return out
	}
	d.flight, d.held = keep(d.flight, true), keep(d.held, false)
}

// arrival returns the next arrival time on d for something issued now with
// latency lat: monotone, and not before an active clog ends.
func (d *dir) arrival(now sim.Time, lat core.Duration) sim.Time {
	at := now + sim.Time(lat)
	if d.clogUntil > now && d.clogUntil+sim.Time(lat) > at {
		at = d.clogUntil + sim.Time(lat)
	}
	if at < d.lastArr {
		at = d.lastArr
	}
	d.lastArr = at
	return at
}

// enqueue puts a data or FIN item in flight on direction di of p.
func (e *Engine) enqueue(ctx sim.EngineCtx, p *pair, di int, it item, lat core.Duration) {
	d := &p.dirs[di]
	d.seq++
	it.seq = d.seq
	at := d.arrival(ctx.Now(), lat)
	it.id = ctx.Self(at, d.lane, landBody{p.id, di, it.seq})
	d.flight = append(d.flight, it)
}

// sendRst schedules an RST toward side `to` of p (carried on direction 1-to).
// It lands after anything already in flight on that direction.
func (e *Engine) sendRst(ctx sim.EngineCtx, p *pair, to int, net bool, lat core.Duration) {
	di := 1 - to
	d := &p.dirs[di]
	d.seq++
	at := d.arrival(ctx.Now(), lat)
	id := ctx.Self(at, d.lane, rstBody{p.id, di, d.seq})
	d.flight = append(d.flight, item{kind: kRst, seq: d.seq, net: net, id: id})
}

func (e *Engine) ack(ctx sim.EngineCtx, to core.NodeRef, p *pair, side int, req core.ReqID, conn core.ConnID, err core.ErrCode) {
	var lane sim.Lane
	if p != nil {
		lane = laneOf(p.id, laneAck+uint64(side))
	} else {
		lane = laneOf(0, laneMisc)
	}
	ctx.Deliver(to, ctx.Now()+sim.Time(e.sample(ctx.Rand())), lane, core.SendDone{Req: req, Conn: conn, Err: err})
}

func (e *Engine) fault(ctx sim.EngineCtx, kind string, a, b core.NodeID, extra ...core.Field) {
	fs := append([]core.Field{{Key: "a", Value: core.Str(a)}, {Key: "b", Value: core.Str(b)}}, extra...)
	ctx.Fault(kind, fs...)
	ctx.Assert().Sometimes(true, core.Prop("net: "+kind))
}

// ---- Execute ----

func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	switch a := a.(type) {
	case core.Dial:
		e.dial(ctx, from, a)
	case core.Send:
		e.send(ctx, from, a)
	case core.CloseConn:
		e.closeConn(ctx, from, a)
	case core.PauseReads:
		if p, s := e.lookup(from.Node, a.Conn); p != nil && p.end[s].ref == from {
			p.end[s].paused = true
		}
	case core.ResumeReads:
		if p, s := e.lookup(from.Node, a.Conn); p != nil && p.end[s].ref == from {
			p.end[s].paused = false
			e.flush(ctx, p, 1-s)
		}
	}
}

func (e *Engine) dial(ctx sim.EngineCtx, from core.NodeRef, a core.Dial) {
	target := core.NodeID(a.Addr)
	// Partition coin first: it applies to this dial.
	e.maybePartition(ctx, from.Node, target)
	lat := e.sample(ctx.Rand()) + e.pairExtra(ctx, from.Node, target)
	at := ctx.Now() + sim.Time(lat)
	if l := e.link(from.Node, target); l.everCut && l.healAt > ctx.Now() && l.healAt+sim.Time(lat) > at {
		at = l.healAt + sim.Time(lat)
	}
	ref, ok := ctx.Current(target)
	if !ok {
		ctx.Deliver(from, at, laneOf(0, laneMisc), core.DialDone{Req: a.Req, Err: core.ErrConnClosed})
		return
	}
	e.nextPair++
	p := &pair{id: e.nextPair}
	e.nextConn[from.Node]++
	e.nextConn[target]++
	p.end[0] = end{ref: from, id: e.nextConn[from.Node]}
	p.end[1] = end{ref: ref, id: e.nextConn[target]}
	p.dirs[0].lane = laneOf(p.id, 0)
	p.dirs[1].lane = laneOf(p.id, 1)
	// Data follows the open notice on each direction.
	p.dirs[0].lastArr, p.dirs[1].lastArr = at, at
	e.pairs = append(e.pairs, p)
	e.ends[endKey{from.Node, p.end[0].id}] = endRef{p.id, 0}
	e.ends[endKey{target, p.end[1].id}] = endRef{p.id, 1}
	ctx.Deliver(from, at, laneOf(p.id, laneMisc), core.DialDone{Req: a.Req, Conn: p.end[0].id})
	ctx.Deliver(ref, at, p.dirs[0].lane, core.ConnOpened{Conn: p.end[1].id, Peer: string(from.Node)})
}

func (e *Engine) maybePartition(ctx sim.EngineCtx, a, b core.NodeID) {
	if e.recovery || e.f.PartitionProb <= 0 || a == b {
		return
	}
	l := e.link(a, b)
	if l.everCut && ctx.Now() < l.healAt {
		return
	}
	if !ctx.Rand().Chance(e.f.PartitionProb) {
		return
	}
	dur := span(ctx.Rand(), e.f.PartitionDur[0], e.f.PartitionDur[1])
	l.everCut = true
	l.healAt = ctx.Now() + sim.Time(dur)
	ctx.Self(l.healAt, laneOf(0, laneMisc+1), healBody{l.a, l.b})
	e.fault(ctx, "partition", l.a, l.b, core.Field{Key: "dur", Value: core.I64(dur)})
}

func (e *Engine) send(ctx sim.EngineCtx, from core.NodeRef, a core.Send) {
	p, s := e.lookup(from.Node, a.Conn)
	if p == nil || p.end[s].ref != from || p.end[s].closed || p.dirs[s].aborted {
		e.ack(ctx, from, p, s, a.Req, a.Conn, core.ErrConnClosed)
		return
	}
	peer := p.end[1-s].ref.Node
	r := ctx.Rand()
	d := &p.dirs[s]
	if !e.recovery {
		if r.Chance(e.f.RandomCloseProb) {
			e.fault(ctx, "random_close", from.Node, peer)
			e.netAbort(ctx, p)
			e.ack(ctx, from, p, s, a.Req, a.Conn, core.ErrConnClosed)
			return
		}
		if r.Chance(e.f.BlackHoleProb) && !d.blackhole {
			d.blackhole = true
			e.fault(ctx, "black_hole", from.Node, peer)
		}
		e.maybePartition(ctx, from.Node, peer)
		if r.Chance(e.f.ClogProb) {
			dur := span(r, e.f.ClogDur[0], e.f.ClogDur[1])
			d.clogUntil = ctx.Now() + sim.Time(dur)
			e.fault(ctx, "clog", from.Node, peer, core.Field{Key: "dur", Value: core.I64(dur)})
		}
	}
	extra := e.pairExtra(ctx, from.Node, peer)
	data := a.Data
	for len(data) > 0 {
		n := len(data)
		if m := e.f.PartialIOMax; m > 0 && n > m {
			n = int(r.Between(1, int64(m)+1))
			if n > len(data) {
				n = len(data)
			}
		}
		chunk := append([]byte(nil), data[:n]...)
		data = data[n:]
		e.enqueue(ctx, p, s, item{kind: kData, data: chunk}, e.sample(r)+extra)
	}
	e.ack(ctx, from, p, s, a.Req, a.Conn, core.OK)
}

func (e *Engine) closeConn(ctx sim.EngineCtx, from core.NodeRef, a core.CloseConn) {
	p, s := e.lookup(from.Node, a.Conn)
	if p == nil || p.end[s].ref != from || p.end[s].closed {
		return
	}
	peer := p.end[1-s].ref.Node
	lat := e.sample(ctx.Rand()) + e.pairExtra(ctx, from.Node, peer)
	p.end[s].closed = true
	d := &p.dirs[s]
	if a.Abort {
		e.clearDir(ctx, d)
		d.aborted = true
		e.clearDir(ctx, &p.dirs[1-s]) // nothing more will be delivered here
		if !p.end[1-s].closed {
			e.sendRst(ctx, p, 1-s, false, lat)
		}
	} else if !d.aborted {
		e.enqueue(ctx, p, s, item{kind: kFin}, lat)
		d.aborted = true // no sends after FIN
	}
	e.maybeRemove(p)
}

// netAbort resets a conn from the network: both open ends get Abort:true.
func (e *Engine) netAbort(ctx sim.EngineCtx, p *pair) {
	for di := 0; di < 2; di++ {
		e.clearDir(ctx, &p.dirs[di])
		p.dirs[di].aborted = true
	}
	for to := 0; to < 2; to++ {
		if !p.end[to].closed {
			e.sendRst(ctx, p, to, true, e.sample(ctx.Rand()))
		}
	}
}

// ---- internal entries ----

func (e *Engine) OnInternal(ctx sim.EngineCtx, body any) {
	switch b := body.(type) {
	case landBody:
		e.land(ctx, b.Pair, b.Dir, b.Seq)
	case rstBody:
		e.land(ctx, b.Pair, b.Dir, b.Seq)
	case healBody:
		for _, p := range e.pairs {
			a, c := p.end[0].ref.Node, p.end[1].ref.Node
			if (a == b.A && c == b.B) || (a == b.B && c == b.A) {
				e.flush(ctx, p, 0)
				e.flush(ctx, p, 1)
			}
		}
		ctx.Assert().Sometimes(true, "net: partition healed")
	}
}

func (e *Engine) land(ctx sim.EngineCtx, pid uint64, di int, seq uint64) {
	p := e.findPair(pid)
	if p == nil {
		return
	}
	d := &p.dirs[di]
	if len(d.flight) == 0 || d.flight[0].seq != seq {
		return // stale: cleared by an abort or crash
	}
	it := d.flight[0]
	d.flight = d.flight[1:]
	d.held = append(d.held, it)
	e.flush(ctx, p, di)
}

// flush delivers held items on direction di in order, stopping at a pause or
// partition (RSTs ignore pause). Data and FIN in a black-holed direction are
// dropped.
func (e *Engine) flush(ctx sim.EngineCtx, p *pair, di int) {
	d := &p.dirs[di]
	r := 1 - di
	re := &p.end[r]
	for len(d.held) > 0 {
		it := d.held[0]
		if it.kind != kRst && d.blackhole {
			d.held = d.held[1:]
			continue
		}
		if e.partitioned(ctx, p.end[0].ref.Node, p.end[1].ref.Node) {
			return
		}
		if it.kind != kRst && re.paused && !re.closed {
			return
		}
		d.held = d.held[1:]
		if re.closed {
			continue // locally closed or crashed: dropped
		}
		switch it.kind {
		case kData:
			ctx.Deliver(re.ref, ctx.Now(), d.lane, core.BytesReceived{Conn: re.id, Data: it.data})
		case kFin:
			re.eof = true
			ctx.Deliver(re.ref, ctx.Now(), d.lane, core.ConnClosed{Conn: re.id})
		case kRst:
			if it.net || !d.blackhole {
				re.closed = true
				e.clearDir(ctx, &p.dirs[r])
				p.dirs[r].aborted = true
				ctx.Deliver(re.ref, ctx.Now(), d.lane, core.ConnClosed{Conn: re.id, Abort: true})
				e.maybeRemove(p)
			}
		}
	}
}

// OnCrash retires every open end at ref; survivors get an RST.
func (e *Engine) OnCrash(ctx sim.EngineCtx, ref core.NodeRef) {
	// Snapshot: maybeRemove edits e.pairs.
	ps := append([]*pair(nil), e.pairs...)
	for _, p := range ps {
		for s := 0; s < 2; s++ {
			if p.end[s].ref != ref || p.end[s].closed {
				continue
			}
			p.end[s].closed = true
			p.end[s].paused = false
			e.clearDir(ctx, &p.dirs[1-s]) // bytes to the dead node
			p.dirs[1-s].aborted = true
			p.dirs[s].aborted = true
			if !p.end[1-s].closed {
				e.sendRst(ctx, p, 1-s, true, e.sample(ctx.Rand()))
			}
			e.maybeRemove(p)
			ctx.Assert().Sometimes(true, "net: crash reset a peer")
		}
	}
}

// ---- hash ----

type hasher uint64

func (h *hasher) u64(v uint64) {
	for i := 0; i < 8; i++ {
		*h = (*h ^ hasher(byte(v>>(8*i)))) * 1099511628211
	}
}
func (h *hasher) b(v bool) {
	if v {
		h.u64(1)
	} else {
		h.u64(0)
	}
}
func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		*h = (*h ^ hasher(s[i])) * 1099511628211
	}
}
func (h *hasher) items(its []item) {
	h.u64(uint64(len(its)))
	for _, it := range its {
		h.u64(uint64(it.kind))
		h.u64(uint64(len(it.data)))
		h.u64(uint64(crc32.ChecksumIEEE(it.data)))
		h.b(it.net)
	}
}

// StateHash covers every open conn (ascending id), queued bytes, pause flags
// and links (partitions, pair latency).
func (e *Engine) StateHash() uint64 {
	h := hasher(14695981039346656037)
	h.u64(uint64(len(e.pairs)))
	for _, p := range e.pairs {
		h.u64(p.id)
		for s := 0; s < 2; s++ {
			en := &p.end[s]
			h.str(string(en.ref.Node))
			h.u64(uint64(len(en.ref.Inc)))
			for _, c := range en.ref.Inc {
				h.u64(uint64(c))
			}
			h.u64(uint64(en.id))
			h.b(en.closed)
			h.b(en.eof)
			h.b(en.paused)
			d := &p.dirs[s]
			h.u64(uint64(d.lastArr))
			h.u64(uint64(d.clogUntil))
			h.b(d.aborted)
			h.b(d.blackhole)
			h.items(d.flight)
			h.items(d.held)
		}
	}
	h.u64(uint64(len(e.links)))
	for _, l := range e.links {
		h.str(string(l.a))
		h.str(string(l.b))
		h.u64(uint64(l.extra))
		h.b(l.everCut)
		h.u64(uint64(l.healAt))
	}
	h.b(e.recovery)
	return uint64(h)
}

var _ sim.Engine = (*Engine)(nil)
