// Package fakenetwork is the simulated network service: TCP-like byte streams
// between nodes (decision 6). Address = NodeID string.
//
// It is a kernel.Service. A node hands it one of five calls (Dial, Write, Close,
// Pause, Resume). The service answers the caller, and it also sends events to
// other nodes (ConnOpened, BytesReceived, ConnClosed). The host adds every node
// that may be dialed with AddNode.
//
// Model
//   - A connection is a pair of ends (dialer = side 0, listener = side 1) and
//     two directions; direction d carries bytes from side d to side 1-d. Each
//     direction has its own Lane (decision C) and monotone arrival times, so
//     bytes never reorder even when chunks draw different latencies.
//   - Connect: live target -> ConnIDs allocated now (per-node counters, never
//     reused across incarnations), ConnectDone{OK} to the dialer and
//     ConnOpened to the listener, both after latency. Down target ->
//     ConnectDone{ErrConnClosed} after latency. A partitioned pair delays the
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
//   - Faults are drawn at issue (Send; Connect draws partition only) and never
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
package fakenetwork

import (
	"bytes"
	"hash/crc32"
	"slices"
	"sort"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/span"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Cx is the context the kernel lends to this service. The network answers nodes
// with core.EventData values.
type Cx = kernel.ServiceContext[core.EventData]

// Ref names one node incarnation.
type Ref = kernel.Ref[core.EventData]

// Addr names a node, whatever incarnation is up.
type Addr = kernel.Addr[core.EventData]

// Call is a request to the network: the sum of the five network calls. A node
// route function wraps the core action in the matching case, for example
// fakenetwork.Write{Send: x}. The cases embed the core actions, so they fold
// into the trace hash with the bytes of the core action.
//
//sumtype:decl
type Call interface {
	machine.Message
	isCall()
}

// The cases of Call.
type (
	// Dial opens a connection (core.Connect).
	Dial struct{ core.Connect }
	// Write sends bytes on a connection (core.Send).
	Write struct{ core.Send }
	// Close closes a connection (core.CloseConn).
	Close struct{ core.CloseConn }
	// Pause stops reading from a connection (core.PauseReads).
	Pause struct{ core.PauseReads }
	// Resume undoes Pause (core.ResumeReads).
	Resume struct{ core.ResumeReads }
)

func (Dial) isCall()   {}
func (Write) isCall()  {}
func (Close) isCall()  {}
func (Pause) isCall()  {}
func (Resume) isCall() {}

// Faults are connection-level faults (lesson 6). The clock skew of the old
// NetworkFaults is now kernel.Config.ClockSkewMax.
type Faults struct {
	Latency         kernel.FastWithRareSlow
	PartialIOMax    int     // max bytes per delivered chunk / accepted write
	PartitionProb   float64 // sustained, correlated; freezes in-flight bytes
	PartitionDur    [2]machine.Duration
	ClogProb        float64
	ClogDur         [2]machine.Duration
	RandomCloseProb float64
	BlackHoleProb   float64
	PairLatencyMax  machine.Duration // per-pair permanent extra latency, sampled once
}

// Engine is the network service. Not safe for concurrent use.
type Engine struct {
	f        Faults
	lanes    *kernel.Lanes[laneKey]
	nodes    map[machine.NodeID]Addr // the nodes that may be dialed; looked up by name only
	recovery bool
	nextPair uint64
	nextConn map[machine.NodeID]core.ConnID
	pairs    []*connectionPair // ascending id
	ends     map[connectionEndKey]connectionEndRef
	links    []*link // sorted by (a, b), a <= b
}

var _ kernel.Service[Call, core.EventData] = (*Engine)(nil)

// New returns a network service with the given fault menu. The world gives it
// its lanes. Add the dialable nodes with AddNode.
func New(w *kernel.World, f Faults) *Engine {
	return &Engine{
		f: f, lanes: kernel.NewLanes[laneKey](w), nodes: map[machine.NodeID]Addr{},
		nextConn: map[machine.NodeID]core.ConnID{}, ends: map[connectionEndKey]connectionEndRef{},
	}
}

// AddNode tells the network that a node exists, so that a Dial to its name can
// find the live incarnation. A name that was not added is a down node.
func (e *Engine) AddNode(a Addr) { e.nodes[a.ID()] = a }

type connectionEndKey struct {
	node machine.NodeID
	id   core.ConnID
}

type connectionEndRef struct {
	pair uint64
	side int
}

type connectionEnd struct {
	ref    Ref
	id     core.ConnID
	closed bool // closed locally, reset, or crashed: no more sends or delivery
	eof    bool // saw peer FIN
	paused bool
}

type itemKind uint8

const (
	kindData itemKind = iota
	kindClose
	kindReset
)

type item struct {
	kind itemKind
	data []byte
	crc  uint32 // crc32 of data, so StateHash does not rescan it
	seq  uint64
	id   kernel.EntryID
	net  bool // network-originated RST: passes a black hole
}

type dir struct {
	lane      kernel.Lane
	lastArr   kernel.Time
	clogUntil kernel.Time
	seq       uint64
	flight    []item // scheduled, not yet landed; lands strictly in order
	held      []item // landed, not yet delivered (pause, partition)
	aborted   bool   // no new sends accepted
	blackhole bool
}

type connectionPair struct {
	id   uint64
	end  [2]connectionEnd
	dirs [2]dir // dirs[d]: side d -> side 1-d

	// StateHash cache: out is the hash state after folding this pair on top of
	// in. Valid while ok; every access to a pair clears ok (touch).
	in, out uint64
	ok      bool
}

// link is per unordered node pair: permanent extra latency and partition state.
type link struct {
	a, b     machine.NodeID
	extra    machine.Duration
	healAt   kernel.Time
	everCut  bool
	hasExtra bool
}

// Name implements kernel.Service.
func (e *Engine) Name() string { return "fakenetwork" }

// EnterRecovery implements kernel.Service. It switches the service to recovery mode.
func (e *Engine) EnterRecovery() { e.recovery = true }

// ---- lanes ----

// laneKind tells the lane keys apart. The old string keys were "net/misc" (dial
// failures), "net/heal", and per pair "net/<pair>/<dir>" (data, 0 or 1),
// "net/<pair>/ack<side>" and "net/<pair>/dial". Lanes are made in first-use
// order, so the lane numbers are the old ones.
type laneKind uint8

const (
	laneMisc laneKind = iota
	laneHeal
	laneDir0
	laneDir1
	laneAck0
	laneAck1
	laneDial
)

// laneKey is the typed key of a lane. pair is 0 for laneMisc and laneHeal.
type laneKey struct {
	pair uint64
	kind laneKind
}

func (e *Engine) dirLane(pair uint64, di int) kernel.Lane {
	return e.lanes.Get(laneKey{pair, laneDir0 + laneKind(di)})
}

// ---- helpers ----

// timeSpan draws a duration in [lo, hi), like the old broker Between.
func timeSpan(r *rng.Rand, lo, hi machine.Duration) machine.Duration {
	if hi <= lo {
		return lo
	}
	return machine.Duration(span.Between(r, int64(lo), int64(hi)))
}

func (e *Engine) sample(r *rng.Rand) machine.Duration {
	return e.f.Latency.Sample(r)
}

// current returns the live incarnation of the node with that name. A name that
// was never added is a down node.
func (e *Engine) current(cx *Cx, n machine.NodeID) (Ref, bool) {
	a, known := e.nodes[n]
	if !known {
		return Ref{}, false
	}
	return cx.Current(a)
}

// findPair returns the pair with that id. Every caller may mutate it, so it
// is marked for rehashing.
func (e *Engine) findPair(id uint64) *connectionPair {
	i := sort.Search(len(e.pairs), func(i int) bool { return e.pairs[i].id >= id })
	if i < len(e.pairs) && e.pairs[i].id == id {
		e.pairs[i].ok = false
		return e.pairs[i]
	}
	return nil
}

func (e *Engine) lookup(n machine.NodeID, c core.ConnID) (*connectionPair, int) {
	r, ok := e.ends[connectionEndKey{n, c}]
	if !ok {
		return nil, 0
	}
	p := e.findPair(r.pair)
	if p == nil {
		return nil, 0
	}
	return p, r.side
}

func (e *Engine) maybeRemove(p *connectionPair) {
	if !p.end[0].closed || !p.end[1].closed {
		return
	}
	for s := range 2 {
		delete(e.ends, connectionEndKey{p.end[s].ref.Addr().ID(), p.end[s].id})
	}
	i := sort.Search(len(e.pairs), func(i int) bool { return e.pairs[i].id >= p.id })
	if i < len(e.pairs) && e.pairs[i] == p {
		e.pairs = append(e.pairs[:i], e.pairs[i+1:]...)
	}
}

func (e *Engine) link(a, b machine.NodeID) *link {
	a, b = min(a, b), max(a, b)
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
func (e *Engine) pairExtra(cx *Cx, a, b machine.NodeID) machine.Duration {
	l := e.link(a, b)
	if !l.hasExtra {
		l.hasExtra = true
		if m := e.f.PairLatencyMax; m > 0 {
			l.extra = timeSpan(cx.Rand(), 0, m+1)
		}
	}
	return l.extra
}

func (e *Engine) partitioned(cx *Cx, a, b machine.NodeID) bool {
	l := e.link(a, b)
	return l.everCut && cx.Now() < l.healAt
}

// clearDir drops undelivered data and FIN on d; pending RSTs stand.
func (e *Engine) clearDir(cx *Cx, d *dir) {
	keep := func(its []item, cancel bool) []item {
		var out []item
		for _, it := range its {
			if it.kind == kindReset {
				out = append(out, it)
			} else if cancel {
				cx.Cancel(it.id)
			}
		}
		return out
	}
	d.flight, d.held = keep(d.flight, true), keep(d.held, false)
}

// arrival returns the next arrival time on d for something issued now with
// latency lat: monotone, and not before an active clog ends.
func (d *dir) arrival(now kernel.Time, lat machine.Duration) kernel.Time {
	at := now + kernel.Time(lat)
	if d.clogUntil > now && d.clogUntil+kernel.Time(lat) > at {
		at = d.clogUntil + kernel.Time(lat)
	}
	if at < d.lastArr {
		at = d.lastArr
	}
	d.lastArr = at
	return at
}

// enqueue puts a data or FIN item in flight on direction di of p.
func (e *Engine) enqueue(cx *Cx, p *connectionPair, di int, it item, lat machine.Duration) {
	d := &p.dirs[di]
	d.seq++
	it.seq = d.seq
	it.crc = crc32.ChecksumIEEE(it.data)
	at := d.arrival(cx.Now(), lat)
	pid, seq := p.id, it.seq
	it.id = cx.Later(at, d.lane, func(cx *Cx) { e.land(cx, pid, di, seq) })
	d.flight = append(d.flight, it)
}

// sendRst schedules an RST toward side `to` of p (carried on direction 1-to).
// It lands after anything already in flight on that direction.
func (e *Engine) sendRst(cx *Cx, p *connectionPair, to int, net bool, lat machine.Duration) {
	di := 1 - to
	d := &p.dirs[di]
	d.seq++
	at := d.arrival(cx.Now(), lat)
	pid, seq := p.id, d.seq
	id := cx.Later(at, d.lane, func(cx *Cx) { e.land(cx, pid, di, seq) })
	d.flight = append(d.flight, item{kind: kindReset, seq: d.seq, net: net, id: id})
}

func (e *Engine) ack(cx *Cx, to Ref, p *connectionPair, side int, request machine.RequestID, conn core.ConnID, err core.ErrCode) {
	var lane kernel.Lane
	if p != nil {
		lane = e.lanes.Get(laneKey{p.id, laneAck0 + laneKind(side)})
	} else {
		lane = e.lanes.Get(laneKey{0, laneMisc})
	}
	cx.Deliver(to, cx.Now()+kernel.Time(e.sample(cx.Rand())), lane, core.SendDone{Req: request, Conn: conn, Err: err})
}

func (e *Engine) fault(cx *Cx, kind netFault, a, b machine.NodeID, extra ...machine.Field) {
	fields := append([]machine.Field{{Key: "a", Value: machine.StringValue(a)}, {Key: "b", Value: machine.StringValue(b)}}, extra...)
	cx.Fault(kind.name, fields...)
	cx.Assert().Sometimes(true, kind.happened)
	cx.Assert().Reachable(kind.injected)
}

// ---- Handle ----

// Handle applies a network call issued by node from. It implements kernel.Service.
func (e *Engine) Handle(cx *Cx, from Ref, req Call) {
	switch x := req.(type) {
	case Dial:
		e.dial(cx, from, x.Connect)
	case Write:
		e.send(cx, from, x.Send)
	case Close:
		e.closeConn(cx, from, x.CloseConn)
	case Pause:
		if p, s := e.lookup(from.Addr().ID(), x.Conn); p != nil && p.end[s].ref == from {
			p.end[s].paused = true
		}
	case Resume:
		if p, s := e.lookup(from.Addr().ID(), x.Conn); p != nil && p.end[s].ref == from {
			p.end[s].paused = false
			e.flush(cx, p, 1-s)
		}
	}
}

func (e *Engine) dial(cx *Cx, from Ref, a core.Connect) {
	fromNode := from.Addr().ID()
	target := machine.NodeID(a.Addr)
	// Partition coin first: it applies to this dial.
	e.maybePartition(cx, fromNode, target)
	lat := e.sample(cx.Rand()) + e.pairExtra(cx, fromNode, target)
	at := cx.Now() + kernel.Time(lat)
	if l := e.link(fromNode, target); l.everCut && l.healAt > cx.Now() && l.healAt+kernel.Time(lat) > at {
		at = l.healAt + kernel.Time(lat)
	}
	ref, ok := e.current(cx, target)
	if !ok {
		cx.Deliver(from, at, e.lanes.Get(laneKey{0, laneMisc}), core.ConnectDone{Req: a.Req, Err: core.ErrConnClosed})
		return
	}
	e.nextPair++
	p := &connectionPair{id: e.nextPair}
	e.nextConn[fromNode]++
	e.nextConn[target]++
	p.end[0] = connectionEnd{ref: from, id: e.nextConn[fromNode]}
	p.end[1] = connectionEnd{ref: ref, id: e.nextConn[target]}
	p.dirs[0].lane = e.dirLane(p.id, 0)
	p.dirs[1].lane = e.dirLane(p.id, 1)
	// Data follows the open notice on each direction.
	p.dirs[0].lastArr, p.dirs[1].lastArr = at, at
	e.pairs = append(e.pairs, p) // ok is false: hashed on first use
	e.ends[connectionEndKey{fromNode, p.end[0].id}] = connectionEndRef{p.id, 0}
	e.ends[connectionEndKey{target, p.end[1].id}] = connectionEndRef{p.id, 1}
	cx.Deliver(from, at, e.lanes.Get(laneKey{p.id, laneDial}), core.ConnectDone{Req: a.Req, Conn: p.end[0].id})
	cx.Deliver(ref, at, p.dirs[0].lane, core.ConnOpened{Conn: p.end[1].id, Peer: string(fromNode)})
}

func (e *Engine) maybePartition(cx *Cx, a, b machine.NodeID) {
	if e.recovery || e.f.PartitionProb <= 0 || a == b {
		return
	}
	l := e.link(a, b)
	if l.everCut && cx.Now() < l.healAt {
		return
	}
	if !cx.Rand().Chance(e.f.PartitionProb) {
		return
	}
	dur := timeSpan(cx.Rand(), e.f.PartitionDur[0], e.f.PartitionDur[1])
	l.everCut = true
	l.healAt = cx.Now() + kernel.Time(dur)
	ha, hb := l.a, l.b
	cx.Later(l.healAt, e.lanes.Get(laneKey{0, laneHeal}), func(cx *Cx) { e.heal(cx, ha, hb) })
	e.fault(cx, faultPartition, l.a, l.b, machine.Field{Key: "dur", Value: machine.Int64Value(dur)})
}

func (e *Engine) send(cx *Cx, from Ref, a core.Send) {
	fromNode := from.Addr().ID()
	p, s := e.lookup(fromNode, a.Conn)
	if p == nil || p.end[s].ref != from || p.end[s].closed || p.dirs[s].aborted {
		e.ack(cx, from, p, s, a.Req, a.Conn, core.ErrConnClosed)
		return
	}
	peer := p.end[1-s].ref.Addr().ID()
	r := cx.Rand()
	d := &p.dirs[s]
	if !e.recovery {
		if r.Chance(e.f.RandomCloseProb) {
			e.fault(cx, faultRandomClose, fromNode, peer)
			e.netAbort(cx, p)
			e.ack(cx, from, p, s, a.Req, a.Conn, core.ErrConnClosed)
			return
		}
		if r.Chance(e.f.BlackHoleProb) && !d.blackhole {
			d.blackhole = true
			e.fault(cx, faultBlackHole, fromNode, peer)
		}
		e.maybePartition(cx, fromNode, peer)
		if r.Chance(e.f.ClogProb) {
			dur := timeSpan(r, e.f.ClogDur[0], e.f.ClogDur[1])
			d.clogUntil = cx.Now() + kernel.Time(dur)
			e.fault(cx, faultClog, fromNode, peer, machine.Field{Key: "dur", Value: machine.Int64Value(dur)})
		}
	}
	extra := e.pairExtra(cx, fromNode, peer)
	data := a.Data
	for len(data) > 0 {
		n := len(data)
		if m := e.f.PartialIOMax; m > 0 && n > m {
			n = min(int(span.Between(r, 1, int64(m)+1)), len(data))
		}
		chunk := bytes.Clone(data[:n])
		data = data[n:]
		e.enqueue(cx, p, s, item{kind: kindData, data: chunk}, e.sample(r)+extra)
	}
	e.ack(cx, from, p, s, a.Req, a.Conn, core.OK)
}

func (e *Engine) closeConn(cx *Cx, from Ref, a core.CloseConn) {
	fromNode := from.Addr().ID()
	p, s := e.lookup(fromNode, a.Conn)
	if p == nil || p.end[s].ref != from || p.end[s].closed {
		return
	}
	peer := p.end[1-s].ref.Addr().ID()
	lat := e.sample(cx.Rand()) + e.pairExtra(cx, fromNode, peer)
	p.end[s].closed = true
	d := &p.dirs[s]
	if a.Abort {
		e.clearDir(cx, d)
		d.aborted = true
		e.clearDir(cx, &p.dirs[1-s]) // nothing more will be delivered here
		if !p.end[1-s].closed {
			e.sendRst(cx, p, 1-s, false, lat)
		}
	} else if !d.aborted {
		e.enqueue(cx, p, s, item{kind: kindClose}, lat)
		d.aborted = true // no sends after FIN
	}
	e.maybeRemove(p)
}

// netAbort resets a conn from the network: both open ends get Abort:true.
func (e *Engine) netAbort(cx *Cx, p *connectionPair) {
	for di := range 2 {
		e.clearDir(cx, &p.dirs[di])
		p.dirs[di].aborted = true
	}
	for to := range 2 {
		if !p.end[to].closed {
			e.sendRst(cx, p, to, true, e.sample(cx.Rand()))
		}
	}
}

// ---- later steps ----

// heal ends a partition of the pair (a, b): every held item of its conns flows.
func (e *Engine) heal(cx *Cx, a, b machine.NodeID) {
	for _, p := range e.pairs {
		x, y := p.end[0].ref.Addr().ID(), p.end[1].ref.Addr().ID()
		if (x == a && y == b) || (x == b && y == a) {
			p.ok = false
			e.flush(cx, p, 0)
			e.flush(cx, p, 1)
		}
	}
	cx.Assert().Sometimes(true, partitionHealed)
}

func (e *Engine) land(cx *Cx, pid uint64, di int, seq uint64) {
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
	e.flush(cx, p, di)
}

// flush delivers held items on direction di in order, stopping at a pause or
// partition (RSTs ignore pause). Data and FIN in a black-holed direction are
// dropped.
func (e *Engine) flush(cx *Cx, p *connectionPair, di int) {
	d := &p.dirs[di]
	r := 1 - di
	re := &p.end[r]
	for len(d.held) > 0 {
		it := d.held[0]
		if it.kind != kindReset && d.blackhole {
			d.held = d.held[1:]
			continue
		}
		if e.partitioned(cx, p.end[0].ref.Addr().ID(), p.end[1].ref.Addr().ID()) {
			return
		}
		if it.kind != kindReset && re.paused && !re.closed {
			return
		}
		d.held = d.held[1:]
		if re.closed {
			continue // locally closed or crashed: dropped
		}
		switch it.kind {
		case kindData:
			cx.Deliver(re.ref, cx.Now(), d.lane, core.BytesReceived{Conn: re.id, Data: it.data})
		case kindClose:
			re.eof = true
			cx.Deliver(re.ref, cx.Now(), d.lane, core.ConnClosed{Conn: re.id})
		case kindReset:
			if it.net || !d.blackhole {
				re.closed = true
				e.clearDir(cx, &p.dirs[r])
				p.dirs[r].aborted = true
				cx.Deliver(re.ref, cx.Now(), d.lane, core.ConnClosed{Conn: re.id, Abort: true})
				e.maybeRemove(p)
			}
		}
	}
}

// OnCrash retires every open end at who; survivors get an RST. It implements kernel.Service.
func (e *Engine) OnCrash(cx *Cx, who machine.NodeInstance) {
	// Snapshot: maybeRemove edits e.pairs.
	ps := slices.Clone(e.pairs)
	for _, p := range ps {
		for s := range 2 {
			if p.end[s].ref.Instance() != who || p.end[s].closed {
				continue
			}
			p.ok = false
			p.end[s].closed = true
			p.end[s].paused = false
			e.clearDir(cx, &p.dirs[1-s]) // bytes to the dead node
			p.dirs[1-s].aborted = true
			p.dirs[s].aborted = true
			if !p.end[1-s].closed {
				e.sendRst(cx, p, 1-s, true, e.sample(cx.Rand()))
			}
			e.maybeRemove(p)
			cx.Assert().Sometimes(true, crashResetPeer)
		}
	}
}

// ---- hash ----

type hasher = trace.Hash

func hashItems(h *hasher, its []item) {
	h.Uint64(uint64(len(its)))
	for _, it := range its {
		h.Uint64(uint64(it.kind))
		h.Uint64(uint64(len(it.data)))
		h.Uint64(uint64(it.crc))
		h.Bool(it.net)
	}
}

// StateHash covers every open conn (ascending id), queued bytes, pause flags
// and links (partitions, pair latency). FNV is one running stream, so a pair's
// fold is reused only while the state it starts from is unchanged: a changed
// pair rehashes itself and the pairs after it.
func (e *Engine) StateHash() uint64 {
	h := trace.New()
	h.Uint64(uint64(len(e.pairs)))
	for _, p := range e.pairs {
		if p.ok && p.in == uint64(h) {
			h = hasher(p.out)
			continue
		}
		p.in = uint64(h)
		hashConnectionPair(&h, p)
		p.out, p.ok = uint64(h), true
	}
	return e.finishHash(h)
}

// fullStateHash is StateHash without the per-pair cache.
func (e *Engine) fullStateHash() uint64 {
	h := trace.New()
	h.Uint64(uint64(len(e.pairs)))
	for _, p := range e.pairs {
		p.in = uint64(h)
		hashConnectionPair(&h, p)
		p.out, p.ok = uint64(h), false
	}
	return e.finishHash(h)
}

func (e *Engine) finishHash(h hasher) uint64 {
	h.Uint64(uint64(len(e.links)))
	for _, l := range e.links {
		h.String(string(l.a))
		h.String(string(l.b))
		h.Uint64(uint64(l.extra))
		h.Bool(l.everCut)
		h.Uint64(uint64(l.healAt))
	}
	h.Bool(e.recovery)
	return uint64(h)
}

func hashConnectionPair(h *hasher, p *connectionPair) {
	h.Uint64(p.id)
	for s := range 2 {
		en := &p.end[s]
		who := en.ref.Instance()
		h.String(string(who.Node))
		h.Uint64(uint64(len(who.Inc)))
		for _, c := range who.Inc {
			h.Uint64(uint64(c))
		}
		h.Uint64(uint64(en.id))
		h.Bool(en.closed)
		h.Bool(en.eof)
		h.Bool(en.paused)
		d := &p.dirs[s]
		h.Uint64(uint64(d.lastArr))
		h.Uint64(uint64(d.clogUntil))
		h.Bool(d.aborted)
		h.Bool(d.blackhole)
		hashItems(h, d.flight)
		hashItems(h, d.held)
	}
}
