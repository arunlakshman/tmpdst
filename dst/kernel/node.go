package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Tag numbers of the trace hash for kernel bodies. They are frozen.
const (
	tagWake    = 901
	tagPhase   = 902
	tagOnPhase = 903
)

// NodeSpec says how to run one node. In and Out are the sum types of its
// machine.
type NodeSpec[In, Out machine.Message] struct {
	Name machine.NodeID
	// New builds the machine. It is called on every boot.
	New machine.Factory[In, Out]
	// Route handles one output of a Step. Its body is one exhaustive type
	// switch over Out; each case calls typed kernel functions.
	Route func(r *Router[In], out Out)
	// Boot makes the first input of every boot. It goes into the Control
	// inbox, so it is always the first Step. Nil: no boot input.
	Boot func(self machine.NodeInstance) In
	// StopMsg makes the graceful stop request. Nil: Stop is not allowed.
	StopMsg func(deadline machine.NodeTime) In
	// Class picks the inbox of an input. Nil: every input is Data.
	Class func(in In) machine.Class
	// Cost is how long the node is busy after one Step. Zero: no time.
	Cost CostParams
	// Restart says when the next incarnation starts after one ends. Nil, or
	// ok=false: the node stays down until host code calls Boot.
	Restart func(how machine.End, r *rng.Rand) (after machine.Duration, ok bool)
	// Attrition lets the kernel kill this node during PhaseFaults.
	Attrition bool
	// Driver marks a workload (a test client). A driver boots after every
	// other node and never ends: an End, a Crash, a Stop or a panic stops the
	// run with a failure. It hears phase changes through OnPhase.
	Driver bool
	// OnPhase makes the input a driver gets when the phase changes. ok=false:
	// no input.
	OnPhase func(p Phase) (msg In, ok bool)
	// AfterStep runs after each Step, after the outputs are routed. Host code
	// uses it for checks on one node.
	AfterStep func(self Ref[In], in In, out []Out)
}

// Addr names a node, whatever incarnation is up. Only Spawn makes one. A
// message for an Addr goes to the incarnation that is up when it arrives.
// The zero Addr, or an Addr of another World, is a protocol failure.
type Addr[In machine.Message] struct{ n *nodeIn[In] }

// ID is the name of the node.
func (a Addr[In]) ID() machine.NodeID {
	if a.n == nil {
		return ""
	}
	return a.n.name
}

func (a Addr[In]) node() nodeKey {
	if a.n == nil {
		return nodeKey{}
	}
	return nodeKey{w: a.n.w, idx: a.n.idx}
}

// Ref names one incarnation of a node. A message for a Ref is dropped if that
// incarnation has ended. Services answer callers by Ref.
type Ref[In machine.Message] struct {
	a   Addr[In]
	inc machine.BootNumber
}

// Addr is the node of this incarnation.
func (r Ref[In]) Addr() Addr[In] { return r.a }

// Instance is the plain name of this incarnation.
func (r Ref[In]) Instance() machine.NodeInstance {
	return machine.NodeInstance{Node: r.a.ID(), Inc: r.inc}
}

func (r Ref[In]) node() nodeKey {
	k := r.a.node()
	k.inc, k.ref = r.inc, true
	return k
}

// Node is any Addr or Ref, for lifecycle calls that do not need the type.
// Only this package implements it.
type Node interface{ node() nodeKey }

// nodeKey is what a Node gives the World: which node, and for a Ref, which
// incarnation.
type nodeKey struct {
	w   *World
	idx int
	inc machine.BootNumber
	ref bool
}

// nodeBase is the part of a node that does not depend on its types. The
// struct outlives incarnations; the fields marked "per incarnation" are reset
// at each boot.
type nodeBase struct {
	w         *World
	idx       int
	name      machine.NodeID
	skew      machine.Duration
	driver    bool
	attrition bool
	restart   func(how machine.End, r *rng.Rand) (machine.Duration, bool)

	up      bool
	broken  bool // the factory failed: never booted again
	lastEnd machine.End
	boots   int

	// per incarnation
	inc         machine.BootNumber
	stopping    bool
	paused      bool
	stepped     uint64
	busy        Time
	wakePending bool
	timers      map[machine.TimerID]EntryID

	// the cached machine StateHash; cleared whenever the kernel calls the machine
	hashCache
}

func (b *nodeBase) self() machine.NodeInstance {
	return machine.NodeInstance{Node: b.name, Inc: b.inc}
}

// local is the node clock: the virtual time plus the skew of the node.
func (b *nodeBase) local() machine.NodeTime {
	return machine.NodeTime(b.w.q.now) + machine.NodeTime(b.skew)
}

// nodeLane is a lane of the kernel for node idx: k=0 timers, k=1 wake-ups.
func nodeLane(idx int, k uint64) Lane {
	return HostLane + 1 + Lane(idx)<<8 + Lane(k)
}

// wakeBody is the body of a wake-up entry: the node should try to Step.
type wakeBody struct{}

// Fold writes tag 901.
func (wakeBody) Fold(h *trace.Hash) { h.Uint64(tagWake) }

// ensureWake queues one wake-up of the node at time at, unless one is pending.
func (w *World) ensureWake(b *nodeBase, at Time) {
	if b.wakePending {
		return
	}
	b.wakePending = true
	inc := b.inc
	e := &entry{at: at, lane: nodeLane(b.idx, 1), kind: kindDeliver, node: b.idx, inc: inc, body: wakeBody{}}
	e.run = func() {
		if !b.up || b.inc != inc {
			w.counters[countDroppedStale]++
			return
		}
		b.wakePending = false
		w.counters[countWakes]++
		w.logMsg("wake "+string(b.name), nil)
		w.nodes[b.idx].pump()
	}
	w.q.add(e)
}

// nodeIn is the part of a node that depends only on its input type: the two
// inboxes. Addr, Ref and Router point to it.
type nodeIn[In machine.Message] struct {
	nodeBase
	class   func(in In) machine.Class
	control []inboxItem[In]
	data    []inboxItem[In]
}

// inboxItem is one waiting input. boot marks the boot input.
type inboxItem[In machine.Message] struct {
	msg  In
	boot bool
}

func (n *nodeIn[In]) ref() Ref[In] { return Ref[In]{a: Addr[In]{n: n}, inc: n.inc} }

func (n *nodeIn[In]) empty() bool { return len(n.control) == 0 && len(n.data) == 0 }

// push puts msg in its inbox. It reports false when Class panicked (the run
// halts).
func (n *nodeIn[In]) push(msg In) bool {
	c := machine.Data
	if n.class != nil && !n.w.guard("Class "+string(n.name), func() { c = n.class(msg) }) {
		n.w.halted = true
		return false
	}
	if c == machine.Control {
		n.control = append(n.control, inboxItem[In]{msg: msg})
	} else {
		n.data = append(n.data, inboxItem[In]{msg: msg})
	}
	return true
}

// next takes the next input: Control first, then Data.
func (n *nodeIn[In]) next() (inboxItem[In], bool) {
	var zero inboxItem[In]
	if len(n.control) > 0 {
		it := n.control[0]
		n.control[0] = zero
		n.control = n.control[1:]
		return it, true
	}
	if len(n.data) > 0 {
		it := n.data[0]
		n.data[0] = zero
		n.data = n.data[1:]
		return it, true
	}
	return zero, false
}

// arrive handles a delivery that reached the front of the queue. With toRef,
// it is for incarnation inc only.
func (n *nodeIn[In]) arrive(msg In, inc machine.BootNumber, toRef bool) {
	w := n.w
	if !n.up || (toRef && n.inc != inc) {
		w.counters[countDroppedStale]++
		w.logMsg("drop stale "+string(n.name), msg)
		return
	}
	w.counters[countDeliveries]++
	w.logMsg("deliver "+string(n.name)+" <-", msg)
	if n.push(msg) {
		w.nodes[n.idx].pump()
	}
}

// deliver queues msg for node n at time at on lane. With toRef, the message is
// for incarnation inc only: it is dropped if that incarnation ended.
func deliver[In machine.Message](n *nodeIn[In], at Time, lane Lane, msg In, inc machine.BootNumber, toRef bool) EntryID {
	e := &entry{at: at, lane: lane, kind: kindDeliver, node: n.idx, body: msg}
	if toRef {
		e.inc = inc
	}
	e.run = func() { n.arrive(msg, inc, toRef) }
	return n.w.q.add(e)
}

// nodeHost is one node with its machine.
type nodeHost[In, Out machine.Message] struct {
	*nodeIn[In]
	spec NodeSpec[In, Out]
	m    machine.Machine[In, Out]
}

// Spawn adds a node to the world and returns its address. Start (or Boot)
// boots it. A spec without New or Route, or with a name already used, is a
// protocol failure and gives the zero Addr.
func Spawn[In, Out machine.Message](w *World, s NodeSpec[In, Out]) Addr[In] {
	switch {
	case s.Name == "":
		w.Fail(FailProtocol, "spawn", "a node needs a Name")
		return Addr[In]{}
	case s.New == nil || s.Route == nil:
		w.Fail(FailProtocol, "spawn", fmt.Sprintf("node %s: New and Route are required", s.Name))
		return Addr[In]{}
	case w.names[s.Name]:
		w.Fail(FailProtocol, "spawn", fmt.Sprintf("node %s: the name is used twice", s.Name))
		return Addr[In]{}
	}
	n := &nodeIn[In]{
		nodeBase: nodeBase{
			w: w, idx: len(w.nodes), name: s.Name,
			driver: s.Driver, attrition: s.Attrition, restart: s.Restart,
		},
		class: s.Class,
	}
	if limit := w.cfg.ClockSkewMax; limit > 0 {
		n.skew = machine.Duration(w.rand.Between(0, int64(limit)))
	}
	w.names[s.Name] = true
	w.nodes = append(w.nodes, &nodeHost[In, Out]{nodeIn: n, spec: s})
	return Addr[In]{n: n}
}

// checkAddr reports whether a is an address of w. If not, it is a protocol
// failure.
func checkAddr[In machine.Message](w *World, a Addr[In]) bool {
	switch {
	case a.n == nil:
		w.Fail(FailProtocol, "zero address", "a zero Addr was used")
		return false
	case a.n.w != w:
		w.Fail(FailProtocol, "foreign address", "an Addr of another World was used")
		return false
	}
	return true
}

// CurrentRef returns the incarnation of a that is up now.
func CurrentRef[In machine.Message](w *World, a Addr[In]) (Ref[In], bool) {
	if !checkAddr(w, a) || !a.n.up {
		return Ref[In]{}, false
	}
	return a.n.ref(), true
}

// Inject delivers msg from host code (a client request, a nemesis) to the
// incarnation of `to` that is up when it arrives, after a delay. It has no
// lane.
func Inject[In machine.Message](w *World, to Addr[In], after machine.Duration, msg In) {
	if checkAddr(w, to) {
		deliver(to.n, w.q.now+Time(max(after, 0)), NoLane, msg, machine.BootNumber{}, false)
	}
}

// InjectOn is Inject at delay 0 on a lane: messages on one lane at one time
// keep their order.
func InjectOn[In machine.Message](w *World, to Addr[In], lane Lane, msg In) {
	if checkAddr(w, to) {
		deliver(to.n, w.q.now, lane, msg, machine.BootNumber{}, false)
	}
}

func (h *nodeHost[In, Out]) base() *nodeBase { return &h.nodeBase }

func (h *nodeHost[In, Out]) stateHash() uint64 { return h.m.StateHash() }

func (h *nodeHost[In, Out]) inboxLens() (int, int) { return len(h.control), len(h.data) }

func (h *nodeHost[In, Out]) forget() {
	var zero machine.Machine[In, Out]
	h.m = zero
	h.control, h.data, h.timers = nil, nil, nil
	h.busy = 0
}

func (h *nodeHost[In, Out]) stopRequest(deadline machine.NodeTime) bool {
	if h.spec.StopMsg == nil {
		return false
	}
	var msg In
	if !h.w.guard("StopMsg "+string(h.name), func() { msg = h.spec.StopMsg(deadline) }) {
		h.w.halted = true
		return true
	}
	h.control = append(h.control, inboxItem[In]{msg: msg})
	return true
}

// boot starts a new incarnation: a new boot number from the run stream, a
// fresh machine from the factory, the boot input in the Control inbox and a
// wake-up.
func (h *nodeHost[In, Out]) boot() {
	w := h.w
	if h.up || h.broken {
		return
	}
	h.inc = machine.BootNumber(env{w}.NewID())
	h.control, h.data = nil, nil
	h.timers = map[machine.TimerID]EntryID{}
	h.stepped, h.busy = 0, 0
	h.wakePending, h.stopping, h.paused = false, false, false
	h.boots++
	self := h.self()
	var m machine.Machine[In, Out]
	ok := w.guard("factory "+string(h.name), func() { m = h.spec.New(env{w}, self) })
	if ok && m == nil {
		w.Fail(FailProtocol, "factory", string(h.name)+": the factory returned nil")
	}
	if !ok || m == nil {
		h.broken = true
		if h.driver {
			w.halted = true
		}
		return
	}
	h.m, h.up, h.ok = m, true, false
	if h.boots > 1 {
		w.counters[countRestarts]++
		w.kernelFact("sim_restarted", machine.Field{Key: "node", Value: machine.StringValue(h.name)})
	}
	if h.spec.Boot != nil {
		var in In
		if !w.guard("Boot "+string(h.name), func() { in = h.spec.Boot(self) }) {
			w.halted = true
			return
		}
		h.control = append(h.control, inboxItem[In]{msg: in, boot: true})
	}
	if !h.empty() {
		w.ensureWake(&h.nodeBase, w.q.now)
	}
}

// pump gives the node one Step if it is up, not paused, not busy, and has an
// input waiting. A busy node gets a wake-up at the end of its busy time.
func (h *nodeHost[In, Out]) pump() {
	w := h.w
	if !h.up || h.paused || w.halted {
		return
	}
	now := w.q.now
	if h.busy > now {
		if !h.empty() {
			w.ensureWake(&h.nodeBase, h.busy)
		}
		return
	}
	item, ok := h.next()
	if !ok {
		return
	}
	w.counters[countNodeSteps]++
	if h.stepped == 0 && h.spec.Boot != nil && !item.boot {
		w.Fail(FailProtocol, "boot_first", string(h.name)+": the first Step did not take the boot input")
	}
	h.stepped++
	w.hash.String(string(h.name))
	item.msg.Fold(&w.hash)
	w.logMsg("step "+string(h.name), item.msg)
	if !h.stepOnce(item.msg) {
		return
	}
	if !h.up {
		return
	}
	cost := max(h.spec.Cost.Sample(w.rand), 0)
	h.busy = now + Time(cost)
	if !h.empty() {
		w.ensureWake(&h.nodeBase, h.busy)
	}
	w.attr++
}

// stepOnce runs one Step with in, routes the outputs and runs AfterStep. It
// reports false when the Step panicked: then a driver halts the run and any
// other node crashes.
func (h *nodeHost[In, Out]) stepOnce(in In) bool {
	w := h.w
	self := h.ref()
	h.ok = false
	var out []Out
	now := machine.Now{Mono: h.local(), Wall: w.Wall()}
	if !w.guard("Step "+string(h.name), func() { out = h.m.Step(now, in) }) {
		if h.driver {
			w.halted = true
		} else {
			w.end(h, machine.Crash, "panic")
		}
		return false
	}
	h.route(self, out)
	if h.spec.AfterStep != nil && !w.halted {
		if !w.guard("AfterStep "+string(h.name), func() { h.spec.AfterStep(self, in, out) }) {
			w.halted = true
		}
	}
	return true
}

// route folds and routes the outputs of one Step, in order. It stops when the
// incarnation ends. An output after End is a protocol failure.
func (h *nodeHost[In, Out]) route(self Ref[In], out []Out) {
	w := h.w
	r := &Router[In]{n: h.nodeIn, self: self}
	for i, o := range out {
		if !h.up || h.inc != self.inc || w.halted {
			break
		}
		if !w.guard("Route "+string(h.name), func() { o.Fold(&w.hash); h.spec.Route(r, o) }) {
			w.halted = true
			break
		}
		if r.ended {
			if i != len(out)-1 {
				w.Fail(FailProtocol, "output_after_end", string(h.name)+": an output came after End")
			}
			break
		}
	}
	r.done = true
}

// phaseStep gives a live driver its OnPhase input as one Step. It takes no
// node time and skips the inbox.
func (h *nodeHost[In, Out]) phaseStep(p Phase) {
	w := h.w
	if h.spec.OnPhase == nil || !h.up || w.halted {
		return
	}
	w.hash.String(string(h.name))
	w.hash.Uint64(tagOnPhase)
	w.hash.Uint64(uint64(p))
	var msg In
	var ok bool
	if !w.guard("OnPhase "+string(h.name), func() { msg, ok = h.spec.OnPhase(p) }) {
		w.halted = true
		return
	}
	if !ok {
		return
	}
	w.logMsg(fmt.Sprintf("on_phase %s %d", h.name, p), msg)
	h.stepOnce(msg)
}
