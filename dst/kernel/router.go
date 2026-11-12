package kernel

import "github.com/manifesterio/mds/dst/machine"

// Router is lent to a route function for the outputs of one Step. In is the
// input type of the node that stepped, so operations on the node itself are
// methods. Do not keep a Router after the route function returns: using it
// later is a protocol failure.
type Router[In machine.Message] struct {
	n     *nodeIn[In]
	self  Ref[In]
	ended bool
	done  bool
}

// usable reports whether the router may still act. After the Step it is a
// protocol failure. After the incarnation ended it is a quiet no-op.
func (r *Router[In]) usable() bool {
	if r.done {
		r.n.w.Fail(FailProtocol, "router_after_step", string(r.n.name)+": a Router was used after its Step")
		return false
	}
	return r.n.up && r.n.inc == r.self.inc
}

// Self is the incarnation that stepped.
func (r *Router[In]) Self() Ref[In] { return r.self }

// Now is the virtual time of the Step.
func (r *Router[In]) Now() Time { return r.n.w.q.now }

// SetTimer delivers msg to this incarnation after `after` of node time. A
// pending timer with the same id is replaced: the old one never fires.
func (r *Router[In]) SetTimer(id machine.TimerID, after machine.Duration, msg In) {
	if !r.usable() {
		return
	}
	n, w := r.n, r.n.w
	w.counters[countTimersSet]++
	if old, ok := n.timers[id]; ok {
		w.q.cancel(old)
	}
	inc := n.inc
	var eid EntryID
	e := &entry{at: w.q.now + Time(max(after, 0)), lane: nodeLane(n.idx, 0), kind: kindDeliver, node: n.idx, inc: inc, body: msg}
	e.run = func() {
		if n.inc == inc && n.timers[id] == eid {
			delete(n.timers, id)
		}
		n.arrive(msg, inc, true)
	}
	eid = w.q.add(e)
	n.timers[id] = eid
}

// CancelTimer cancels a pending timer of this incarnation. An unknown id, or a
// timer that already fired, is ignored.
func (r *Router[In]) CancelTimer(id machine.TimerID) {
	if !r.usable() {
		return
	}
	n := r.n
	if eid, ok := n.timers[id]; ok {
		n.w.counters[countTimersCancelled]++
		n.w.q.cancel(eid)
		delete(n.timers, id)
	}
}

// SendSelf delivers msg to this incarnation at delay 0. It is dropped if the
// incarnation ends first.
func (r *Router[In]) SendSelf(msg In) {
	if r.usable() {
		deliver(r.n, r.n.w.q.now, NoLane, msg, r.self.inc, true)
	}
}

// Fact appends f to the timeline with this incarnation as the source.
func (r *Router[In]) Fact(f machine.Fact) {
	if r.usable() {
		r.n.w.appendFact(r.self.Instance(), f)
	}
}

// End ends this incarnation (usually Trip or Exit). It must be the last
// output of the Step; an output after it is a protocol failure. A driver may
// not end: that is a protocol failure that halts the run.
func (r *Router[In]) End(how machine.End, cause string) {
	if !r.usable() {
		return
	}
	w := r.n.w
	s := w.nodes[r.n.idx]
	if w.refuseDriver(s, "end") {
		return
	}
	r.ended = true
	w.end(s, how, cause)
}

// Send delivers msg to the incarnation of node `to` that is up when it
// arrives, at delay 0, with a fresh random tie. Go methods cannot have their
// own type parameters, so this is a package function: In is the input type of
// the sender, To the input type of the receiver.
func Send[In, To machine.Message](r *Router[In], to Addr[To], msg To) {
	SendOn(r, NoLane, to, msg)
}

// SendOn is Send on a lane: messages on one lane at one time keep their order.
func SendOn[In, To machine.Message](r *Router[In], lane Lane, to Addr[To], msg To) {
	if !r.usable() || !checkAddr(r.n.w, to) {
		return
	}
	deliver(to.n, r.n.w.q.now, lane, msg, machine.BootNumber{}, false)
}

// Call hands req to a service. The service answers later, through its
// ServiceContext, with values of the input type In of the caller. The request
// is folded into the trace hash.
func Call[Req, In machine.Message](r *Router[In], s ServiceAddr[Req, In], req Req) {
	w := r.n.w
	if !r.usable() || !checkService(w, s) {
		return
	}
	req.Fold(&w.hash)
	s.h.handle(r.self, req)
}
