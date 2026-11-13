package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
)

// Attrition kills nodes at random during PhaseFaults. After each node Step and
// each service step, the kernel rolls CrashProb once. On a hit it picks one
// live node that has NodeSpec.Attrition, and crashes it or (by weight) stops it
// gracefully. MaxDead caps the nodes that are down or stopping.
type Attrition struct {
	MaxDead        int              // nodes down or stopping; 0 means attrition is off
	CrashProb      float64          // rolled after each node Step and each service step
	CrashWeight    float64          // hard crash ...
	GracefulWeight float64          // ... or graceful Stop, by weight
	StopGrace      machine.Duration // 0 means 5 s
}

// lookup finds the node of n. A nil Node, a zero address or an address of
// another world is a protocol failure.
func (w *World) lookup(n Node) (nodeSlot, nodeKey, bool) {
	if n == nil {
		w.Fail(FailProtocol, "zero address", "a nil Node was used")
		return nil, nodeKey{}, false
	}
	k := n.node()
	switch {
	case k.w == nil:
		w.Fail(FailProtocol, "zero address", "a zero Addr was used")
		return nil, k, false
	case k.w != w:
		w.Fail(FailProtocol, "foreign address", "an Addr of another World was used")
		return nil, k, false
	}
	return w.nodes[k.idx], k, true
}

// live finds the node of n if it is up, and for a Ref, if that incarnation is
// the one that is up.
func (w *World) live(n Node) (nodeSlot, bool) {
	s, k, ok := w.lookup(n)
	if !ok {
		return nil, false
	}
	b := s.base()
	if !b.up || (k.ref && b.inc != k.inc) {
		return nil, false
	}
	return s, true
}

// Up reports whether the node is up (for a Ref: whether that incarnation is
// still the one that is up).
func (w *World) Up(n Node) bool {
	_, ok := w.live(n)
	return ok
}

// Current returns the incarnation that is up now.
func (w *World) Current(n Node) (machine.NodeInstance, bool) {
	s, ok := w.live(n)
	if !ok {
		return machine.NodeInstance{}, false
	}
	return s.base().self(), true
}

// Down reports, while the node is down, how its last incarnation ended. A
// node that never booted reports Crash.
func (w *World) Down(n Node) (machine.End, bool) {
	s, _, ok := w.lookup(n)
	if !ok || s.base().up {
		return machine.Crash, false
	}
	return s.base().lastEnd, true
}

// Boot starts a new incarnation now, if the node is down and not broken.
func (w *World) Boot(n Node) {
	if s, _, ok := w.lookup(n); ok {
		s.boot()
	}
}

// Crash ends the incarnation now (for a Ref: only if it is still the one that
// is up). Crashing a driver is a protocol failure that halts the run.
func (w *World) Crash(n Node, cause string) {
	s, ok := w.live(n)
	if !ok || w.refuseDriver(s, "crash") {
		return
	}
	w.end(s, machine.Crash, cause)
}

// Stop sends NodeSpec.StopMsg(deadline) into the Control inbox and arms a kill
// at now+grace, addressed to this incarnation (an Exit before it drops the
// kill). A node without StopMsg, or a driver, is a protocol failure.
func (w *World) Stop(n Node, grace machine.Duration) {
	s, ok := w.live(n)
	if !ok || s.base().stopping || w.refuseDriver(s, "stop") {
		return
	}
	w.stop(s, grace)
}

// Pause stops the node from taking a Step until Resume. Inputs keep arriving
// and wait in its inbox, in order. What it started keeps going. The pause ends
// with the incarnation.
func (w *World) Pause(n Node) {
	if s, ok := w.live(n); ok {
		s.base().paused = true
		w.logMsg("pause "+string(s.base().name), nil)
	}
}

// Resume ends a Pause. The waiting inputs run in order.
func (w *World) Resume(n Node) {
	s, ok := w.live(n)
	if !ok || !s.base().paused {
		return
	}
	b := s.base()
	b.paused = false
	w.logMsg("resume "+string(b.name), nil)
	if c, d := s.inboxLens(); c+d > 0 {
		w.ensureWake(b, w.q.now)
	}
}

// refuseDriver reports true, after a protocol failure that halts the run, when
// s is a driver: a driver never ends.
func (w *World) refuseDriver(s nodeSlot, what string) bool {
	if !s.base().driver {
		return false
	}
	w.Fail(FailProtocol, "driver_ended", fmt.Sprintf("%s: a driver may not %s", s.base().name, what))
	w.halted = true
	return true
}

// end finishes the incarnation of s. Pending entries for it are dropped, every
// service hears OnCrash, the machine is thrown away and, if NodeSpec.Restart
// says so, the next boot is queued. Effects that services already applied
// stand.
func (w *World) end(s nodeSlot, how machine.End, cause string) {
	b := s.base()
	if !b.up {
		return
	}
	who := b.self()
	b.up, b.stopping, b.paused, b.lastEnd = false, false, false, how
	w.counters[countDroppedByCrash] += uint64(w.q.dropIncarnation(b.idx, b.inc))
	name := "sim_crashed"
	switch how {
	case machine.Crash:
		w.crashes++
	case machine.Trip:
		w.trips++
		name = "sim_tripped"
	case machine.Exit:
		w.exits++
		name = "sim_exited"
	}
	w.logMsg(fmt.Sprintf("%s %s (%s)", name, b.name, cause), nil)
	w.kernelFact(name, machine.Field{Key: "node", Value: machine.StringValue(b.name)},
		machine.Field{Key: "cause", Value: machine.StringValue(cause)})
	for _, sv := range w.services {
		sv.base().ok = false
		if !w.guard("OnCrash "+sv.base().name, func() { sv.onCrash(who) }) {
			w.halted = true
		}
	}
	s.forget()
	if b.restart == nil {
		return
	}
	var after machine.Duration
	var again bool
	if !w.guard("Restart "+string(b.name), func() { after, again = b.restart(how, w.rand) }) {
		w.halted = true
		return
	}
	if again {
		w.queueBoot(s, w.q.now+Time(max(after, 0)))
	}
}

// queueBoot queues the next boot of s at time at, on HostLane.
func (w *World) queueBoot(s nodeSlot, at Time) {
	b := s.base()
	e := &entry{at: at, lane: HostLane, kind: kindRestart, node: b.idx}
	e.run = func() {
		w.counters[countKernelSteps]++
		s.boot()
	}
	w.q.add(e)
}

// stop asks s to shut down gracefully and arms the kill at the deadline. The
// kill is addressed to this incarnation, so an Exit first drops it.
func (w *World) stop(s nodeSlot, grace machine.Duration) {
	b := s.base()
	deadline := w.q.now + Time(max(grace, 0))
	if !s.stopRequest(machine.NodeTime(deadline) + machine.NodeTime(b.skew)) {
		w.Fail(FailProtocol, "stop_not_allowed", string(b.name)+": the node has no StopMsg")
		return
	}
	b.stopping = true
	w.counters[countGracefulStops]++
	inc := b.inc
	e := &entry{at: deadline, lane: HostLane, kind: kindCrash, node: b.idx, inc: inc}
	e.run = func() {
		w.counters[countKernelSteps]++
		if !b.up || b.inc != inc {
			return
		}
		if b.stopping {
			w.counters[countDeadlineKills]++
		}
		w.end(s, machine.Crash, "stop deadline")
	}
	w.q.add(e)
	w.kernelFact("sim_stop_requested", machine.Field{Key: "node", Value: machine.StringValue(b.name)})
	s.pump()
}

// attrition makes one attrition roll (PhaseFaults only): pick a live node that
// allows attrition, then crash it or, by weight, stop it gracefully.
func (w *World) attrition() {
	a := w.cfg.Attrition
	if w.phase != PhaseFaults || !w.rand.Chance(a.CrashProb) {
		return
	}
	var live []nodeSlot
	down := 0
	for _, s := range w.nodes {
		b := s.base()
		switch {
		case b.driver || !b.attrition:
		case !b.up || b.stopping:
			down++
		default:
			live = append(live, s)
		}
	}
	if down >= a.MaxDead || len(live) == 0 {
		return
	}
	s := live[w.rand.Intn(len(live))]
	if total := a.CrashWeight + a.GracefulWeight; total > 0 && w.rand.Float64()*total < a.GracefulWeight {
		grace := a.StopGrace
		if grace <= 0 {
			grace = defaultStopGrace
		}
		w.stop(s, grace)
		return
	}
	w.end(s, machine.Crash, "attrition")
}
