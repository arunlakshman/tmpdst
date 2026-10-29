// Package fakeservicetest is a hand-written sim.EngineCtx for engine tests: a tiny queue ordered by (At, seq).
package fakeservicetest

import (
	"fmt"
	"slices"
	"strings"

	"manifester/internal/core"
	"manifester/internal/verify/simulator"
)

// DeliveredEvent is one completion handed to a node.
type DeliveredEvent struct {
	At   simulator.Time
	To   core.NodeInstance
	Body core.EventData
}

type entry struct {
	at   simulator.Time
	seq  int
	to   core.NodeInstance // deliver when self is false
	self bool
	body any
	dead bool
}

// Ctx runs one engine against a queue.
type Ctx struct {
	R         simulator.CountedRand
	T         simulator.Time
	Down      map[core.NodeID]bool // test-only
	Delivered []DeliveredEvent
	Faults    []string
	Reached   []string
	// Violations lists the properties an Always found false.
	Violations []string
	q          []*entry
	seq        int
	eng        simulator.FakeService
	lanes      map[string]simulator.DeliveryLane
}

// New returns a Ctx bound to eng.
func New(eng simulator.FakeService, seed uint64) *Ctx {
	return &Ctx{R: simulator.NewRand(seed), Down: map[core.NodeID]bool{}, eng: eng, lanes: map[string]simulator.DeliveryLane{}}
}

// Now returns the fake clock.
func (c *Ctx) Now() simulator.Time { return c.T }

// Rand returns the seeded random source.
func (c *Ctx) Rand() simulator.CountedRand { return c.R }

// Lane numbers keys sequentially from 1, like the kernel.
func (c *Ctx) Lane(key string) simulator.DeliveryLane {
	if l, ok := c.lanes[key]; ok {
		return l
	}
	l := simulator.DeliveryLane(len(c.lanes) + 1)
	c.lanes[key] = l
	return l
}
func (c *Ctx) add(e *entry) simulator.EntryID {
	c.seq++
	e.seq = c.seq
	c.q = append(c.q, e)
	return simulator.EntryID(e.seq)
}

// Deliver queues an event for a node.
func (c *Ctx) Deliver(to core.NodeInstance, at simulator.Time, _ simulator.DeliveryLane, b core.EventData) simulator.EntryID {
	return c.add(&entry{at: max(at, c.T), to: to, body: b})
}

// Self queues an engine-private entry.
func (c *Ctx) Self(at simulator.Time, _ simulator.DeliveryLane, b any) simulator.EntryID {
	return c.add(&entry{at: max(at, c.T), self: true, body: b})
}

// Cancel marks a queued entry dead.
func (c *Ctx) Cancel(id simulator.EntryID) {
	for _, e := range c.q {
		if simulator.EntryID(e.seq) == id {
			e.dead = true
		}
	}
}

// Fault records an injected fault kind.
func (c *Ctx) Fault(kind string, _ ...core.Field) { c.Faults = append(c.Faults, kind) }

// Assert returns the assertion recorder.
func (c *Ctx) Assert() core.Assertions { return checks{c} }

// Current returns the live incarnation of n.
func (c *Ctx) Current(n core.NodeID) (core.NodeInstance, bool) {
	if c.Down[n] {
		return core.NodeInstance{}, false
	}
	return Ref(string(n)), true
}

var _ simulator.NodeCurrent = (*Ctx)(nil)

// Ref is the live incarnation of node n in tests.
func Ref(n string) core.NodeInstance {
	return core.NodeInstance{Node: core.NodeID(n), Inc: core.BootNumber{1}}
}

// Crash marks n down and drops its pending deliveries (the scheduler's DropNode).
func (c *Ctx) Crash(n string) {
	c.Down[core.NodeID(n)] = true
	for _, e := range c.q {
		if !e.self && e.to.Node == core.NodeID(n) {
			e.dead = true
		}
	}
	c.eng.OnCrash(c, Ref(n))
}

// Run drains the queue in (At, seq) order.
func (c *Ctx) Run() {
	for c.step() {
	}
}

// RunUntil runs the queue in order up to and including time at, for engines
// whose own timers never let the queue drain. The clock ends at `at`.
func (c *Ctx) RunUntil(at simulator.Time) {
	for {
		c.dropDead()
		if len(c.q) == 0 {
			break
		}
		slices.SortStableFunc(c.q, entryLess)
		if c.q[0].at > at {
			break
		}
		c.step()
	}
	c.T = max(c.T, at)
}

// dropDead removes cancelled entries from the queue.
func (c *Ctx) dropDead() {
	var live []*entry
	for _, e := range c.q {
		if !e.dead {
			live = append(live, e)
		}
	}
	c.q = live
}

func entryLess(a, b *entry) int {
	if a.at != b.at {
		return int(a.at - b.at)
	}
	return a.seq - b.seq
}

// step runs the earliest live entry; false when the queue is empty.
func (c *Ctx) step() bool {
	c.dropDead()
	if len(c.q) == 0 {
		return false
	}
	slices.SortStableFunc(c.q, entryLess)
	e := c.q[0]
	c.q = c.q[1:]
	c.T = e.at
	if e.self {
		c.eng.OnInternal(c, e.body)
	} else {
		c.Delivered = append(c.Delivered, DeliveredEvent{At: e.at, To: e.to, Body: e.body.(core.EventData)})
	}
	return true
}

// Trace renders the deliveries for equality checks.
func (c *Ctx) Trace() string {
	var s strings.Builder
	for _, d := range c.Delivered {
		fmt.Fprintf(&s, "%d %s %#v\n", d.At, d.To.Node, d.Body)
	}
	return s.String()
}

type checks struct{ c *Ctx }

func (a checks) Always(cond bool, p core.PropertyID, _ ...core.ContextPair) {
	if !cond {
		a.c.Violations = append(a.c.Violations, string(p))
	}
}
func (a checks) AlwaysOrUnreachable(bool, core.PropertyID, ...core.ContextPair) {}
func (a checks) Sometimes(cond bool, p core.PropertyID) {
	if cond {
		a.c.Reached = append(a.c.Reached, string(p))
	}
}
func (a checks) Reachable(p core.PropertyID)                      { a.c.Reached = append(a.c.Reached, string(p)) }
func (a checks) Unreachable(core.PropertyID, ...core.ContextPair) {}

// Find returns the delivered completion for req (nil if none).
func (c *Ctx) Find(req core.RequestID) core.IOResult {
	for _, d := range c.Delivered {
		if cp, ok := d.Body.(core.IOResult); ok && cp.RequestID() == req {
			return cp
		}
	}
	return nil
}
