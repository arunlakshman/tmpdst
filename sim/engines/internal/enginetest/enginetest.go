// Package enginetest is a hand-written sim.EngineCtx for engine tests: a tiny queue ordered by (At, seq).
package enginetest

import (
	"fmt"
	"slices"

	"manifester/core"
	"manifester/sim"
)

// Delivery is one completion handed to a node.
type Delivery struct {
	At   sim.Time
	To   core.NodeRef
	Body core.EventBody
}

type entry struct {
	at   sim.Time
	seq  int
	to   core.NodeRef // deliver when self is false
	self bool
	body any
	dead bool
}

// Ctx runs one engine against a queue.
type Ctx struct {
	R         sim.CountedRand
	T         sim.Time
	Down      map[core.NodeID]bool // test-only
	Delivered []Delivery
	Faults    []string
	Reached   []string
	q         []*entry
	seq       int
	eng       sim.Engine
	lanes     map[string]sim.Lane
}

// New returns a Ctx bound to eng.
func New(eng sim.Engine, seed uint64) *Ctx {
	return &Ctx{R: sim.NewRand(seed), Down: map[core.NodeID]bool{}, eng: eng, lanes: map[string]sim.Lane{}}
}

func (c *Ctx) Now() sim.Time         { return c.T }
func (c *Ctx) Rand() sim.CountedRand { return c.R }

// Lane numbers keys sequentially from 1, like the kernel.
func (c *Ctx) Lane(key string) sim.Lane {
	if l, ok := c.lanes[key]; ok {
		return l
	}
	l := sim.Lane(len(c.lanes) + 1)
	c.lanes[key] = l
	return l
}
func (c *Ctx) add(e *entry) sim.EntryID {
	c.seq++
	e.seq = c.seq
	c.q = append(c.q, e)
	return sim.EntryID(e.seq)
}
func (c *Ctx) Deliver(to core.NodeRef, at sim.Time, _ sim.Lane, b core.EventBody) sim.EntryID {
	return c.add(&entry{at: max(at, c.T), to: to, body: b})
}
func (c *Ctx) Self(at sim.Time, _ sim.Lane, b any) sim.EntryID {
	return c.add(&entry{at: max(at, c.T), self: true, body: b})
}
func (c *Ctx) Cancel(id sim.EntryID) {
	for _, e := range c.q {
		if sim.EntryID(e.seq) == id {
			e.dead = true
		}
	}
}
func (c *Ctx) Fault(kind string, _ ...core.Field) { c.Faults = append(c.Faults, kind) }
func (c *Ctx) Assert() core.Assertions            { return asserts{c} }
func (c *Ctx) Current(n core.NodeID) (core.NodeRef, bool) {
	if c.Down[n] {
		return core.NodeRef{}, false
	}
	return Ref(string(n)), true
}

// Ref is the live incarnation of node n in tests.
func Ref(n string) core.NodeRef {
	return core.NodeRef{Node: core.NodeID(n), Inc: core.Incarnation{1}}
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
		c.Delivered = append(c.Delivered, Delivery{At: e.at, To: e.to, Body: e.body.(core.EventBody)})
	}
	return true
}

// Trace renders the deliveries for equality checks.
func (c *Ctx) Trace() string {
	s := ""
	for _, d := range c.Delivered {
		s += fmt.Sprintf("%d %s %#v\n", d.At, d.To.Node, d.Body)
	}
	return s
}

type asserts struct{ c *Ctx }

func (a asserts) Always(bool, core.Prop, ...core.Detail)              {}
func (a asserts) AlwaysOrUnreachable(bool, core.Prop, ...core.Detail) {}
func (a asserts) Sometimes(cond bool, p core.Prop) {
	if cond {
		a.c.Reached = append(a.c.Reached, string(p))
	}
}
func (a asserts) Reachable(p core.Prop)                 { a.c.Reached = append(a.c.Reached, string(p)) }
func (a asserts) Unreachable(core.Prop, ...core.Detail) {}

// Find returns the delivered completion for req (nil if none).
func (c *Ctx) Find(req core.ReqID) core.Completion {
	for _, d := range c.Delivered {
		if cp, ok := d.Body.(core.Completion); ok && cp.ReqID() == req {
			return cp
		}
	}
	return nil
}
