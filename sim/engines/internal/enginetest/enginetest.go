// Package enginetest is a hand-written sim.EngineCtx and a SplitMix64
// sim.CountedRand for engine tests: a tiny queue ordered by (At, seq).
package enginetest

import (
	"fmt"
	"slices"

	"manifester/core"
	"manifester/sim"
)

// Rand is a SplitMix64 CountedRand.
type Rand struct{ s, calls uint64 }

// NewRand returns a stream for seed.
func NewRand(seed uint64) *Rand { return &Rand{s: seed} }

func (r *Rand) Uint64() uint64 {
	r.calls++
	r.s += 0x9e3779b97f4a7c15
	z := r.s
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}
func (r *Rand) Intn(n int) int      { return int(r.Uint64() % uint64(n)) }
func (r *Rand) Float64() float64    { return float64(r.Uint64()>>11) / (1 << 53) }
func (r *Rand) Reseed(seed uint64)  { r.s = seed }
func (r *Rand) Calls() uint64       { return r.calls }
func (r *Rand) Fingerprint() uint64 { return r.s ^ r.calls<<48 }
func (r *Rand) NewID() (id core.ID) {
	for i := 0; i < 16; i += 8 {
		v := r.Uint64()
		for j := 0; j < 8; j++ {
			id[i+j] = byte(v >> (8 * j))
		}
	}
	return id
}
func (r *Rand) Chance(p float64) bool {
	if p <= 0 {
		return false
	}
	return r.Float64() < p
}
func (r *Rand) Between(lo, hi int64) int64 { return lo + int64(r.Uint64()%uint64(hi-lo)) }
func (r *Rand) Derive(label string) sim.CountedRand {
	d := &Rand{s: r.s}
	for i := 0; i < len(label); i++ {
		d.s = (d.s ^ uint64(label[i])) * 1099511628211
	}
	return d
}

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
	R         *Rand
	T         sim.Time
	Down      map[core.NodeID]bool // test-only
	Delivered []Delivery
	Faults    []string
	Reached   []string
	q         []*entry
	seq       int
	eng       sim.Engine
}

// New returns a Ctx bound to eng.
func New(eng sim.Engine, seed uint64) *Ctx {
	return &Ctx{R: NewRand(seed), Down: map[core.NodeID]bool{}, eng: eng}
}

func (c *Ctx) Now() sim.Time         { return c.T }
func (c *Ctx) Rand() sim.CountedRand { return c.R }
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
	for {
		var live []*entry
		for _, e := range c.q {
			if !e.dead {
				live = append(live, e)
			}
		}
		c.q = live
		if len(c.q) == 0 {
			return
		}
		slices.SortStableFunc(c.q, func(a, b *entry) int {
			if a.at != b.at {
				return int(a.at - b.at)
			}
			return a.seq - b.seq
		})
		e := c.q[0]
		c.q = c.q[1:]
		c.T = e.at
		if e.self {
			c.eng.OnInternal(c, e.body)
		} else {
			c.Delivered = append(c.Delivered, Delivery{At: e.at, To: e.to, Body: e.body.(core.EventBody)})
		}
	}
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
