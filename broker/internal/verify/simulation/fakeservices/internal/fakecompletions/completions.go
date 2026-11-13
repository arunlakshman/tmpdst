// Package fakecompletions is the shared plan/land/deliver core of the fake object store
// and control plane services: faults + timing around a speccheck spec.
//
// Issue (Start) draws every coin and delay, in a fixed order, and schedules
// everything that does not depend on state. Landing (a Later step) runs the spec
// and delivers the response. A node crash drops only the delivery; the landing
// step belongs to the service and still applies.
//
// Outage design: a correlated window. On Start, outside recovery, if no
// window is active and Chance(Outage.Prob), one starts at now and lasts
// Between(Outage.Dur). Every op issued while a window is active fails with
// Unavailable503 (NotApplied). Windows never overlap and are re-rolled only
// after the last one ends.
//
// The draws are the same as in the old tree (internal/verify/simulator/fakeservices/internal/fakecompletions),
// in the same order. A parity test proves it.
package fakecompletions

import (
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/span"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/dst/trace"
)

// Cx is the context a service gets from the kernel. The broker services answer
// nodes with core.EventData values.
type Cx = kernel.ServiceContext[core.EventData]

// Ref names the node incarnation that made a call.
type Ref = kernel.Ref[core.EventData]

// faultAsserts are the assertions of one fake service. The object store has the
// prefix "obj" and the control plane has the prefix "cp".
type faultAsserts struct {
	outage, unavailable, timeout, lost assert.Reachable
	applied, notApplied                assert.Sometimes
}

// The two sets of assertions. Each name is written out in full, as the catalog
// needs it, and does not change.
var (
	objAsserts = faultAsserts{ //determinismcheck:ok catalog handles, set once at start
		outage:      assert.NewReachable("obj outage started"),
		unavailable: assert.NewReachable("obj 503 injected"),
		timeout:     assert.NewReachable("obj timeout injected"),
		lost:        assert.NewReachable("obj lost response injected"),
		applied:     assert.NewSometimes("obj timed-out op applied"),
		notApplied:  assert.NewSometimes("obj timed-out op not applied"),
	}
	cpAsserts = faultAsserts{ //determinismcheck:ok catalog handles, set once at start
		outage:      assert.NewReachable("cp outage started"),
		unavailable: assert.NewReachable("cp 503 injected"),
		timeout:     assert.NewReachable("cp timeout injected"),
		lost:        assert.NewReachable("cp lost response injected"),
		applied:     assert.NewSometimes("cp timed-out op applied"),
		notApplied:  assert.NewSometimes("cp timed-out op not applied"),
	}
)

// assertsFor returns the assertions of the service with the given prefix.
func assertsFor(prefix string) faultAsserts {
	switch prefix {
	case "obj":
		return objAsserts
	case "cp":
		return cpAsserts
	}
	panic("fakecompletions: unknown fault prefix " + prefix)
}

// ServiceOutage is a correlated, sustained failure window: every op fails while it
// lasts.
type ServiceOutage struct {
	Prob float64
	Dur  [2]machine.Duration
}

// Params is the fault and timing config common to both services.
type Params struct {
	Apply, Respond kernel.FastWithRareSlow
	P503           float64
	PTimeout       float64
	PLost          float64
	Outage         ServiceOutage
	CallerTimeout  machine.Duration
}

// Hooks is what a service plugs into the core.
type Hooks struct {
	Name   string // service name, also the prefix of its lane keys
	Prefix string // fault name prefix: "obj" or "cp"
	Params Params
	// Spec executes one call on the spec state.
	Spec func(a core.IOCall) core.IOResult
	// Mutating reports whether a may change spec state (reads need no landing
	// when their response is dropped).
	Mutating func(a core.IOCall) bool
	// Fail builds an error completion. Reads ignore the Outcome.
	Fail func(a core.IOCall, e core.ErrCode, o core.Outcome) core.IOResult
	// Changed reports whether c, the spec's answer, changed spec state.
	Changed func(c core.IOResult) bool
	// Extra draws optional per-op entropy at issue (outside recovery); the
	// bool and value are kept on the plan. Nil means none.
	Extra func(r *rng.Rand, a core.IOCall) (bool, uint64)
	// Finish rewrites the response of a normally answered op (corruption).
	Finish func(cx *Cx, c core.IOResult, p Plan) core.IOResult
	// SpecHash is the spec's StateHash.
	SpecHash func() uint64
}

// Kind is how an issued op resolves.
type Kind uint8

// Kind values: ReplyNormal, ReplyUnavailable, ReplyTimeout and ReplyLost, as commented below.
const (
	// ReplyNormal applies at T1 and responds at T2.
	ReplyNormal      Kind = iota // apply at T1, respond at T2
	ReplyUnavailable             // refused, NotApplied
	ReplyTimeout                 // caller sees timeout; op applies or not (drawn at T1)
	ReplyLost                    // applies at T1, response never arrives; caller sees timeout
)

// Plan is one in-flight op.
type Plan struct {
	ID     uint64
	From   Ref
	Act    core.IOCall
	Kind   Kind
	T1, T2 kernel.Time
	Flag   bool // Extra's bool
	Bits   uint64
	Lane   kernel.Lane
}

// Core is the service state besides the spec.
type Core struct {
	h        Hooks
	lanes    *kernel.Lanes[machine.NodeID] // one lane per calling node
	nextID   uint64
	plans    []Plan // sorted by ID (append order)
	until    kernel.Time
	recovery bool
	gen      uint64
	// specHash caches h.SpecHash(). It is valid only while specOK; every path
	// that can change the spec clears specOK (landing a mutating op, Bump).
	specHash uint64
	specOK   bool
}

// New returns a Core. The world gives it the lane table.
func New(w *kernel.World, h Hooks) *Core {
	return &Core{h: h, lanes: kernel.NewLanes[machine.NodeID](w)}
}

// Gen is the applied-change counter; Bump adds an out-of-band change.
func (c *Core) Gen() uint64 { return c.gen }

// Bump records an out-of-band spec change (and invalidates the spec hash cache).
func (c *Core) Bump() { c.gen++; c.specOK = false }

// specStateHash is the spec's StateHash, recomputed only after a mutation.
func (c *Core) specStateHash() uint64 {
	if !c.specOK {
		c.specHash, c.specOK = c.h.SpecHash(), true
	}
	return c.specHash
}

// SpecStateHash is the cached spec hash, exported so conformance (C2) can
// compare it with the spec's own StateHash (a stale cache shows up as a diff).
func (c *Core) SpecStateHash() uint64 { return c.specStateHash() }

// Name returns the hooks name.
func (c *Core) Name() string { return c.h.Name }

// EnterRecovery stops new faults for later calls.
func (c *Core) EnterRecovery() { c.recovery = true }

// OnCrash does nothing: effects stand and landings stay scheduled; deliveries to the dead
// incarnation are dropped by the kernel or skipped at landing.
func (c *Core) OnCrash(*Cx, machine.NodeInstance) {}

// Draw draws the boundary fault coins (outage, 503, timeout, lost, in that
// order) and emits the matching fault named pre+"_...". until is the end of
// the outage window and is extended when one starts. fields lead the
// 503/timeout/lost fault fields.
func (p Params) Draw(cx *Cx, pre string, until *kernel.Time, fields ...machine.Field) Kind {
	r, now := cx.Rand(), cx.Now()
	names := assertsFor(pre)
	if now >= *until && r.Chance(p.Outage.Prob) {
		d := p.Outage.Dur[0]
		if p.Outage.Dur[1] > d {
			d = machine.Duration(span.Between(r, int64(p.Outage.Dur[0]), int64(p.Outage.Dur[1])))
		}
		*until = now + kernel.Time(d)
		cx.Fault(pre+"_outage", machine.Field{Key: "dur", Value: machine.Int64Value(d)})
		cx.Assert().Reachable(names.outage)
	}
	inOutage := now < *until
	is503, isTimeout, isLost := r.Chance(p.P503), r.Chance(p.PTimeout), r.Chance(p.PLost)
	switch {
	case inOutage || is503:
		cause := "coin"
		if inOutage {
			cause = "outage"
		}
		cx.Fault(pre+"_503", append(fields, machine.Field{Key: "cause", Value: machine.StringValue(cause)})...)
		cx.Assert().Reachable(names.unavailable)
		return ReplyUnavailable
	case isTimeout:
		cx.Fault(pre+"_timeout", fields...)
		cx.Assert().Reachable(names.timeout)
		return ReplyTimeout
	case isLost:
		cx.Fault(pre+"_lost_response", fields...)
		cx.Assert().Reachable(names.lost)
		return ReplyLost
	}
	return ReplyNormal
}

// Start plans an op. Draw order: fault coins (Draw), apply latency, respond
// latency, extra.
func (c *Core) Start(cx *Cx, from Ref, a core.IOCall) {
	r, now, p := cx.Rand(), cx.Now(), c.h.Params
	request := machine.Field{Key: "req", Value: machine.Uint64Value(uint64(a.RequestID()))}
	kind := ReplyNormal
	if !c.recovery {
		kind = p.Draw(cx, c.h.Prefix, &c.until, request)
	}
	t1 := now + kernel.Time(p.Apply.Sample(r))
	t2 := t1 + kernel.Time(p.Respond.Sample(r))
	var flag bool
	var bits uint64
	if !c.recovery && c.h.Extra != nil {
		flag, bits = c.h.Extra(r, a)
	}

	c.nextID++
	pl := Plan{ID: c.nextID, From: from, Act: a, Kind: kind, T1: t1, T2: t2, Flag: flag, Bits: bits, Lane: c.lanes.Get(from.Addr().ID())}
	switch kind {
	case ReplyUnavailable:
		cx.Deliver(from, t2, pl.Lane, c.h.Fail(a, core.ErrUnavailable, core.NotApplied))
		return
	case ReplyTimeout, ReplyLost:
		cx.Deliver(from, now+kernel.Time(p.CallerTimeout), pl.Lane, c.h.Fail(a, core.ErrTimeout, core.MaybeApplied))
		if !c.h.Mutating(a) {
			return // nothing to apply, nothing to answer
		}
	}
	id := pl.ID
	cx.Later(t1, pl.Lane, func(cx *Cx) { c.land(cx, id) })
	c.plans = append(c.plans, pl)
}

// land lands one planned op.
func (c *Core) land(cx *Cx, id uint64) {
	i := -1
	for j := range c.plans {
		if c.plans[j].ID == id {
			i = j
		}
	}
	if i < 0 {
		return
	}
	pl := c.plans[i]
	c.plans = slices.Delete(c.plans, i, i+1)
	apply := true
	if pl.Kind == ReplyTimeout {
		apply = cx.Rand().Chance(0.5)
		names := assertsFor(c.h.Prefix)
		cx.Assert().Sometimes(apply, names.applied)
		cx.Assert().Sometimes(!apply, names.notApplied)
	}
	if !apply {
		return
	}
	comp := c.h.Spec(pl.Act)
	if c.h.Mutating(pl.Act) {
		c.specOK = false // whatever Changed says, the cache must not outlive a mutating call
	}
	if c.h.Changed(comp) {
		c.gen++
		c.specOK = false // a change Mutating did not name (a CreateTopic call) must not leave a stale hash
	}
	if pl.Kind != ReplyNormal {
		return
	}
	if cur, up := cx.Current(pl.From.Addr()); !up || cur != pl.From {
		return // caller died; the effect stands, nobody to tell
	}
	if c.h.Finish != nil {
		comp = c.h.Finish(cx, comp, pl)
	}
	at := max(pl.T2, cx.Now())
	cx.Deliver(pl.From, at, pl.Lane, comp)
}

// StateHash folds the spec hash, outage window, recovery flag and pending
// plans in issue order.
func (c *Core) StateHash() uint64 {
	h := trace.New()
	mix := h.Uint64
	mix(c.specStateHash())
	mix(uint64(c.until))
	h.Bool(c.recovery)
	mix(uint64(len(c.plans)))
	for _, p := range c.plans {
		mix(p.ID)
		who := p.From.Instance()
		for i := range len(who.Node) {
			mix(uint64(who.Node[i]))
		}
		for _, b := range who.Inc {
			mix(uint64(b))
		}
		mix(uint64(p.Act.RequestID()))
		mix(uint64(p.Kind))
		mix(uint64(p.T1))
		mix(uint64(p.T2))
		mix(p.Bits)
	}
	return h.Sum()
}
