// Package fakeio is the shared plan/land/deliver core of the fake object store
// and control plane engines: faults + timing around a veritas spec.
//
// Issue (Execute) draws every coin and delay, in a fixed order, and schedules
// everything that does not depend on state. Landing (OnInternal) runs the spec
// and delivers the response. A node crash drops only the delivery; the landing
// entry is engine-internal and still applies.
//
// Outage design: a correlated window. On Execute, outside recovery, if no
// window is active and Chance(Outage.Prob), one starts at now and lasts
// Between(Outage.Dur). Every op issued while a window is active fails with
// Unavailable503 (NotApplied). Windows never overlap and are re-rolled only
// after the last one ends.
package fakeio

import (
	"slices"

	"manifester/core"
	"manifester/sim"
)

// Params is the fault and timing config common to both engines.
type Params struct {
	Apply, Respond sim.TwoSpeed
	P503           float64
	PTimeout       float64
	PLost          float64
	Outage         sim.Outage
	CallerTimeout  core.Duration
}

// Hooks is what an engine plugs into the core.
type Hooks struct {
	Name   string // engine name, also the lane namespace
	Prefix string // fault name prefix: "obj" or "cp"
	Params Params
	// Spec executes one action on the spec state.
	Spec func(a core.IOAction) core.Completion
	// Mutating reports whether a may change spec state (reads need no landing
	// when their response is dropped).
	Mutating func(a core.IOAction) bool
	// Fail builds an error completion. Reads ignore the Outcome.
	Fail func(a core.IOAction, e core.ErrCode, o core.Outcome) core.Completion
	// Changed reports whether c, the spec's answer, changed spec state.
	Changed func(c core.Completion) bool
	// Extra draws optional per-op entropy at issue (outside recovery); the
	// bool and value are kept on the plan. Nil means none.
	Extra func(r sim.CountedRand, a core.IOAction) (bool, uint64)
	// Finish rewrites the response of a normally answered op (corruption).
	Finish func(ctx sim.EngineCtx, c core.Completion, p Plan) core.Completion
	// SpecHash is the spec's StateHash.
	SpecHash func() uint64
}

// Kind is how an issued op resolves.
type Kind uint8

// Kind values: Normal, Unavailable, Timeout and Lost, as commented below.
const (
	// Normal applies at T1 and responds at T2.
	Normal      Kind = iota // apply at T1, respond at T2
	Unavailable             // refused, NotApplied
	Timeout                 // caller sees timeout; op applies or not (drawn at T1)
	Lost                    // applies at T1, response never arrives; caller sees timeout
)

// Plan is one in-flight op.
type Plan struct {
	ID     uint64
	From   core.NodeRef
	Act    core.IOAction
	Kind   Kind
	T1, T2 sim.Time
	Flag   bool // Extra's bool
	Bits   uint64
	Lane   sim.Lane
}

type landBody struct{ ID uint64 }

// Core is the engine state besides the spec.
type Core struct {
	h        Hooks
	nextID   uint64
	plans    []Plan // sorted by ID (append order)
	until    sim.Time
	recovery bool
	gen      uint64
	// specHash caches h.SpecHash(). It is valid only while specOK; every path
	// that can change the spec clears specOK (landing a mutating op, Bump).
	specHash uint64
	specOK   bool
}

// New returns a Core.
func New(h Hooks) *Core { return &Core{h: h} }

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

// EnterRecovery stops new faults for later actions.
func (c *Core) EnterRecovery() { c.recovery = true }

// OnCrash does nothing: effects stand and landings stay scheduled; deliveries to the dead
// incarnation are dropped by the scheduler or skipped at landing.
func (c *Core) OnCrash(sim.EngineCtx, core.NodeRef) {}

// Execute plans an op. Draw order: outage, 503, timeout, lost coins, apply
// latency, respond latency, extra.
func (c *Core) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.IOAction) {
	r, now, p, pre := ctx.Rand(), ctx.Now(), c.h.Params, c.h.Prefix
	var is503, isTimeout, isLost, inOutage bool
	if !c.recovery {
		if now >= c.until && r.Chance(p.Outage.Prob) {
			d := core.Duration(0)
			if p.Outage.Dur[1] > p.Outage.Dur[0] {
				d = core.Duration(r.Between(int64(p.Outage.Dur[0]), int64(p.Outage.Dur[1])))
			} else {
				d = p.Outage.Dur[0]
			}
			c.until = now + sim.Time(d)
			ctx.Fault(pre+"_outage", core.Field{Key: "dur", Value: core.I64(d)})
			ctx.Assert().Reachable(core.Prop(pre + " outage started"))
		}
		inOutage = now < c.until
		is503 = r.Chance(p.P503)
		isTimeout = r.Chance(p.PTimeout)
		isLost = r.Chance(p.PLost)
	}
	t1 := now + sim.Time(p.Apply.Sample(r))
	t2 := t1 + sim.Time(p.Respond.Sample(r))
	var flag bool
	var bits uint64
	if !c.recovery && c.h.Extra != nil {
		flag, bits = c.h.Extra(r, a)
	}

	c.nextID++
	pl := Plan{ID: c.nextID, From: from, Act: a, T1: t1, T2: t2, Flag: flag, Bits: bits, Lane: ctx.Lane(c.h.Name + "/" + string(from.Node))}
	req := core.Field{Key: "req", Value: core.U64(a.ReqID())}
	timeoutAt := now + sim.Time(p.CallerTimeout)
	switch {
	case inOutage || is503:
		pl.Kind = Unavailable
		cause := "coin"
		if inOutage {
			cause = "outage"
		}
		ctx.Fault(pre+"_503", req, core.Field{Key: "cause", Value: core.Str(cause)})
		ctx.Assert().Reachable(core.Prop(pre + " 503 injected"))
		ctx.Deliver(from, t2, pl.Lane, c.h.Fail(a, core.ErrUnavailable, core.NotApplied))
		return
	case isTimeout:
		pl.Kind = Timeout
		ctx.Fault(pre+"_timeout", req)
		ctx.Assert().Reachable(core.Prop(pre + " timeout injected"))
	case isLost:
		pl.Kind = Lost
		ctx.Fault(pre+"_lost_response", req)
		ctx.Assert().Reachable(core.Prop(pre + " lost response injected"))
	}
	if pl.Kind == Timeout || pl.Kind == Lost {
		ctx.Deliver(from, timeoutAt, pl.Lane, c.h.Fail(a, core.ErrTimeout, core.MaybeApplied))
		if !c.h.Mutating(a) {
			return // nothing to apply, nothing to answer
		}
	}
	ctx.Self(t1, pl.Lane, landBody{ID: pl.ID})
	c.plans = append(c.plans, pl)
}

// OnInternal lands one planned op.
func (c *Core) OnInternal(ctx sim.EngineCtx, body any) {
	b, ok := body.(landBody)
	if !ok {
		return
	}
	i := -1
	for j := range c.plans {
		if c.plans[j].ID == b.ID {
			i = j
		}
	}
	if i < 0 {
		return
	}
	pl := c.plans[i]
	c.plans = slices.Delete(c.plans, i, i+1)
	apply := true
	if pl.Kind == Timeout {
		apply = ctx.Rand().Chance(0.5)
		ctx.Assert().Sometimes(apply, core.Prop(c.h.Prefix+" timed-out op applied"))
		ctx.Assert().Sometimes(!apply, core.Prop(c.h.Prefix+" timed-out op not applied"))
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
	}
	if pl.Kind != Normal {
		return
	}
	if cur, up := ctx.(sim.NodeCurrent).Current(pl.From.Node); !up || cur != pl.From {
		return // caller died; the effect stands, nobody to tell
	}
	if c.h.Finish != nil {
		comp = c.h.Finish(ctx, comp, pl)
	}
	at := pl.T2
	if at < ctx.Now() {
		at = ctx.Now()
	}
	ctx.Deliver(pl.From, at, pl.Lane, comp)
}

// StateHash folds the spec hash, outage window, recovery flag and pending
// plans in issue order.
func (c *Core) StateHash() uint64 {
	h := uint64(14695981039346656037)
	mix := func(v uint64) {
		for i := 0; i < 8; i++ {
			h = (h ^ (v >> (8 * i) & 0xff)) * 1099511628211
		}
	}
	mix(c.specStateHash())
	mix(uint64(c.until))
	if c.recovery {
		mix(1)
	} else {
		mix(0)
	}
	mix(uint64(len(c.plans)))
	for _, p := range c.plans {
		mix(p.ID)
		for i := 0; i < len(p.From.Node); i++ {
			mix(uint64(p.From.Node[i]))
		}
		for _, b := range p.From.Inc {
			mix(uint64(b))
		}
		mix(uint64(p.Act.ReqID()))
		mix(uint64(p.Kind))
		mix(uint64(p.T1))
		mix(uint64(p.T2))
		mix(p.Bits)
	}
	return h
}
