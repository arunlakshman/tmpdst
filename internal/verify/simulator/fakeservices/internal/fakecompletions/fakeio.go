// Package fakecompletions is the shared plan/land/deliver core of the fake object store
// and control plane engines: faults + timing around a speccheck spec.
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
package fakecompletions

import (
	"slices"

	"manifester/internal/core"
	"manifester/internal/verify/simulator"
)

// Params is the fault and timing config common to both engines.
type Params struct {
	Apply, Respond simulator.FastWithRareSlow
	P503           float64
	PTimeout       float64
	PLost          float64
	Outage         simulator.ServiceOutage
	CallerTimeout  core.Duration
}

// CompletionHooks is what an engine plugs into the core.
type CompletionHooks struct {
	Name   string // engine name, also the lane namespace
	Prefix string // fault name prefix: "obj" or "cp"
	Params Params
	// Spec executes one action on the spec state.
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
	Extra func(r simulator.CountedRand, a core.IOCall) (bool, uint64)
	// Finish rewrites the response of a normally answered op (corruption).
	Finish func(ctx simulator.FakeServiceContext, c core.IOResult, p CompletionPlan) core.IOResult
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

// CompletionPlan is one in-flight op.
type CompletionPlan struct {
	ID     uint64
	From   core.NodeInstance
	Act    core.IOCall
	Kind   Kind
	T1, T2 simulator.Time
	Flag   bool // Extra's bool
	Bits   uint64
	Lane   simulator.DeliveryLane
}

type landBody struct{ ID uint64 }

// CompletionCore is the engine state besides the spec.
type CompletionCore struct {
	h        CompletionHooks
	nextID   uint64
	plans    []CompletionPlan // sorted by ID (append order)
	until    simulator.Time
	recovery bool
	gen      uint64
	// specHash caches h.SpecHash(). It is valid only while specOK; every path
	// that can change the spec clears specOK (landing a mutating op, Bump).
	specHash uint64
	specOK   bool
}

// New returns a CompletionCore.
func New(h CompletionHooks) *CompletionCore { return &CompletionCore{h: h} }

// Gen is the applied-change counter; Bump adds an out-of-band change.
func (c *CompletionCore) Gen() uint64 { return c.gen }

// Bump records an out-of-band spec change (and invalidates the spec hash cache).
func (c *CompletionCore) Bump() { c.gen++; c.specOK = false }

// specStateHash is the spec's StateHash, recomputed only after a mutation.
func (c *CompletionCore) specStateHash() uint64 {
	if !c.specOK {
		c.specHash, c.specOK = c.h.SpecHash(), true
	}
	return c.specHash
}

// SpecStateHash is the cached spec hash, exported so conformance (C2) can
// compare it with the spec's own StateHash (a stale cache shows up as a diff).
func (c *CompletionCore) SpecStateHash() uint64 { return c.specStateHash() }

// Name returns the hooks name.
func (c *CompletionCore) Name() string { return c.h.Name }

// EnterRecovery stops new faults for later actions.
func (c *CompletionCore) EnterRecovery() { c.recovery = true }

// OnCrash does nothing: effects stand and landings stay scheduled; deliveries to the dead
// incarnation are dropped by the scheduler or skipped at landing.
func (c *CompletionCore) OnCrash(simulator.FakeServiceContext, core.NodeInstance) {}

// Draw draws the boundary fault coins (outage, 503, timeout, lost, in that
// order) and emits the matching fault named pre+"_...". until is the end of
// the outage window and is extended when one starts. fields lead the
// 503/timeout/lost fault fields.
func (p Params) Draw(ctx simulator.FakeServiceContext, pre string, until *simulator.Time, fields ...core.Field) Kind {
	r, now := ctx.Rand(), ctx.Now()
	if now >= *until && r.Chance(p.Outage.Prob) {
		d := p.Outage.Dur[0]
		if p.Outage.Dur[1] > d {
			d = core.Duration(r.Between(int64(p.Outage.Dur[0]), int64(p.Outage.Dur[1])))
		}
		*until = now + simulator.Time(d)
		ctx.Fault(pre+"_outage", core.Field{Key: "dur", Value: core.Int64Value(d)})
		ctx.Assert().Reachable(core.PropertyID(pre + " outage started"))
	}
	inOutage := now < *until
	is503, isTimeout, isLost := r.Chance(p.P503), r.Chance(p.PTimeout), r.Chance(p.PLost)
	switch {
	case inOutage || is503:
		cause := "coin"
		if inOutage {
			cause = "outage"
		}
		ctx.Fault(pre+"_503", append(fields, core.Field{Key: "cause", Value: core.StringValue(cause)})...)
		ctx.Assert().Reachable(core.PropertyID(pre + " 503 injected"))
		return ReplyUnavailable
	case isTimeout:
		ctx.Fault(pre+"_timeout", fields...)
		ctx.Assert().Reachable(core.PropertyID(pre + " timeout injected"))
		return ReplyTimeout
	case isLost:
		ctx.Fault(pre+"_lost_response", fields...)
		ctx.Assert().Reachable(core.PropertyID(pre + " lost response injected"))
		return ReplyLost
	}
	return ReplyNormal
}

// Execute plans an op. Draw order: fault coins (Draw), apply latency, respond
// latency, extra.
func (c *CompletionCore) Execute(ctx simulator.FakeServiceContext, from core.NodeInstance, a core.IOCall) {
	r, now, p := ctx.Rand(), ctx.Now(), c.h.Params
	req := core.Field{Key: "req", Value: core.Uint64Value(a.RequestID())}
	kind := ReplyNormal
	if !c.recovery {
		kind = p.Draw(ctx, c.h.Prefix, &c.until, req)
	}
	t1 := now + simulator.Time(p.Apply.Sample(r))
	t2 := t1 + simulator.Time(p.Respond.Sample(r))
	var flag bool
	var bits uint64
	if !c.recovery && c.h.Extra != nil {
		flag, bits = c.h.Extra(r, a)
	}

	c.nextID++
	pl := CompletionPlan{ID: c.nextID, From: from, Act: a, Kind: kind, T1: t1, T2: t2, Flag: flag, Bits: bits, Lane: ctx.Lane(c.h.Name + "/" + string(from.Node))}
	switch kind {
	case ReplyUnavailable:
		ctx.Deliver(from, t2, pl.Lane, c.h.Fail(a, core.ErrUnavailable, core.NotApplied))
		return
	case ReplyTimeout, ReplyLost:
		ctx.Deliver(from, now+simulator.Time(p.CallerTimeout), pl.Lane, c.h.Fail(a, core.ErrTimeout, core.MaybeApplied))
		if !c.h.Mutating(a) {
			return // nothing to apply, nothing to answer
		}
	}
	ctx.Self(t1, pl.Lane, landBody{ID: pl.ID})
	c.plans = append(c.plans, pl)
}

// OnInternal lands one planned op.
func (c *CompletionCore) OnInternal(ctx simulator.FakeServiceContext, body any) {
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
	if pl.Kind == ReplyTimeout {
		apply = ctx.Rand().Chance(0.5)
		ctx.Assert().Sometimes(apply, core.PropertyID(c.h.Prefix+" timed-out op applied"))
		ctx.Assert().Sometimes(!apply, core.PropertyID(c.h.Prefix+" timed-out op not applied"))
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
	if cur, up := ctx.(simulator.NodeCurrent).Current(pl.From.Node); !up || cur != pl.From {
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
func (c *CompletionCore) StateHash() uint64 {
	h := core.NewHasher()
	mix := h.Uint64Value
	mix(c.specStateHash())
	mix(uint64(c.until))
	h.Bool(c.recovery)
	mix(uint64(len(c.plans)))
	for _, p := range c.plans {
		mix(p.ID)
		for i := range len(p.From.Node) {
			mix(uint64(p.From.Node[i]))
		}
		for _, b := range p.From.Inc {
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
