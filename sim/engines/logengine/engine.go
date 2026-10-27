// Package logengine is the sim bridge from broker CPCall to control-plane
// replica processes (CP-DSM-8). Replica apply lives in controlplane/machine;
// the shared Dynamo log and CPTruth live in sim/engines/logstore. This package
// keeps boundary faults (503, timeout, lost response, outage) and routes
// CPOffer/CPReply. Bug constants stay numeric for LogParams.Bug and
// machine.Config.Bug.
package logengine

import (
	"slices"

	"manifester/core"
	"manifester/sim"
)

// Bug selects a mutant (sim.LogParams.Bug). Each is caught by a property.
const (
	BugNone             uint8 = iota
	BugWriterOffsets          // offsets chosen by the writer instead of by apply
	BugTrimDelete             // trimmed slots are deleted, not tombstoned
	BugPerReplicaWindow       // each replica forgets keys on its own clock
	BugHoleSkip               // a replica passes an empty slot without a no-op
)

// Engine bridges CPCall to CP replica nodes via CPOffer/CPReply.
type Engine struct {
	f    sim.ControlPlaneFaults
	p    sim.LogParams
	name string

	now      sim.Time
	until    sim.Time
	recovery bool

	pending map[pendKey]*pending
	topics  []TopicSeed
}

// TopicSeed is one topic pre-created before the run for CP process folds.
type TopicSeed struct {
	Name  string
	Parts int32
}

// pendKey namespaces broker ReqIDs: each broker allocates ReqIDs from its own
// counter, so Req alone collides across brokers.
type pendKey struct {
	node core.NodeID
	req  core.ReqID
}

type pending struct {
	from     core.NodeRef
	op       core.CPOp
	to       core.NodeRef
	failover bool
	muted    bool
	deadline sim.EntryID
	done     bool
}

type deadlineEv struct {
	node core.NodeID
	req  core.ReqID
}

var _ sim.Engine = (*Engine)(nil)

// DefaultParams are scaled-down production values for a 45 s run.
func DefaultParams() sim.LogParams {
	ms := core.Millisecond
	return sim.LogParams{Enabled: true, K: 4, GroupMax: 4, Tick: 10 * ms,
		HoleAfter: 60 * ms, TakeoverAfter: 600 * ms, PutRetry: 80 * ms, Heartbeat: 250 * ms,
		PutFail: 0.01, PutLost: 0.02, ReadFail: 0.05,
		StallProb: 0.0002, CrashProb: 0.0001,
		StallDur: [2]core.Duration{800 * ms, 2500 * ms}, DownDur: [2]core.Duration{300 * ms, 1500 * ms},
		WindowSlots: 1 << 20, TrimStride: 16, CkptEvery: 24, KeepSlots: 8}
}

// New returns a CPCall bridge. Log knobs are f.Log (zero fields take DefaultParams).
func New(f sim.ControlPlaneFaults) *Engine {
	return &Engine{
		f: f, p: withDefaults(f.Log), name: "logengine",
		pending: map[pendKey]*pending{},
	}
}

func withDefaults(p sim.LogParams) sim.LogParams {
	d := DefaultParams()
	if p.K == 0 {
		p.K = d.K
	}
	if p.GroupMax == 0 {
		p.GroupMax = d.GroupMax
	}
	if p.Tick == 0 {
		p.Tick = d.Tick
	}
	if p.HoleAfter == 0 {
		p.HoleAfter = d.HoleAfter
	}
	if p.TakeoverAfter == 0 {
		p.TakeoverAfter = d.TakeoverAfter
	}
	if p.Heartbeat == 0 {
		p.Heartbeat = d.Heartbeat
	}
	if p.WindowSlots == 0 {
		p.WindowSlots = d.WindowSlots
	}
	if p.CkptEvery == 0 {
		p.CkptEvery = d.CkptEvery
	}
	if p.KeepSlots == 0 {
		p.KeepSlots = d.KeepSlots
	}
	if p.TrimStride == 0 {
		p.TrimStride = d.TrimStride
	}
	p.Enabled = true
	return p
}

// Params returns the resolved log knobs (for kafka CP process factories).
func (e *Engine) Params() sim.LogParams { return e.p }

// CreateTopic records a topic name for CP process factories (folds seed too).
func (e *Engine) CreateTopic(name string, partitions int32) core.ErrCode {
	if name == "" || partitions <= 0 {
		return core.ErrInvalid
	}
	e.topics = append(e.topics, TopicSeed{Name: name, Parts: partitions})
	return core.OK
}

// Topics returns topics created at setup for CP process factories.
func (e *Engine) Topics() []TopicSeed { return slices.Clone(e.topics) }

// Name implements sim.Engine.
func (e *Engine) Name() string { return e.name }

// Handles reports whether a is a CPCall or CPReply.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.CPCall, core.CPReply:
		return true
	}
	return false
}

// StateHash is stable for the bridge (truth lives on logstore).
func (e *Engine) StateHash() uint64 { return uint64(len(e.pending)) ^ uint64(len(e.topics)) }

// EnterRecovery stops new boundary faults.
func (e *Engine) EnterRecovery() { e.recovery = true }

// OnCrash is a no-op: replica crash is attrition on CP processes.
func (e *Engine) OnCrash(sim.EngineCtx, core.NodeRef) {}

// OnInternal handles caller-timeout deadlines.
func (e *Engine) OnInternal(ctx sim.EngineCtx, body any) {
	d, ok := body.(deadlineEv)
	if !ok {
		return
	}
	key := pendKey(d)
	p := e.pending[key]
	if p == nil || p.done {
		return
	}
	p.done = true
	p.muted = true // late CPReply must not deliver a second answer
	e.deliver(ctx, p.from, core.CPDone{Req: d.req, Err: core.ErrTimeout, Outcome: core.MaybeApplied})
}

type fault uint8

const (
	fNone fault = iota
	fUnavailable
	fTimeout
	fLost
)

// Execute starts a CPCall or lands a CPReply.
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	e.now = ctx.Now()
	switch a := a.(type) {
	case core.CPCall:
		e.execCall(ctx, from, a)
	case core.CPReply:
		e.execReply(ctx, a)
	default:
		panic("logengine: not a CP action")
	}
}

func (e *Engine) execCall(ctx sim.EngineCtx, from core.NodeRef, call core.CPCall) {
	k := e.plan(ctx)
	if k == fUnavailable {
		e.deliver(ctx, from, core.CPDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	cp, ok := e.pickCP(ctx, core.NodeRef{})
	if !ok {
		e.deliver(ctx, from, core.CPDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	key := pendKey{from.Node, call.Req}
	p := &pending{from: from, op: call.Op, to: cp, muted: k == fLost || k == fTimeout}
	to := ctx.Now() + sim.Time(max(e.f.CallerTimeout, 500*core.Millisecond))
	p.deadline = ctx.Self(to, ctx.Lane("logengine/deadline"), deadlineEv{node: from.Node, req: call.Req})
	e.pending[key] = p
	if k == fTimeout && ctx.Rand().Chance(0.5) {
		return // never reached the service; deadline answers the broker
	}
	e.offer(ctx, cp, from, call.Req, call.Op)
}

func (e *Engine) execReply(ctx sim.EngineCtx, r core.CPReply) {
	// CPReply.To is the broker (machine stamps replyTo from the offer).
	key := pendKey{r.To.Node, r.Done.Req}
	p := e.pending[key]
	if p == nil || p.done {
		return
	}
	// Standby (or booting) refuses: one failover offer to the other live CP.
	if r.Done.Err == core.ErrUnavailable && !p.failover {
		if other, ok := e.pickCP(ctx, p.to); ok {
			p.failover = true
			p.to = other
			e.offer(ctx, other, p.from, r.Done.Req, p.op)
			return
		}
	}
	p.done = true
	ctx.Cancel(p.deadline)
	delete(e.pending, key)
	if p.muted {
		return
	}
	lat := e.f.RespondLatency.Sample(ctx.Rand())
	e.deliverAt(ctx, p.from, ctx.Now()+sim.Time(lat), r.Done)
}

func (e *Engine) offer(ctx sim.EngineCtx, cp, from core.NodeRef, req core.ReqID, op core.CPOp) {
	lat := e.f.ApplyLatency.Sample(ctx.Rand())
	at := ctx.Now() + sim.Time(lat)
	ctx.Deliver(cp, at, ctx.Lane("logengine/offer/"+string(cp.Node)),
		core.CPOffer{From: from, Req: req, Op: op})
}

func (e *Engine) pickCP(ctx sim.EngineCtx, exclude core.NodeRef) (core.NodeRef, bool) {
	refs := e.liveCPs(ctx)
	for _, ref := range refs {
		if exclude.Node != "" && ref.Node == exclude.Node {
			continue
		}
		return ref, true
	}
	return core.NodeRef{}, false
}

func (e *Engine) liveCPs(ctx sim.EngineCtx) []core.NodeRef {
	cur, ok := ctx.(sim.NodeCurrent)
	if !ok {
		return nil
	}
	var out []core.NodeRef
	for _, name := range []core.NodeID{"cp-0", "cp-1"} {
		if ref, ok := cur.Current(name); ok {
			out = append(out, ref)
		}
	}
	return out
}

func (e *Engine) plan(ctx sim.EngineCtx) fault {
	if e.recovery {
		return fNone
	}
	r, now, p := ctx.Rand(), ctx.Now(), e.f
	if now >= e.until && r.Chance(p.Outage.Prob) {
		d := p.Outage.Dur[0]
		if p.Outage.Dur[1] > d {
			d = core.Duration(r.Between(int64(p.Outage.Dur[0]), int64(p.Outage.Dur[1])))
		}
		e.until = now + sim.Time(d)
		ctx.Fault("cp_outage", core.Field{Key: "dur", Value: core.I64(d)})
		ctx.Assert().Reachable("cp outage started")
	}
	out := now < e.until
	is503, isTimeout, isLost := r.Chance(p.Unavailable503), r.Chance(p.Timeout), r.Chance(p.LostResponse)
	switch {
	case out || is503:
		ctx.Fault("cp_503", core.Field{Key: "cause", Value: core.Str(cause(out))})
		ctx.Assert().Reachable("cp 503 injected")
		return fUnavailable
	case isTimeout:
		ctx.Fault("cp_timeout")
		ctx.Assert().Reachable("cp timeout injected")
		return fTimeout
	case isLost:
		ctx.Fault("cp_lost_response")
		ctx.Assert().Reachable("cp lost response injected")
		return fLost
	}
	return fNone
}

func cause(outage bool) string {
	if outage {
		return "outage"
	}
	return "coin"
}

func (e *Engine) deliver(ctx sim.EngineCtx, to core.NodeRef, c core.CPDone) {
	e.deliverAt(ctx, to, ctx.Now(), c)
}

func (e *Engine) deliverAt(ctx sim.EngineCtx, to core.NodeRef, at sim.Time, c core.CPDone) {
	ctx.Deliver(to, at, ctx.Lane("logengine/"+string(to.Node)), c)
}
