// Package fakelogservice is the sim bridge from broker ControlPlaneCall to control-plane
// replica processes (CP-DSM-8). Replica apply lives in controlplane/machine;
// the shared Dynamo log and ControlPlaneRealState live in sim/engines/logstore. This package
// keeps boundary faults (503, timeout, lost response, outage) and routes
// ControlPlaneDeliver/ControlPlaneReply. InjectedBug constants stay numeric for LogParams.Bug and
// machine.Config.InjectedBug.
package fakelogservice

import (
	"slices"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/internal/fakecompletions"
)

// The InjectedBug constants select a mutant (simulator.LogParams.Bug). Each is caught by a property.
const (
	InjectedBugNone             uint8 = iota
	InjectedBugWriterOffsets          // offsets chosen by the writer instead of by apply
	InjectedBugTrimDelete             // trimmed slots are deleted, not tombstoned
	InjectedBugPerReplicaWindow       // each replica forgets keys on its own clock
	InjectedBugGapSkip                // a replica passes an empty slot without a no-op
)

// Engine bridges ControlPlaneCall to CP replica nodes via ControlPlaneDeliver/ControlPlaneReply.
type Engine struct {
	f    simulator.ControlPlaneFaults
	p    simulator.LogParams
	name string

	now      simulator.Time
	until    simulator.Time
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
	req  core.RequestID
}

type pending struct {
	from     core.NodeInstance
	op       core.ControlPlaneOperation
	to       core.NodeInstance
	failover bool
	muted    bool
	deadline simulator.EntryID
	done     bool
}

type deadlineEv struct {
	node core.NodeID
	req  core.RequestID
}

var _ simulator.FakeService = (*Engine)(nil)

// DefaultParams are scaled-down production values for a 45 s run.
func DefaultParams() simulator.LogParams {
	ms := core.Millisecond
	return simulator.LogParams{Enabled: true, K: 4, GroupMax: 4, Tick: 10 * ms,
		GapAfter: 60 * ms, TakeoverAfter: 600 * ms, PutRetry: 80 * ms, Heartbeat: 250 * ms,
		PutFail: 0.01, PutLost: 0.02, ReadFail: 0.05,
		StallProb: 0.0002, CrashProb: 0.0001,
		StallDur: [2]core.Duration{800 * ms, 2500 * ms}, DownDur: [2]core.Duration{300 * ms, 1500 * ms},
		WindowSlots: 1 << 20, TrimStride: 16, CheckpointEvery: 24, KeepSlots: 8}
}

// New returns a ControlPlaneCall bridge. Log knobs are f.Log (zero fields take DefaultParams).
func New(f simulator.ControlPlaneFaults) *Engine {
	return &Engine{
		f: f, p: withDefaults(f.Log), name: "fakelogservice",
		pending: map[pendKey]*pending{},
	}
}

func withDefaults(p simulator.LogParams) simulator.LogParams {
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
	if p.GapAfter == 0 {
		p.GapAfter = d.GapAfter
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
	if p.CheckpointEvery == 0 {
		p.CheckpointEvery = d.CheckpointEvery
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
func (e *Engine) Params() simulator.LogParams { return e.p }

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

// Name implements simulator.Engine.
func (e *Engine) Name() string { return e.name }

// Handles reports whether a is a ControlPlaneCall or ControlPlaneReply.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.ControlPlaneCall, core.ControlPlaneReply:
		return true
	}
	return false
}

// StateHash is stable for the bridge (truth lives on logstore).
func (e *Engine) StateHash() uint64 { return uint64(len(e.pending)) ^ uint64(len(e.topics)) }

// EnterRecovery stops new boundary faults.
func (e *Engine) EnterRecovery() { e.recovery = true }

// OnCrash is a no-op: replica crash is attrition on CP processes.
func (e *Engine) OnCrash(simulator.FakeServiceContext, core.NodeInstance) {}

// OnInternal handles caller-timeout deadlines.
func (e *Engine) OnInternal(ctx simulator.FakeServiceContext, body any) {
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
	p.muted = true // late ControlPlaneReply must not deliver a second answer
	delete(e.pending, key)
	e.deliver(ctx, p.from, core.ControlPlaneDone{Req: d.req, Err: core.ErrTimeout, Outcome: core.MaybeApplied})
}

// Execute starts a ControlPlaneCall or lands a ControlPlaneReply.
func (e *Engine) Execute(ctx simulator.FakeServiceContext, from core.NodeInstance, a core.Action) {
	e.now = ctx.Now()
	switch a := a.(type) {
	case core.ControlPlaneCall:
		e.execCall(ctx, from, a)
	case core.ControlPlaneReply:
		e.execReply(ctx, a)
	default:
		panic("fakelogservice: not a CP action")
	}
}

func (e *Engine) execCall(ctx simulator.FakeServiceContext, from core.NodeInstance, call core.ControlPlaneCall) {
	k := e.plan(ctx)
	if k == fakecompletions.ReplyUnavailable {
		e.deliver(ctx, from, core.ControlPlaneDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	cp, ok := e.pickCP(ctx, core.NodeInstance{})
	if !ok {
		e.deliver(ctx, from, core.ControlPlaneDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	key := pendKey{from.Node, call.Req}
	p := &pending{from: from, op: call.Op, to: cp, muted: k == fakecompletions.ReplyLost || k == fakecompletions.ReplyTimeout}
	to := ctx.Now() + simulator.Time(max(e.f.CallerTimeout, 500*core.Millisecond))
	p.deadline = ctx.Self(to, ctx.Lane("fakelogservice/deadline"), deadlineEv{node: from.Node, req: call.Req})
	e.pending[key] = p
	if k == fakecompletions.ReplyTimeout && ctx.Rand().Chance(0.5) {
		return // never reached the service; deadline answers the broker
	}
	e.offer(ctx, cp, from, call.Req, call.Op)
}

func (e *Engine) execReply(ctx simulator.FakeServiceContext, r core.ControlPlaneReply) {
	// ControlPlaneReply.To is the broker (machine stamps replyTo from the offer).
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
		// Lost/timeout fault: never forward the service reply. Answer the broker
		// now (same ErrTimeout+MaybeApplied the deadline would have sent) so a
		// cancelled deadline cannot leave the commit hung.
		e.deliver(ctx, p.from, core.ControlPlaneDone{
			Req: r.Done.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied,
		})
		return
	}
	lat := e.f.RespondLatency.Sample(ctx.Rand())
	e.deliverAt(ctx, p.from, ctx.Now()+simulator.Time(lat), r.Done)
}

func (e *Engine) offer(ctx simulator.FakeServiceContext, cp, from core.NodeInstance, req core.RequestID, op core.ControlPlaneOperation) {
	lat := e.f.ApplyLatency.Sample(ctx.Rand())
	at := ctx.Now() + simulator.Time(lat)
	ctx.Deliver(cp, at, ctx.Lane("fakelogservice/offer/"+string(cp.Node)),
		core.ControlPlaneDeliver{From: from, Req: req, Op: op})
}

func (e *Engine) pickCP(ctx simulator.FakeServiceContext, exclude core.NodeInstance) (core.NodeInstance, bool) {
	refs := e.liveCPs(ctx)
	for _, ref := range refs {
		if exclude.Node != "" && ref.Node == exclude.Node {
			continue
		}
		return ref, true
	}
	return core.NodeInstance{}, false
}

func (e *Engine) liveCPs(ctx simulator.FakeServiceContext) []core.NodeInstance {
	cur, ok := ctx.(simulator.NodeCurrent)
	if !ok {
		return nil
	}
	var out []core.NodeInstance
	for _, name := range []core.NodeID{"cp-0", "cp-1"} {
		if ref, ok := cur.Current(name); ok {
			out = append(out, ref)
		}
	}
	return out
}

func (e *Engine) plan(ctx simulator.FakeServiceContext) fakecompletions.Kind {
	if e.recovery {
		return fakecompletions.ReplyNormal
	}
	p := fakecompletions.Params{P503: e.f.Unavailable503, PTimeout: e.f.Timeout, PLost: e.f.LostResponse, Outage: e.f.Outage}
	return p.Draw(ctx, "cp", &e.until)
}

func (e *Engine) deliver(ctx simulator.FakeServiceContext, to core.NodeInstance, c core.ControlPlaneDone) {
	e.deliverAt(ctx, to, ctx.Now(), c)
}

func (e *Engine) deliverAt(ctx simulator.FakeServiceContext, to core.NodeInstance, at simulator.Time, c core.ControlPlaneDone) {
	ctx.Deliver(to, at, ctx.Lane("fakelogservice/"+string(to.Node)), c)
}
