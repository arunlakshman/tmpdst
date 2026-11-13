// Package fakelogservice is the sim bridge from broker ControlPlaneCall to control-plane
// replica processes (CP-DSM-8). Replica apply lives in controlplane/machine;
// the shared Dynamo log and the real state live in sim/engines/logstore. This package
// keeps boundary faults (503, timeout, lost response, outage) and routes
// ControlPlaneDeliver/ControlPlaneReply. InjectedBug constants stay numeric for Params.Bug and
// machine.Config.InjectedBug.
//
// It is a kernel.Service. Its requests are a broker call (Call) or a reply from a
// replica process (Reply). It answers brokers with core.ControlPlaneDone, and it
// offers calls to the replica nodes with core.ControlPlaneDeliver. The host tells
// it which nodes are the replicas with UseReplicas.
package fakelogservice

import (
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/fakecompletions"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// The InjectedBug constants select a mutant (Params.Bug). Each is caught by a property.
const (
	InjectedBugNone             uint8 = iota
	InjectedBugWriterOffsets              // offsets chosen by the writer instead of by apply
	InjectedBugPerReplicaWindow uint8 = 3 // each replica forgets keys on its own clock
	InjectedBugNoFloorCheck     uint8 = 4 // a put at or below the floor is answered as a normal success
)

// Cx is the context the kernel lends to this service.
type Cx = kernel.ServiceContext[core.EventData]

// Ref names one node incarnation.
type Ref = kernel.Ref[core.EventData]

// Addr names a node, whatever incarnation is up.
type Addr = kernel.Addr[core.EventData]

// Request is a request to the bridge: a broker call or a reply from a replica
// process. The cases embed the core actions, so they fold into the trace hash
// with the bytes of the core action.
//
//sumtype:decl
type Request interface {
	machine.Message
	isRequest()
}

// The cases of Request.
type (
	// Call is a ControlPlaneCall from a broker.
	Call struct{ core.ControlPlaneCall }
	// Reply is a ControlPlaneReply from a replica process.
	Reply struct{ core.ControlPlaneReply }
)

func (Call) isRequest()  {}
func (Reply) isRequest() {}

// Params are the knobs of the simulated log control plane (docs/sim-log-engine.md).
// Latencies of a log Put and of its answer are Faults.ApplyLatency
// and RespondLatency. Slot counts (window, stride, checkpoint, keep) are scaled
// down from production (131072, 1024, ...) so a 45 s run crosses them.
type Params struct {
	Enabled bool
	// GroupMax commands per log entry.
	GroupMax int
	// Tick is the service's polling period (tail reads, role checks, flushes).
	Tick machine.Duration
	// TakeoverAfter without a slot from another writer before the standby
	// becomes active; PutRetry before an unanswered Put is repeated; Heartbeat
	// between no-op appends of an idle active node.
	TakeoverAfter, PutRetry, Heartbeat machine.Duration
	// PutFail: a Put is refused (not applied). PutLost: a Put is applied, its
	// answer is lost. ReadFail: a replica's tail read fails this tick.
	PutFail, PutLost, ReadFail float64
	// StallProb and CrashProb are per replica per tick (chaos only). A stalled
	// replica does nothing for StallDur; a crashed one loses its memory and
	// restarts from the latest checkpoint after DownDur.
	StallProb, CrashProb float64
	StallDur, DownDur    [2]machine.Duration
	// WindowSlots: a key is forgotten this many slots after its first slot,
	// evaluated every TrimStride slots. CheckpointEvery slots between checkpoints;
	// KeepSlots below a checkpoint stay untrimmed.
	WindowSlots, TrimStride, CheckpointEvery, KeepSlots int
	// Bug selects a mutant of the log service (InjectedBug*); 0 = none.
	Bug uint8
}

// Engine bridges ControlPlaneCall to CP replica nodes via ControlPlaneDeliver/ControlPlaneReply.
type Engine struct {
	f     fakecontrolplane.Faults
	p     Params
	lanes *kernel.Lanes[laneKey]

	replicas []Addr // the replica nodes, in the order the bridge tries them
	until    kernel.Time
	recovery bool

	pending map[pendKey]*pending
	topics  []TopicSeed
}

var _ kernel.Service[Request, core.EventData] = (*Engine)(nil)

// TopicSeed is one topic pre-created before the run for CP process folds.
type TopicSeed struct {
	Name  string
	Parts int32
}

// pendKey namespaces broker ReqIDs: each broker allocates ReqIDs from its own
// counter, so Req alone collides across brokers.
type pendKey struct {
	node    machine.NodeID
	request machine.RequestID
}

type pending struct {
	from      Ref
	operation contract.Operation
	to        Ref
	failover  bool
	muted     bool
	deadline  kernel.EntryID
	done      bool
}

// laneKind tells the lane keys apart. The old string keys were
// "fakelogservice/deadline", "fakelogservice/offer/<node>" and "fakelogservice/<node>".
type laneKind uint8

const (
	laneDeadline laneKind = iota
	laneOffer
	laneReply
)

// laneKey is the typed key of a lane. node is empty for laneDeadline.
type laneKey struct {
	kind laneKind
	node machine.NodeID
}

// DefaultParams are scaled-down production values for a 45 s run.
func DefaultParams() Params {
	milliseconds := machine.Millisecond
	return Params{Enabled: true, GroupMax: 4, Tick: 10 * milliseconds,
		TakeoverAfter: 600 * milliseconds, PutRetry: 80 * milliseconds, Heartbeat: 250 * milliseconds,
		PutFail: 0.01, PutLost: 0.02, ReadFail: 0.05,
		StallProb: 0.0002, CrashProb: 0.0001,
		StallDur: [2]machine.Duration{800 * milliseconds, 2500 * milliseconds}, DownDur: [2]machine.Duration{300 * milliseconds, 1500 * milliseconds},
		WindowSlots: 1 << 20, TrimStride: 16, CheckpointEvery: 24, KeepSlots: 8}
}

// New returns a ControlPlaneCall bridge. Log knobs are p (zero fields take DefaultParams).
// The world gives it its lanes. The boundary faults are f.
func New(w *kernel.World, f fakecontrolplane.Faults, p Params) *Engine {
	return &Engine{
		f: f, p: withDefaults(p), lanes: kernel.NewLanes[laneKey](w),
		pending: map[pendKey]*pending{},
	}
}

func withDefaults(p Params) Params {
	d := DefaultParams()
	if p.GroupMax == 0 {
		p.GroupMax = d.GroupMax
	}
	if p.Tick == 0 {
		p.Tick = d.Tick
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

// UseReplicas tells the bridge which nodes are the control plane replicas. It
// tries them in this order (the old bridge looked for the names cp-0 and cp-1).
func (e *Engine) UseReplicas(replicas ...Addr) { e.replicas = slices.Clone(replicas) }

// Params returns the resolved log knobs (for kafka CP process factories).
func (e *Engine) Params() Params { return e.p }

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

// Name implements kernel.Service.
func (e *Engine) Name() string { return "fakelogservice" }

// StateHash is stable for the bridge (truth lives on logstore).
func (e *Engine) StateHash() uint64 { return uint64(len(e.pending)) ^ uint64(len(e.topics)) }

// EnterRecovery stops new boundary faults.
func (e *Engine) EnterRecovery() { e.recovery = true }

// OnCrash is a no-op: replica crash is attrition on CP processes.
func (e *Engine) OnCrash(*Cx, machine.NodeInstance) {}

// onDeadline handles a caller-timeout deadline.
func (e *Engine) onDeadline(cx *Cx, key pendKey) {
	p := e.pending[key]
	if p == nil || p.done {
		return
	}
	p.done = true
	p.muted = true // late ControlPlaneReply must not deliver a second answer
	delete(e.pending, key)
	e.deliver(cx, p.from, core.ControlPlaneDone{Req: key.request, Err: core.ErrTimeout, Outcome: core.MaybeApplied})
}

// Handle starts a ControlPlaneCall or lands a ControlPlaneReply. It implements kernel.Service.
func (e *Engine) Handle(cx *Cx, from Ref, req Request) {
	switch x := req.(type) {
	case Call:
		e.execCall(cx, from, x.ControlPlaneCall)
	case Reply:
		e.execReply(cx, x.ControlPlaneReply)
	}
}

func (e *Engine) execCall(cx *Cx, from Ref, call core.ControlPlaneCall) {
	k := e.plan(cx)
	if k == fakecompletions.ReplyUnavailable {
		e.deliver(cx, from, core.ControlPlaneDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	cp, ok := e.pickCP(cx, "")
	if !ok {
		e.deliver(cx, from, core.ControlPlaneDone{Req: call.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied})
		return
	}
	key := pendKey{from.Addr().ID(), call.Req}
	p := &pending{from: from, operation: call.Op, to: cp, muted: k == fakecompletions.ReplyLost || k == fakecompletions.ReplyTimeout}
	to := cx.Now() + kernel.Time(max(e.f.CallerTimeout, 500*machine.Millisecond))
	p.deadline = cx.Later(to, e.lanes.Get(laneKey{laneDeadline, ""}), func(cx *Cx) { e.onDeadline(cx, key) })
	e.pending[key] = p
	if k == fakecompletions.ReplyTimeout && cx.Rand().Chance(0.5) {
		return // never reached the service; deadline answers the broker
	}
	e.offer(cx, cp, from, call.Req, call.Op)
}

func (e *Engine) execReply(cx *Cx, r core.ControlPlaneReply) {
	// ControlPlaneReply.To is the broker (machine stamps replyTo from the offer).
	key := pendKey{r.To.Node, r.Done.Req}
	p := e.pending[key]
	if p == nil || p.done {
		return
	}
	// Standby (or booting) refuses: one failover offer to the other live CP.
	if r.Done.Err == core.ErrUnavailable && !p.failover {
		if other, ok := e.pickCP(cx, p.to.Addr().ID()); ok {
			p.failover = true
			p.to = other
			e.offer(cx, other, p.from, r.Done.Req, p.operation)
			return
		}
	}
	p.done = true
	cx.Cancel(p.deadline)
	delete(e.pending, key)
	if p.muted {
		// Lost/timeout fault: never forward the service reply. Answer the broker
		// now (same ErrTimeout+MaybeApplied the deadline would have sent) so a
		// cancelled deadline cannot leave the commit hung.
		e.deliver(cx, p.from, core.ControlPlaneDone{
			Req: r.Done.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied,
		})
		return
	}
	lat := e.f.RespondLatency.Sample(cx.Rand())
	e.deliverAt(cx, p.from, cx.Now()+kernel.Time(lat), r.Done)
}

func (e *Engine) offer(cx *Cx, cp, from Ref, request machine.RequestID, operation contract.Operation) {
	lat := e.f.ApplyLatency.Sample(cx.Rand())
	at := cx.Now() + kernel.Time(lat)
	cx.Deliver(cp, at, e.lanes.Get(laneKey{laneOffer, cp.Addr().ID()}),
		core.ControlPlaneDeliver{From: from.Instance(), Req: request, Op: operation})
}

// pickCP returns the first live replica whose name is not exclude (an empty
// exclude excludes nothing).
func (e *Engine) pickCP(cx *Cx, exclude machine.NodeID) (Ref, bool) {
	for _, a := range e.replicas {
		ref, ok := cx.Current(a)
		if !ok {
			continue
		}
		if exclude != "" && ref.Addr().ID() == exclude {
			continue
		}
		return ref, true
	}
	return Ref{}, false
}

func (e *Engine) plan(cx *Cx) fakecompletions.Kind {
	if e.recovery {
		return fakecompletions.ReplyNormal
	}
	p := fakecompletions.Params{P503: e.f.Unavailable503, PTimeout: e.f.Timeout, PLost: e.f.LostResponse, Outage: e.f.Outage}
	return p.Draw(cx, "cp", &e.until)
}

func (e *Engine) deliver(cx *Cx, to Ref, c core.ControlPlaneDone) {
	e.deliverAt(cx, to, cx.Now(), c)
}

func (e *Engine) deliverAt(cx *Cx, to Ref, at kernel.Time, c core.ControlPlaneDone) {
	cx.Deliver(to, at, e.lanes.Get(laneKey{laneReply, to.Addr().ID()}), c)
}
