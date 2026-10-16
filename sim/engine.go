package sim

import "manifester/core"

// Engine simulates one external system (object store, control plane, network).
// It owns that system's state, draws only from the run's CountedRand, and never
// completes anything synchronously: every result is a scheduled entry.
//
// Every fault coin and delay is drawn in Execute, at issue time, so draws
// happen in issue order. How a fault resolves (applied or not) may be drawn
// when it lands.
type Engine interface {
	core.Hashable // folded into the trace hash after every step
	Name() string
	// Handles reports whether this engine executes the given action type. The
	// simulator routes every action except Emit, timers, Trip and Exit to
	// exactly one engine. Not only IOActions: the network engine also takes
	// CloseConn, PauseReads and ResumeReads, which have no completion.
	Handles(a core.Action) bool
	// Execute starts an action issued by node `from`. Typical object-store PUT:
	// schedule "apply" at t1 and "respond" at t2, or drop the response.
	Execute(ctx EngineCtx, from core.NodeRef, a core.Action)
	// OnInternal consumes an entry this engine scheduled for itself.
	OnInternal(ctx EngineCtx, body any)
	// OnCrash is told that an incarnation died; effects already applied stand.
	// DropNode removes only entries addressed to the node: engine-internal
	// apply entries survive, so a PUT or commit already on the wire can land
	// after the broker dies.
	OnCrash(ctx EngineCtx, ref core.NodeRef)
	// EnterRecovery stops generating new faults for actions issued after it;
	// plans already in flight stand and existing damage is preserved.
	EnterRecovery()
}

// EngineCtx is what the simulator lends an engine while it runs.
type EngineCtx interface {
	Now() Time
	Rand() CountedRand
	// Deliver schedules a completion or inbound event to a node.
	Deliver(to core.NodeRef, at Time, lane Lane, body core.EventBody) EntryID
	// Self schedules an internal entry back to this engine.
	Self(at Time, lane Lane, body any) EntryID
	Cancel(id EntryID)
	// Fault records an injected fault on the timeline as a "sim_fault" fact.
	Fault(kind string, fields ...core.Field)
	// Assert records fault coverage ("lost CP response happened").
	Assert() core.Assertions
	// Current returns the live incarnation of a node, ok=false while it is
	// down. The network engine uses it to route Dial and deliver ConnOpened.
	Current(node core.NodeID) (ref core.NodeRef, ok bool)
}

// Latency is a sampled delay distribution.
type Latency interface {
	Sample(r CountedRand) core.Duration
}

// TwoSpeed is mostly fast with a rare slow tail (decision D).
type TwoSpeed struct {
	FastMin, FastMax core.Duration
	SlowMin, SlowMax core.Duration
	SlowProb         float64
}

// Outage is a correlated, sustained failure window: every op fails while it
// lasts.
type Outage struct {
	Prob float64
	Dur  [2]core.Duration
}

// ObjectStoreFaults are the fault families of the fake object store (decision D).
type ObjectStoreFaults struct {
	ApplyLatency   TwoSpeed // issue to effect
	RespondLatency TwoSpeed // effect to reply
	Unavailable503 float64  // refused before admission: NotApplied
	Timeout        float64  // caller gives up; the op may or may not apply: MaybeApplied
	LostResponse   float64  // applied, response never arrives (surfaces as timeout)
	GetCorruptProb float64  // flip one bit in ObjGetDone.Data; the stored object stays intact
	Outage         Outage
	CallerTimeout  core.Duration
}

// ControlPlaneFaults are the fault families of the fake control plane.
type ControlPlaneFaults struct {
	ApplyLatency   TwoSpeed
	RespondLatency TwoSpeed
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64 // commit applied, response lost: retry must be idempotent
	Outage         Outage
	CallerTimeout  core.Duration
}

// NetworkFaults are connection-level faults (lesson 6).
type NetworkFaults struct {
	Latency         TwoSpeed
	PartialIOMax    int     // max bytes per delivered chunk / accepted write
	PartitionProb   float64 // sustained, correlated; freezes in-flight bytes
	PartitionDur    [2]core.Duration
	ClogProb        float64
	ClogDur         [2]core.Duration
	RandomCloseProb float64
	BlackHoleProb   float64
	PairLatencyMax  core.Duration // per-pair permanent extra latency, sampled once
	ClockSkewMax    core.Duration // per-node skew applied to Event.At
}
