package sim

import "manifester/core"

// Engine simulates one external system (object store, control plane, network).
// It owns that system's state, draws only from the run's CountedRand, and never
// completes anything synchronously: every result is a scheduled entry.
type Engine interface {
	Name() string
	// Handles reports whether this engine executes the given action type.
	Handles(a core.Action) bool
	// Execute starts an action issued by node `from`. Typical object-store PUT:
	// schedule "apply" at t1 and "respond" at t2, or drop the response.
	Execute(ctx EngineCtx, from core.NodeRef, a core.Action)
	// OnInternal consumes an entry this engine scheduled for itself.
	OnInternal(ctx EngineCtx, body any)
	// OnCrash is told that an incarnation died; effects already applied stand.
	OnCrash(ctx EngineCtx, ref core.NodeRef)
	// EnterRecovery stops generating new faults; existing damage is preserved.
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

// ObjectStoreFaults are the fault families of the fake object store (decision D).
type ObjectStoreFaults struct {
	Latency        TwoSpeed
	Unavailable503 float64 // refused before admission: NotApplied
	Timeout        float64 // caller gives up; the op may or may not apply: MaybeApplied
	LostResponse   float64 // applied, response never arrives (surfaces as timeout)
	CallerTimeout  core.Duration
}

// ControlPlaneFaults are the fault families of the fake control plane.
type ControlPlaneFaults struct {
	Latency        TwoSpeed
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64 // commit applied, response lost: retry must be idempotent
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
