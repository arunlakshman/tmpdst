package sim

import "manifester/internal/core"

// FakeService simulates one external system (object store, control plane, network).
// It owns that system's state, draws only from the run's CountedRand, and never
// completes anything synchronously: every result is a scheduled entry.
//
// Every fault coin and delay is drawn in Execute, at issue time, so draws
// happen in issue order. How a fault resolves (applied or not) may be drawn
// when it lands.
type FakeService interface {
	core.HasStateHash // folded into the trace hash after every step
	Name() string
	// Handles reports whether this engine executes the given action type. The
	// simulator routes every action except RecordFact, timers, StopForRestart and ShutdownDone to
	// exactly one engine. Not only IOCalls: the network engine also takes
	// CloseConn, PauseReads and ResumeReads, which have no completion.
	Handles(a core.Action) bool
	// Execute starts an action issued by node `from`. Typical object-store PUT:
	// schedule "apply" at t1 and "respond" at t2, or drop the response.
	Execute(ctx FakeServiceContext, from core.NodeInstance, a core.Action)
	// OnInternal consumes an entry this engine scheduled for itself.
	OnInternal(ctx FakeServiceContext, body any)
	// OnCrash is told that an incarnation died; effects already applied stand.
	// DropNode removes only entries addressed to the node: engine-internal
	// apply entries survive, so a PUT or commit already on the wire can land
	// after the broker dies.
	OnCrash(ctx FakeServiceContext, ref core.NodeInstance)
	// EnterRecovery stops generating new faults for actions issued after it;
	// plans already in flight stand and existing damage is preserved.
	EnterRecovery()
}

// FakeServiceContext is what the simulator lends an engine while it runs.
type FakeServiceContext interface {
	Now() Time
	Rand() CountedRand
	// Lane returns the ordering lane for key, allocated by the kernel on first
	// use (sequentially, no randomness) from a range disjoint from the lanes
	// the kernel reserves for itself. The same key yields the same lane for the
	// whole run; distinct keys never share one, so FIFO ordering (decision C)
	// holds exactly within a key. Engines obtain every lane here, never
	// construct a Lane value. Key convention: "<engine>/<whatever>".
	Lane(key string) DeliveryLane
	// Deliver schedules a completion or inbound event to a node.
	Deliver(to core.NodeInstance, at Time, lane DeliveryLane, body core.EventData) EntryID
	// Self schedules an internal entry back to this engine.
	Self(at Time, lane DeliveryLane, body any) EntryID
	Cancel(id EntryID)
	// Fault records an injected fault on the timeline as a "sim_fault" fact.
	Fault(kind string, fields ...core.Field)
	// Assert records fault coverage ("lost CP response happened").
	Assert() core.Assertions
}

// NodeCurrent is the live-incarnation lookup split from FakeServiceContext so that
// interface stays within eight methods. Every context the kernel lends
// implements it; engines type-assert.
type NodeCurrent interface {
	// Current returns the live incarnation of a node, ok=false while it is
	// down. The network engine uses it to route Connect and deliver ConnOpened.
	Current(node core.NodeID) (ref core.NodeInstance, ok bool)
}

// Latency is a sampled delay distribution.
type Latency interface {
	Sample(r CountedRand) core.Duration
}

// FastWithRareSlow is mostly fast with a rare slow tail (decision D).
type FastWithRareSlow struct {
	FastMin, FastMax core.Duration
	SlowMin, SlowMax core.Duration
	SlowProb         float64
}

// ServiceOutage is a correlated, sustained failure window: every op fails while it
// lasts.
type ServiceOutage struct {
	Prob float64
	Dur  [2]core.Duration
}

// ObjectStoreFaults are the fault families of the fake object store (decision D).
type ObjectStoreFaults struct {
	ApplyLatency   FastWithRareSlow // issue to effect
	RespondLatency FastWithRareSlow // effect to reply
	Unavailable503 float64          // refused before admission: NotApplied
	Timeout        float64          // caller gives up; the op may or may not apply: MaybeApplied
	LostResponse   float64          // applied, response never arrives (surfaces as timeout)
	GetCorruptProb float64          // flip one bit in ObjectGetDone.Data; the stored object stays intact
	Outage         ServiceOutage
	CallerTimeout  core.Duration
}

// ControlPlaneFaults are the fault families of the fake control plane.
type ControlPlaneFaults struct {
	ApplyLatency   FastWithRareSlow
	RespondLatency FastWithRareSlow
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64 // commit applied, response lost: retry must be idempotent
	Outage         ServiceOutage
	CallerTimeout  core.Duration
	// MaxCommitItems models a store that commits a segment in one bounded
	// transaction (DynamoDB: DynamoMaxCommitItems). A CommitSegment that needs
	// more items is refused with ErrTooLarge and changes nothing. The item
	// count is an upper bound: 1 (the segment) + distinct partitions with at
	// least one batch + topics the commit grows. 0 = no limit.
	MaxCommitItems int
	// Log, when Enabled, replaces the commit model by the log-backed control
	// plane (sim/engines/logengine): the same fault families above apply at the
	// service boundary (503, timeout, lost response, outage) and Log adds the
	// store-side ones. MaxCommitItems is ignored there: the log has no item limit.
	Log LogParams
}

// LogParams are the knobs of the simulated log control plane (docs/sim-log-engine.md).
// Latencies of a log Put and of its answer are ControlPlaneFaults.ApplyLatency
// and RespondLatency. Slot counts (window, stride, checkpoint, keep) are scaled
// down from production (131072, 1024, ...) so a 45 s run crosses them.
type LogParams struct {
	Enabled bool
	// K appends in flight per writer; GroupMax commands per log entry.
	K, GroupMax int
	// Tick is the engine's polling period (tail reads, role checks, flushes).
	Tick core.Duration
	// HoleAfter before an empty slot below the tail is filled with a no-op;
	// TakeoverAfter without a slot from another writer before the standby
	// becomes active; PutRetry before an unanswered Put is repeated; Heartbeat
	// between no-op appends of an idle active node.
	HoleAfter, TakeoverAfter, PutRetry, Heartbeat core.Duration
	// PutFail: a Put is refused (not applied). PutLost: a Put is applied, its
	// answer is lost. ReadFail: a replica's tail read fails this tick.
	PutFail, PutLost, ReadFail float64
	// StallProb and CrashProb are per replica per tick (chaos only). A stalled
	// replica does nothing for StallDur; a crashed one loses its memory and
	// restarts from the latest checkpoint after DownDur.
	StallProb, CrashProb float64
	StallDur, DownDur    [2]core.Duration
	// WindowSlots: a key is forgotten this many slots after its first slot,
	// evaluated every TrimStride slots. CkptEvery slots between checkpoints;
	// KeepSlots below a checkpoint stay untrimmed.
	WindowSlots, TrimStride, CkptEvery, KeepSlots int
	// Bug selects a mutant of the log engine (logengine.Bug*); 0 = none.
	Bug uint8
}

// DynamoMaxCommitItems is DynamoDB's TransactWriteItems limit, the value the
// Kafka preset uses. It models the item cap of the removed transactional
// store (decision 32), a limit the log control plane does not have.
const DynamoMaxCommitItems = 100

// LogStoreFaults are the fault families of the fake Dynamo loglet engine
// (sim/engines/logstore). Enabled gates construction in scenario Engines funcs.
// Same issue-time draw rules as ObjectStoreFaults (decision 14).
type LogStoreFaults struct {
	Enabled        bool
	ApplyLatency   FastWithRareSlow
	RespondLatency FastWithRareSlow
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64
	Outage         ServiceOutage
	CallerTimeout  core.Duration
	// Bug selects a logstore mutant (logengine.BugTrimDelete deletes instead of
	// tombstoning on LogDeleteOld). Zero is none.
	Bug uint8
	// WindowSlots and TrimStride mirror LogParams for ControlPlaneRealState idempotency trim
	// during reference replay (decision 48). Zero WindowSlots skips trim.
	WindowSlots, TrimStride int
}

// NetworkFaults are connection-level faults (lesson 6).
type NetworkFaults struct {
	Latency         FastWithRareSlow
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
