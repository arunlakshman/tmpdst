package sim

import "manifester/internal/core"

// Process describes a system-under-test role (the broker). Built fresh from the
// factory on every boot; nothing in memory survives a crash.
type Process struct {
	Name    string
	Count   int
	Factory core.MachineFactory
	// FactoryFor, when set, replaces Factory and sees this seed's (swarmed)
	// SeedConfig, so a scenario can vary the process's own tuning per seed.
	FactoryFor func(sc SeedConfig) core.MachineFactory
	// NoAttrition excludes this process from kernel attrition. CP replicas use
	// LogParams StallProb/CrashProb instead (kafka-log).
	NoAttrition bool
}

// Workload is a test driver: a deterministic Kafka client model (kgo cannot run
// inside the simulator). It never crashes and reconciles ambiguous outcomes. It
// is a hashed Machine and records every op in the History; it does not judge
// them. Linearizability and the other oracles are Invariants.
type Workload interface {
	core.Machine
	Name() string
	// Quiescent reports that no ops are in flight: an oracle may close a window.
	Quiescent() bool
}

// PhaseAware is an optional Workload interface. At every phase change (into
// PhaseSettling and into PhaseSettle) the kernel appends the frozen fact
// "sim_phase" {phase: U64 of the new Phase}, puts engines into recovery when
// Settling starts, and then calls OnPhase on each live workload that
// implements it, in node order. The call is treated like a Step: the returned
// actions are dispatched the same way (a workload may not StopForRestart or ShutdownDone), the
// trace hash folds the call and its actions, and every StateHash is folded
// after it. It takes no node time and is not an inbox event, so a busy
// workload hears it immediately. The initial phase (PhaseChaos) is not
// announced; a workload starts in it. OnPhase must be deterministic, like Step.
type PhaseAware interface {
	OnPhase(p Phase) []core.Action
}

// WorkloadFactory builds a fresh workload per seed (never reuse instances).
type WorkloadFactory func(env core.Environment, self core.NodeInstance, brokers []core.NodeInstance) Workload

// Invariant checks one property (lesson 10). Config.Invariants builds a fresh
// instance per seed, so there is no Reset. Violations go through View.Assert.
type Invariant interface {
	Name() string
	// Observe runs after every step.
	Observe(v View)
	// AtEnd runs once after the drain: liveness, the final oracle window.
	AtEnd(v View)
}

// Phase is the run's current fault regime. Append-only.
type Phase uint8

// Phase values, in run order: Chaos, Settling, Settle.
const (
	// PhaseChaos injects faults.
	PhaseChaos Phase = iota // faults injected
	// PhaseSettling is recovery mode with nodes alive.
	PhaseSettling // recovery mode, nodes alive
	PhaseSettle   // draining remaining events
)

// View is what an invariant sees after a step: the fact timeline, the history
// and read-only ground truth from the engines. Whether a node is up is
// NodeAlive; type-assert a View when an invariant needs it.
type View interface {
	Step() uint64
	Now() Time
	Phase() Phase
	Timeline() Timeline
	History() History
	Objects() ObjectTruth
	CP() CPTruth
	// Assert is how invariants report.
	Assert() core.Assertions
}

// NodeAlive reports whether one incarnation is up. It is separate from View
// so the invariant surface stays within eight methods.
type NodeAlive interface {
	// Alive reports whether ref is the live incarnation of its node.
	Alive(ref core.NodeInstance) bool
}

// ObjectTruth is what the object store really holds, as builtin types only
// (anti-corruption layer). Iteration is in sorted key order.
type ObjectTruth interface {
	Object(key string) ([]byte, bool)
	EachObject(fn func(key string, data []byte))
	// Gen bumps whenever anything applied changes, so checks can skip steps.
	Gen() uint64
}

// CPTruth is what the control plane really committed.
type CPTruth interface {
	Committed(tp core.TopicPartition) []core.BatchRange
	HighWatermark(tp core.TopicPartition) core.Offset
	// Gen bumps whenever anything applied changes.
	Gen() uint64
}

// OpOutcome is what is known about one history op. Append-only.
type OpOutcome uint8

const (
	// OpPending has no result yet.
	OpPending OpOutcome = iota
	// OpDefinite means the result is known; the effect lies inside [Invoke, Return].
	OpDefinite
	// OpIndeterminate means the client saw MaybeApplied. The op may take effect at
	// ANY time after Invoke, including after the client moved on (Jepsen
	// :info). It never closes; it stays a candidate in every later window.
	OpIndeterminate
	// OpAbandoned means the client gave up and will not retry this op id.
	OpAbandoned
)

// HistOp is one client operation. ID names the request; Value names the
// write's identity and is shared by its retries (idempotent-producer retries
// are two requests with one Value).
type HistOp struct {
	ID             core.ID
	Value          core.ID
	Client         core.NodeInstance
	Op             any
	InvSeq, RetSeq uint64 // logical order; virtual time ties
	Invoke, Return Time
	Outcome        OpOutcome
	Result         any
}

// History is the workload's append-only record of ops.
type History interface {
	Invoke(client core.NodeInstance, id, value core.ID, op any)
	// Return completes id. It reports false for an unknown or duplicate id
	// (a protocol failure).
	Return(id core.ID, result any, o OpOutcome) bool
	Ops() []HistOp
}

// Cursor marks how far an invariant has read one fact name.
type Cursor struct{ next int }

// Timeline is the append-only log of facts from nodes, workloads and engines.
type Timeline interface {
	// Since returns facts named name after cursor c and advances it.
	Since(name string, c *Cursor) []TimedFact
	Snapshot(name string) []TimedFact
}

// TimedFact is a core.Fact stamped by the simulator.
type TimedFact struct {
	Seq    uint64
	At     Time
	Source core.NodeInstance // zero for engine facts ("sim_fault")
	Fact   core.Fact
}
