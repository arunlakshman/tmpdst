package sim

import "manifester/core"

// Process describes a system-under-test role (the broker). Built fresh from the
// factory on every boot; nothing in memory survives a crash.
type Process struct {
	Name    string
	Count   int
	Factory core.Factory
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

// WorkloadFactory builds a fresh workload per seed (never reuse instances).
type WorkloadFactory func(env core.Env, self core.NodeRef, brokers []core.NodeRef) Workload

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

const (
	PhaseChaos    Phase = iota // faults injected
	PhaseSettling              // recovery mode, nodes alive
	PhaseSettle                // draining remaining events
)

// View is what an invariant sees after a step: the fact timeline, the history
// and read-only ground truth from the engines.
type View interface {
	Step() uint64
	Now() Time
	Phase() Phase
	Alive(ref core.NodeRef) bool
	Timeline() Timeline
	History() History
	Objects() ObjectTruth
	CP() CPTruth
	// Assert is how invariants report.
	Assert() core.Assertions
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
	OpPending OpOutcome = iota
	// OpDefinite: the result is known; the effect lies inside [Invoke, Return].
	OpDefinite
	// OpIndeterminate: the client saw MaybeApplied. The op may take effect at
	// ANY time after Invoke, including after the client moved on (Jepsen
	// :info). It never closes; it stays a candidate in every later window.
	OpIndeterminate
	// OpAbandoned: the client gave up and will not retry this op id.
	OpAbandoned
)

// HistOp is one client operation. ID names the request; Value names the
// write's identity and is shared by its retries (idempotent-producer retries
// are two requests with one Value).
type HistOp struct {
	ID             core.ID
	Value          core.ID
	Client         core.NodeRef
	Op             any
	InvSeq, RetSeq uint64 // logical order; virtual time ties
	Invoke, Return Time
	Outcome        OpOutcome
	Result         any
}

// History is the workload's append-only record of ops.
type History interface {
	Invoke(client core.NodeRef, id, value core.ID, op any)
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
	Source core.NodeRef // zero for engine facts ("sim_fault")
	Fact   core.Fact
}
