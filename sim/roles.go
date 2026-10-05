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
// inside the simulator). It never crashes, holds the reference model, and
// reconciles ambiguous outcomes. It is itself a sans-I/O StateMachine.
type Workload interface {
	core.StateMachine
	Name() string
	// Check runs after settle; report violations through the Assertions it was built with.
	Check(tl Timeline)
}

// WorkloadFactory builds a fresh workload per seed (never reuse instances).
type WorkloadFactory func(env core.Env, self core.NodeRef, brokers []core.NodeRef) Workload

// Invariant observes the fact timeline after every step (lesson 10).
type Invariant interface {
	Name() string
	Observe(tl Timeline, now Time)
	Reset()
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
