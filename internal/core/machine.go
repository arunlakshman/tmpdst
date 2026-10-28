package core

// StateMachine is the shape of every stateful component (broker, sim workload).
// Step is the only place state changes. It never blocks and never does I/O.
// An IOResult with an unknown, already-finished or wrong-kind RequestID must leave
// state unchanged.
type StateMachine interface {
	Step(Event) []Action
}

// HasStateHash lets the simulator fold state into a running trace hash after every
// step; the same seed must produce the same hash sequence (lesson 3).
type HasStateHash interface {
	StateHash() uint64
}

// Machine is a StateMachine whose state is hashed. Hashing is not optional.
type Machine interface {
	StateMachine
	HasStateHash
}

// MachineFactory builds a fresh Machine for one boot. It is called on first boot
// and after every crash, with a new BootNumber each time (lessons 4, 10).
type MachineFactory func(env Environment, self NodeInstance) Machine
