package core

// StateMachine is the shape of every stateful component (broker, sim workload).
// Step is the only place state changes. It never blocks and never does I/O.
// A Completion with an unknown, already-finished or wrong-kind ReqID must leave
// state unchanged.
type StateMachine interface {
	Step(Event) []Action
}

// Hashable lets the simulator fold state into a running trace hash after every
// step; the same seed must produce the same hash sequence (lesson 3).
type Hashable interface {
	StateHash() uint64
}

// Machine is a StateMachine whose state is hashed. Hashing is not optional.
type Machine interface {
	StateMachine
	Hashable
}

// Factory builds a fresh Machine for one boot. It is called on first boot
// and after every crash, with a new Incarnation each time (lessons 4, 10).
type Factory func(env Env, self NodeRef) Machine
