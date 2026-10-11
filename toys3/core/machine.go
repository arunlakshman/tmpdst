package core

// StateMachine is the shape of the server: Step is the only place state
// changes. It never blocks and never does I/O.
type StateMachine interface {
	Step(Event) []Action
}

// Hashable lets the simulator fold state into a running trace hash after every
// step. The same seed must produce the same hash sequence; a divergence means
// nondeterminism leaked in.
type Hashable interface {
	StateHash() uint64
}

// Machine is what a runtime drives: a hashable state machine.
type Machine interface {
	StateMachine
	Hashable
}

// Factory builds a fresh Machine for one boot. Runtimes call it on first boot
// and after every crash, then deliver Started. Taking a Factory (rather than
// calling New directly) lets the simulator run deliberately buggy variants.
type Factory func(env Env, cfg Config) Machine

// NewMachine is the Factory for the real server.
func NewMachine(env Env, cfg Config) Machine { return New(env, cfg) }

var _ Factory = NewMachine
