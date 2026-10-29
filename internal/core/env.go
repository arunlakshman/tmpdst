package core

// Environment is injected into every StateMachine at construction. It is the only way
// nondeterminism enters the core. Time is NOT here: it arrives on each Event.
type Environment interface {
	Rand
	FaultInjector
	Assertions
}

// Rand draws from the single seeded stream in simulation (lesson 3).
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). n > 0.
	Intn(n int) int
	// NewID returns a fresh 128-bit ID (segment keys, incarnations, request IDs).
	NewID() ID
}

// FaultPointID is the stable identity of a buggify point. Never interpolate values.
type FaultPointID string

// FaultInjector is FoundationDB-style cooperative fault injection (lesson 7).
// Production implementations always return false.
type FaultInjector interface {
	// Maybe reports whether site fires (per-run activation, then default per-call
	// probability). Use for rare-but-valid paths; keeps firing in the quiet tail.
	Maybe(site FaultPointID) bool
	// MaybeP is Maybe with an explicit per-call probability in [0, 1].
	MaybeP(site FaultPointID, p float64) bool
	// Fault is for points that make an operation fail. It never fires once the
	// simulator has entered recovery mode.
	Fault(site FaultPointID, p float64) bool
}

// PropertyID is a stable property ID for an assertion. Never interpolate values.
type PropertyID string

// ContextPair is one ordered key/value pair of assertion context.
type ContextPair struct {
	Key   string
	Value any
}

// Assertions record and continue; they never panic (lesson 11).
type Assertions interface {
	// Always must hold every time it is evaluated and must be reached at least once.
	Always(cond bool, p PropertyID, details ...ContextPair)
	// AlwaysOrUnreachable must hold whenever reached; not reaching it is fine.
	AlwaysOrUnreachable(cond bool, p PropertyID, details ...ContextPair)
	// Sometimes must be true at least once across a campaign.
	Sometimes(cond bool, p PropertyID)
	// Reachable marks a path that must execute at least once across a campaign.
	Reachable(p PropertyID)
	// Unreachable marks a path that must never execute.
	Unreachable(p PropertyID, details ...ContextPair)
}
