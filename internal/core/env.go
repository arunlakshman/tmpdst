package core

// Env is injected into every StateMachine at construction. It is the only way
// nondeterminism enters the core. Time is NOT here: it arrives on each Event.
type Env interface {
	Rand
	Buggify
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

// Site is the stable identity of a buggify point. Never interpolate values.
type Site string

// Buggify is FoundationDB-style cooperative fault injection (lesson 7).
// Production implementations always return false.
type Buggify interface {
	// Maybe reports whether site fires (per-run activation, then default per-call
	// probability). Use for rare-but-valid paths; keeps firing in the quiet tail.
	Maybe(site Site) bool
	// MaybeP is Maybe with an explicit per-call probability in [0, 1].
	MaybeP(site Site, p float64) bool
	// Fault is for points that make an operation fail. It never fires once the
	// simulator has entered recovery mode.
	Fault(site Site, p float64) bool
}

// Prop is a stable property ID for an assertion. Never interpolate values.
type Prop string

// Detail is one ordered key/value pair of assertion context.
type Detail struct {
	Key   string
	Value any
}

// Assertions record and continue; they never panic (lesson 11).
type Assertions interface {
	// Always must hold every time it is evaluated and must be reached at least once.
	Always(cond bool, p Prop, details ...Detail)
	// AlwaysOrUnreachable must hold whenever reached; not reaching it is fine.
	AlwaysOrUnreachable(cond bool, p Prop, details ...Detail)
	// Sometimes must be true at least once across a campaign.
	Sometimes(cond bool, p Prop)
	// Reachable marks a path that must execute at least once across a campaign.
	Reachable(p Prop)
	// Unreachable marks a path that must never execute.
	Unreachable(p Prop, details ...Detail)
}
