package control

// Env is injected into the server at construction. It is the only way
// nondeterminism enters the control. Time is NOT here: it arrives on each Event.
type Env interface {
	Rand
	Buggify
}

// Rand is a source of randomness. In simulation it draws from the run's single
// seeded stream; in production it may be any generator.
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). It panics if n <= 0.
	Intn(n int) int
}

// Site is the stable identity of a buggify point. Never interpolate values.
type Site string

// Buggify is FoundationDB-style cooperative fault injection: code asks "should
// I take the rare-but-legal path here?" (e.g. process the queue in a different
// legal order) so the simulator can explore it. Production always says no.
type Buggify interface {
	Maybe(site Site) bool
}
