package runtime

import "github.com/manifesterio/mds/dst/machine"

// Environment is injected into every replica at construction. It is the only way
// nondeterminism enters the runtime. Time is NOT here: it arrives from the process context (sim.Context.Now).
//
// Faults (buggify sites) and Assertions are the dst/machine types. A site or an
// assertion is a declared value (dst/fault NewSite, dst/assert NewAlways and so on),
// never a string. Rand stays here until the machines take the v2 machine.Env (plan T1.11).
type Environment interface {
	Rand
	machine.Faults
	machine.Assertions
}

// Rand draws from the single seeded stream in simulation (lesson 3).
type Rand interface {
	Uint64() uint64
	// Intn returns a value in [0, n). n > 0.
	Intn(n int) int
	// NewID returns a fresh 128-bit ID (segment keys, incarnations, request IDs).
	NewID() machine.ID
}
