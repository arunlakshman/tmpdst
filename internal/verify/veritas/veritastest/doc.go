// Package veritastest runs lockstep conformance checks: random operation
// sequences against an implementation and its executable spec (package
// veritas), comparing every result and checking invariants after every step.
// A failing sequence is shrunk and printed as a pasteable Go literal.
//
// Deterministic: its own SplitMix64 stream, fixed shrink order, no testing,
// time or flag imports (TB is satisfied by *testing.T; callers map their
// flags onto Options).
package veritastest
