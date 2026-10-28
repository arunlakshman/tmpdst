// Package veritastest compares an implementation with its executable spec
// (package veritas) using random operations. It runs a random list of
// operations on both sides, compares every result, and checks the invariants
// after every step. When a list fails, it makes the list smaller and prints
// it as Go source text that you can paste into a regression test.
//
// The package is deterministic. It uses its own SplitMix64 random stream and
// a fixed shrink order. It must not import testing, time, flag or os. A
// *testing.T satisfies Reporter, and callers map their flags onto Options.
//
// Terms:
//
//   - lockstep: compare the implementation with the spec after every step.
//   - shrink: make the failing case smaller.
//   - spec: a simple model that says what the right result is.
//   - invariant: a rule that must hold after every step.
//   - seed: a number that fixes the random operations, so a run can repeat.
package veritastest
