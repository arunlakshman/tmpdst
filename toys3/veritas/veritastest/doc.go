// Package veritastest runs conformance checks: random sequences of operations
// against an implementation and its veritas (../) in lockstep, comparing every
// result and checking invariants after every operation. A failing sequence is
// shrunk to a short one and printed as a Go literal, ready to paste into a
// regression test. See ../../VERITAS-PROPOSAL.md, Part 2.
//
// Everything is hand-written and seeded from its own SplitMix64 stream (the
// same algorithm as sim.Rand; sim's tests import this package, so it cannot
// import sim). A seed replays exactly, like a DST seed, and shrinking is
// deterministic too.
//
// The package does not import testing (nor time or flag): it takes a TB,
// which *testing.T satisfies, and the caller maps its own -seeds, -seed and
// -duration flags onto Options. That keeps it checkable by tools/detcheck.
//
// Check and Run are generic; CheckFS is the conformance check for the Storage
// port against veritas.FS, shared by every implementation of the port.
package veritastest
