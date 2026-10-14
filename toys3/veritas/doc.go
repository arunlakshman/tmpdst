// Package veritas holds the veritas of each toys3 component: the small,
// executable, obviously correct spec of what that component must do. A
// veritas has no crashes, no faults, no concurrency and no performance
// concerns; it is short enough to check by reading. See ../VERITAS-PROPOSAL.md.
//
// The word replaces "reference model", because "model" is already taken in
// toys3 (sim.StorageModel, the oracle's register model).
//
// Each veritas does two jobs:
//
//   - Spec: a conformance check (see veritastest) runs random operations
//     against an implementation and its veritas in lockstep and compares
//     every result.
//   - Test fake: tests that need the component, but not its failure modes,
//     run on the veritas instead. Because tests depend on it, it cannot fall
//     behind the real port: a new storage command that FS does not handle
//     breaks the controller tests.
//
// The veritases:
//
//	Store  the object store (control.Op → Status and body): a map from key
//	       to payload, with the controller's 400 and 404 rules
//	FS     the Storage port (control.Actuation → control.Feedback), with every
//	       change immediately durable
//
// Like control and sim, this package is checked by tools/detcheck.
package veritas
