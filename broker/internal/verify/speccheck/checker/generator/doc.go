// Package generator makes the random operations that every conformance suite
// feeds to a spec and to the subject under test. One generator exists for each
// spec, so every layer (sim engine, S3 adapter, applier, memstore) sees the
// same kinds of operations. An InputProfile changes the weights and the value ranges,
// and the coverage test in this package shows which interesting cases each
// profile reaches.
//
// The package is deterministic. It takes all random values from a
// *checker.Rand, never lets map order decide anything, and must not import
// testing (outside test files), os, time, flag or sync, and must not start
// goroutines.
//
// Each operation type also has shrink and format helpers. Shrink returns
// simpler versions of one operation, simplest first. Format returns a Go
// expression that you can paste into a regression test.
//
// Terms:
//
//   - profile: a named set of weights and value ranges for a generator.
//   - replay: a commit that repeats a segment key already committed, with the
//     same batches. The control plane must answer it with the first answer.
//   - conflict: a commit that repeats a segment key already committed, with
//     different batches. The control plane must answer ErrConflict.
//   - shrink: make the failing case smaller.
//   - seed: a number that fixes the random operations, so a run can repeat.
package generator
