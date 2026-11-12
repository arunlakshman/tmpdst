// Package runtime holds the plain-data vocabulary of the control plane: the values
// that go into a dst process (inbox values) and the calls it returns for the
// log and the object store, and the Environment through which the only permitted
// nondeterminism (randomness, buggify) and assertion recording enter. The process
// contract itself is dst's sim.Process; see docs/process-contract.md.
//
// The base types (the time types, the ids, NodeInstance, Clock) are not defined here.
// They come from dst/machine, which the broker and the control plane share.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - inbox values and calls are plain data; byte slices are immutable once sent.
//
// The control plane's own request and response types are not here. They are in
// contract (a public package that the broker also imports). This package imports
// contract, never the reverse.
//
// See docs/lessons.md and docs/decisions.md.
package runtime
