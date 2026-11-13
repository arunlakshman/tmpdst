// Package core is the deterministic heart of the engine: plain-data Events and
// Actions, the StateMachine contract, and the Environment through which the only
// permitted nondeterminism (randomness, buggify) and assertion recording enter.
//
// The base types (the time types, the ids, NodeInstance, Clock, Fact and its field
// values, the inbox class) are not defined here. They come from dst/machine, which the
// broker and the control plane share. A Fact name and its field encoding are trace-hash
// inputs: frozen, past-tense constants, never derived from Go type names. An ack fact is
// emitted in the same Step, immediately before the Send that acks, so invariants can
// check "acked implies committed" on that Step.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - Events and Actions are plain data; byte slices are immutable once sent.
//
// See docs/lessons.md and docs/decisions.md.
package core
