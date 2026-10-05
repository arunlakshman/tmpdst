// Package core is the deterministic heart of the engine: plain-data Events and
// Actions, the StateMachine contract, and the Env through which the only
// permitted nondeterminism (randomness, buggify) and assertion recording enter.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - Events and Actions are plain data; byte slices are immutable once sent.
//
// See docs/lessons.md and docs/decisions.md.
package core
