// Package suites holds one conformance suite for each spec in package
// veritas: ObjectStore, ControlPlane and Partition. A suite takes a subject,
// which is the code under test, and a generator profile. It runs the same
// random operations on the subject and on a fresh spec, compares every result
// and the state hash after every step, and checks the invariants of the spec.
//
// Every layer of the system (a simulator engine, the S3 adapter, an applier, a
// memory store, a gRPC client) becomes a subject by implementing a small
// interface and passing a factory. The suite code is shared, so a rule is
// written once and checked on every layer.
//
// A subject returns an error only when the test plumbing is broken, for
// example when a completion never arrives. A wrong answer is a normal result,
// and the suite reports it as a difference from the spec.
//
// The package is deterministic. It takes all random values from the
// veritastest.Rand of the comparator. It must not import testing, os, time,
// flag or sync, and it must not import package plantedbug.
//
// Terms:
//
//   - subject: the code under test. The suite compares it with the spec.
//   - spec: a simple model that says what the right result is.
//   - oracle: a checker we trust to say if a result is correct.
//   - invariant: a rule that must hold after every step.
//   - profile: a named set of weights and value ranges for a generator.
//   - shrink: make the failing case smaller.
package suites
