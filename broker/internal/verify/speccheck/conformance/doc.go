// Package conformance holds the conformance suites of the broker tests: the
// suite for the object store spec (shared/objectstore), the suite for the
// control plane spec (controlplane/spec) and the suite for the broker spec
// (broker/spec). A suite takes a subject, which is the code under test, and a
// generator profile. It runs the same random operations on the subject and on
// a fresh spec, compares every result and the state hash after every step, and
// checks the invariants of the spec.
//
// The suites are thin: the machinery is dst/check/spectest. A suite is a
// spectest.Lockstep, made by ObjectStoreLockstep, ControlPlaneLockstep or
// BrokerLockstep. Rapid draws the list of operations and shrinks a failing
// list; spectest compares the results and prints the seed, the shrunk list as
// Go code and the command that runs it again. This package adds what is
// specific to the product:
//
//   - the generators (ObjectOps, ControlPlaneOps, BrokerOps) and their
//     profiles (InputProfile), which keep the weights of the old generators;
//   - the result values and how they are compared (ObjectResult,
//     ControlPlaneResult);
//   - the subjects (ObjectSubject, ControlPlaneSubject, BrokerSubject) and the
//     spec as a subject;
//   - the extra checks of each suite (a naive model of the control plane, the
//     invariants).
//
// Every layer of the system (a simulator engine, the S3 adapter, a memory
// store) becomes a subject by implementing one method, Apply. The suite code is
// shared, so a rule is written once and checked on every layer.
//
// A subject returns an error only when the test plumbing is broken, for
// example when a completion never arrives. A wrong answer is a normal result,
// and the suite reports it as a difference from the spec.
//
// A subject can have the method StateHash() uint64. Then the suite compares it
// with the hash of the spec after every step. Real S3 has none.
//
// The suites number the calls: the call at position i has the request id i+1.
// The generators make calls with no request id.
//
// The planted bugs that prove that a suite can fail are in the package
// injectedbug, which imports this package. This package must not import it.
//
// Terms:
//
//   - subject: the code under test. The suite compares it with the spec.
//   - spec: a simple model that says what the right result is.
//   - oracle: a checker we trust to say if a result is correct.
//   - invariant: a rule that must hold after every step.
//   - profile: a named set of weights and value ranges for a generator.
//   - replay: a commit that repeats a segment key already committed, with the
//     same batches. The control plane must answer it with the first answer.
//   - conflict: a commit that repeats a segment key already committed, with
//     different batches. The control plane must answer ErrConflict.
package conformance
