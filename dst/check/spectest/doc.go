// Package spectest is the part of the dst spec toolkit that runs in tests,
// on rapid (pgregory.net/rapid). Its parent package check holds the parts
// that run inside a simulation.
//
//   - Spec: a small, fault-free program that says what the right answer is.
//   - Lockstep: draws a list of ops with rapid, runs each op on a spec and on
//     the code under test (the subject), and compares the answers and the
//     state hashes. It can also draw reboot, crash (dirty reboot) and
//     checkpoint steps. Rapid shrinks a failure to a short list. The failure
//     message has the seed, the shrunk list as Go code, and the command that
//     runs it again (Rerun).
//   - SimplestFirst: draws ops so that rapid shrinks toward the simple ones.
//   - SelfCheck: compares a spec with a second, naive model.
//   - Bug, RequireCaught, RequireClean: planted bugs. A planted bug is a
//     mistake that a test puts into the code on purpose. A test proves that
//     each planted bug is caught within a seed budget (a fixed number of
//     seeds), and that a run with no bug does not fail.
//
// The product gives the prefix of its environment variables (Rerun.EnvPrefix),
// so dst has no product words.
//
// This package is separate from check on purpose: check is imported by
// simulation code, which must not depend on rapid, testing, os or flag.
package spectest
