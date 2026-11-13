// Package injectedbug holds known bugs that a conformance suite must find. A
// planted bug (an injected bug) is a mistake that we put into a correct subject
// on purpose, to prove that the suite can fail. Each bug wraps a correct
// subject and changes the operation before it goes in, or the result after it
// comes out. Because a bug only wraps the interface of a subject, it works on
// every layer.
//
// A suite that cannot fail proves nothing. The helpers in this package run
// every planted bug through the same suite code that the real subjects use
// (conformance.ObjectStoreLockstep and conformance.ControlPlaneLockstep). They
// use the planted-bug API of dst/check/spectest (Bug, RequireCaught,
// RequireClean). They fail the test if a bug is not caught within the seed
// budget, and they log the profile and the seed that caught each bug.
//
// The package conformance must not import this package.
//
// Terms:
//
//   - planted bug: a wrong behavior that we add on purpose, to check that a
//     suite catches it.
//   - catch: the suite reports a failure for a planted bug.
//   - seed: a number that fixes the random operations, so a run can repeat.
//   - budget: the number of seeds (1 to budget) that a bug has to be caught in.
package injectedbug
