// Package injectedbug holds known bugs that a conformance suite must find. Each
// injected bug wraps a correct subject and changes the operation before it goes
// in, or the result after it comes out. Because a bug only wraps the interface
// of a subject, it works on every layer.
//
// A suite that cannot fail proves nothing. The helpers in this package run
// every injected bug through the same suite code that the real subjects use. They
// fail the test if a bug is not caught within the seed budget, and they print a
// table of the seed that caught each bug and the length of the shrunk case.
//
// The package is deterministic. It must not import testing, os, time, flag or
// sync. The package suites must not import this package.
//
// Terms:
//
//   - injected bug: a wrong behavior that we add on purpose, to check that a
//     suite catches it.
//   - catch: the suite reports a failure for an injected bug.
//   - shrink: make the failing case smaller.
//   - seed: a number that fixes the random operations, so a run can repeat.
package injectedbug
