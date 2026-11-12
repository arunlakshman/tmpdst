// Package observation holds plain values that describe what a subject
// answered. A subject can be the spec, a sim engine or a real adapter. Tests
// compare these values with Equal, and they build text only for failure
// messages, so a wrong answer is never hidden by how it was printed.
//
// The package is deterministic. It does not import testing, os, time, flag or
// sync, it starts no goroutines, and no result depends on map order.
//
// Terms:
//   - Observation: the plain value a subject answered, such as ObjectResult.
//   - Normalize: make an empty slice and a nil slice the same value.
//   - Spec: the small executable model that says what the right answer is.
//   - Subject: the code under test that is compared with the spec.
package observation
