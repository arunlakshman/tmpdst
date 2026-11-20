// Package spec is the reference model of the control plane: a small program
// that shows the correct answer to every operation. It keeps plain maps and
// slices, runs one operation at a time, and has no faults. A reader can check
// by eye that it is right, and tests compare the real code with it (see
// README.md).
//
// The model is deterministic. It has no clock, no network, no goroutines and no
// files, and no map order reaches an answer. It is used by tests and by the
// simulator only. No program of the product imports it.
package spec

import "github.com/manifesterio/mds/controlplane/contract"

// Model is anything that answers control plane operations. The real code and
// the simulator oracle are compared with a Model.
type Model interface {
	// Apply runs one operation (a write or a read) and returns its answer.
	// Outcome is Applied when the state changed, else NotApplied.
	Apply(op contract.Operation) contract.Done
}
