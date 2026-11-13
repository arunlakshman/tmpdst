package spectest

import (
	"testing"

	"pgregory.net/rapid"
)

// SelfCheck compares a spec with a naive model on random ops. A naive model
// is a second, plain program written only from the rules (for example with a
// sorted slice and loops, and no cache). A mistake in the spec is unlikely to
// be in the naive model too, so the two must agree.
//
// Only the results are compared. The state hashes are not, because the two
// programs keep their state in different shapes.
func SelfCheck[Op, Res any](t *testing.T, spec, naive func() Spec[Op, Res],
	ops *rapid.Generator[Op], equal func(a, b Res) error,
) {
	t.Helper()
	l := selfCheck(spec, naive, ops, equal)
	l.check(t)
}

// selfCheck builds the Lockstep of SelfCheck.
func selfCheck[Op, Res any](spec, naive func() Spec[Op, Res],
	ops *rapid.Generator[Op], equal func(a, b Res) error,
) Lockstep[Op, Res, naiveSubject[Op, Res]] {
	return Lockstep[Op, Res, naiveSubject[Op, Res]]{
		NewSpec:    spec,
		NewSubject: func(*rapid.T) naiveSubject[Op, Res] { return naiveSubject[Op, Res]{model: naive()} },
		Ops:        ops,
		Equal:      equal,
	}
}

// naiveSubject runs a naive model as a Subject. It has no StateHash method on
// purpose, so Lockstep does not compare the hashes.
type naiveSubject[Op, Res any] struct {
	model Spec[Op, Res]
}

// Apply runs op on the naive model. It never fails.
func (n naiveSubject[Op, Res]) Apply(op Op) (Res, error) {
	return n.model.Apply(op), nil
}
