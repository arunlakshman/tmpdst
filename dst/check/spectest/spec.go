package spectest

import (
	"pgregory.net/rapid"
)

// Spec is a small, fault-free program that says what the right answer is.
// Apply runs one op and returns its result. StateHash returns a hash of the
// whole state, so two programs with the same state give the same hash.
type Spec[Op, Res any] interface {
	Apply(op Op) Res
	StateHash() uint64
}

// Subject is the code we compare with the spec. The error means "no answer at
// all" (for example a call that timed out). It is not a business error: a
// business error is part of Res.
type Subject[Op, Res any] interface {
	Apply(op Op) (Res, error)
}

// HasStateHash is optional on a Subject. When the subject has it, Lockstep
// compares the subject hash with the spec hash after every op and after every
// reboot.
type HasStateHash interface {
	StateHash() uint64
}

// SimplestFirst builds a generator that picks one of gens. Rapid shrinks a
// pick toward the first generators, so put the simple ops first: reads and
// no-ops, then writes, then the rare and complex ops.
func SimplestFirst[V any](gens ...*rapid.Generator[V]) *rapid.Generator[V] {
	return rapid.OneOf(gens...)
}
