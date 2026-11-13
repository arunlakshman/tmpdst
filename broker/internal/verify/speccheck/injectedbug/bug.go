package injectedbug

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/dst/check/spectest"
)

// stateHashBugName is the name of the planted bug that only a state hash can
// show.
const stateHashBugName = "StateHashDiffers"

// Bug is a planted bug for the subjects of one suite. Wrap returns a subject
// that behaves like the correct one, except for the bug.
type Bug[Op, Res any] struct {
	name, doc string
	wrap      func(correct spectest.Subject[Op, Res]) spectest.Subject[Op, Res]
}

// NewBug returns a named bug. Wrap is called once for each fresh subject, so the
// wrapper may keep state.
func NewBug[Op, Res any](name, doc string, wrap func(spectest.Subject[Op, Res]) spectest.Subject[Op, Res]) Bug[Op, Res] {
	return Bug[Op, Res]{name: name, doc: doc, wrap: wrap}
}

// Name is the short name of the bug.
func (b Bug[Op, Res]) Name() string { return b.name }

// Doc is one sentence that says what the bug does wrong.
func (b Bug[Op, Res]) Doc() string { return b.doc }

// Wrap returns correct with the bug added.
func (b Bug[Op, Res]) Wrap(correct spectest.Subject[Op, Res]) spectest.Subject[Op, Res] {
	return b.wrap(correct)
}

// plain is a subject made from a function. It has no state hash.
type plain[Op, Res any] struct {
	do func(operation Op) (Res, error)
}

func (s plain[Op, Res]) Apply(operation Op) (Res, error) { return s.do(operation) }

// hashed is a plain subject that also has a state hash.
type hashed[Op, Res any] struct {
	plain[Op, Res]
	hash func() uint64
}

func (s hashed[Op, Res]) StateHash() uint64 { return s.hash() }

// wrap makes a subject that runs do, and that has a state hash exactly when
// inner has one. The hash is the hash of inner, changed by changeHash when
// that is not nil. A bug that changes only the answers passes nil.
func wrap[Op, Res any](inner spectest.Subject[Op, Res], do func(operation Op) (Res, error), changeHash func(uint64) uint64) spectest.Subject[Op, Res] {
	base := plain[Op, Res]{do: do}
	h, ok := inner.(spectest.HasStateHash)
	if !ok {
		return base
	}
	return hashed[Op, Res]{plain: base, hash: func() uint64 {
		v := h.StateHash()
		if changeHash != nil {
			v = changeHash(v)
		}
		return v
	}}
}

// hasHash reports whether s has a state hash.
func hasHash(s any) bool {
	_, ok := s.(spectest.HasStateHash)
	return ok
}

// after makes a bug that changes the result after the correct subject has
// answered. change gets the operation and the correct result.
func after[Op, Res any](name, doc string, change func(operation Op, r Res) Res) Bug[Op, Res] {
	return stateful(name, doc, func(spectest.Subject[Op, Res]) func(Op, Res) Res { return change })
}

// stateful makes a bug whose change function keeps state. newChange is called
// once for each fresh subject.
func stateful[Op, Res any](name, doc string, newChange func(spectest.Subject[Op, Res]) func(Op, Res) Res) Bug[Op, Res] {
	return NewBug(name, doc, func(inner spectest.Subject[Op, Res]) spectest.Subject[Op, Res] {
		change := newChange(inner)
		return wrap(inner, func(operation Op) (Res, error) {
			r, err := inner.Apply(operation)
			if err != nil {
				return r, err
			}
			return change(operation, r), nil
		}, nil)
	})
}

// stateHashDiffers makes the bug that reports a wrong state hash. It checks
// that the suite compares the hash at all. It has no effect on a subject with
// no hash.
func stateHashDiffers[Op, Res any]() Bug[Op, Res] {
	return NewBug(stateHashBugName, "StateHash returns a value that is not the hash of the spec state.",
		func(inner spectest.Subject[Op, Res]) spectest.Subject[Op, Res] {
			return wrap(inner, inner.Apply, func(h uint64) uint64 { return h ^ 1 })
		})
}

// runSeed runs one lockstep check of a suite, on one profile, with one seed.
// failed is true when the check found a failure; then steps is the length of
// the shrunk list and message says what went wrong.
type runSeed[Op, Res any] func(p conformance.InputProfile, newSubject func() spectest.Subject[Op, Res], seed uint64) (message string, steps int, failed bool)

// failureOf turns the result of RunSeed into the values that a runSeed gives.
func failureOf[Op any](f *spectest.Failure[Op]) (string, int, bool) {
	if f == nil {
		return "", 0, false
	}
	return f.Message, len(f.Steps), true
}

// bugsCaught runs every bug in bugs through the suite, on every profile, for the
// seeds 1 to budget (spectest.RequireCaught). It fails t for each bug that no
// seed catches. newCorrect must return a fresh correct subject on each call. A
// subject with no spec hash cannot show a wrong hash, so the bug StateHashDiffers
// is skipped for such a subject, and the log says so.
func bugsCaught[Op, Res any](t testing.TB, kind string, bugs []Bug[Op, Res], newCorrect func() spectest.Subject[Op, Res], budget int, run runSeed[Op, Res]) {
	t.Helper()
	subjectHasHash := hasHash(newCorrect())
	var names []spectest.Bug
	byName := map[spectest.Bug]Bug[Op, Res]{}
	for _, b := range bugs {
		if !subjectHasHash && b.Name() == stateHashBugName {
			t.Logf("The planted bug %s is skipped because this subject has no spec state hash.", b.Name())
			continue
		}
		name := spectest.Bug(b.Name())
		names, byName[name] = append(names, name), b
	}
	spectest.RequireCaught(t, names, budget, func(name spectest.Bug, seed uint64) bool {
		bug := byName[name]
		for _, profile := range conformance.InputProfiles() {
			message, steps, failed := run(profile, func() spectest.Subject[Op, Res] { return bug.Wrap(newCorrect()) }, seed)
			if failed {
				t.Logf("%s bug %s: profile %s, seed %d, shrunk to %d steps: %s", kind, name, profile.Name, seed, steps, message)
				return true
			}
		}
		return false
	})
}

// suiteIsClean runs the suite on newCorrect, with no bug, on every profile, for
// the seeds 1 to budget (spectest.RequireClean). It fails t on the first
// failure: a failure here is a false alarm.
func suiteIsClean[Op, Res any](t testing.TB, newCorrect func() spectest.Subject[Op, Res], budget int, run runSeed[Op, Res]) {
	t.Helper()
	spectest.RequireClean(t, budget, func(seed uint64) bool {
		for _, profile := range conformance.InputProfiles() {
			if message, _, failed := run(profile, newCorrect, seed); failed {
				t.Logf("false alarm: profile %s, seed %d: %s", profile.Name, seed, message)
				return true
			}
		}
		return false
	})
}
