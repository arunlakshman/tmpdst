// Package model provides lockstep model/subject checks on top of
// pgregory.net/rapid. Each product supplies a Model and a System; this
// package compares observations and runs the paper DirtyReboot checks.
//
// Prefer simplest ops first when building a generator. SimplestFirst wraps
// rapid.OneOf so earlier generators shrink first. Put reads and no-ops ahead
// of writes, and crash/reboot ops last.
package model

import (
	"fmt"

	"pgregory.net/rapid"
)

// Model is the sequential reference. Apply is fault-free.
type Model[Op, Obs any] interface {
	Apply(Op) (Obs, error)
}

// System is the subject under test.
type System[Op, Obs any] interface {
	Apply(Op) (Obs, error)
}

// Step runs a reference model and a subject in lockstep for one op.
// Equal compares model observation (want) to subject observation (got).
// Invariant is optional; nil skips it.
type Step[Op, Obs any] struct {
	ModelApply   func(Op) (Obs, error)
	SubjectApply func(Op) (Obs, error)
	Equal        func(want, got Obs) error
	Invariant    func() error
}

// NewStep builds a Step from a Model, a System, and an observation comparer.
func NewStep[Op, Obs any](m Model[Op, Obs], sys System[Op, Obs], equal func(want, got Obs) error) Step[Op, Obs] {
	return Step[Op, Obs]{
		ModelApply:   m.Apply,
		SubjectApply: sys.Apply,
		Equal:        equal,
	}
}

// Check applies op to model then subject and compares observations.
// Model errors fail the step. Subject errors fail unless Equal accepts them
// through a custom Equal; use CheckAllowFailed when a crashed subject may
// diverge while the model succeeded.
func (s Step[Op, Obs]) Check(op Op) error {
	want, err := s.ModelApply(op)
	if err != nil {
		return fmt.Errorf("model: %w", err)
	}
	got, err := s.SubjectApply(op)
	if err != nil {
		return fmt.Errorf("subject: %w", err)
	}
	if err := s.Equal(want, got); err != nil {
		return fmt.Errorf("equal: %w", err)
	}
	if s.Invariant != nil {
		if err := s.Invariant(); err != nil {
			return fmt.Errorf("invariant: %w", err)
		}
	}
	return nil
}

// CheckAllowFailed is like Check, but when failed is true a subject error is
// allowed while the model succeeded. Observations are still compared when
// both applies succeed. When failed is false, behavior matches Check.
func (s Step[Op, Obs]) CheckAllowFailed(op Op, failed bool) error {
	want, merr := s.ModelApply(op)
	got, serr := s.SubjectApply(op)
	if merr != nil {
		return fmt.Errorf("model: %w", merr)
	}
	if serr != nil {
		if !failed {
			return fmt.Errorf("subject: %w", serr)
		}
		return s.checkInvariant()
	}
	if err := s.Equal(want, got); err != nil {
		return fmt.Errorf("equal: %w", err)
	}
	return s.checkInvariant()
}

func (s Step[Op, Obs]) checkInvariant() error {
	if s.Invariant == nil {
		return nil
	}
	if err := s.Invariant(); err != nil {
		return fmt.Errorf("invariant: %w", err)
	}
	return nil
}

// CheckGen draws ops from gen and locksteps each draw with CheckAllowFailed.
// failed, when non-nil, reports the paper "has failed" flag for that step.
func (s Step[Op, Obs]) CheckGen(t *rapid.T, gen *rapid.Generator[Op], failed func() bool) {
	t.Repeat(map[string]func(*rapid.T){
		"step": func(rt *rapid.T) {
			op := gen.Draw(rt, "op")
			allow := failed != nil && failed()
			if err := s.CheckAllowFailed(op, allow); err != nil {
				rt.Fatalf("%v", err)
			}
		},
	})
}

// SimplestFirst builds a generator that picks among gens. Earlier gens are
// simpler for shrink: rapid.OneOf shrinks toward lower indices.
func SimplestFirst[V any](gens ...*rapid.Generator[V]) *rapid.Generator[V] {
	return rapid.OneOf(gens...)
}

// Write is one answered key/value pair used by CheckPersistence.
type Write[K, V any] struct {
	Key   K
	Value V
}

// CheckPersistence verifies that every answered write is still present after
// a subject crash and recover. present(key, value) reports whether durable
// state contains that write. answered is the set of writes the client
// observed as successful before the crash.
func CheckPersistence[K, V any](answered []Write[K, V], present func(K, V) bool) error {
	for i, w := range answered {
		if !present(w.Key, w.Value) {
			return fmt.Errorf("persistence: answered write %d missing", i)
		}
	}
	return nil
}

// CheckForwardProgress compares a clean model observation to the subject's
// observation after a clean restart. Both callbacks produce observations;
// equal reports mismatch.
func CheckForwardProgress[Obs any](modelClean, subjectRestart func() (Obs, error), equal func(want, got Obs) error) error {
	want, err := modelClean()
	if err != nil {
		return fmt.Errorf("model clean: %w", err)
	}
	got, err := subjectRestart()
	if err != nil {
		return fmt.Errorf("subject restart: %w", err)
	}
	if err := equal(want, got); err != nil {
		return fmt.Errorf("forward progress: %w", err)
	}
	return nil
}

// DirtyReboot runs the paper crash checks: answered writes remain, then a
// clean restart matches the model again.
func DirtyReboot[K, V, Obs any](
	answered []Write[K, V],
	present func(K, V) bool,
	modelClean, subjectRestart func() (Obs, error),
	equal func(want, got Obs) error,
) error {
	if err := CheckPersistence(answered, present); err != nil {
		return err
	}
	return CheckForwardProgress(modelClean, subjectRestart, equal)
}
