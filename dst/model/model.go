// Package model provides lockstep model/subject checks on top of
// pgregory.net/rapid. Each product supplies a Model and a System; this
// package compares observations and runs the paper DirtyReboot checks.
//
// Prefer simplest ops first when building a generator. SimplestFirst wraps
// rapid.OneOf so earlier generators shrink first. Put reads and no-ops ahead
// of writes, and crash/reboot ops last.
//
// This package moved to dst/check: spectest.Lockstep replaces Step and
// CheckGen, SimplestFirst is in spectest, and Write and the crash helpers are
// in check. It stays
// here only until its last user moves (plan task T1.14 or T1.16 deletes it).
// New code uses dst/check and dst/check/spectest.
package model

import (
	"fmt"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/dst/check"
	"github.com/manifesterio/mds/dst/check/spectest"
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
// simpler for shrink: rapid.OneOf shrinks toward lower indices. It is
// spectest.SimplestFirst.
func SimplestFirst[V any](gens ...*rapid.Generator[V]) *rapid.Generator[V] {
	return spectest.SimplestFirst(gens...)
}

// Write is one answered key/value pair used by CheckPersistence. It is
// check.Write.
type Write[K, V any] = check.Write[K, V]

// CheckPersistence verifies that every answered write is still present after
// a subject crash and recover. It is check.Persistence.
func CheckPersistence[K, V any](answered []Write[K, V], present func(K, V) bool) error {
	return check.Persistence(answered, present)
}

// CheckForwardProgress compares a clean model observation to the subject's
// observation after a clean restart. It is check.ForwardProgress.
func CheckForwardProgress[Obs any](modelClean, subjectRestart func() (Obs, error), equal func(want, got Obs) error) error {
	return check.ForwardProgress(modelClean, subjectRestart, equal)
}

// DirtyReboot runs the paper crash checks: answered writes remain, then a
// clean restart matches the model again. It is check.DirtyReboot.
func DirtyReboot[K, V, Obs any](
	answered []Write[K, V],
	present func(K, V) bool,
	modelClean, subjectRestart func() (Obs, error),
	equal func(want, got Obs) error,
) error {
	return check.DirtyReboot(answered, present, modelClean, subjectRestart, equal)
}
