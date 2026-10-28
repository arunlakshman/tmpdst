package suites

import (
	"errors"

	"manifester/internal/core"
	"manifester/internal/verify/veritas"
	"manifester/internal/verify/veritas/veritastest/generator"
	"manifester/internal/verify/veritas/veritastest/observation"
)

// ObjectSubject is an object store under test. Do runs one operation and
// returns its result. A completion for a Put or a Delete must carry the same
// Outcome as the spec (Applied or NotApplied). The error is for broken test
// plumbing, such as a missing completion, and never for a wrong answer.
// StateHash returns the same hash as veritas.ObjectStore.StateHash would for
// the same content. It returns ok = false when the subject has no spec hash,
// for example real S3.
type ObjectSubject interface {
	Do(op core.IOCall) (observation.ObjectResult, error)
	StateHash() (hash uint64, ok bool)
}

// ControlPlaneSubject is a control plane under test. Do runs one operation.
// For CreateTopic the suite compares only Code, because the direct call has no
// Outcome in every layer. The error is for broken test plumbing only.
// StateHash is like ObjectSubject.StateHash, with veritas.ControlPlane as the
// spec.
type ControlPlaneSubject interface {
	Do(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error)
	StateHash() (hash uint64, ok bool)
}

// PartitionSubject is a partition log under test. The methods have the same
// rules as veritas.Partition. StateHash returns ok = false when the subject
// has no spec hash.
type PartitionSubject interface {
	Append(batch [][]byte) core.Offset
	Read(from core.Offset, max int) ([]veritas.Record, bool)
	HighWatermark() core.Offset
	StateHash() (hash uint64, ok bool)
}

type specObjectSubject struct{ store *veritas.ObjectStore }

// NewSpecObjectSubject returns a fresh veritas.ObjectStore as a subject. Use it
// to run a suite spec against spec, and as the correct subject that a planted
// bug wraps.
func NewSpecObjectSubject() ObjectSubject {
	return &specObjectSubject{store: veritas.NewObjectStore()}
}

func (s *specObjectSubject) Do(op core.IOCall) (observation.ObjectResult, error) {
	return observation.FromObjectCompletion(s.store.Exec(op)), nil
}

func (s *specObjectSubject) StateHash() (uint64, bool) { return s.store.StateHash(), true }

type specControlPlaneSubject struct{ cp *veritas.ControlPlane }

// NewSpecControlPlaneSubject returns a fresh veritas.ControlPlane as a subject.
func NewSpecControlPlaneSubject() ControlPlaneSubject {
	return &specControlPlaneSubject{cp: veritas.NewControlPlane()}
}

func (s *specControlPlaneSubject) Do(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
	switch {
	case op.CreateTopic != nil:
		code := s.cp.CreateTopic(op.CreateTopic.Name, op.CreateTopic.Partitions)
		return observation.ControlPlaneResult{Code: code}, nil
	case op.Call != nil:
		done, ok := s.cp.Exec(*op.Call).(core.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, errors.New("the spec control plane did not return a CPDone")
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (s *specControlPlaneSubject) StateHash() (uint64, bool) { return s.cp.StateHash(), true }

type specPartitionSubject struct{ p *veritas.Partition }

// NewSpecPartitionSubject returns a fresh veritas.Partition as a subject.
func NewSpecPartitionSubject() PartitionSubject {
	return &specPartitionSubject{p: &veritas.Partition{}}
}

func (s *specPartitionSubject) Append(batch [][]byte) core.Offset { return s.p.Append(batch) }

func (s *specPartitionSubject) Read(from core.Offset, max int) ([]veritas.Record, bool) {
	return s.p.Read(from, max)
}

func (s *specPartitionSubject) HighWatermark() core.Offset { return s.p.HighWatermark() }

func (s *specPartitionSubject) StateHash() (uint64, bool) { return s.p.StateHash(), true }

// AllObjectProfiles returns every generator profile. A caller runs the object
// store suite once for each profile, as a subtest named after the profile.
func AllObjectProfiles() []generator.Profile { return generator.Profiles() }

// AllControlPlaneProfiles returns every generator profile. A caller runs the
// control plane suite once for each profile, as a subtest named after it.
func AllControlPlaneProfiles() []generator.Profile { return generator.Profiles() }
