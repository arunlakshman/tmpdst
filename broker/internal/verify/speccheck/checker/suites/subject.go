package suites

import (
	"errors"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker/generator"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker/observation"
)

// ObjectSubject is an object store under test. Do runs one operation and
// returns its result. A completion for a Put or a Delete must carry the same
// Outcome as the spec (Applied or NotApplied). The error is for broken test
// plumbing, such as a missing completion, and never for a wrong answer.
// StateHash returns the same hash as speccheck.ObjectStore.StateHash would for
// the same content. It returns ok = false when the subject has no spec hash,
// for example real S3.
type ObjectSubject interface {
	Do(operation core.IOCall) (observation.ObjectResult, error)
	StateHash() (hash uint64, ok bool)
}

// ControlPlaneSubject is a control plane under test. Do runs one operation.
// For CreateTopic the suite compares only Code, because the direct call has no
// Outcome in every layer. The error is for broken test plumbing only.
// StateHash is like ObjectSubject.StateHash, with speccheck.ControlPlane as the
// spec.
type ControlPlaneSubject interface {
	Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error)
	StateHash() (hash uint64, ok bool)
}

// BrokerSubject is a log under test: the log of one Kafka partition. The methods have the same
// rules as speccheck.Broker. StateHash returns ok = false when the subject
// has no spec hash.
type BrokerSubject interface {
	Append(batch [][]byte) core.Offset
	Read(from core.Offset, max int) ([]speccheck.StoredRecord, bool)
	HighWatermark() core.Offset
	StateHash() (hash uint64, ok bool)
}

type specObjectSubject struct{ store *speccheck.ObjectStore }

// NewSpecObjectSubject returns a fresh speccheck.ObjectStore as a subject. Use it
// to run a suite spec against spec, and as the correct subject that an injected
// bug wraps.
func NewSpecObjectSubject() ObjectSubject {
	return &specObjectSubject{store: speccheck.NewObjectStore()}
}

func (s *specObjectSubject) Do(operation core.IOCall) (observation.ObjectResult, error) {
	return observation.FromObjectCompletion(s.store.Exec(operation)), nil
}

func (s *specObjectSubject) StateHash() (uint64, bool) { return s.store.StateHash(), true }

type specControlPlaneSubject struct{ cp *speccheck.ControlPlane }

// NewSpecControlPlaneSubject returns a fresh speccheck.ControlPlane as a subject.
func NewSpecControlPlaneSubject() ControlPlaneSubject {
	return &specControlPlaneSubject{cp: speccheck.NewControlPlane()}
}

func (s *specControlPlaneSubject) Do(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, error) {
	switch {
	case operation.CreateTopic != nil:
		code := s.cp.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)
		return observation.ControlPlaneResult{Code: code}, nil
	case operation.Call != nil:
		done, ok := s.cp.Exec(*operation.Call).(core.ControlPlaneDone)
		if !ok {
			return observation.ControlPlaneResult{}, errors.New("the spec control plane did not return a CPDone")
		}
		return observation.FromControlPlaneDone(done), nil
	}
	return observation.ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (s *specControlPlaneSubject) StateHash() (uint64, bool) { return s.cp.StateHash(), true }

type specBrokerSubject struct{ p *speccheck.Broker }

// NewSpecBrokerSubject returns a fresh speccheck.Broker as a subject.
func NewSpecBrokerSubject() BrokerSubject {
	return &specBrokerSubject{p: &speccheck.Broker{}}
}

func (s *specBrokerSubject) Append(batch [][]byte) core.Offset { return s.p.Append(batch) }

func (s *specBrokerSubject) Read(from core.Offset, max int) ([]speccheck.StoredRecord, bool) {
	return s.p.Read(from, max)
}

func (s *specBrokerSubject) HighWatermark() core.Offset { return s.p.HighWatermark() }

func (s *specBrokerSubject) StateHash() (uint64, bool) { return s.p.StateHash(), true }

// AllObjectProfiles returns every generator profile. A caller runs the object
// store suite once for each profile, as a subtest named after the profile.
func AllObjectProfiles() []generator.InputProfile { return generator.InputProfiles() }

// AllControlPlaneProfiles returns every generator profile. A caller runs the
// control plane suite once for each profile, as a subtest named after it.
func AllControlPlaneProfiles() []generator.InputProfile { return generator.InputProfiles() }
