package conformance

import (
	"errors"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specbridge"
	"github.com/manifesterio/mds/dst/check/spectest"
	"github.com/manifesterio/mds/shared/objectstore"
)

// EnvPrefix is the prefix of the environment variables of the lockstep
// checks: MDS_SEED (run one seed), MDS_SEEDS (the number of checks) and
// MDS_CAMPAIGN (run until a time has passed). The parser is the one in
// dst/campaign; see spectest.Lockstep.Check.
const EnvPrefix = "MDS"

// ObjectSubject is an object store under test. It is a spectest.Subject: Apply runs one operation and
// returns its result. A completion for a Put or a Delete must carry the same
// Outcome as the spec (Applied or NotApplied). The error is for broken test
// plumbing, such as a missing completion, and never for a wrong answer.
//
// A subject that can give the same state hash as objectstore.Spec also has the
// method StateHash() uint64 (spectest.HasStateHash). The run then compares it
// with the spec hash after every step. A subject with no spec hash, for
// example real S3, has no such method.
type ObjectSubject = spectest.Subject[objectstore.Call, ObjectResult]

// ControlPlaneSubject is a control plane under test. Apply runs one operation.
// For CreateTopic the suite compares only Code, because the direct call has no
// Outcome in every layer. The error is for broken test plumbing only.
// StateHash is optional, like the one of ObjectSubject, with
// controlplane/spec.ControlPlane as the spec.
type ControlPlaneSubject = spectest.Subject[ControlPlaneTestStep, ControlPlaneResult]

// hasStateHash is spectest.HasStateHash, to ask a subject for its hash.
type hasStateHash = spectest.HasStateHash

// withoutHashObject hides the state hash of a subject. It embeds the interface
// type, so only Apply is promoted.
type withoutHashObject struct{ ObjectSubject }

// WithoutStateHashObject returns s with its state hash hidden, as if s were
// real S3.
func WithoutStateHashObject(s ObjectSubject) ObjectSubject { return withoutHashObject{s} }

type withoutHashControlPlane struct{ ControlPlaneSubject }

// WithoutStateHashControlPlane returns s with its state hash hidden.
func WithoutStateHashControlPlane(s ControlPlaneSubject) ControlPlaneSubject {
	return withoutHashControlPlane{s}
}

// specObjectSubject is the object store spec as a subject.
type specObjectSubject struct{ store *objectstore.Spec }

// NewSpecObjectSubject returns a fresh objectstore.Spec as a subject. Use it
// to run a suite spec against spec, and as the correct subject that a planted
// bug wraps.
func NewSpecObjectSubject() ObjectSubject {
	return &specObjectSubject{store: objectstore.NewSpec()}
}

func (s *specObjectSubject) Apply(operation objectstore.Call) (ObjectResult, error) {
	return FromObjectResult(s.store.Exec(operation)), nil
}

func (s *specObjectSubject) StateHash() uint64 { return s.store.StateHash() }

// specControlPlaneSubject is the control plane spec as a subject.
type specControlPlaneSubject struct{ cp *specbridge.ControlPlane }

// NewSpecControlPlaneSubject returns a fresh controlplane/spec.ControlPlane as
// a subject.
func NewSpecControlPlaneSubject() ControlPlaneSubject {
	return &specControlPlaneSubject{cp: specbridge.New()}
}

func (s *specControlPlaneSubject) Apply(operation ControlPlaneTestStep) (ControlPlaneResult, error) {
	switch {
	case operation.CreateTopic != nil:
		code := s.cp.CreateTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)
		return ControlPlaneResult{Code: code}, nil
	case operation.Call != nil:
		done, ok := s.cp.Exec(*operation.Call).(core.ControlPlaneDone)
		if !ok {
			return ControlPlaneResult{}, errors.New("the spec control plane did not return a CPDone")
		}
		return FromControlPlaneDone(done), nil
	}
	return ControlPlaneResult{}, errors.New("the operation has neither CreateTopic nor Call")
}

func (s *specControlPlaneSubject) StateHash() uint64 { return s.cp.StateHash() }
