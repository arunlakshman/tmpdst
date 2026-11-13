package conformance

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/check/spectest"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// suiteSteps is the average number of steps in one check of a suite. The old
// framework ran lists of exactly 120 operations; rapid draws lists whose length
// is random, with this average.
const suiteSteps = 120

// extraReqBase is the first request number of the extra calls that the suites
// make for invariants. The calls of the lockstep list use small numbers (1, 2,
// 3 and so on), so the two groups never share a request number.
const extraReqBase machine.RequestID = 1 << 30

// ObjectStoreLockstep returns the lockstep check of the object store suite for
// profile p. Rapid draws a list of operations (ObjectOps), and each one runs on
// a fresh objectstore.Spec and on the subject. A caller sets Checks, and runs
// Check, or RunSeed for a planted bug. Each call of newSubject must return an
// empty subject.
//
// Checks after every step: the subject result equals the spec result; the
// state hash equals the spec hash when the subject has one; a List with the
// empty prefix returns the sorted keys of the spec. The suite gives the
// subject its own copy of the data of a Put and writes over that copy after the
// call, so a subject that keeps the caller's slice returns wrong data later.
func ObjectStoreLockstep(p InputProfile, newSubject func() ObjectSubject) spectest.Lockstep[objectstore.Call, ObjectResult, *ObjectPair] {
	return spectest.Lockstep[objectstore.Call, ObjectResult, *ObjectPair]{
		Name: "objectstore.Call",
		NewSpec: func() spectest.Spec[objectstore.Call, ObjectResult] {
			return objectSpec{store: objectstore.NewSpec()}
		},
		NewSubject: func(*rapid.T) *ObjectPair {
			return &ObjectPair{mirror: objectstore.NewSpec(), subject: newSubject()}
		},
		Ops:           ObjectOps(p),
		Equal:         equalObject,
		Invariant:     (*ObjectPair).invariant,
		Format:        FormatObjectOp,
		Rerun:         spectest.Rerun{EnvPrefix: EnvPrefix},
		StepsPerCheck: suiteSteps,
	}
}

// objectSpec is the object store spec as the spec of the lockstep run.
type objectSpec struct{ store *objectstore.Spec }

func (s objectSpec) Apply(operation objectstore.Call) ObjectResult {
	return FromObjectResult(s.store.Exec(operation))
}

func (s objectSpec) StateHash() uint64 { return s.store.StateHash() }

func equalObject(want, got ObjectResult) error {
	if diff := Diff(want, got); diff != "" {
		return errors.New(diff)
	}
	return nil
}

// ObjectPair is the subject of one run of the object store suite. It holds the
// subject under test, and its own mirror of the spec (the lockstep gives an
// invariant only the subject, but the invariant needs the keys of the spec).
// It gives each call the request number of its position.
type ObjectPair struct {
	mirror  *objectstore.Spec
	subject ObjectSubject
	calls   machine.RequestID // calls made so far
	extra   machine.RequestID // extra calls made so far
}

func (p *ObjectPair) nextReq() machine.RequestID {
	p.extra++
	return extraReqBase + p.extra
}

// Apply numbers the call, runs it on the mirror and on the subject, and
// returns the subject result.
func (p *ObjectPair) Apply(operation objectstore.Call) (ObjectResult, error) {
	p.calls++
	operation = withRequest(operation, p.calls)
	p.mirror.Exec(operation)
	subjectOp, overwrite := objectOpForSubject(operation)
	result, err := p.subject.Apply(subjectOp)
	overwrite()
	return result, err
}

// StateHash is the hash of the subject. A subject with no spec hash cannot show
// one, so the run uses the hash of the mirror, which always equals the hash of
// the spec: there is nothing to compare.
func (p *ObjectPair) StateHash() uint64 {
	if h, ok := p.subject.(hasStateHash); ok {
		return h.StateHash()
	}
	return p.mirror.StateHash()
}

// withRequest returns operation with the request number n.
func withRequest(operation objectstore.Call, n machine.RequestID) objectstore.Call {
	switch operation := operation.(type) {
	case objectstore.Put:
		operation.Req = n
		return operation
	case objectstore.Get:
		operation.Req = n
		return operation
	case objectstore.Delete:
		operation.Req = n
		return operation
	case objectstore.List:
		operation.Req = n
		return operation
	}
	return operation
}

// objectOpForSubject returns op with its own copy of the Put data, and a
// function that writes over that copy. The copy keeps the generated list
// intact for replay while still testing that the subject copies its input.
func objectOpForSubject(operation objectstore.Call) (objectstore.Call, func()) {
	put, ok := operation.(objectstore.Put)
	if !ok {
		return operation, func() {}
	}
	put.Data = bytes.Clone(put.Data)
	return put, func() {
		for i := range put.Data {
			put.Data[i] = '!'
		}
	}
}

func (p *ObjectPair) invariant() error {
	result, err := p.subject.Apply(objectstore.List{Req: p.nextReq()})
	if err != nil {
		return fmt.Errorf("the subject could not list all keys: %w", err)
	}
	specKeys := p.mirror.Keys()
	if result.Code != core.OK {
		return fmt.Errorf("listing all keys returned the code %v, but it must return OK", result.Code)
	}
	if !slices.IsSorted(result.Keys) {
		return fmt.Errorf("the key list %q is not sorted", result.Keys)
	}
	if !slices.Equal(specKeys, result.Keys) && (len(specKeys) != 0 || len(result.Keys) != 0) {
		return fmt.Errorf("the subject lists %d keys %q, but the spec lists %d keys %q",
			len(result.Keys), result.Keys, len(specKeys), specKeys)
	}
	return nil
}
