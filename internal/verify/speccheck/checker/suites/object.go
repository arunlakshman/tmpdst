package suites

import (
	"bytes"
	"fmt"
	"slices"

	"mds/internal/core"
	"mds/internal/verify/speccheck"
	"mds/internal/verify/speccheck/checker"
	"mds/internal/verify/speccheck/checker/generator"
	"mds/internal/verify/speccheck/checker/observation"
)

const (
	// suiteOps is the number of operations in each generated sequence.
	suiteOps = 120
	// suiteMinimizeRuns is the most sequences that shrinking may run.
	suiteMinimizeRuns = 300
	// extraReqBase is the first request number of the extra calls that the
	// suites make for invariants. Generated operations use small numbers, so
	// the two groups never share a request number.
	extraReqBase core.RequestID = 1 << 30
)

// ObjectPair is a fresh spec and a fresh subject of the object store suite.
type ObjectPair struct {
	spec    *speccheck.ObjectStore
	subject ObjectSubject
	extra   core.RequestID
}

func (p *ObjectPair) nextReq() core.RequestID {
	p.extra++
	return extraReqBase + p.extra
}

// ObjectStoreComparator returns the comparator of the object store suite. A
// caller can run one seed with it, for example to replay a failure. Each call
// of newSubject must return an empty subject.
//
// Checks after every step: the subject result equals the spec result; the
// state hash equals the spec hash when the subject has one; a List with the
// empty prefix returns the sorted keys of the spec. The suite gives the
// subject its own copy of the data of a Put and writes over that copy after the
// call, so a subject that keeps the caller's slice returns wrong data later.
func ObjectStoreComparator(p generator.InputProfile, newSubject func() ObjectSubject) checker.SpecChecker[*ObjectPair, core.IOCall] {
	return checker.SpecChecker[*ObjectPair, core.IOCall]{
		Name:          "core.IOCall",
		New:           func() *ObjectPair { return &ObjectPair{spec: speccheck.NewObjectStore(), subject: newSubject()} },
		Gen:           generator.ObjectOps(p),
		Step:          objectStep,
		Invariant:     objectInvariant,
		Shrink:        generator.MinimizeObjectOp,
		Format:        generator.FormatObjectOp,
		Ops:           suiteOps,
		MaxShrinkRuns: suiteMinimizeRuns,
	}
}

// ObjectStore runs the object store suite for profile p on the seeds that opts
// chooses. A caller runs it once for each profile in AllObjectProfiles.
func ObjectStore(t checker.Reporter, p generator.InputProfile, newSubject func() ObjectSubject, opts checker.Options) {
	t.Helper()
	checker.Run(t, ObjectStoreComparator(p, newSubject), opts)
}

func objectStep(p *ObjectPair, op core.IOCall) error {
	specResult := observation.FromObjectCompletion(p.spec.Exec(op))
	subjectOp, overwrite := objectOpForSubject(op)
	subjectResult, err := p.subject.Do(subjectOp)
	overwrite()
	text := generator.FormatObjectOp(op)
	if err != nil {
		return fmt.Errorf("the subject could not run %s: %w", text, err)
	}
	if diff := observation.Diff(specResult, subjectResult); diff != "" {
		return fmt.Errorf("for %s: %s The spec result is: %s. The subject result is: %s",
			text, diff, specResult.Describe(), subjectResult.Describe())
	}
	if hash, ok := p.subject.StateHash(); ok {
		if specHash := p.spec.StateHash(); hash != specHash {
			return fmt.Errorf("after %s the subject state hash is %#x, but the spec state hash is %#x", text, hash, specHash)
		}
	}
	return nil
}

// objectOpForSubject returns op with its own copy of the Put data, and a
// function that writes over that copy. The copy keeps the generated sequence
// intact for replay while still testing that the subject copies its input.
func objectOpForSubject(op core.IOCall) (core.IOCall, func()) {
	put, ok := op.(core.ObjectPut)
	if !ok {
		return op, func() {}
	}
	put.Data = bytes.Clone(put.Data)
	return put, func() {
		for i := range put.Data {
			put.Data[i] = '!'
		}
	}
}

func objectInvariant(p *ObjectPair) error {
	result, err := p.subject.Do(core.ObjectList{Req: p.nextReq()})
	if err != nil {
		return fmt.Errorf("the subject could not list all keys: %w", err)
	}
	specKeys := p.spec.Keys()
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
