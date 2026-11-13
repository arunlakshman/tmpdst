package conformance

import (
	"errors"
	"fmt"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specbridge"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/spectest"
	"github.com/manifesterio/mds/dst/machine"
)

// ControlPlaneLockstep returns the lockstep check of the control plane suite
// for profile p. Rapid draws a list of operations (ControlPlaneOps), and each
// one runs on a fresh spec and on the subject. A caller sets Checks, and runs
// Check, or RunSeed for a planted bug. Each call of newSubject must return an
// empty subject.
//
// Checks after every step:
//
//   - The subject result equals the spec result. For CreateTopic only the
//     code is compared.
//   - The state hash of the subject equals the spec hash, when the subject has
//     one.
//   - The spec result equals the result of a second, naive model. The model
//     keeps its own list of runs and adds up the counts to find an end offset.
//   - A commit that repeats a segment key with the same batches returns the
//     first answer again (idempotent commit). The same key with different
//     batches returns ErrConflict. An invalid commit returns ErrInvalid. None
//     of the three changes the state hash of the spec or of the subject. A
//     lookup or a describe never changes it either.
//   - For every partition that has a commit, on the spec and on the subject:
//     the runs start at offset 0, each run starts where the one before ends
//     (no gap and no overlap), the end offset equals the end of the last run,
//     and the end offset equals the number of committed records.
//   - DescribeTopics with no names returns the topics of the model, sorted by
//     name, so a partition count never shrinks.
//   - Commits are all or nothing: an invalid batch in a commit leaves no run
//     behind, which the run check above finds.
func ControlPlaneLockstep(p InputProfile, newSubject func() ControlPlaneSubject) spectest.Lockstep[ControlPlaneTestStep, ControlPlaneResult, *ControlPlanePair] {
	return spectest.Lockstep[ControlPlaneTestStep, ControlPlaneResult, *ControlPlanePair]{
		Name: "conformance.ControlPlaneTestStep",
		NewSpec: func() spectest.Spec[ControlPlaneTestStep, ControlPlaneResult] {
			return controlPlaneSpec{inner: &specControlPlaneSubject{cp: specbridge.New()}}
		},
		NewSubject: func(*rapid.T) *ControlPlanePair {
			return &ControlPlanePair{
				mirror:  &specControlPlaneSubject{cp: specbridge.New()},
				subject: newSubject(),
				model:   newCPModel(),
			}
		},
		Ops:           ControlPlaneOps(p),
		Equal:         equalControlPlane,
		Invariant:     (*ControlPlanePair).invariant,
		Format:        FormatControlPlaneOp,
		Rerun:         spectest.Rerun{EnvPrefix: EnvPrefix},
		StepsPerCheck: suiteSteps,
	}
}

// controlPlaneSpec is the control plane spec as the spec of the lockstep run.
type controlPlaneSpec struct{ inner *specControlPlaneSubject }

func (s controlPlaneSpec) Apply(operation ControlPlaneTestStep) ControlPlaneResult {
	r, err := s.inner.Apply(operation)
	if err != nil {
		// The spec subject fails only when the test builds a wrong operation.
		panic("conformance: " + err.Error())
	}
	return r
}

func (s controlPlaneSpec) StateHash() uint64 { return s.inner.StateHash() }

func equalControlPlane(want, got ControlPlaneResult) error {
	if diff := DiffControlPlane(want, got); diff != "" {
		return errors.New(diff)
	}
	return nil
}

// ControlPlanePair is the subject of one run of the control plane suite. It
// holds the subject under test, a mirror of the spec and the naive model (the
// lockstep gives an invariant only the subject, but the checks need the spec
// and the model too). It gives each call the request number of its position.
//
// A rule that breaks inside Apply cannot be returned as an answer, so Apply
// keeps it in problem, and the invariant of the same step reports it.
type ControlPlanePair struct {
	mirror  ControlPlaneSubject
	subject ControlPlaneSubject
	model   *controlPlaneModel
	calls   machine.RequestID // calls made so far
	extra   machine.RequestID // extra calls made so far
	problem error             // the first broken rule found in Apply
}

func (p *ControlPlanePair) nextReq() machine.RequestID {
	p.extra++
	return extraReqBase + p.extra
}

// stateHashOf returns the hash of s. It returns false when s has no hash.
func stateHashOf(s any) (uint64, bool) {
	h, ok := s.(hasStateHash)
	if !ok {
		return 0, false
	}
	return h.StateHash(), true
}

// StateHash is the hash of the subject. A subject with no spec hash cannot
// show one, so the run uses the hash of the mirror, which always equals the
// hash of the spec: there is nothing to compare.
func (p *ControlPlanePair) StateHash() uint64 {
	if h, ok := stateHashOf(p.subject); ok {
		return h
	}
	h, _ := stateHashOf(p.mirror)
	return h
}

// withRequestID returns operation with the request number n. A direct
// CreateTopic has no request number.
func withRequestID(operation ControlPlaneTestStep, n machine.RequestID) ControlPlaneTestStep {
	if operation.Call == nil {
		return operation
	}
	call := *operation.Call
	call.Req = n
	return NewCall(call)
}

// Apply numbers the call and runs it on the subject. It also runs it on the
// mirror and the model, and keeps the first rule that breaks (see problem).
func (p *ControlPlanePair) Apply(operation ControlPlaneTestStep) (ControlPlaneResult, error) {
	p.calls++
	operation = withRequestID(operation, p.calls)
	text := FormatControlPlaneOp(operation)
	mirrorBefore, _ := stateHashOf(p.mirror)
	subjectBefore, subjectHasHash := stateHashOf(p.subject)

	mirrorResult, err := p.mirror.Apply(operation)
	if err != nil {
		return ControlPlaneResult{}, fmt.Errorf("the spec could not run %s: %w", text, err)
	}
	subjectResult, err := p.subject.Apply(operation)
	if err != nil {
		return ControlPlaneResult{}, err
	}

	expected, changed := p.modelApply(operation)
	if diff := diffControlPlane(operation, expected, mirrorResult); diff != "" {
		p.fail(fmt.Errorf("for %s: the spec disagrees with the naive model. %s The model result is: %s. The spec result is: %s",
			text, diff, expected.Describe(), mirrorResult.Describe()))
	}
	if after, _ := stateHashOf(p.mirror); !changed && after != mirrorBefore {
		p.fail(fmt.Errorf("after %s the spec state hash changed from %#x to %#x, but this call must not change the state",
			text, mirrorBefore, after))
	}
	if after, ok := stateHashOf(p.subject); subjectHasHash && ok && !changed && after != subjectBefore {
		p.fail(fmt.Errorf("after %s the subject state hash changed from %#x to %#x, but this call must not change the state",
			text, subjectBefore, after))
	}
	if operation.CreateTopic != nil {
		// A direct CreateTopic is compared by its code only.
		subjectResult = ControlPlaneResult{Code: subjectResult.Code}
	}
	return subjectResult, nil
}

// fail keeps the first broken rule.
func (p *ControlPlanePair) fail(err error) {
	if p.problem == nil {
		p.problem = err
	}
}

// diffControlPlane compares two results. For CreateTopic it compares the code
// only.
func diffControlPlane(operation ControlPlaneTestStep, expected, actual ControlPlaneResult) string {
	if operation.CreateTopic != nil {
		if expected.Code != actual.Code {
			return fmt.Sprintf("The field code differs: the expected result has %v, but the actual result has %v.",
				expected.Code, actual.Code)
		}
		return ""
	}
	return DiffControlPlane(expected, actual)
}

func (p *ControlPlanePair) modelApply(operation ControlPlaneTestStep) (ControlPlaneResult, bool) {
	switch {
	case operation.CreateTopic != nil:
		return p.model.createTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)
	case operation.Call != nil:
		return p.model.apply(operation.Call.Op)
	}
	return ControlPlaneResult{Code: core.ErrInvalid}, false
}

func (p *ControlPlanePair) invariant() error {
	if p.problem != nil {
		return p.problem
	}
	for _, who := range []struct {
		name    string
		subject ControlPlaneSubject
	}{{"spec", p.mirror}, {"subject", p.subject}} {
		for _, topicPartition := range p.model.partitions() {
			if err := p.checkRuns(who.name, who.subject, topicPartition); err != nil {
				return err
			}
		}
		if err := p.checkTopics(who.name, who.subject); err != nil {
			return err
		}
	}
	return nil
}

// checkRuns looks up a whole partition and checks that its runs have no gap
// and no overlap and that the end offset equals the committed record count.
func (p *ControlPlanePair) checkRuns(who string, subject ControlPlaneSubject, topicPartition core.TopicPartition) error {
	result, err := subject.Apply(NewCall(core.ControlPlaneCall{Req: p.nextReq(), Op: contract.FindSegments{TopicPartition: topicPartition}}))
	if err != nil {
		return fmt.Errorf("the %s could not look up %v: %w", who, topicPartition, err)
	}
	if result.Code != core.OK {
		return fmt.Errorf("the %s returned the code %v for a lookup of %v from offset 0, but it must return OK", who, result.Code, topicPartition)
	}
	committed := p.model.end(topicPartition)
	next, sum := core.Offset(0), int64(0)
	for i, r := range result.Ranges {
		if r.Base != next {
			return fmt.Errorf("the %s returned run %d of %v at offset %d, but the previous run ends at %d, so offsets are not contiguous",
				who, i, topicPartition, r.Base, next)
		}
		next, sum = r.Base+core.Offset(r.Count), sum+r.Count
	}
	if result.End != next || int64(result.End) != committed || sum != committed {
		return fmt.Errorf("the %s returned for %v the end offset %d, the end of the last run %d and %d records in the runs, but %d records were committed",
			who, topicPartition, result.End, next, sum, committed)
	}
	return nil
}

func (p *ControlPlanePair) checkTopics(who string, subject ControlPlaneSubject) error {
	result, err := subject.Apply(NewCall(core.ControlPlaneCall{Req: p.nextReq(), Op: contract.DescribeTopics{}}))
	if err != nil {
		return fmt.Errorf("the %s could not describe the topics: %w", who, err)
	}
	expected := ControlPlaneResult{Topics: p.model.topicList()}
	if diff := DiffControlPlane(expected, result); diff != "" {
		return fmt.Errorf("the %s describes the topics wrongly. %s", who, diff)
	}
	return nil
}
