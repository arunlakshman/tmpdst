package suites

import (
	"fmt"

	"mds/internal/core"
	"mds/internal/verify/speccheck"
	"mds/internal/verify/speccheck/checker"
	"mds/internal/verify/speccheck/checker/generator"
	"mds/internal/verify/speccheck/checker/observation"
)

// ControlPlanePair is a fresh spec, a fresh subject and a naive model of the
// control plane suite.
type ControlPlanePair struct {
	spec        *speccheck.ControlPlane
	specSubject ControlPlaneSubject
	subject     ControlPlaneSubject
	model       *controlPlaneModel
	extra       core.RequestID
}

func (p *ControlPlanePair) nextReq() core.RequestID {
	p.extra++
	return extraReqBase + p.extra
}

// ControlPlaneComparator returns the comparator of the control plane suite. A
// caller can run one seed with it, for example to replay a failure. Each call
// of newSubject must return an empty subject.
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
func ControlPlaneComparator(p generator.InputProfile, newSubject func() ControlPlaneSubject) checker.SpecChecker[*ControlPlanePair, generator.ControlPlaneTestStep] {
	return checker.SpecChecker[*ControlPlanePair, generator.ControlPlaneTestStep]{
		Name: "generator.ControlPlaneTestStep",
		New: func() *ControlPlanePair {
			spec := speccheck.NewControlPlane()
			return &ControlPlanePair{
				spec:        spec,
				specSubject: &specControlPlaneSubject{cp: spec},
				subject:     newSubject(),
				model:       newCPModel(),
			}
		},
		Gen:           generator.ControlPlaneOps(p),
		Step:          controlPlaneStep,
		Invariant:     controlPlaneInvariant,
		Shrink:        generator.MinimizeControlPlaneOp,
		Format:        generator.FormatControlPlaneOp,
		Ops:           suiteOps,
		MaxShrinkRuns: suiteMinimizeRuns,
	}
}

// ControlPlane runs the control plane suite for profile p on the seeds that
// opts chooses. A caller runs it once for each profile in
// AllControlPlaneProfiles.
func ControlPlane(t checker.Reporter, p generator.InputProfile, newSubject func() ControlPlaneSubject, opts checker.Options) {
	t.Helper()
	checker.Run(t, ControlPlaneComparator(p, newSubject), opts)
}

// diffControlPlane compares two results. For CreateTopic it compares the code
// only.
func diffControlPlane(operation generator.ControlPlaneTestStep, expected, actual observation.ControlPlaneResult) string {
	if operation.CreateTopic != nil {
		if expected.Code != actual.Code {
			return fmt.Sprintf("The field code differs: the expected result has %v, but the actual result has %v.",
				expected.Code, actual.Code)
		}
		return ""
	}
	return observation.DiffControlPlane(expected, actual)
}

func controlPlaneStep(p *ControlPlanePair, operation generator.ControlPlaneTestStep) error {
	text := generator.FormatControlPlaneOp(operation)
	specBefore, _ := p.specSubject.StateHash()
	subjectBefore, subjectHasHash := p.subject.StateHash()

	specResult, err := p.specSubject.Do(operation)
	if err != nil {
		return fmt.Errorf("the spec could not run %s: %w", text, err)
	}
	subjectResult, err := p.subject.Do(operation)
	if err != nil {
		return fmt.Errorf("the subject could not run %s: %w", text, err)
	}
	if diff := diffControlPlane(operation, specResult, subjectResult); diff != "" {
		return fmt.Errorf("for %s: %s The spec result is: %s. The subject result is: %s",
			text, diff, specResult.Describe(), subjectResult.Describe())
	}

	expected, changed := p.modelApply(operation)
	if diff := diffControlPlane(operation, expected, specResult); diff != "" {
		return fmt.Errorf("for %s: the spec disagrees with the naive model. %s The model result is: %s. The spec result is: %s",
			text, diff, expected.Describe(), specResult.Describe())
	}

	specAfter, _ := p.specSubject.StateHash()
	if !changed && specAfter != specBefore {
		return fmt.Errorf("after %s the spec state hash changed from %#x to %#x, but this call must not change the state",
			text, specBefore, specAfter)
	}
	if !subjectHasHash {
		return nil
	}
	subjectAfter, _ := p.subject.StateHash()
	if subjectAfter != specAfter {
		return fmt.Errorf("after %s the subject state hash is %#x, but the spec state hash is %#x",
			text, subjectAfter, specAfter)
	}
	if !changed && subjectAfter != subjectBefore {
		return fmt.Errorf("after %s the subject state hash changed from %#x to %#x, but this call must not change the state",
			text, subjectBefore, subjectAfter)
	}
	return nil
}

func (p *ControlPlanePair) modelApply(operation generator.ControlPlaneTestStep) (observation.ControlPlaneResult, bool) {
	switch {
	case operation.CreateTopic != nil:
		return p.model.createTopic(operation.CreateTopic.Name, operation.CreateTopic.Partitions)
	case operation.Call != nil:
		return p.model.apply(operation.Call.Op)
	}
	return observation.ControlPlaneResult{Code: core.ErrInvalid}, false
}

func controlPlaneInvariant(p *ControlPlanePair) error {
	for _, who := range []struct {
		name    string
		subject ControlPlaneSubject
	}{{"spec", p.specSubject}, {"subject", p.subject}} {
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
	result, err := subject.Do(generator.NewCall(core.ControlPlaneCall{Req: p.nextReq(), Op: core.FindSegments{TopicPartition: topicPartition}}))
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
	result, err := subject.Do(generator.NewCall(core.ControlPlaneCall{Req: p.nextReq(), Op: core.DescribeTopics{}}))
	if err != nil {
		return fmt.Errorf("the %s could not describe the topics: %w", who, err)
	}
	expected := observation.ControlPlaneResult{Topics: p.model.topicList()}
	if diff := observation.DiffControlPlane(expected, result); diff != "" {
		return fmt.Errorf("the %s describes the topics wrongly. %s", who, diff)
	}
	return nil
}
