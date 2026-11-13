package injectedbug

import (
	"slices"
	"strconv"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
)

// ControlPlaneBug is a planted bug for a control plane.
type ControlPlaneBug = Bug[conformance.ControlPlaneTestStep, conformance.ControlPlaneResult]

// NewControlPlaneBug returns a named bug. Wrap is called once for each fresh
// subject, so the wrapper may keep state.
func NewControlPlaneBug(name, doc string, wrap func(conformance.ControlPlaneSubject) conformance.ControlPlaneSubject) ControlPlaneBug {
	return NewBug(name, doc, wrap)
}

// changeFunc changes the result of one operation after the correct subject has
// answered.
type changeFunc func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult

func afterControlPlane(name, doc string, change changeFunc) ControlPlaneBug {
	return after(name, doc, change)
}

// statefulControlPlane makes a bug whose change function keeps state.
// newChange is called once for each fresh subject.
func statefulControlPlane(name, doc string, newChange func() changeFunc) ControlPlaneBug {
	return stateful(name, doc, func(conformance.ControlPlaneSubject) func(conformance.ControlPlaneTestStep, conformance.ControlPlaneResult) conformance.ControlPlaneResult {
		return newChange()
	})
}

// beforeControlPlane makes a bug that changes the operation before the correct
// subject sees it. change may keep state and may call inner more than once.
func beforeControlPlane(name, doc string, newChange func() func(inner conformance.ControlPlaneSubject, operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error)) ControlPlaneBug {
	return NewControlPlaneBug(name, doc, func(inner conformance.ControlPlaneSubject) conformance.ControlPlaneSubject {
		change := newChange()
		return wrap(inner, func(operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
			return change(inner, operation)
		}, nil)
	})
}

// commitOf returns the CommitSegment of op, if op is one.
func commitOf(operation conformance.ControlPlaneTestStep) (core.ControlPlaneCall, contract.CommitSegment, bool) {
	if operation.Call == nil {
		return core.ControlPlaneCall{}, contract.CommitSegment{}, false
	}
	c, ok := operation.Call.Op.(contract.CommitSegment)
	return *operation.Call, c, ok
}

// ControlPlaneBugs returns every planted control plane bug, in a fixed order.
func ControlPlaneBugs() []ControlPlaneBug {
	return []ControlPlaneBug{
		BaseOffByOne(),
		ReplayGivesFreshOffsets(),
		ReplayWithDifferentRequestNotRejected(),
		PartialApplyOnInvalidBatch(),
		LookupMaxBytesUsesGreater(),
		LookupSkipsRunContainingFrom(),
		DescribeUnsorted(),
		TopicPartitionCountShrinks(),
		ControlPlaneStateHashDiffers(),
	}
}

// BaseOffByOne adds one to every assigned base offset.
func BaseOffByOne() ControlPlaneBug {
	return afterControlPlane("BaseOffByOne",
		"A commit returns base offsets that are one too high.",
		func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
			if _, _, ok := commitOf(operation); ok && len(r.Assigned) > 0 {
				r.Assigned = slices.Clone(r.Assigned)
				for i := range r.Assigned {
					r.Assigned[i].Base++
				}
			}
			return r
		})
}

// ReplayGivesFreshOffsets treats a repeated segment key as a new commit.
func ReplayGivesFreshOffsets() ControlPlaneBug {
	return beforeControlPlane("ReplayGivesFreshOffsets",
		"A commit with a repeated segment key gets new offsets instead of the first answer.",
		func() func(conformance.ControlPlaneSubject, conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
			n := 0
			return func(inner conformance.ControlPlaneSubject, operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
				if call, commit, ok := commitOf(operation); ok && commit.SegmentKey != "" {
					n++
					commit.SegmentKey += "#" + strconv.Itoa(n)
					call.Op = commit
					operation = conformance.NewCall(call)
				}
				return inner.Apply(operation)
			}
		})
}

// ReplayWithDifferentRequestNotRejected answers a changed request under a used
// key with the first answer, instead of ErrConflict.
func ReplayWithDifferentRequestNotRejected() ControlPlaneBug {
	return statefulControlPlane("ReplayWithDifferentRequestNotRejected",
		"A commit that reuses a segment key with different batches returns the first answer instead of ErrConflict.",
		func() changeFunc {
			first := map[string][]contract.BatchRange{}
			return func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
				_, commit, ok := commitOf(operation)
				if !ok {
					return r
				}
				if _, seen := first[commit.SegmentKey]; !seen && r.Code == core.OK {
					first[commit.SegmentKey] = r.Assigned
				}
				if r.Code == core.ErrConflict {
					return conformance.ControlPlaneResult{Assigned: first[commit.SegmentKey], Outcome: core.Applied}
				}
				return r
			}
		})
}

// PartialApplyOnInvalidBatch commits the valid batches in front of an invalid
// one, and then rejects the request.
func PartialApplyOnInvalidBatch() ControlPlaneBug {
	return beforeControlPlane("PartialApplyOnInvalidBatch",
		"A commit with an invalid batch applies the valid batches before it, instead of nothing.",
		func() func(conformance.ControlPlaneSubject, conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
			n := machine.RequestID(0)
			return func(inner conformance.ControlPlaneSubject, operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
				call, commit, ok := commitOf(operation)
				if ok && commit.SegmentKey != "" {
					valid := 0
					for valid < len(commit.Batches) && commit.Batches[valid].Records > 0 &&
						commit.Batches[valid].TopicPartition.Topic != "" && commit.Batches[valid].TopicPartition.Partition >= 0 {
						valid++
					}
					if valid > 0 && valid < len(commit.Batches) {
						n++
						prefix := contract.CommitSegment{SegmentKey: commit.SegmentKey + "#partial", Batches: commit.Batches[:valid]}
						extra := core.ControlPlaneCall{Req: call.Req + 1<<28 + n, Op: prefix}
						if _, err := inner.Apply(conformance.NewCall(extra)); err != nil {
							return conformance.ControlPlaneResult{}, err
						}
					}
				}
				return inner.Apply(operation)
			}
		})
}

// LookupMaxBytesUsesGreater stops one batch late when the byte total equals
// MaxBytes. The comparison bytes > max is the same as bytes >= max + 1.
func LookupMaxBytesUsesGreater() ControlPlaneBug {
	return beforeControlPlane("LookupMaxBytesUsesGreater",
		"Lookup stops when the byte total is greater than MaxBytes, not when it is equal or greater.",
		func() func(conformance.ControlPlaneSubject, conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
			return func(inner conformance.ControlPlaneSubject, operation conformance.ControlPlaneTestStep) (conformance.ControlPlaneResult, error) {
				if operation.Call != nil {
					if l, ok := operation.Call.Op.(contract.FindSegments); ok && l.MaxBytes > 0 && l.MaxBytes < 1<<30 {
						l.MaxBytes++
						operation = conformance.NewCall(core.ControlPlaneCall{Req: operation.Call.Req, Op: l})
					}
				}
				return inner.Apply(operation)
			}
		})
}

// LookupSkipsRunContainingFrom drops a run that starts before From, although
// it holds offset From.
func LookupSkipsRunContainingFrom() ControlPlaneBug {
	return afterControlPlane("LookupSkipsRunContainingFrom",
		"Lookup leaves out the run that starts before From but contains offset From.",
		func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
			if operation.Call == nil {
				return r
			}
			l, ok := operation.Call.Op.(contract.FindSegments)
			if !ok || r.Code != core.OK {
				return r
			}
			var kept []contract.BatchRange
			for _, run := range r.Ranges {
				if run.Base >= l.From {
					kept = append(kept, run)
				}
			}
			r.Ranges = kept
			return r
		})
}

// DescribeUnsorted returns topics in reverse name order.
func DescribeUnsorted() ControlPlaneBug {
	return afterControlPlane("DescribeUnsorted",
		"DescribeTopics returns the topics in reverse order instead of sorted by name.",
		func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
			if operation.Call != nil {
				if _, ok := operation.Call.Op.(contract.DescribeTopics); ok {
					r.Topics = slices.Clone(r.Topics)
					slices.Reverse(r.Topics)
				}
			}
			return r
		})
}

// TopicPartitionCountShrinks lets CreateTopic lower a partition count. It keeps
// its own counts and reports them in DescribeTopics.
func TopicPartitionCountShrinks() ControlPlaneBug {
	return statefulControlPlane("TopicPartitionCountShrinks",
		"CreateTopic with a smaller count lowers the partition count of the topic instead of keeping the larger one.",
		func() changeFunc {
			counts := map[string]int32{}
			return func(operation conformance.ControlPlaneTestStep, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
				switch {
				case operation.CreateTopic != nil:
					if r.Code == core.OK {
						counts[operation.CreateTopic.Name] = operation.CreateTopic.Partitions
					}
				case operation.Call != nil:
					r = minimizeCountsForCall(counts, operation.Call.Op, r)
				}
				return r
			}
		})
}

// minimizeCountsForCall updates the own partition counts of the bug after a
// commit, and replaces the counts in a describe result with them.
func minimizeCountsForCall(counts map[string]int32, operation contract.Operation, r conformance.ControlPlaneResult) conformance.ControlPlaneResult {
	switch operation.(type) {
	case contract.CommitSegment:
		for _, a := range r.Assigned {
			counts[a.TopicPartition.Topic] = max(counts[a.TopicPartition.Topic], a.TopicPartition.Partition+1)
		}
	case contract.DescribeTopics:
		r.Topics = slices.Clone(r.Topics)
		for i, topic := range r.Topics {
			if n, ok := counts[topic.Name]; ok {
				r.Topics[i].Partitions = n
			}
		}
	}
	return r
}

// ControlPlaneStateHashDiffers reports a wrong state hash. It checks that the
// suite compares the hash at all. It has no effect on a subject with no hash.
func ControlPlaneStateHashDiffers() ControlPlaneBug {
	return stateHashDiffers[conformance.ControlPlaneTestStep, conformance.ControlPlaneResult]()
}

// controlPlaneRunSeed runs the control plane suite on one profile with one seed.
func controlPlaneRunSeed(p conformance.InputProfile, newSubject func() conformance.ControlPlaneSubject, seed uint64) (string, int, bool) {
	return failureOf(conformance.ControlPlaneLockstep(p, newSubject).RunSeed(seed))
}

// ControlPlaneBugsCaught runs every planted control plane bug through the
// control plane suite, on every profile, for the seeds 1 to budget. It fails t
// for each bug that no seed catches. newCorrect must return a fresh correct
// subject on each call. A subject with no spec hash cannot show a wrong hash,
// so that one bug is skipped for such a subject and the log says so.
func ControlPlaneBugsCaught(t testing.TB, newCorrect func() conformance.ControlPlaneSubject, budget int) {
	t.Helper()
	bugsCaught(t, "control plane", ControlPlaneBugs(), newCorrect, budget, controlPlaneRunSeed)
}

// ControlPlaneSuiteIsClean runs the control plane suite on newCorrect, with no
// bug, on every profile, for the seeds 1 to budget. It fails t on the first
// failure: a failure here is a false alarm.
func ControlPlaneSuiteIsClean(t testing.TB, newCorrect func() conformance.ControlPlaneSubject, budget int) {
	t.Helper()
	suiteIsClean(t, newCorrect, budget, controlPlaneRunSeed)
}
