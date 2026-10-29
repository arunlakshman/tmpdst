package plantedbug

import (
	"slices"
	"strconv"

	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest"
	"manifester/internal/verify/veritas/veritastest/generator"
	"manifester/internal/verify/veritas/veritastest/observation"
	"manifester/internal/verify/veritas/veritastest/suites"
)

// ControlPlaneBug is a planted bug for a control plane. Wrap returns a subject
// that behaves like the correct one, except for the bug.
type ControlPlaneBug struct {
	name, doc string
	wrap      func(correct suites.ControlPlaneSubject) suites.ControlPlaneSubject
}

// NewControlPlaneBug returns a named bug. Wrap is called once for each fresh
// subject, so the wrapper may keep state.
func NewControlPlaneBug(name, doc string, wrap func(suites.ControlPlaneSubject) suites.ControlPlaneSubject) ControlPlaneBug {
	return ControlPlaneBug{name: name, doc: doc, wrap: wrap}
}

// Name is the short name of the bug.
func (b ControlPlaneBug) Name() string { return b.name }

// Doc is one sentence that says what the bug does wrong.
func (b ControlPlaneBug) Doc() string { return b.doc }

// Wrap returns correct with the bug added.
func (b ControlPlaneBug) Wrap(correct suites.ControlPlaneSubject) suites.ControlPlaneSubject {
	return b.wrap(correct)
}

type controlPlaneSubject struct {
	inner suites.ControlPlaneSubject
	do    func(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error)
	hash  func() (uint64, bool)
}

func (s *controlPlaneSubject) Do(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
	return s.do(op)
}

func (s *controlPlaneSubject) StateHash() (uint64, bool) {
	if s.hash != nil {
		return s.hash()
	}
	return s.inner.StateHash()
}

// changeFunc changes the result of one operation after the correct subject has
// answered.
type changeFunc func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult

func afterControlPlane(name, doc string, change changeFunc) ControlPlaneBug {
	return statefulControlPlane(name, doc, func() changeFunc { return change })
}

// statefulControlPlane makes a bug whose change function keeps state.
// newChange is called once for each fresh subject.
func statefulControlPlane(name, doc string, newChange func() changeFunc) ControlPlaneBug {
	return NewControlPlaneBug(name, doc, func(inner suites.ControlPlaneSubject) suites.ControlPlaneSubject {
		change := newChange()
		return &controlPlaneSubject{inner: inner, do: func(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
			r, err := inner.Do(op)
			if err != nil {
				return r, err
			}
			return change(op, r), nil
		}}
	})
}

// beforeControlPlane makes a bug that changes the operation before the correct
// subject sees it. change may keep state and may call inner more than once.
func beforeControlPlane(name, doc string, newChange func() func(inner suites.ControlPlaneSubject, op generator.ControlPlaneOp) (observation.ControlPlaneResult, error)) ControlPlaneBug {
	return NewControlPlaneBug(name, doc, func(inner suites.ControlPlaneSubject) suites.ControlPlaneSubject {
		change := newChange()
		return &controlPlaneSubject{inner: inner, do: func(op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
			return change(inner, op)
		}}
	})
}

// commitOf returns the CommitSegment of op, if op is one.
func commitOf(op generator.ControlPlaneOp) (core.ControlPlaneCall, core.CommitSegment, bool) {
	if op.Call == nil {
		return core.ControlPlaneCall{}, core.CommitSegment{}, false
	}
	c, ok := op.Call.Op.(core.CommitSegment)
	return *op.Call, c, ok
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
		func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult {
			if _, _, ok := commitOf(op); ok && len(r.Assigned) > 0 {
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
		func() func(suites.ControlPlaneSubject, generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
			n := 0
			return func(inner suites.ControlPlaneSubject, op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
				if call, commit, ok := commitOf(op); ok && commit.SegmentKey != "" {
					n++
					commit.SegmentKey += "#" + strconv.Itoa(n)
					call.Op = commit
					op = generator.NewCall(call)
				}
				return inner.Do(op)
			}
		})
}

// ReplayWithDifferentRequestNotRejected answers a changed request under a used
// key with the first answer, instead of ErrConflict.
func ReplayWithDifferentRequestNotRejected() ControlPlaneBug {
	return statefulControlPlane("ReplayWithDifferentRequestNotRejected",
		"A commit that reuses a segment key with different batches returns the first answer instead of ErrConflict.",
		func() changeFunc {
			first := map[string][]core.BatchRange{}
			return func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult {
				_, commit, ok := commitOf(op)
				if !ok {
					return r
				}
				if _, seen := first[commit.SegmentKey]; !seen && r.Code == core.OK {
					first[commit.SegmentKey] = r.Assigned
				}
				if r.Code == core.ErrConflict {
					return observation.ControlPlaneResult{Assigned: first[commit.SegmentKey], Outcome: core.Applied}
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
		func() func(suites.ControlPlaneSubject, generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
			n := core.RequestID(0)
			return func(inner suites.ControlPlaneSubject, op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
				call, commit, ok := commitOf(op)
				if ok && commit.SegmentKey != "" {
					valid := 0
					for valid < len(commit.Batches) && commit.Batches[valid].Records > 0 &&
						commit.Batches[valid].TP.Topic != "" && commit.Batches[valid].TP.Partition >= 0 {
						valid++
					}
					if valid > 0 && valid < len(commit.Batches) {
						n++
						prefix := core.CommitSegment{SegmentKey: commit.SegmentKey + "#partial", Batches: commit.Batches[:valid]}
						extra := core.ControlPlaneCall{Req: call.Req + 1<<28 + n, Op: prefix}
						if _, err := inner.Do(generator.NewCall(extra)); err != nil {
							return observation.ControlPlaneResult{}, err
						}
					}
				}
				return inner.Do(op)
			}
		})
}

// LookupMaxBytesUsesGreater stops one batch late when the byte total equals
// MaxBytes. The comparison bytes > max is the same as bytes >= max + 1.
func LookupMaxBytesUsesGreater() ControlPlaneBug {
	return beforeControlPlane("LookupMaxBytesUsesGreater",
		"Lookup stops when the byte total is greater than MaxBytes, not when it is equal or greater.",
		func() func(suites.ControlPlaneSubject, generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
			return func(inner suites.ControlPlaneSubject, op generator.ControlPlaneOp) (observation.ControlPlaneResult, error) {
				if op.Call != nil {
					if l, ok := op.Call.Op.(core.FindSegments); ok && l.MaxBytes > 0 && l.MaxBytes < 1<<30 {
						l.MaxBytes++
						op = generator.NewCall(core.ControlPlaneCall{Req: op.Call.Req, Op: l})
					}
				}
				return inner.Do(op)
			}
		})
}

// LookupSkipsRunContainingFrom drops a run that starts before From, although
// it holds offset From.
func LookupSkipsRunContainingFrom() ControlPlaneBug {
	return afterControlPlane("LookupSkipsRunContainingFrom",
		"Lookup leaves out the run that starts before From but contains offset From.",
		func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult {
			if op.Call == nil {
				return r
			}
			l, ok := op.Call.Op.(core.FindSegments)
			if !ok || r.Code != core.OK {
				return r
			}
			var kept []core.BatchRange
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
		func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult {
			if op.Call != nil {
				if _, ok := op.Call.Op.(core.DescribeTopics); ok {
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
			return func(op generator.ControlPlaneOp, r observation.ControlPlaneResult) observation.ControlPlaneResult {
				switch {
				case op.CreateTopic != nil:
					if r.Code == core.OK {
						counts[op.CreateTopic.Name] = op.CreateTopic.Partitions
					}
				case op.Call != nil:
					r = minimizeCountsForCall(counts, op.Call.Op, r)
				}
				return r
			}
		})
}

// minimizeCountsForCall updates the own partition counts of the bug after a
// commit, and replaces the counts in a describe result with them.
func minimizeCountsForCall(counts map[string]int32, op core.ControlPlaneOperation, r observation.ControlPlaneResult) observation.ControlPlaneResult {
	switch op.(type) {
	case core.CommitSegment:
		for _, a := range r.Assigned {
			counts[a.TP.Topic] = max(counts[a.TP.Topic], a.TP.Partition+1)
		}
	case core.DescribeTopics:
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
// suite compares the hash at all.
func ControlPlaneStateHashDiffers() ControlPlaneBug {
	return newHashBug(NewControlPlaneBug, withControlPlaneStateHash)
}

func withControlPlaneStateHash(inner suites.ControlPlaneSubject, hash func() (uint64, bool)) suites.ControlPlaneSubject {
	return &controlPlaneSubject{inner: inner, do: inner.Do, hash: hash}
}

// FindControlPlaneDetection runs the control plane suite on a subject with bug, for
// every profile and the seeds 1 to seeds. It returns the first catch.
// newCorrect must return a fresh correct subject on each call.
func FindControlPlaneDetection(bug ControlPlaneBug, newCorrect func() suites.ControlPlaneSubject, seeds int) (Detection, bool) {
	return findSubjectDetection(seeds, newCorrect, bug.Wrap, suites.ControlPlaneComparator)
}

// RequireAllControlPlaneBugsDetected runs every planted control plane bug through
// the control plane suite. It reports an error for each bug that no profile
// finds within seeds seeds, and it logs the catch table. A subject with no spec
// hash cannot show a wrong hash, so that one bug is skipped for such a subject
// and the log says so.
func RequireAllControlPlaneBugsDetected(t veritastest.Reporter, newCorrect func() suites.ControlPlaneSubject, seeds int) {
	requireAllDetected(t, "control plane", newCorrect, seeds, ControlPlaneBugs(), FindControlPlaneDetection)
}
