package suites

import (
	"slices"
	"strings"

	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest/observation"
)

// cpModel is a second, naive model of the control plane. It is written from
// the rules in the veritas.ControlPlane comment and shares no code with the
// spec: it keeps plain lists and adds up counts to find an end offset. The
// suite uses it as an oracle for the invariants, so a mistake in the spec is
// unlikely to repeat here.
type cpModel struct {
	segments map[string]cpModelSegment
	runs     map[core.TopicPartition][]core.BatchRange
	topics   map[string]int32
}

type cpModelSegment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

func newCPModel() *cpModel {
	return &cpModel{
		segments: map[string]cpModelSegment{},
		runs:     map[core.TopicPartition][]core.BatchRange{},
		topics:   map[string]int32{},
	}
}

// end adds up the counts of all runs. The rules say offsets are contiguous
// from 0, so the sum is the next offset.
func (m *cpModel) end(tp core.TopicPartition) int64 {
	var total int64
	for _, r := range m.runs[tp] {
		total += r.Count
	}
	return total
}

// partitions returns every partition that has a run, in a fixed order.
func (m *cpModel) partitions() []core.TopicPartition {
	out := make([]core.TopicPartition, 0, len(m.runs))
	for tp := range m.runs {
		out = append(out, tp)
	}
	slices.SortFunc(out, func(a, b core.TopicPartition) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		return int(a.Partition) - int(b.Partition)
	})
	return out
}

// topicList returns every topic sorted by name.
func (m *cpModel) topicList() []core.TopicInfo {
	var out []core.TopicInfo
	for name, n := range m.topics {
		out = append(out, core.TopicInfo{Name: name, Partitions: n})
	}
	slices.SortFunc(out, func(a, b core.TopicInfo) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// apply runs op on the model. It returns the result that the rules require and
// whether the state may change. When the second value is false, the state hash of the
// spec and of the subject must stay the same.
func (m *cpModel) apply(op core.ControlPlaneOperation) (observation.ControlPlaneResult, bool) {
	switch o := op.(type) {
	case core.CommitSegment:
		return m.commit(o)
	case core.FindSegments:
		return m.lookup(o), false
	case core.DescribeTopics:
		return m.describe(o), false
	}
	return observation.ControlPlaneResult{Code: core.ErrInvalid}, false
}

func (m *cpModel) createTopic(name string, partitions int32) (observation.ControlPlaneResult, bool) {
	if name == "" || partitions <= 0 {
		return observation.ControlPlaneResult{Code: core.ErrInvalid}, false
	}
	m.topics[name] = max(m.topics[name], partitions)
	return observation.ControlPlaneResult{Code: core.OK}, true
}

func sameBatches(a, b []core.PendingBatch) bool {
	return slices.Equal(a, b)
}

func (m *cpModel) commit(op core.CommitSegment) (observation.ControlPlaneResult, bool) {
	if old, found := m.segments[op.SegmentKey]; found {
		if !sameBatches(old.batches, op.Batches) {
			return observation.ControlPlaneResult{Code: core.ErrConflict}, false
		}
		return observation.ControlPlaneResult{Assigned: old.ranges, Outcome: core.Applied}, false
	}
	invalid := observation.ControlPlaneResult{Code: core.ErrInvalid}
	if op.SegmentKey == "" || len(op.Batches) == 0 {
		return invalid, false
	}
	for _, b := range op.Batches {
		if b.Records <= 0 || b.TP.Topic == "" || b.TP.Partition < 0 {
			return invalid, false
		}
	}
	var assigned []core.BatchRange
	for _, b := range op.Batches {
		r := core.BatchRange{TP: b.TP, Base: core.Offset(m.end(b.TP)), Count: b.Records, Loc: b.Loc}
		m.runs[b.TP] = append(m.runs[b.TP], r)
		m.topics[b.TP.Topic] = max(m.topics[b.TP.Topic], b.TP.Partition+1)
		assigned = append(assigned, r)
	}
	m.segments[op.SegmentKey] = cpModelSegment{batches: slices.Clone(op.Batches), ranges: assigned}
	return observation.ControlPlaneResult{Assigned: assigned, Outcome: core.Applied}, true
}

func (m *cpModel) lookup(op core.FindSegments) observation.ControlPlaneResult {
	if op.From < 0 {
		return observation.ControlPlaneResult{Code: core.ErrInvalid}
	}
	result := observation.ControlPlaneResult{End: core.Offset(m.end(op.TP))}
	var sum int64
	for _, r := range m.runs[op.TP] {
		if int64(r.Base)+r.Count-1 < int64(op.From) {
			continue
		}
		result.Ranges = append(result.Ranges, r)
		sum += r.Loc.Len
		if op.MaxBytes > 0 && sum >= int64(op.MaxBytes) {
			break
		}
	}
	return result
}

func (m *cpModel) describe(op core.DescribeTopics) observation.ControlPlaneResult {
	var result observation.ControlPlaneResult
	for _, t := range m.topicList() {
		if len(op.Topics) == 0 || slices.Contains(op.Topics, t.Name) {
			result.Topics = append(result.Topics, t)
		}
	}
	return result
}
