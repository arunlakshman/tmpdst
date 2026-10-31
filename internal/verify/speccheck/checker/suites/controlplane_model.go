package suites

import (
	"slices"
	"strings"

	"mds/internal/core"
	"mds/internal/verify/speccheck/checker/observation"
)

// controlPlaneModel is a second, naive model of the control plane. It is written from
// the rules in the speccheck.ControlPlane comment and shares no code with the
// spec: it keeps plain lists and adds up counts to find an end offset. The
// suite uses it as an oracle for the invariants, so a mistake in the spec is
// unlikely to repeat here.
type controlPlaneModel struct {
	segments map[string]controlPlaneModelSegment
	runs     map[core.TopicPartition][]core.BatchRange
	topics   map[string]int32
}

type controlPlaneModelSegment struct {
	batches []core.PendingBatch
	ranges  []core.BatchRange
}

func newCPModel() *controlPlaneModel {
	return &controlPlaneModel{
		segments: map[string]controlPlaneModelSegment{},
		runs:     map[core.TopicPartition][]core.BatchRange{},
		topics:   map[string]int32{},
	}
}

// end adds up the counts of all runs. The rules say offsets are contiguous
// from 0, so the sum is the next offset.
func (m *controlPlaneModel) end(topicPartition core.TopicPartition) int64 {
	var total int64
	for _, r := range m.runs[topicPartition] {
		total += r.Count
	}
	return total
}

// partitions returns every partition that has a run, in a fixed order.
func (m *controlPlaneModel) partitions() []core.TopicPartition {
	out := make([]core.TopicPartition, 0, len(m.runs))
	for topicPartition := range m.runs {
		out = append(out, topicPartition)
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
func (m *controlPlaneModel) topicList() []core.TopicInfo {
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
func (m *controlPlaneModel) apply(operation core.ControlPlaneOperation) (observation.ControlPlaneResult, bool) {
	switch o := operation.(type) {
	case core.CommitSegment:
		return m.commit(o)
	case core.FindSegments:
		return m.lookup(o), false
	case core.DescribeTopics:
		return m.describe(o), false
	case core.CreateTopic:
		return m.createTopicCall(o)
	}
	return observation.ControlPlaneResult{Code: core.ErrInvalid}, false
}

// createTopicCall is a CreateTopic call. An OK answer has Outcome Applied, as
// the spec gives. With FailIfExists, a topic that is already there is
// ErrTopicExists and nothing changes.
func (m *controlPlaneModel) createTopicCall(operation core.CreateTopic) (observation.ControlPlaneResult, bool) {
	if operation.Name != "" && operation.Partitions > 0 && operation.FailIfExists {
		if _, there := m.topics[operation.Name]; there {
			return observation.ControlPlaneResult{Code: core.ErrTopicExists}, false
		}
	}
	r, changed := m.createTopic(operation.Name, operation.Partitions)
	if r.Code == core.OK {
		r.Outcome = core.Applied
	}
	return r, changed
}

func (m *controlPlaneModel) createTopic(name string, partitions int32) (observation.ControlPlaneResult, bool) {
	if name == "" || partitions <= 0 {
		return observation.ControlPlaneResult{Code: core.ErrInvalid}, false
	}
	m.topics[name] = max(m.topics[name], partitions)
	return observation.ControlPlaneResult{Code: core.OK}, true
}

func sameBatches(a, b []core.PendingBatch) bool {
	return slices.Equal(a, b)
}

func (m *controlPlaneModel) commit(operation core.CommitSegment) (observation.ControlPlaneResult, bool) {
	if old, found := m.segments[operation.SegmentKey]; found {
		if !sameBatches(old.batches, operation.Batches) {
			return observation.ControlPlaneResult{Code: core.ErrConflict}, false
		}
		return observation.ControlPlaneResult{Assigned: old.ranges, Outcome: core.Applied}, false
	}
	invalid := observation.ControlPlaneResult{Code: core.ErrInvalid}
	if operation.SegmentKey == "" || len(operation.Batches) == 0 {
		return invalid, false
	}
	for _, b := range operation.Batches {
		if b.Records <= 0 || b.TopicPartition.Topic == "" || b.TopicPartition.Partition < 0 {
			return invalid, false
		}
	}
	var assigned []core.BatchRange
	for _, b := range operation.Batches {
		r := core.BatchRange{TopicPartition: b.TopicPartition, Base: core.Offset(m.end(b.TopicPartition)), Count: b.Records, Loc: b.Loc}
		m.runs[b.TopicPartition] = append(m.runs[b.TopicPartition], r)
		m.topics[b.TopicPartition.Topic] = max(m.topics[b.TopicPartition.Topic], b.TopicPartition.Partition+1)
		assigned = append(assigned, r)
	}
	m.segments[operation.SegmentKey] = controlPlaneModelSegment{batches: slices.Clone(operation.Batches), ranges: assigned}
	return observation.ControlPlaneResult{Assigned: assigned, Outcome: core.Applied}, true
}

func (m *controlPlaneModel) lookup(operation core.FindSegments) observation.ControlPlaneResult {
	if operation.From < 0 {
		return observation.ControlPlaneResult{Code: core.ErrInvalid}
	}
	result := observation.ControlPlaneResult{End: core.Offset(m.end(operation.TopicPartition))}
	var sum int64
	for _, r := range m.runs[operation.TopicPartition] {
		if int64(r.Base)+r.Count-1 < int64(operation.From) {
			continue
		}
		result.Ranges = append(result.Ranges, r)
		sum += r.Loc.Len
		if operation.MaxBytes > 0 && sum >= int64(operation.MaxBytes) {
			break
		}
	}
	return result
}

func (m *controlPlaneModel) describe(operation core.DescribeTopics) observation.ControlPlaneResult {
	var result observation.ControlPlaneResult
	for _, t := range m.topicList() {
		if len(operation.Topics) == 0 || slices.Contains(operation.Topics, t.Name) {
			result.Topics = append(result.Topics, t)
		}
	}
	return result
}
