// Package spec is the reference model of the control plane: a small program
// that shows the correct answer to every operation. It keeps plain maps and
// slices, runs one operation at a time, and has no faults. A reader can check
// by eye that it is right, and tests compare the real code with it (see
// README.md).
//
// The model is deterministic. It has no clock, no network, no goroutines and no
// files, and no map order reaches an answer. It is used by tests and by the
// simulator only. No program of the product imports it.
package spec

import (
	"slices"

	"github.com/manifesterio/mds/controlplane/contract"
)

// Model is anything that answers control plane operations. The real code and
// the simulator oracle are compared with a Model.
type Model interface {
	// Apply runs one operation (a write or a read) and returns its answer.
	// Outcome is Applied when the state changed, else NotApplied.
	Apply(op contract.Operation) contract.Done
}

// ControlPlane is the model of topics, segments and consumer groups.
//
// Rules for topics and segments (the group rules are in groups.go):
//
//	Log            A partition has a list of runs (contract.BatchRange), each
//	               [Base, Base+Count). Its end offset is the Base+Count of the
//	               newest run, or 0 when it has none.
//	CommitSegment  1. SegmentKey already committed: the same request gives the
//	                  original ranges again (OK); a different one is ErrConflict.
//	               2. Empty key or no batches, a batch with Records <= 0, an
//	                  empty topic or a negative partition: ErrInvalid.
//	               3. Else each batch, in request order, gets Base = the end
//	                  offset of its partition, and the end grows by Records.
//	                  It is all or nothing. A topic is created by a commit and
//	                  grows to cover the partition.
//	               A failed commit changes nothing, so its key can be used again.
//	FindSegments   From < 0 is ErrInvalid. Else the runs of the partition whose
//	               last offset is >= From, in order, and the end offset. MaxBytes
//	               > 0 stops after the run that brings the sum of Loc.Len to
//	               MaxBytes or more, but one run is always returned if any match.
//	               An unknown partition is empty with End 0.
//	DescribeTopics Sorted by name. No names means all topics. Unknown names are
//	               left out.
//	CreateTopic    ErrInvalid for an empty name or partitions <= 0. With
//	               FailIfExists, a topic that is there gives ErrTopicExists.
//	               Else it is OK and the topic never gets smaller.
//
// An operation that is not known is ErrInvalid.
type ControlPlane struct {
	segments map[string]segment
	runs     map[contract.TopicPartition][]contract.BatchRange
	topics   map[string]int32 // topic -> partition count
	groups   map[string]*groupState
	offsets  map[string]map[contract.TopicPartition]committedOffset // by group
}

type segment struct {
	batches []contract.PendingBatch
	ranges  []contract.BatchRange
}

// New returns an empty control plane model.
func New() *ControlPlane {
	return &ControlPlane{
		segments: map[string]segment{},
		runs:     map[contract.TopicPartition][]contract.BatchRange{},
		topics:   map[string]int32{},
		groups:   map[string]*groupState{},
		offsets:  map[string]map[contract.TopicPartition]committedOffset{},
	}
}

var _ Model = (*ControlPlane)(nil)

// Apply implements Model.
func (c *ControlPlane) Apply(op contract.Operation) contract.Done {
	switch op := op.(type) {
	case contract.CommitSegment:
		return c.commit(op)
	case contract.FindSegments:
		return c.find(op)
	case contract.DescribeTopics:
		return c.describeTopics(op)
	case contract.CreateTopic:
		return written(c.createTopic(op))
	case contract.GroupInstall:
		return written(c.groupInstall(op))
	case contract.GroupAssign:
		return written(c.groupAssign(op))
	case contract.GroupRemove:
		return written(c.groupRemove(op))
	case contract.OffsetCommit:
		return c.offsetCommit(op)
	case contract.OffsetFetch:
		return answer(c.offsetFetch(op))
	case contract.DescribeGroup:
		return answer(c.describeGroup(op))
	case contract.ListGroups:
		return answer(c.listGroups())
	}
	return contract.Done{Err: contract.ErrInvalid}
}

// written is the answer to a write that has no response data.
func written(code contract.ErrCode) contract.Done {
	if code != contract.OK {
		return contract.Done{Err: code}
	}
	return contract.Done{Outcome: contract.Applied}
}

// answer is the answer to a read.
func answer(resp contract.Response) contract.Done {
	return contract.Done{Resp: resp}
}

func (c *ControlPlane) createTopic(op contract.CreateTopic) contract.ErrCode {
	if op.Name == "" || op.Partitions <= 0 {
		return contract.ErrInvalid
	}
	if _, there := c.topics[op.Name]; there && op.FailIfExists {
		return contract.ErrTopicExists
	}
	c.topics[op.Name] = max(c.topics[op.Name], op.Partitions)
	return contract.OK
}

// end is the next offset of a partition.
func (c *ControlPlane) end(tp contract.TopicPartition) contract.Offset {
	runs := c.runs[tp]
	if len(runs) == 0 {
		return 0
	}
	last := runs[len(runs)-1]
	return last.Base + contract.Offset(last.Count)
}

func (c *ControlPlane) commit(op contract.CommitSegment) contract.Done {
	if old, done := c.segments[op.SegmentKey]; done {
		if !slices.Equal(old.batches, op.Batches) {
			return contract.Done{Err: contract.ErrConflict}
		}
		return commitDone(old.ranges)
	}
	if op.SegmentKey == "" || len(op.Batches) == 0 {
		return contract.Done{Err: contract.ErrInvalid}
	}
	for _, b := range op.Batches {
		if b.Records <= 0 || b.TopicPartition.Topic == "" || b.TopicPartition.Partition < 0 {
			return contract.Done{Err: contract.ErrInvalid}
		}
	}
	seg := segment{batches: slices.Clone(op.Batches)}
	for _, b := range op.Batches {
		tp := b.TopicPartition
		r := contract.BatchRange{TopicPartition: tp, Base: c.end(tp), Count: b.Records, Loc: b.Loc}
		c.runs[tp] = append(c.runs[tp], r)
		c.topics[tp.Topic] = max(c.topics[tp.Topic], tp.Partition+1)
		seg.ranges = append(seg.ranges, r)
	}
	c.segments[op.SegmentKey] = seg
	return commitDone(seg.ranges)
}

func commitDone(ranges []contract.BatchRange) contract.Done {
	resp := contract.CommitSegmentResp{Assigned: slices.Clone(ranges)}
	return contract.Done{Resp: resp, Outcome: contract.Applied}
}

func (c *ControlPlane) find(op contract.FindSegments) contract.Done {
	if op.From < 0 {
		return contract.Done{Err: contract.ErrInvalid}
	}
	resp := contract.FindSegmentsResp{End: c.end(op.TopicPartition)}
	var bytes int64
	for _, r := range c.runs[op.TopicPartition] {
		if r.Base+contract.Offset(r.Count) <= op.From {
			continue
		}
		resp.Ranges = append(resp.Ranges, r)
		bytes += r.Loc.Len
		if op.MaxBytes > 0 && bytes >= int64(op.MaxBytes) {
			break
		}
	}
	return answer(resp)
}

func (c *ControlPlane) describeTopics(op contract.DescribeTopics) contract.Done {
	var resp contract.DescribeTopicsResp
	for _, name := range sortedKeys(c.topics) {
		if len(op.Topics) == 0 || slices.Contains(op.Topics, name) {
			resp.Topics = append(resp.Topics, contract.TopicInfo{Name: name, Partitions: c.topics[name]})
		}
	}
	return answer(resp)
}

// sortedKeys returns the keys of m in order. It is the only way the model
// walks a map, so map order never reaches an answer.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m { //determinismcheck:ok keys are sorted before use
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
