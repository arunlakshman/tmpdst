package fakeservices

import (
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specbridge"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
)

// DynamoMaxCommitItems is DynamoDB's TransactWriteItems limit, the value the
// Kafka preset uses. It models the item cap of the removed transactional
// store (decision 32), a limit the log control plane does not have.
const DynamoMaxCommitItems = 100

// ControlPlaneFaults are the faults of the simulated control plane: the faults
// of faulty.Faults, and a limit on the size of one commit.
type ControlPlaneFaults struct {
	faulty.Faults
	// MaxCommitItems models a store that commits a segment in one bounded
	// transaction (DynamoDB: DynamoMaxCommitItems). A CommitSegment that needs
	// more items is refused with ErrTooLarge and changes nothing. The item
	// count is an upper bound: 1 (the segment) + distinct partitions with at
	// least one batch + topics the commit grows. 0 = no limit.
	MaxCommitItems int
}

// ControlPlane is the simulated control plane (decisions 3, 14, 23):
// controlplane/spec (through specbridge) as the state, with the faults of faulty
// around it. ErrConflict and ErrInvalid come only from the spec; ErrTooLarge
// only from the optional commit-size limit (ControlPlaneFaults.MaxCommitItems),
// applied before the spec; a retry of a committed segment returns the original
// offsets whatever the faults did.
//
// It is a kernel.Service. A broker node hands it a core.ControlPlaneCall, and the
// service answers with core.ControlPlaneDone.
type ControlPlane struct {
	*faulty.Service[core.ControlPlaneCall, core.EventData, contract.Operation, contract.Done]
	spec     *specbridge.ControlPlane
	maxItems int // ControlPlaneFaults.MaxCommitItems; 0 = unlimited
}

var _ kernel.Service[core.ControlPlaneCall, core.EventData] = (*ControlPlane)(nil)

// NewControlPlane returns an empty control plane with the given faults. The
// world gives it its lanes.
func NewControlPlane(w *kernel.World, f ControlPlaneFaults) *ControlPlane {
	s := &ControlPlane{spec: specbridge.New(), maxItems: f.MaxCommitItems}
	shape := faulty.Shape[core.ControlPlaneCall, core.EventData, contract.Operation, contract.Done]{
		Name:      "fakecontrolplane",
		Prefix:    "cp",
		Asserts:   controlPlaneAsserts,
		Op:        func(c core.ControlPlaneCall) contract.Operation { return c.Op },
		RequestID: core.ControlPlaneCall.RequestID,
		Mutating: func(op contract.Operation) bool {
			_, ok := op.(contract.CommitSegment)
			return ok
		},
		// A replayed commit is Applied too, so Gen may bump without a change.
		Changed: func(d contract.Done) bool { return d.Outcome == core.Applied },
		Reply: func(req core.ControlPlaneCall, d contract.Done) core.EventData {
			return core.ControlPlaneDone{Req: req.Req, Resp: d.Resp, Err: d.Err, Outcome: d.Outcome}
		},
		Fail: func(req core.ControlPlaneCall, failure faulty.Failure) core.EventData {
			if failure == faulty.TimedOut {
				return core.ControlPlaneDone{Req: req.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
			}
			return core.ControlPlaneDone{Req: req.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied}
		},
		Limit: func(op contract.Operation) (contract.Done, bool) {
			if s.tooLarge(op) {
				return contract.Done{Err: core.ErrTooLarge, Outcome: core.NotApplied}, true
			}
			return contract.Done{}, false
		},
	}
	s.Service = faulty.New(w, faulty.Spec[contract.Operation, contract.Done](s.spec), f.Faults, shape)
	return s
}

// CreateTopic pre-creates a topic (setup, before the run starts).
func (s *ControlPlane) CreateTopic(name string, partitions int32) core.ErrCode {
	err := s.spec.CreateTopic(name, partitions)
	if err == core.OK {
		s.Bump()
	}
	return err
}

// Committed returns the committed ranges of tp in offset order.
func (s *ControlPlane) Committed(topicPartition core.TopicPartition) []contract.BatchRange {
	d := s.spec.Apply(contract.FindSegments{TopicPartition: topicPartition})
	if d.Err != core.OK {
		return nil
	}
	return d.Resp.(contract.FindSegmentsResp).Ranges
}

// HighWatermark is the partition end: the next offset the spec assigns.
func (s *ControlPlane) HighWatermark(topicPartition core.TopicPartition) core.Offset {
	return s.spec.End(topicPartition)
}

// tooLarge reports whether op is a CommitSegment that a store with a bounded
// transaction would refuse. The count is the arithmetic of the removed DynamoDB-transaction store (decisions 39,
// 41, 44: no producer, high-watermark or fencing items): one segment item, one
// range run item per distinct partition and one item per topic the commit
// grows, taken as an upper bound, so it depends on the request and the topic
// table only. The number of batches does not count. The spec stays
// limit-free; a refusal reaches it never, so nothing changes.
func (s *ControlPlane) tooLarge(operation contract.Operation) bool {
	c, ok := operation.(contract.CommitSegment)
	if !ok || s.maxItems <= 0 {
		return false
	}
	return s.commitItems(c) > s.maxItems
}

// commitItems is the item count of c: 1 + distinct partitions + grown topics.
func (s *ControlPlane) commitItems(c contract.CommitSegment) int {
	var tps []core.TopicPartition
	grown := map[string]int32{} // keyed lookups only, never iterated for behaviour
	topics := 0
	known := s.spec.Apply(contract.DescribeTopics{}).Resp.(contract.DescribeTopicsResp).Topics
	for _, b := range c.Batches {
		if !slices.Contains(tps, b.TopicPartition) {
			tps = append(tps, b.TopicPartition)
		}
		have := int32(0)
		for _, t := range known {
			if t.Name == b.TopicPartition.Topic {
				have = t.Partitions
			}
		}
		if want := b.TopicPartition.Partition + 1; want > max(have, grown[b.TopicPartition.Topic]) {
			if _, seen := grown[b.TopicPartition.Topic]; !seen {
				topics++
			}
			grown[b.TopicPartition.Topic] = want
		}
	}
	return 1 + len(tps) + topics
}
