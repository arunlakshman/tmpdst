// Package fakecontrolplane is the simulated control plane (decisions 3, 14, 23):
// controlplane/spec (through specbridge) as the state core, plus injected faults and timing.
// ErrConflict and ErrInvalid come only from the spec; ErrTooLarge
// only from the service's optional commit-size limit (Faults.MaxCommitItems),
// applied before the spec; a retry of a committed segment returns the original
// offsets whatever the faults did.
//
// It is a kernel.Service. A broker node hands it a core.ControlPlaneCall, and the
// service answers with core.ControlPlaneDone.
package fakecontrolplane

import (
	"slices"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/fakecompletions"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specbridge"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// DynamoMaxCommitItems is DynamoDB's TransactWriteItems limit, the value the
// Kafka preset uses. It models the item cap of the removed transactional
// store (decision 32), a limit the log control plane does not have.
const DynamoMaxCommitItems = 100

// ServiceOutage is a correlated, sustained failure window: every call fails while
// it lasts.
type ServiceOutage = fakecompletions.ServiceOutage

// Faults are the fault families of the fake control plane.
type Faults struct {
	ApplyLatency   kernel.FastWithRareSlow
	RespondLatency kernel.FastWithRareSlow
	Unavailable503 float64
	Timeout        float64
	LostResponse   float64 // commit applied, response lost: retry must be idempotent
	Outage         ServiceOutage
	CallerTimeout  machine.Duration
	// MaxCommitItems models a store that commits a segment in one bounded
	// transaction (DynamoDB: DynamoMaxCommitItems). A CommitSegment that needs
	// more items is refused with ErrTooLarge and changes nothing. The item
	// count is an upper bound: 1 (the segment) + distinct partitions with at
	// least one batch + topics the commit grows. 0 = no limit.
	MaxCommitItems int
}

// Engine is the fake control plane.
type Engine struct {
	*fakecompletions.Core
	spec     *specbridge.ControlPlane
	maxItems int // Faults.MaxCommitItems; 0 = unlimited
}

var _ kernel.Service[core.ControlPlaneCall, core.EventData] = (*Engine)(nil)

// New returns an empty control plane with the given faults. The world gives it its lanes.
func New(w *kernel.World, f Faults) *Engine {
	e := &Engine{spec: specbridge.New(), maxItems: f.MaxCommitItems}
	e.Core = fakecompletions.New(w, fakecompletions.Hooks{
		Name:   "fakecontrolplane",
		Prefix: "cp",
		Params: fakecompletions.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOCall) core.IOResult {
			if c, ok := a.(core.ControlPlaneCall); ok && e.tooLarge(c.Op) {
				return core.ControlPlaneDone{Req: c.Req, Err: core.ErrTooLarge, Outcome: core.NotApplied}
			}
			return e.spec.Exec(a)
		},
		Mutating: func(a core.IOCall) bool {
			_, ok := a.(core.ControlPlaneCall).Op.(contract.CommitSegment)
			return ok
		},
		Fail: func(a core.IOCall, err core.ErrCode, o core.Outcome) core.IOResult {
			return core.ControlPlaneDone{Req: a.RequestID(), Err: err, Outcome: o}
		},
		// A replayed commit is Applied too, so Gen may bump without a change.
		Changed:  func(c core.IOResult) bool { return c.(core.ControlPlaneDone).Outcome == core.Applied },
		SpecHash: e.spec.StateHash,
	})
	return e
}

// CreateTopic pre-creates a topic (setup, before the run starts).
func (e *Engine) CreateTopic(name string, partitions int32) core.ErrCode {
	err := e.spec.CreateTopic(name, partitions)
	if err == core.OK {
		e.Bump()
	}
	return err
}

// Handle starts one control plane call. It implements kernel.Service.
func (e *Engine) Handle(cx *fakecompletions.Cx, from fakecompletions.Ref, req core.ControlPlaneCall) {
	e.Start(cx, from, req)
}

// Committed returns the committed ranges of tp in offset order.
func (e *Engine) Committed(topicPartition core.TopicPartition) []contract.BatchRange {
	d := e.spec.Exec(core.ControlPlaneCall{Op: contract.FindSegments{TopicPartition: topicPartition}}).(core.ControlPlaneDone)
	if d.Err != core.OK {
		return nil
	}
	return d.Resp.(contract.FindSegmentsResp).Ranges
}

// HighWatermark is the partition end: the next offset the spec assigns.
func (e *Engine) HighWatermark(topicPartition core.TopicPartition) core.Offset {
	return e.spec.End(topicPartition)
}

// tooLarge reports whether op is a CommitSegment that a store with a bounded
// transaction would refuse. The count is the arithmetic of the removed DynamoDB-transaction store (decisions 39,
// 41, 44: no producer, high-watermark or fencing items): one segment item, one
// range run item per distinct partition and one item per topic the commit
// grows, taken as an upper bound, so it depends on the request and the topic
// table only. The number of batches does not count. The spec stays
// limit-free; a refusal reaches it never, so nothing changes.
func (e *Engine) tooLarge(operation contract.Operation) bool {
	c, ok := operation.(contract.CommitSegment)
	if !ok || e.maxItems <= 0 {
		return false
	}
	return e.commitItems(c) > e.maxItems
}

// commitItems is the item count of c: 1 + distinct partitions + grown topics.
func (e *Engine) commitItems(c contract.CommitSegment) int {
	var tps []core.TopicPartition
	grown := map[string]int32{} // keyed lookups only, never iterated for behaviour
	topics := 0
	known := e.spec.Exec(core.ControlPlaneCall{Op: contract.DescribeTopics{}}).(core.ControlPlaneDone).Resp.(contract.DescribeTopicsResp).Topics
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
