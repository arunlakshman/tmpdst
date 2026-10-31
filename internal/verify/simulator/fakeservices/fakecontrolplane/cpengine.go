// Package fakecontrolplane is the simulated control plane (decisions 3, 14, 23):
// speccheck.ControlPlane as the state core, plus injected faults and timing.
// ErrConflict and ErrInvalid come only from the spec; ErrTooLarge
// only from the engine's optional commit-size limit (ControlPlaneFaults.
// MaxCommitItems), applied before the spec; a retry of a
// committed segment returns the original offsets whatever the faults did.
package fakecontrolplane

import (
	"slices"

	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/verify/simulator/fakeservices/internal/fakecompletions"
	"manifester/internal/verify/speccheck"
)

// Engine is the fake control plane. It is also the simulator.CPTruth.
type Engine struct {
	*fakecompletions.CompletionCore
	spec     *speccheck.ControlPlane
	maxItems int // simulator.ControlPlaneFaults.MaxCommitItems; 0 = unlimited
}

var (
	_ simulator.FakeService           = (*Engine)(nil)
	_ simulator.ControlPlaneRealState = (*Engine)(nil)
)

// New returns an empty control plane with the given faults.
func New(f simulator.ControlPlaneFaults) *Engine {
	e := &Engine{spec: speccheck.NewControlPlane(), maxItems: f.MaxCommitItems}
	e.CompletionCore = fakecompletions.New(fakecompletions.CompletionHooks{
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
			_, ok := a.(core.ControlPlaneCall).Op.(core.CommitSegment)
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

// Handles reports whether a is a control plane call.
func (e *Engine) Handles(a core.Action) bool {
	_, ok := a.(core.ControlPlaneCall)
	return ok
}

// Execute starts a, a core.ControlPlaneCall.
func (e *Engine) Execute(ctx simulator.FakeServiceContext, from core.NodeInstance, a core.Action) {
	e.CompletionCore.Execute(ctx, from, a.(core.IOCall))
}

// Committed returns the committed ranges of tp in offset order.
func (e *Engine) Committed(tp core.TopicPartition) []core.BatchRange {
	d := e.spec.Exec(core.ControlPlaneCall{Op: core.FindSegments{TP: tp}}).(core.ControlPlaneDone)
	if d.Err != core.OK {
		return nil
	}
	return d.Resp.(core.FindSegmentsResp).Ranges
}

// HighWatermark is the partition end: the next offset the spec assigns.
func (e *Engine) HighWatermark(tp core.TopicPartition) core.Offset { return e.spec.End(tp) }

// tooLarge reports whether op is a CommitSegment that a store with a bounded
// transaction would refuse. The count is the arithmetic of the removed DynamoDB-transaction store (decisions 39,
// 41, 44: no producer, high-watermark or fencing items): one segment item, one
// range run item per distinct partition and one item per topic the commit
// grows, taken as an upper bound, so it depends on the request and the topic
// table only. The number of batches does not count. The spec stays
// limit-free; a refusal reaches it never, so nothing changes.
func (e *Engine) tooLarge(op core.ControlPlaneOperation) bool {
	c, ok := op.(core.CommitSegment)
	if !ok || e.maxItems <= 0 {
		return false
	}
	return e.commitItems(c) > e.maxItems
}

// commitItems is the item count of c: 1 + distinct partitions + grown topics.
func (e *Engine) commitItems(c core.CommitSegment) int {
	var tps []core.TopicPartition
	grown := map[string]int32{} // keyed lookups only, never iterated for behaviour
	topics := 0
	known := e.spec.Exec(core.ControlPlaneCall{Op: core.DescribeTopics{}}).(core.ControlPlaneDone).Resp.(core.DescribeTopicsResp).Topics
	for _, b := range c.Batches {
		if !slices.Contains(tps, b.TP) {
			tps = append(tps, b.TP)
		}
		have := int32(0)
		for _, t := range known {
			if t.Name == b.TP.Topic {
				have = t.Partitions
			}
		}
		if want := b.TP.Partition + 1; want > max(have, grown[b.TP.Topic]) {
			if _, seen := grown[b.TP.Topic]; !seen {
				topics++
			}
			grown[b.TP.Topic] = want
		}
	}
	return 1 + len(tps) + topics
}
