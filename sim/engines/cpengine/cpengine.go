// Package cpengine is the simulated control plane (decisions 3, 14, 23):
// veritas.ControlPlane as the state core, plus injected faults and timing.
// ErrConflict and ErrInvalid come only from the spec; ErrTooLarge
// only from the engine's optional commit-size limit (ControlPlaneFaults.
// MaxCommitItems), applied before the spec; a retry of a
// committed segment returns the original offsets whatever the faults did.
package cpengine

import (
	"slices"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/fakeio"
	"manifester/veritas"
)

// Engine is the fake control plane. It is also the sim.CPTruth.
type Engine struct {
	*fakeio.Core
	spec     *veritas.ControlPlane
	maxItems int // sim.ControlPlaneFaults.MaxCommitItems; 0 = unlimited
}

var (
	_ sim.Engine  = (*Engine)(nil)
	_ sim.CPTruth = (*Engine)(nil)
)

// New returns an empty control plane with the given faults.
func New(f sim.ControlPlaneFaults) *Engine {
	e := &Engine{spec: veritas.NewControlPlane(), maxItems: f.MaxCommitItems}
	e.Core = fakeio.New(fakeio.Hooks{
		Name:   "cpengine",
		Prefix: "cp",
		Params: fakeio.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOAction) core.Completion {
			if c, ok := a.(core.CPCall); ok && e.tooLarge(c.Op) {
				return core.CPDone{Req: c.Req, Err: core.ErrTooLarge, Outcome: core.NotApplied}
			}
			return e.spec.Exec(a)
		},
		Mutating: func(a core.IOAction) bool {
			switch a.(core.CPCall).Op.(type) {
			case core.CommitSegment:
				return true
			}
			return false
		},
		Fail: func(a core.IOAction, err core.ErrCode, o core.Outcome) core.Completion {
			return core.CPDone{Req: a.ReqID(), Err: err, Outcome: o}
		},
		// A replayed commit is Applied too, so Gen may bump without a change.
		Changed:  func(c core.Completion) bool { return c.(core.CPDone).Outcome == core.Applied },
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
	_, ok := a.(core.CPCall)
	return ok
}

// Execute starts a, a core.CPCall.
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	e.Core.Execute(ctx, from, a.(core.IOAction))
}

// Committed returns the committed ranges of tp in offset order.
func (e *Engine) Committed(tp core.TopicPartition) []core.BatchRange {
	d := e.spec.Exec(core.CPCall{Op: core.Lookup{TP: tp}}).(core.CPDone)
	if d.Err != core.OK {
		return nil
	}
	return d.Resp.(core.LookupResp).Ranges
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
func (e *Engine) tooLarge(op core.CPOp) bool {
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
	known := e.spec.Exec(core.CPCall{Op: core.DescribeTopics{}}).(core.CPDone).Resp.(core.DescribeTopicsResp).Topics
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
