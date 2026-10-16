// Package cpengine is the simulated control plane (decisions 3, 14, 23):
// veritas.ControlPlane as the state core, plus injected faults and timing.
// ErrConflict, ErrFenced and ErrInvalid come only from the spec; a retry of a
// committed segment returns the original offsets whatever the faults did.
package cpengine

import (
	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/fakeio"
	"manifester/veritas"
)

// Engine is the fake control plane. It is also the sim.CPTruth.
type Engine struct {
	*fakeio.Core
	spec *veritas.ControlPlane
}

var (
	_ sim.Engine  = (*Engine)(nil)
	_ sim.CPTruth = (*Engine)(nil)
)

// New returns an empty control plane with the given faults.
func New(f sim.ControlPlaneFaults) *Engine {
	e := &Engine{spec: veritas.NewControlPlane()}
	e.Core = fakeio.New(fakeio.Hooks{
		Name:   "cpengine",
		Prefix: "cp",
		Params: fakeio.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOAction) core.Completion { return e.spec.Exec(a) },
		Mutating: func(a core.IOAction) bool {
			switch a.(core.CPCall).Op.(type) {
			case core.Register, core.CommitSegment:
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

// HighWatermark is the next offset of tp.
func (e *Engine) HighWatermark(tp core.TopicPartition) core.Offset { return e.spec.HighWatermark(tp) }
