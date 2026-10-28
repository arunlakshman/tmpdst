// Package logstore is the simulated Dynamo loglet (CP-DSM-3): an if-absent Put
// store for LogAppend/LogRead/LogGetEnd/LogDeleteOld, wrapped in the shared
// fakeio fault/timing core. Fault coins and delays are drawn at issue time
// (decision 14). Every live RequestID gets exactly one completion; a lost answer
// surfaces as ErrTimeout+MaybeApplied.
//
// After CP-DSM-8 the engine is also sim.CPTruth: a reference replay of durable
// slots through controlplane/machine.ReplaySlot (decision 21 / log replay).
package logstore

import (
	"slices"

	"manifester/internal/controlplane/applier"
	"manifester/internal/controlplane/machine"
	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/internal/fakeio"
)

// Engine is the fake Dynamo log. Spec state is local; faults come from fakeio.
type Engine struct {
	*fakeio.Core
	spec *store

	ref         *applier.Applier
	applied     core.LogPosition // next slot to fold into ref
	gen         uint64
	windowSlots int64
	trimStride  int64
}

var (
	_ sim.Engine  = (*Engine)(nil)
	_ sim.CPTruth = (*Engine)(nil)
)

// New returns an empty logstore with the given faults.
func New(f sim.LogStoreFaults) *Engine {
	e := &Engine{
		spec:        &store{bug: f.Bug},
		ref:         applier.NewMem(),
		windowSlots: int64(f.WindowSlots),
		trimStride:  int64(f.TrimStride),
	}
	if e.trimStride <= 0 {
		e.trimStride = 16
	}
	e.Core = fakeio.New(fakeio.Hooks{
		Name:   "logstore",
		Prefix: "log",
		Params: fakeio.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec:     e.specAndTruth,
		Mutating: mutating,
		Fail:     fail,
		Changed:  changed,
		SpecHash: e.stateHash,
	})
	return e
}

func (e *Engine) specAndTruth(a core.IOCall) core.IOResult {
	c := e.spec.exec(a)
	switch c := c.(type) {
	case core.LogAppendDone:
		if c.Err == core.OK && c.Outcome == core.Applied && !c.Taken {
			e.advanceRef()
		}
	case core.LogDeleteOldDone:
		if c.Err == core.OK && c.Outcome == core.Applied {
			through := a.(core.LogDeleteOld).Through
			// LogDeleteOld drops durable slots only. The idempotency window is cut on
			// apply stride (decision 48), same as controlplane/machine.
			if e.applied <= through {
				e.applied = through + 1
			}
			e.gen++
		}
	}
	return c
}

func (e *Engine) advanceRef() {
	for {
		data, ok := e.spec.entry(e.applied)
		if !ok {
			return
		}
		slot := e.applied
		if err := machine.ReplaySlot(e.ref, slot, data); err != nil {
			// Foreign / test payloads are durable bytes but not CP entries.
			e.applied++
			e.gen++
			continue
		}
		e.trimWindow(slot)
		e.applied++
		e.gen++
	}
}

func (e *Engine) trimWindow(slot core.LogPosition) {
	window, stride := e.windowSlots, e.trimStride
	if window <= 0 || stride <= 0 {
		return
	}
	if int64(slot)%stride == 0 && int64(slot) >= window {
		_, _ = e.ref.Trim(int64(slot) - window)
	}
}

func (e *Engine) stateHash() uint64 {
	return e.spec.stateHash() ^ e.ref.StateHash() ^ e.gen
}

func mutating(a core.IOCall) bool {
	switch a.(type) {
	case core.LogAppend, core.LogDeleteOld:
		return true
	}
	return false
}

func fail(a core.IOCall, err core.ErrCode, o core.Outcome) core.IOResult {
	switch a := a.(type) {
	case core.LogAppend:
		return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: err, Outcome: o}
	case core.LogRead:
		return core.LogReadDone{Req: a.Req, Err: err, Outcome: o}
	case core.LogGetEnd:
		return core.LogGetEndDone{Req: a.Req, Err: err, Outcome: o}
	case core.LogDeleteOld:
		return core.LogDeleteOldDone{Req: a.Req, Err: err, Outcome: o}
	}
	panic("logstore: not a log action")
}

func changed(c core.IOResult) bool {
	switch c := c.(type) {
	case core.LogAppendDone:
		return c.Outcome == core.Applied && !c.Taken
	case core.LogDeleteOldDone:
		return c.Outcome == core.Applied
	}
	return false
}

// Handles reports whether a is a log IOCall.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.LogAppend, core.LogRead, core.LogGetEnd, core.LogDeleteOld:
		return true
	}
	return false
}

// Execute starts a; a must be a log IOCall (Handles guarantees it).
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeInstance, a core.Action) {
	e.Core.Execute(ctx, from, a.(core.IOCall))
}

// Entry returns a copy of the bytes at slot, or false when empty or tombstoned.
func (e *Engine) Entry(slot core.LogPosition) ([]byte, bool) {
	return e.spec.entry(slot)
}

// CreateTopic seeds a topic on the reference fold before the run.
func (e *Engine) CreateTopic(name string, partitions int32) core.ErrCode {
	err := e.ref.CreateTopic(name, partitions)
	if err == core.OK {
		e.gen++
	}
	return err
}

// Committed implements sim.CPTruth.
func (e *Engine) Committed(tp core.TopicPartition) []core.BatchRange {
	d, err := e.ref.Lookup(core.FindSegments{TP: tp})
	if err != nil || d.Err != core.OK {
		return nil
	}
	return slices.Clone(core.ResponseAs[core.FindSegmentsResp](d).Ranges)
}

// HighWatermark implements sim.CPTruth.
func (e *Engine) HighWatermark(tp core.TopicPartition) core.Offset {
	return e.ref.End(tp)
}

// Gen implements sim.CPTruth.
func (e *Engine) Gen() uint64 { return e.gen }
