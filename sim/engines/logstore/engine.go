// Package logstore is the simulated Dynamo loglet (CP-DSM-3): an if-absent Put
// store for LogAppend/LogRead/LogCheckTail/LogTrim, wrapped in the shared
// fakeio fault/timing core. Fault coins and delays are drawn at issue time
// (decision 14). Every live ReqID gets exactly one completion; a lost answer
// surfaces as ErrTimeout+MaybeApplied.
package logstore

import (
	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/fakeio"
)

// Engine is the fake Dynamo log. Spec state is local; faults come from fakeio.
type Engine struct {
	*fakeio.Core
	spec *store
}

var _ sim.Engine = (*Engine)(nil)

// New returns an empty logstore with the given faults.
func New(f sim.LogStoreFaults) *Engine {
	e := &Engine{spec: &store{}}
	e.Core = fakeio.New(fakeio.Hooks{
		Name:   "logstore",
		Prefix: "log",
		Params: fakeio.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec:     func(a core.IOAction) core.Completion { return e.spec.exec(a) },
		Mutating: mutating,
		Fail:     fail,
		Changed:  changed,
		SpecHash: e.spec.stateHash,
	})
	return e
}

func mutating(a core.IOAction) bool {
	switch a.(type) {
	case core.LogAppend, core.LogTrim:
		return true
	}
	return false
}

func fail(a core.IOAction, err core.ErrCode, o core.Outcome) core.Completion {
	switch a := a.(type) {
	case core.LogAppend:
		return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: err, Outcome: o}
	case core.LogRead:
		return core.LogReadDone{Req: a.Req, Err: err, Outcome: o}
	case core.LogCheckTail:
		return core.LogCheckTailDone{Req: a.Req, Err: err, Outcome: o}
	case core.LogTrim:
		return core.LogTrimDone{Req: a.Req, Err: err, Outcome: o}
	}
	panic("logstore: not a log action")
}

func changed(c core.Completion) bool {
	switch c := c.(type) {
	case core.LogAppendDone:
		return c.Outcome == core.Applied && !c.Taken
	case core.LogTrimDone:
		return c.Outcome == core.Applied
	}
	return false
}

// Handles reports whether a is a log IOAction.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.LogAppend, core.LogRead, core.LogCheckTail, core.LogTrim:
		return true
	}
	return false
}

// Execute starts a; a must be a log IOAction (Handles guarantees it).
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	e.Core.Execute(ctx, from, a.(core.IOAction))
}

// Entry returns a copy of the bytes at slot, or false when empty or tombstoned.
func (e *Engine) Entry(slot core.LogSlot) ([]byte, bool) {
	return e.spec.entry(slot)
}
