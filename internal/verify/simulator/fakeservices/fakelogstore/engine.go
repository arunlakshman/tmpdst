// Package fakelogstore is the simulated Dynamo loglet (CP-DSM-3): an if-absent Put
// store for LogAppend/LogRead/LogGetEnd/LogDeleteOld/LogClaimEpoch, wrapped in the shared
// fakecompletions fault/timing core. Fault coins and delays are drawn at issue time
// (decision 14). Every live RequestID gets exactly one completion; a lost answer
// surfaces as ErrTimeout+MaybeApplied.
//
// After CP-DSM-8 the engine is also simulator.ControlPlaneRealState: a reference replay of durable
// slots through controlplane/statemachine.ReplayPosition (decision 21 / log replay).
package fakelogstore

import (
	"slices"

	"github.com/manifesterio/mds/internal/controlplane/statemachine"
	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
	"github.com/manifesterio/mds/internal/verify/simulator"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/fakelogservice"
	"github.com/manifesterio/mds/internal/verify/simulator/fakeservices/internal/fakecompletions"
)

// Engine is the fake Dynamo log. Spec state is local; faults come from fakecompletions.
type Engine struct {
	*fakecompletions.CompletionCore
	spec *store

	ref         *stateupdater.StateUpdater
	applied     core.LogPosition // next slot to fold into ref
	gen         uint64
	windowSlots int64
	trimStride  int64

	// asserts is the recorder of the landing op being applied (set by OnInternal).
	asserts core.Assertions
	// writers maps each epoch seen in the truth to the one writer name of that epoch.
	writers map[uint64]string
}

var (
	_ simulator.FakeService           = (*Engine)(nil)
	_ simulator.ControlPlaneRealState = (*Engine)(nil)
)

// New returns an empty logstore with the given faults.
func New(f simulator.LogStoreFaults) *Engine {
	e := &Engine{
		spec:        newStore(f.Bug == fakelogservice.InjectedBugNoFloorCheck),
		ref:         stateupdater.NewMem(),
		windowSlots: int64(f.WindowSlots),
		trimStride:  int64(f.TrimStride),
		writers:     map[uint64]string{},
	}
	if e.trimStride <= 0 {
		e.trimStride = 16
	}
	e.CompletionCore = fakecompletions.New(fakecompletions.CompletionHooks{
		Name:   "logstore",
		Prefix: "log",
		Params: fakecompletions.Params{
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

// OnInternal lands one planned op. The truth engine reports through the assertion
// recorder of this context, so the recorder is lent to it for the landing only.
func (e *Engine) OnInternal(ctx simulator.FakeServiceContext, body any) {
	e.asserts = ctx.Assert()
	e.CompletionCore.OnInternal(ctx, body)
	e.asserts = nil
}

// checkOneWriter checks the rule "one epoch has one writer" for an entry that the
// truth replays. An entry with no envelope, or with epoch 0 (no writer claimed
// yet), is skipped.
func (e *Engine) checkOneWriter(slot core.LogPosition, data []byte) {
	epoch, writer, ok := statemachine.EntryWriter(data)
	if !ok || epoch == 0 {
		return
	}
	first, seen := e.writers[epoch]
	if !seen {
		e.writers[epoch] = writer
		first = writer
	}
	e.asserts.Always(first == writer, "logcp-sim: one epoch has one writer",
		core.ContextPair{Key: "epoch", Value: epoch},
		core.ContextPair{Key: "slot", Value: slot},
		core.ContextPair{Key: "writer", Value: writer},
		core.ContextPair{Key: "first_writer", Value: first})
}

func (e *Engine) advanceRef() {
	for {
		data, ok := e.spec.entry(e.applied)
		if !ok {
			return
		}
		slot := e.applied
		e.checkOneWriter(slot, data)
		if err := statemachine.ReplayPosition(e.ref, slot, data); err != nil {
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
	case core.LogAppend, core.LogDeleteOld, core.LogClaimEpoch:
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
	case core.LogClaimEpoch:
		return core.LogClaimEpochDone{Req: a.Req, Err: err, Outcome: o}
	}
	panic("logstore: not a log action")
}

func changed(c core.IOResult) bool {
	switch c := c.(type) {
	case core.LogAppendDone:
		return c.Outcome == core.Applied && !c.Taken
	case core.LogDeleteOldDone:
		return c.Outcome == core.Applied
	case core.LogClaimEpochDone:
		return c.Outcome == core.Applied && c.Won
	}
	return false
}

// Handles reports whether a is a log IOCall.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.LogAppend, core.LogRead, core.LogGetEnd, core.LogDeleteOld, core.LogClaimEpoch:
		return true
	}
	return false
}

// Execute starts a; a must be a log IOCall (Handles guarantees it).
func (e *Engine) Execute(ctx simulator.FakeServiceContext, from core.NodeInstance, a core.Action) {
	e.CompletionCore.Execute(ctx, from, a.(core.IOCall))
}

// Entry returns a copy of the bytes at slot, or false when the slot is empty or
// at or below the floor.
func (e *Engine) Entry(slot core.LogPosition) ([]byte, bool) {
	return e.spec.entry(slot)
}

// Epoch returns the stored writer epoch and its owner.
func (e *Engine) Epoch() (uint64, string) {
	return e.spec.epoch, e.spec.owner
}

// CreateTopic seeds a topic on the reference fold before the run.
func (e *Engine) CreateTopic(name string, partitions int32) core.ErrCode {
	err := e.ref.CreateTopic(name, partitions)
	if err == core.OK {
		e.gen++
	}
	return err
}

// Committed implements simulator.ControlPlaneRealState.
func (e *Engine) Committed(topicPartition core.TopicPartition) []core.BatchRange {
	d, err := e.ref.Lookup(core.FindSegments{TopicPartition: topicPartition})
	if err != nil || d.Err != core.OK {
		return nil
	}
	return slices.Clone(core.ResponseAs[core.FindSegmentsResp](d).Ranges)
}

// HighWatermark implements simulator.ControlPlaneRealState.
func (e *Engine) HighWatermark(topicPartition core.TopicPartition) core.Offset {
	return e.ref.End(topicPartition)
}

// Gen implements simulator.ControlPlaneRealState.
func (e *Engine) Gen() uint64 { return e.gen }
