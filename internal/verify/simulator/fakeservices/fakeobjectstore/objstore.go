// Package fakeobjectstore is the simulated S3 (decision D): speccheck.ObjectStore as the
// state core, plus injected faults and timing. ErrNotFound and ErrInvalid come
// only from the spec.
package fakeobjectstore

import (
	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/verify/simulator/fakeservices/internal/fakecompletions"
	"manifester/internal/verify/speccheck"
)

// Engine is the fake object store. It is also the simulator.ObjectTruth.
type Engine struct {
	*fakecompletions.CompletionCore
	spec *speccheck.ObjectStore
}

var (
	_ simulator.FakeService          = (*Engine)(nil)
	_ simulator.ObjectStoreRealState = (*Engine)(nil)
)

// New returns an empty store with the given faults.
func New(f simulator.ObjectStoreFaults) *Engine {
	e := &Engine{spec: speccheck.NewObjectStore()}
	e.CompletionCore = fakecompletions.New(fakecompletions.CompletionHooks{
		Name:   "fakeobjectstore",
		Prefix: "obj",
		Params: fakecompletions.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOCall) core.IOResult { return e.spec.Exec(a) },
		Mutating: func(a core.IOCall) bool {
			switch a.(type) {
			case core.ObjectPut, core.ObjectDelete:
				return true
			}
			return false
		},
		Fail:     fail,
		Changed:  changed,
		SpecHash: e.spec.StateHash,
		Extra: func(r simulator.CountedRand, a core.IOCall) (bool, uint64) {
			if _, ok := a.(core.ObjectGet); !ok || !r.Chance(f.GetCorruptProb) {
				return false, 0
			}
			return true, r.Uint64()
		},
		Finish: func(ctx simulator.FakeServiceContext, c core.IOResult, p fakecompletions.CompletionPlan) core.IOResult {
			g, ok := c.(core.ObjectGetDone)
			if !ok || !p.Flag || g.Err != core.OK || len(g.Data) == 0 {
				return c
			}
			bit := p.Bits % uint64(len(g.Data)*8)
			g.Data[bit/8] ^= 1 << (bit % 8) // Data is the spec's copy
			ctx.Fault("obj_get_corrupt", core.Field{Key: "req", Value: core.U64(g.Req)}, core.Field{Key: "bit", Value: core.U64(bit)})
			ctx.Assert().Reachable("obj get corrupted")
			return g
		},
	})
	return e
}

func fail(a core.IOCall, e core.ErrCode, o core.Outcome) core.IOResult {
	switch a := a.(type) {
	case core.ObjectPut:
		return core.ObjectPutDone{Req: a.Req, Key: a.Key, Err: e, Outcome: o}
	case core.ObjectGet:
		return core.ObjectGetDone{Req: a.Req, Err: e}
	case core.ObjectDelete:
		return core.ObjectDeleteDone{Req: a.Req, Err: e, Outcome: o}
	case core.ObjectList:
		return core.ObjectListDone{Req: a.Req, Err: e}
	}
	panic("fakeobjectstore: not an object store action")
}

func changed(c core.IOResult) bool {
	switch c := c.(type) {
	case core.ObjectPutDone:
		return c.Outcome == core.Applied
	case core.ObjectDeleteDone:
		return c.Outcome == core.Applied
	}
	return false
}

// Handles reports whether a is an object store action.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.ObjectPut, core.ObjectGet, core.ObjectDelete, core.ObjectList:
		return true
	}
	return false
}

// Execute starts a; a must be an IOCall (Handles guarantees it).
func (e *Engine) Execute(ctx simulator.FakeServiceContext, from core.NodeInstance, a core.Action) {
	e.CompletionCore.Execute(ctx, from, a.(core.IOCall))
}

// Object returns a copy of the stored object.
func (e *Engine) Object(key string) ([]byte, bool) {
	g, ok := e.spec.Exec(core.ObjectGet{Ref: core.SegmentRef{Key: key}}).(core.ObjectGetDone)
	if !ok || g.Err != core.OK {
		return nil, false
	}
	return g.Data, true
}

// EachObject visits every object in sorted key order.
func (e *Engine) EachObject(fn func(key string, data []byte)) {
	for _, k := range e.spec.Keys() {
		if d, ok := e.Object(k); ok {
			fn(k, d)
		}
	}
}
