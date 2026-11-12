// Package fakeobjectstore is the simulated S3 (decision D): objectstore.Spec as the
// state core, plus injected faults and timing. ErrNotFound and ErrInvalid come
// only from the spec.
package fakeobjectstore

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/fakecompletions"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// Engine is the fake object store. It is also the simulator.ObjectTruth.
type Engine struct {
	*fakecompletions.CompletionCore
	spec *objectstore.Spec
}

var (
	_ simulator.FakeService          = (*Engine)(nil)
	_ simulator.ObjectStoreRealState = (*Engine)(nil)
)

// callOf returns the object store call inside an action. The fake gets only
// object store actions (Handles guarantees it).
func callOf(a core.IOCall) objectstore.Call {
	oc, ok := a.(core.ObjectCall)
	if !ok {
		panic("fakeobjectstore: not an object store action")
	}
	return oc.Call
}

// New returns an empty store with the given faults.
func New(f simulator.ObjectStoreFaults) *Engine {
	e := &Engine{spec: objectstore.NewSpec()}
	e.CompletionCore = fakecompletions.New(fakecompletions.CompletionHooks{
		Name:   "fakeobjectstore",
		Prefix: "obj",
		Params: fakecompletions.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOCall) core.IOResult { return core.ObjectDone{Result: e.spec.Exec(callOf(a))} },
		Mutating: func(a core.IOCall) bool {
			switch callOf(a).(type) {
			case objectstore.Put, objectstore.Delete:
				return true
			case objectstore.Get, objectstore.List:
				return false
			}
			return false
		},
		Fail:     fail,
		Changed:  changed,
		SpecHash: e.spec.StateHash,
		Extra: func(r simulator.CountedRand, a core.IOCall) (bool, uint64) {
			if _, ok := callOf(a).(objectstore.Get); !ok || !r.Chance(f.GetCorruptProb) {
				return false, 0
			}
			return true, r.Uint64()
		},
		Finish: func(ctx simulator.FakeServiceContext, c core.IOResult, p fakecompletions.CompletionPlan) core.IOResult {
			done, ok := c.(core.ObjectDone)
			if !ok {
				return c
			}
			g, ok := done.Result.(objectstore.GetDone)
			if !ok || !p.Flag || g.Err != core.OK || len(g.Data) == 0 {
				return c
			}
			bit := p.Bits % uint64(len(g.Data)*8)
			g.Data[bit/8] ^= 1 << (bit % 8) // Data is the spec's copy
			ctx.Fault("obj_get_corrupt", machine.Field{Key: "req", Value: machine.Uint64Value(g.Req)}, machine.Field{Key: "bit", Value: machine.Uint64Value(bit)})
			ctx.Assert().Reachable(objGetCorrupted)
			return core.ObjectDone{Result: g}
		},
	})
	return e
}

func fail(a core.IOCall, e core.ErrCode, o core.Outcome) core.IOResult {
	switch a := callOf(a).(type) {
	case objectstore.Put:
		return core.ObjectDone{Result: objectstore.PutDone{Req: a.Req, Key: a.Key, Err: e, Outcome: o}}
	case objectstore.Get:
		return core.ObjectDone{Result: objectstore.GetDone{Req: a.Req, Err: e}}
	case objectstore.Delete:
		return core.ObjectDone{Result: objectstore.DeleteDone{Req: a.Req, Err: e, Outcome: o}}
	case objectstore.List:
		return core.ObjectDone{Result: objectstore.ListDone{Req: a.Req, Err: e}}
	}
	panic("fakeobjectstore: not an object store action")
}

func changed(c core.IOResult) bool {
	done, ok := c.(core.ObjectDone)
	if !ok {
		return false
	}
	switch r := done.Result.(type) {
	case objectstore.PutDone:
		return r.Outcome == core.Applied
	case objectstore.DeleteDone:
		return r.Outcome == core.Applied
	case objectstore.GetDone, objectstore.ListDone:
		return false
	}
	return false
}

// Handles reports whether a is an object store action.
func (e *Engine) Handles(a core.Action) bool {
	_, ok := a.(core.ObjectCall)
	return ok
}

// Execute starts a; a must be an IOCall (Handles guarantees it).
func (e *Engine) Execute(ctx simulator.FakeServiceContext, from machine.NodeInstance, a core.Action) {
	e.CompletionCore.Execute(ctx, from, a.(core.IOCall))
}

// Object returns a copy of the stored object.
func (e *Engine) Object(key string) ([]byte, bool) {
	g, ok := e.spec.Exec(objectstore.Get{Ref: core.SegmentRef{Key: key}}).(objectstore.GetDone)
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
