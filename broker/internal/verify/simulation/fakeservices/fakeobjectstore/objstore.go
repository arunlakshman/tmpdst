// Package fakeobjectstore is the simulated S3 (decision D): objectstore.Spec as the
// state core, plus injected faults and timing. ErrNotFound and ErrInvalid come
// only from the spec.
//
// It is a kernel.Service. A broker node hands it a core.ObjectCall, and the
// service answers with core.ObjectDone.
package fakeobjectstore

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/fakecompletions"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ServiceOutage is a correlated, sustained failure window: every call fails while
// it lasts.
type ServiceOutage = fakecompletions.ServiceOutage

// Faults are the fault families of the fake object store (decision D).
type Faults struct {
	ApplyLatency   kernel.FastWithRareSlow // issue to effect
	RespondLatency kernel.FastWithRareSlow // effect to reply
	Unavailable503 float64                 // refused before admission: NotApplied
	Timeout        float64                 // caller gives up; the call may or may not apply: MaybeApplied
	LostResponse   float64                 // applied, response never arrives (surfaces as timeout)
	GetCorruptProb float64                 // flip one bit in GetDone.Data; the stored object stays intact
	Outage         ServiceOutage
	CallerTimeout  machine.Duration
}

// Engine is the fake object store.
type Engine struct {
	*fakecompletions.Core
	spec *objectstore.Spec
}

var _ kernel.Service[core.ObjectCall, core.EventData] = (*Engine)(nil)

// callOf returns the object store call inside an action. The fake gets only
// object store calls.
func callOf(a core.IOCall) objectstore.Call {
	oc, ok := a.(core.ObjectCall)
	if !ok {
		panic("fakeobjectstore: not an object store call")
	}
	return oc.Call
}

// New returns an empty store with the given faults. The world gives it its lanes.
func New(w *kernel.World, f Faults) *Engine {
	e := &Engine{spec: objectstore.NewSpec()}
	e.Core = fakecompletions.New(w, fakecompletions.Hooks{
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
		Extra: func(r *rng.Rand, a core.IOCall) (bool, uint64) {
			if _, ok := callOf(a).(objectstore.Get); !ok || !r.Chance(f.GetCorruptProb) {
				return false, 0
			}
			return true, r.Uint64()
		},
		Finish: func(cx *fakecompletions.Cx, c core.IOResult, p fakecompletions.Plan) core.IOResult {
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
			cx.Fault("obj_get_corrupt", machine.Field{Key: "req", Value: machine.Uint64Value(uint64(g.Req))}, machine.Field{Key: "bit", Value: machine.Uint64Value(bit)})
			cx.Assert().Reachable(objGetCorrupted)
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
	panic("fakeobjectstore: not an object store call")
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

// Handle starts one object store call. It implements kernel.Service.
func (e *Engine) Handle(cx *fakecompletions.Cx, from fakecompletions.Ref, req core.ObjectCall) {
	e.Start(cx, from, req)
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
