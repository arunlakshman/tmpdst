// Package objstore is the simulated S3 (decision D): veritas.ObjectStore as the
// state core, plus injected faults and timing. ErrNotFound and ErrInvalid come
// only from the spec.
package objstore

import (
	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/internal/fakeio"
	"manifester/internal/verify/veritas"
)

// Engine is the fake object store. It is also the sim.ObjectTruth.
type Engine struct {
	*fakeio.Core
	spec *veritas.ObjectStore
}

var (
	_ sim.Engine      = (*Engine)(nil)
	_ sim.ObjectTruth = (*Engine)(nil)
)

// New returns an empty store with the given faults.
func New(f sim.ObjectStoreFaults) *Engine {
	e := &Engine{spec: veritas.NewObjectStore()}
	e.Core = fakeio.New(fakeio.Hooks{
		Name:   "objstore",
		Prefix: "obj",
		Params: fakeio.Params{
			Apply: f.ApplyLatency, Respond: f.RespondLatency,
			P503: f.Unavailable503, PTimeout: f.Timeout, PLost: f.LostResponse,
			Outage: f.Outage, CallerTimeout: f.CallerTimeout,
		},
		Spec: func(a core.IOAction) core.Completion { return e.spec.Exec(a) },
		Mutating: func(a core.IOAction) bool {
			switch a.(type) {
			case core.ObjPut, core.ObjDelete:
				return true
			}
			return false
		},
		Fail:     fail,
		Changed:  changed,
		SpecHash: e.spec.StateHash,
		Extra: func(r sim.CountedRand, a core.IOAction) (bool, uint64) {
			if _, ok := a.(core.ObjGet); !ok || !r.Chance(f.GetCorruptProb) {
				return false, 0
			}
			return true, r.Uint64()
		},
		Finish: func(ctx sim.EngineCtx, c core.Completion, p fakeio.Plan) core.Completion {
			g, ok := c.(core.ObjGetDone)
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

func fail(a core.IOAction, e core.ErrCode, o core.Outcome) core.Completion {
	switch a := a.(type) {
	case core.ObjPut:
		return core.ObjPutDone{Req: a.Req, Key: a.Key, Err: e, Outcome: o}
	case core.ObjGet:
		return core.ObjGetDone{Req: a.Req, Err: e}
	case core.ObjDelete:
		return core.ObjDeleteDone{Req: a.Req, Err: e, Outcome: o}
	case core.ObjList:
		return core.ObjListDone{Req: a.Req, Err: e}
	}
	panic("objstore: not an object store action")
}

func changed(c core.Completion) bool {
	switch c := c.(type) {
	case core.ObjPutDone:
		return c.Outcome == core.Applied
	case core.ObjDeleteDone:
		return c.Outcome == core.Applied
	}
	return false
}

// Handles reports whether a is an object store action.
func (e *Engine) Handles(a core.Action) bool {
	switch a.(type) {
	case core.ObjPut, core.ObjGet, core.ObjDelete, core.ObjList:
		return true
	}
	return false
}

// Execute starts a; a must be an IOAction (Handles guarantees it).
func (e *Engine) Execute(ctx sim.EngineCtx, from core.NodeRef, a core.Action) {
	e.Core.Execute(ctx, from, a.(core.IOAction))
}

// Object returns a copy of the stored object.
func (e *Engine) Object(key string) ([]byte, bool) {
	g, ok := e.spec.Exec(core.ObjGet{Ref: core.SegmentRef{Key: key}}).(core.ObjGetDone)
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
