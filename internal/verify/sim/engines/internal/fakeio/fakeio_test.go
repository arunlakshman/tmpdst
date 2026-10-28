package fakeio

import (
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/internal/enginetest"
	"manifester/internal/verify/veritas"
)

const ms = core.Duration(1_000_000)

// stub is a minimal engine around a Core so enginetest can drive it.
type stub struct{ *Core }

func (s stub) Handles(core.Action) bool { return true }
func (s stub) Execute(ctx sim.EngineCtx, f core.NodeInstance, a core.Action) {
	s.Core.Execute(ctx, f, a.(core.IOCall))
}

func params() Params {
	ts := sim.TwoSpeed{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return Params{Apply: ts, Respond: ts, P503: 0.1, PTimeout: 0.1, PLost: 0.1, CallerTimeout: 1000 * ms}
}

// fresh recomputes the state hash with the cache invalidated.
func fresh(c *Core) uint64 {
	c.specOK = false
	return c.StateHash()
}

func TestSpecHashCacheObjectStore(t *testing.T) {
	spec := veritas.NewObjectStore()
	c := New(Hooks{
		Name: "o", Prefix: "obj", Params: params(),
		Spec: func(a core.IOCall) core.IOResult { return spec.Exec(a) },
		Mutating: func(a core.IOCall) bool {
			switch a.(type) {
			case core.ObjectPut, core.ObjectDelete:
				return true
			}
			return false
		},
		Fail: func(a core.IOCall, e core.ErrCode, o core.Outcome) core.IOResult {
			switch a := a.(type) {
			case core.ObjectPut:
				return core.ObjectPutDone{Req: a.Req, Err: e, Outcome: o}
			case core.ObjectDelete:
				return core.ObjectDeleteDone{Req: a.Req, Err: e, Outcome: o}
			case core.ObjectGet:
				return core.ObjectGetDone{Req: a.Req, Err: e}
			}
			return core.ObjectListDone{Req: a.ReqID(), Err: e}
		},
		Changed: func(c core.IOResult) bool {
			switch c := c.(type) {
			case core.ObjectPutDone:
				return c.Outcome == core.Applied
			case core.ObjectDeleteDone:
				return c.Outcome == core.Applied
			}
			return false
		},
		SpecHash: spec.StateHash,
	})
	ctx := enginetest.New(stub{c}, 5)
	from := enginetest.Ref("a")
	r := sim.NewRand(99)
	for i := range 400 {
		ctx.T += sim.Time(ms)
		k := string(rune('a' + r.Between(0, 6)))
		req := core.ReqID(i)
		switch r.Between(0, 4) {
		case 0, 1:
			c.Execute(ctx, from, core.ObjectPut{Req: req, Key: k, Data: []byte{byte(i)}})
		case 2:
			c.Execute(ctx, from, core.ObjectDelete{Req: req, Key: k})
		default:
			c.Execute(ctx, from, core.ObjectGet{Req: req, Ref: core.SegmentRef{Key: k}})
		}
		if r.Chance(0.3) {
			ctx.Run()
		}
		if got, want := c.StateHash(), fresh(c); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	ctx.Run()
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("final: cached hash stale")
	}
}

func TestSpecHashCacheControlPlane(t *testing.T) {
	spec := veritas.NewControlPlane()
	c := New(Hooks{
		Name: "cp", Prefix: "cp", Params: params(),
		Spec: func(a core.IOCall) core.IOResult { return spec.Exec(a) },
		Mutating: func(a core.IOCall) bool {
			_, ok := a.(core.ControlPlaneCall).Op.(core.CommitSegment)
			return ok
		},
		Fail: func(a core.IOCall, err core.ErrCode, o core.Outcome) core.IOResult {
			return core.ControlPlaneDone{Req: a.ReqID(), Err: err, Outcome: o}
		},
		Changed:  func(c core.IOResult) bool { return c.(core.ControlPlaneDone).Outcome == core.Applied },
		SpecHash: spec.StateHash,
	})
	ctx := enginetest.New(stub{c}, 6)
	r := sim.NewRand(77)
	nodes := []core.NodeInstance{enginetest.Ref("a"), enginetest.Ref("b"), enginetest.Ref("c")}
	tps := []core.TopicPartition{core.TP("t", 0), core.TP("t", 1)}
	for _, tp := range tps[:1] {
		if spec.CreateTopic(tp.Topic, 2) == core.OK {
			c.Bump()
		}
	}
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("after CreateTopic: stale")
	}
	for i := range 400 {
		ctx.T += sim.Time(ms)
		n := nodes[r.Between(0, 3)]
		req := core.ReqID(i)
		switch r.Between(0, 2) {
		case 0:
			// Keys repeat (replays, and conflicts when the request differs).
			c.Execute(ctx, n, core.ControlPlaneCall{Req: req, Op: core.CommitSegment{
				SegmentKey: string(rune('a' + r.Between(0, 8))),
				Batches:    []core.PendingBatch{{TP: tps[r.Between(0, 2)], Records: 1 + r.Between(0, 3)}},
			}})
		default:
			c.Execute(ctx, n, core.ControlPlaneCall{Req: req, Op: core.FindSegments{TP: tps[0]}})
		}
		if r.Chance(0.3) {
			ctx.Run()
		}
		if got, want := c.StateHash(), fresh(c); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	ctx.Run()
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("final: cached hash stale")
	}
}
