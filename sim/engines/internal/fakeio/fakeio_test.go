package fakeio

import (
	"testing"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/enginetest"
	"manifester/veritas"
)

const ms = core.Duration(1_000_000)

// stub is a minimal engine around a Core so enginetest can drive it.
type stub struct{ *Core }

func (s stub) Handles(core.Action) bool { return true }
func (s stub) Execute(ctx sim.EngineCtx, f core.NodeRef, a core.Action) {
	s.Core.Execute(ctx, f, a.(core.IOAction))
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
		Spec: func(a core.IOAction) core.Completion { return spec.Exec(a) },
		Mutating: func(a core.IOAction) bool {
			switch a.(type) {
			case core.ObjPut, core.ObjDelete:
				return true
			}
			return false
		},
		Fail: func(a core.IOAction, e core.ErrCode, o core.Outcome) core.Completion {
			switch a := a.(type) {
			case core.ObjPut:
				return core.ObjPutDone{Req: a.Req, Err: e, Outcome: o}
			case core.ObjDelete:
				return core.ObjDeleteDone{Req: a.Req, Err: e, Outcome: o}
			case core.ObjGet:
				return core.ObjGetDone{Req: a.Req, Err: e}
			}
			return core.ObjListDone{Req: a.ReqID(), Err: e}
		},
		Changed: func(c core.Completion) bool {
			switch c := c.(type) {
			case core.ObjPutDone:
				return c.Outcome == core.Applied
			case core.ObjDeleteDone:
				return c.Outcome == core.Applied
			}
			return false
		},
		SpecHash: spec.StateHash,
	})
	ctx := enginetest.New(stub{c}, 5)
	from := enginetest.Ref("a")
	r := sim.NewRand(99)
	for i := 0; i < 400; i++ {
		ctx.T += sim.Time(ms)
		k := string(rune('a' + r.Between(0, 6)))
		req := core.ReqID(i)
		switch r.Between(0, 4) {
		case 0, 1:
			c.Execute(ctx, from, core.ObjPut{Req: req, Key: k, Data: []byte{byte(i)}})
		case 2:
			c.Execute(ctx, from, core.ObjDelete{Req: req, Key: k})
		default:
			c.Execute(ctx, from, core.ObjGet{Req: req, Ref: core.SegmentRef{Key: k}})
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
		Spec: func(a core.IOAction) core.Completion { return spec.Exec(a) },
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
		Changed:  func(c core.Completion) bool { return c.(core.CPDone).Outcome == core.Applied },
		SpecHash: spec.StateHash,
	})
	ctx := enginetest.New(stub{c}, 6)
	r := sim.NewRand(77)
	nodes := []core.NodeRef{enginetest.Ref("a"), enginetest.Ref("b"), enginetest.Ref("c")}
	tps := []core.TopicPartition{{Topic: "t", Partition: 0}, {Topic: "t", Partition: 1}}
	for _, tp := range tps[:1] {
		if spec.CreateTopic(tp.Topic, 2) == core.OK {
			c.Bump()
		}
	}
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("after CreateTopic: stale")
	}
	for i := 0; i < 400; i++ {
		ctx.T += sim.Time(ms)
		n := nodes[r.Between(0, 3)]
		req := core.ReqID(i)
		switch r.Between(0, 3) {
		case 0:
			c.Execute(ctx, n, core.CPCall{Req: req, Op: core.Register{Self: n}})
		case 1:
			// Epoch guesses: often stale (fenced), sometimes right; keys repeat (replays).
			c.Execute(ctx, n, core.CPCall{Req: req, Op: core.CommitSegment{
				SegmentKey: string(rune('a' + r.Between(0, 8))), Owner: n, Epoch: core.Epoch(r.Between(0, 6)),
				Batches: []core.PendingBatch{{TP: tps[r.Between(0, 2)], Records: 1 + r.Between(0, 3)}},
			}})
		default:
			c.Execute(ctx, n, core.CPCall{Req: req, Op: core.Lookup{TP: tps[0]}})
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
