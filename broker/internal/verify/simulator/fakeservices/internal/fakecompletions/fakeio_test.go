package fakecompletions

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/fakeservicetest"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = machine.Duration(1_000_000)

// stub is a minimal engine around a CompletionCore so fakeservicetest can drive it.
type stub struct{ *CompletionCore }

func (s stub) Handles(core.Action) bool { return true }
func (s stub) Execute(ctx simulator.FakeServiceContext, f machine.NodeInstance, a core.Action) {
	s.CompletionCore.Execute(ctx, f, a.(core.IOCall))
}

func params() Params {
	ts := simulator.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return Params{Apply: ts, Respond: ts, P503: 0.1, PTimeout: 0.1, PLost: 0.1, CallerTimeout: 1000 * ms}
}

// fresh recomputes the state hash with the cache invalidated.
func fresh(c *CompletionCore) uint64 {
	c.specOK = false
	return c.StateHash()
}

// objectCall returns the object store call inside an action.
func objectCall(a core.IOCall) objectstore.Call { return a.(core.ObjectCall).Call }

func TestSpecHashCacheObjectStore(t *testing.T) {
	spec := objectstore.NewSpec()
	c := New(CompletionHooks{
		Name: "o", Prefix: "obj", Params: params(),
		Spec: func(a core.IOCall) core.IOResult { return core.ObjectDone{Result: spec.Exec(objectCall(a))} },
		Mutating: func(a core.IOCall) bool {
			switch objectCall(a).(type) {
			case objectstore.Put, objectstore.Delete:
				return true
			case objectstore.Get, objectstore.List:
				return false
			}
			return false
		},
		Fail: func(a core.IOCall, e core.ErrCode, o core.Outcome) core.IOResult {
			switch a := objectCall(a).(type) {
			case objectstore.Put:
				return core.ObjectDone{Result: objectstore.PutDone{Req: a.Req, Err: e, Outcome: o}}
			case objectstore.Delete:
				return core.ObjectDone{Result: objectstore.DeleteDone{Req: a.Req, Err: e, Outcome: o}}
			case objectstore.Get:
				return core.ObjectDone{Result: objectstore.GetDone{Req: a.Req, Err: e}}
			case objectstore.List:
				return core.ObjectDone{Result: objectstore.ListDone{Req: a.Req, Err: e}}
			}
			panic("not an object store call")
		},
		Changed: func(c core.IOResult) bool {
			switch c := c.(core.ObjectDone).Result.(type) {
			case objectstore.PutDone:
				return c.Outcome == core.Applied
			case objectstore.DeleteDone:
				return c.Outcome == core.Applied
			case objectstore.GetDone, objectstore.ListDone:
				return false
			}
			return false
		},
		SpecHash: spec.StateHash,
	})
	ctx := fakeservicetest.New(stub{c}, 5)
	from := fakeservicetest.Ref("a")
	r := simulator.NewRand(99)
	for i := range 400 {
		ctx.T += simulator.Time(ms)
		k := string(rune('a' + r.Between(0, 6)))
		request := machine.RequestID(i)
		switch r.Between(0, 4) {
		case 0, 1:
			c.Execute(ctx, from, core.ObjectCall{Call: objectstore.Put{Req: request, Key: k, Data: []byte{byte(i)}}})
		case 2:
			c.Execute(ctx, from, core.ObjectCall{Call: objectstore.Delete{Req: request, Key: k}})
		default:
			c.Execute(ctx, from, core.ObjectCall{Call: objectstore.Get{Req: request, Ref: core.SegmentRef{Key: k}}})
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
	spec := speccheck.NewControlPlane()
	c := New(CompletionHooks{
		Name: "cp", Prefix: "cp", Params: params(),
		Spec: func(a core.IOCall) core.IOResult { return spec.Exec(a) },
		Mutating: func(a core.IOCall) bool {
			_, ok := a.(core.ControlPlaneCall).Op.(contract.CommitSegment)
			return ok
		},
		Fail: func(a core.IOCall, err core.ErrCode, o core.Outcome) core.IOResult {
			return core.ControlPlaneDone{Req: a.RequestID(), Err: err, Outcome: o}
		},
		Changed:  func(c core.IOResult) bool { return c.(core.ControlPlaneDone).Outcome == core.Applied },
		SpecHash: spec.StateHash,
	})
	ctx := fakeservicetest.New(stub{c}, 6)
	r := simulator.NewRand(77)
	nodes := []machine.NodeInstance{fakeservicetest.Ref("a"), fakeservicetest.Ref("b"), fakeservicetest.Ref("c")}
	tps := []core.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1)}
	for _, topicPartition := range tps[:1] {
		if spec.CreateTopic(topicPartition.Topic, 2) == core.OK {
			c.Bump()
		}
	}
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("after CreateTopic: stale")
	}
	for i := range 400 {
		ctx.T += simulator.Time(ms)
		n := nodes[r.Between(0, 3)]
		request := machine.RequestID(i)
		switch r.Between(0, 2) {
		case 0:
			// Keys repeat (replays, and conflicts when the request differs).
			c.Execute(ctx, n, core.ControlPlaneCall{Req: request, Op: contract.CommitSegment{
				SegmentKey: string(rune('a' + r.Between(0, 8))),
				Batches:    []contract.PendingBatch{{TopicPartition: tps[r.Between(0, 2)], Records: 1 + r.Between(0, 3)}},
			}})
		default:
			c.Execute(ctx, n, core.ControlPlaneCall{Req: request, Op: contract.FindSegments{TopicPartition: tps[0]}})
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
