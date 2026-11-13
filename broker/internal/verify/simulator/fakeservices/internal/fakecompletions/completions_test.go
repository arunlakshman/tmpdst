package fakecompletions

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/specbridge"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = machine.Duration(1_000_000)

// stub is a minimal service around a Core so that a kernel world can drive it.
// Its request type is any core action that is an IOCall.
type stub struct{ *Core }

func (s stub) Handle(cx *Cx, from Ref, req core.Action) { s.Start(cx, from, req.(core.IOCall)) }

func params() Params {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return Params{Apply: ts, Respond: ts, P503: 0.1, PTimeout: 0.1, PLost: 0.1, CallerTimeout: 1000 * ms}
}

// fresh recomputes the state hash with the cache invalidated.
func fresh(c *Core) uint64 {
	c.specOK = false
	return c.StateHash()
}

// objectCall returns the object store call inside an action.
func objectCall(a core.IOCall) objectstore.Call { return a.(core.ObjectCall).Call }

// newObjectCore makes a core around an object store spec, with the hooks the
// fake object store uses.
func newObjectCore(w *kernel.World, spec *objectstore.Spec) *Core {
	return New(w, Hooks{
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
}

func TestSpecHashCacheObjectStore(t *testing.T) {
	spec := objectstore.NewSpec()
	// The core needs the world for its lanes, and the world needs the core as a service.
	h := servicetest.New(t, 5, "a", "b", "c")
	c := newObjectCore(h.W, spec)
	svc := kernel.AddService[core.Action, core.EventData](h.W, stub{c})
	h.Route = func(r *servicetest.Router, out core.Action) { kernel.Call(r, svc, out) }
	r := rng.New(99)
	for i := range 400 {
		h.RunFor(ms)
		k := string(rune('a' + r.Between(0, 5)))
		request := machine.RequestID(i)
		switch r.Between(0, 3) {
		case 0, 1:
			h.Issue("a", core.ObjectCall{Call: objectstore.Put{Req: request, Key: k, Data: []byte{byte(i)}}})
		case 2:
			h.Issue("a", core.ObjectCall{Call: objectstore.Delete{Req: request, Key: k}})
		default:
			h.Issue("a", core.ObjectCall{Call: objectstore.Get{Req: request, Ref: core.SegmentRef{Key: k}}})
		}
		if r.Chance(0.3) {
			h.Run()
		}
		if got, want := c.StateHash(), fresh(c); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("final: cached hash stale")
	}
}

func TestSpecHashCacheControlPlane(t *testing.T) {
	spec := specbridge.New()
	h := servicetest.New(t, 6, "a", "b", "c")
	c := New(h.W, Hooks{
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
	svc := kernel.AddService[core.Action, core.EventData](h.W, stub{c})
	h.Route = func(r *servicetest.Router, out core.Action) { kernel.Call(r, svc, out) }
	r := rng.New(77)
	nodes := []string{"a", "b", "c"}
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
		h.RunFor(ms)
		n := nodes[r.Between(0, 2)]
		request := machine.RequestID(i)
		switch r.Between(0, 1) {
		case 0:
			// Keys repeat (replays, and conflicts when the request differs).
			h.Issue(n, core.ControlPlaneCall{Req: request, Op: contract.CommitSegment{
				SegmentKey: string(rune('a' + r.Between(0, 7))),
				Batches:    []contract.PendingBatch{{TopicPartition: tps[r.Between(0, 1)], Records: 1 + r.Between(0, 2)}},
			}})
		default:
			h.Issue(n, core.ControlPlaneCall{Req: request, Op: contract.FindSegments{TopicPartition: tps[0]}})
		}
		if r.Chance(0.3) {
			h.Run()
		}
		if got, want := c.StateHash(), fresh(c); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := c.StateHash(), fresh(c); got != want {
		t.Fatal("final: cached hash stale")
	}
}
