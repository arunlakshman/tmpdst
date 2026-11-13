package fakeservices

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
	"github.com/manifesterio/mds/shared/objectstore"
)

const ms = machine.Duration(1_000_000)

// noisy is a mix of timing and fault chances that gives refusals, timeouts and
// lost replies often.
func noisy() faulty.Faults {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return faulty.Faults{ApplyLatency: ts, RespondLatency: ts, Unavailable503: 0.1, Timeout: 0.1, LostResponse: 0.1, CallerTimeout: 1000 * ms}
}

// These two tests keep the old guard against a stale cached spec hash, now for
// the two specs behind Faulty: the hooks Mutating and Changed (and Bump) must
// clear the cache on every path that can change the spec. The generic test of
// the cache is in dst/check/faulty.

func TestSpecHashCacheObjectStore(t *testing.T) {
	h := servicetest.New(t, 5, "a", "b", "c")
	e := NewObjectStore(h.W, ObjectStoreFaults{Faults: noisy()})
	svc := kernel.AddService[core.ObjectCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) { kernel.Call(r, svc, out.(core.ObjectCall)) }
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
		if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
		t.Fatal("final: cached hash stale")
	}
}

func TestSpecHashCacheControlPlane(t *testing.T) {
	h := servicetest.New(t, 6, "a", "b", "c")
	e := NewControlPlane(h.W, ControlPlaneFaults{Faults: noisy()})
	svc := kernel.AddService[core.ControlPlaneCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) { kernel.Call(r, svc, out.(core.ControlPlaneCall)) }
	r := rng.New(77)
	nodes := []string{"a", "b", "c"}
	tps := []core.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1)}
	if e.CreateTopic(tps[0].Topic, 2) != core.OK {
		t.Fatal("create topic")
	}
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
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
		if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
			t.Fatalf("op %d: cached hash stale", i)
		}
	}
	h.Run()
	if got, want := e.SpecStateHash(), e.spec.StateHash(); got != want {
		t.Fatal("final: cached hash stale")
	}
}
