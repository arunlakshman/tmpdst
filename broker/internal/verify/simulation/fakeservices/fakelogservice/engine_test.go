package fakelogservice_test

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/fakelogservice"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

const ms = machine.Duration(1_000_000)

func faults() fakecontrolplane.Faults {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms}
	return fakecontrolplane.Faults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 500 * ms}
}

// setup makes a world with the callers broker-0, broker-1, cp-0 and cp-1, and the
// bridge. The bridge uses cp-0 and cp-1 as its replicas.
func setup(t *testing.T, f fakecontrolplane.Faults, seed uint64) (*fakelogservice.Engine, *servicetest.Harness) {
	t.Helper()
	h := servicetest.New(t, seed, "broker-0", "broker-1", "cp-0", "cp-1")
	e := fakelogservice.New(h.W, f, fakelogservice.DefaultParams())
	e.UseReplicas(h.Addr("cp-0"), h.Addr("cp-1"))
	svc := kernel.AddService[fakelogservice.Request, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		switch x := out.(type) {
		case core.ControlPlaneCall:
			kernel.Call(r, svc, fakelogservice.Request(fakelogservice.Call{ControlPlaneCall: x}))
		case core.ControlPlaneReply:
			kernel.Call(r, svc, fakelogservice.Request(fakelogservice.Reply{ControlPlaneReply: x}))
		}
	}
	return e, h
}

func TestBridgeNameAndDefaults(t *testing.T) {
	e, h := setup(t, faults(), 1)
	if e.Name() != "fakelogservice" {
		t.Fatal(e.Name())
	}
	if p := e.Params(); !p.Enabled || p.GroupMax != fakelogservice.DefaultParams().GroupMax {
		t.Fatalf("defaults: %+v", p)
	}
	if e.CreateTopic("t", 2) != core.OK || len(e.Topics()) != 1 {
		t.Fatal(e.Topics())
	}
	if e.CreateTopic("", 2) != core.ErrInvalid || e.CreateTopic("t", 0) != core.ErrInvalid {
		t.Fatal("a bad topic must be refused")
	}
	h.Recover()
	h.Crash("cp-0") // OnCrash is a no-op
	h.Run()
}

func TestBridgeUnavailableWithoutCP(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	_, h := setup(t, f, 1)
	h.Crash("cp-0")
	h.Crash("cp-1")
	h.Do("broker-0", core.ControlPlaneCall{Req: 1, Op: contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 0)}})
	h.Run()
	if len(h.Delivered) != 1 {
		t.Fatalf("got %d %#v", len(h.Delivered), h.Delivered)
	}
	d := h.Delivered[0].Body.(core.ControlPlaneDone)
	if d.Err != core.ErrUnavailable {
		t.Fatal(d)
	}
}

func TestBridgeOfferAndReply(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	f.CallerTimeout = 10 * machine.Second
	_, h := setup(t, f, 2)
	broker := h.Ref("broker-0")
	h.Do("broker-0", core.ControlPlaneCall{Req: 7, Op: contract.DescribeTopics{}})
	h.RunTo(10 * kernel.Time(ms))
	var offer core.ControlPlaneDeliver
	found := false
	for _, d := range h.Delivered {
		if o, ok := d.Body.(core.ControlPlaneDeliver); ok {
			offer, found = o, true
			break
		}
	}
	if !found || offer.Req != 7 || offer.From != broker {
		t.Fatalf("%#v", h.Delivered)
	}
	h.Delivered = nil
	h.Do("cp-0", core.ControlPlaneReply{
		To: broker, Done: core.ControlPlaneDone{Req: 7, Err: core.OK, Outcome: core.Applied},
	})
	h.RunTo(20 * kernel.Time(ms))
	ok := false
	for _, d := range h.Delivered {
		if done, is := d.Body.(core.ControlPlaneDone); is && done.Req == 7 && done.Err == core.OK && d.To == "broker-0" {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("%#v", h.Delivered)
	}
}

func TestBridgeFaultCoins(t *testing.T) {
	f := faults()
	f.Unavailable503 = 1
	_, h := setup(t, f, 3)
	h.Do("broker-0", core.ControlPlaneCall{Req: 1, Op: contract.FindSegments{}})
	h.Run()
	d := h.Delivered[0].Body.(core.ControlPlaneDone)
	if d.Err != core.ErrUnavailable || d.Outcome != core.NotApplied {
		t.Fatal(d)
	}
}

func TestBridgeLostReplyStillTimesOut(t *testing.T) {
	// LostResponse mutes the service reply; the caller-timeout deadline must
	// still answer the broker (a cancelled deadline left commits hung).
	// execCall floors CallerTimeout at 500ms.
	f := faults()
	f.Unavailable503, f.Timeout, f.Outage.Prob = 0, 0, 0
	f.LostResponse = 1
	f.CallerTimeout = 500 * ms
	_, h := setup(t, f, 5)
	broker := h.Ref("broker-0")
	h.Do("broker-0", core.ControlPlaneCall{Req: 9, Op: contract.FindSegments{}})
	h.RunTo(10 * kernel.Time(ms))
	h.Delivered = nil
	h.Do("cp-0", core.ControlPlaneReply{
		To: broker, Done: core.ControlPlaneDone{Req: 9, Err: core.OK, Outcome: core.Applied},
	})
	h.RunTo(20 * kernel.Time(ms))
	var got *core.ControlPlaneDone
	for _, d := range h.Delivered {
		if done, ok := d.Body.(core.ControlPlaneDone); ok && done.Req == 9 {
			got = &done
		}
	}
	if got == nil || got.Err != core.ErrTimeout || got.Outcome != core.MaybeApplied {
		t.Fatalf("after lost reply got %#v faults=%v delivered=%s", got, h.Faults(), h.Trace())
	}
}

func TestBridgeFailoverOnUnavailable(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	f.CallerTimeout = 10 * machine.Second
	_, h := setup(t, f, 4)
	broker := h.Ref("broker-1")
	h.Do("broker-1", core.ControlPlaneCall{Req: 3, Op: contract.FindSegments{}})
	h.RunTo(10 * kernel.Time(ms))
	h.Delivered = nil
	h.Do("cp-0", core.ControlPlaneReply{
		To: broker, Done: core.ControlPlaneDone{Req: 3, Err: core.ErrUnavailable},
	})
	h.RunTo(20 * kernel.Time(ms))
	found := false
	for _, d := range h.Delivered {
		if o, ok := d.Body.(core.ControlPlaneDeliver); ok && o.Req == 3 {
			found = true
			if d.To != "cp-1" {
				t.Fatalf("failover to %s", d.To)
			}
		}
	}
	if !found {
		t.Fatal("no failover offer", h.Delivered)
	}
}
