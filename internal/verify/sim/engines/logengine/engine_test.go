package logengine_test

import (
	"testing"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/internal/enginetest"
	"manifester/internal/verify/sim/engines/logengine"
)

const ms = core.Duration(1_000_000)

func faults() sim.ControlPlaneFaults {
	ts := sim.TwoSpeed{FastMin: ms, FastMax: 2 * ms}
	return sim.ControlPlaneFaults{
		ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 500 * ms,
		Log: logengine.DefaultParams(),
	}
}

func TestBridgeHandlesAndDefaults(t *testing.T) {
	e := logengine.New(faults())
	if e.Name() != "logengine" || !e.Handles(core.CPCall{}) || !e.Handles(core.CPReply{}) {
		t.Fatal(e.Name(), e.Handles(core.CPCall{}))
	}
	if e.Handles(core.LogAppend{}) {
		t.Fatal("must not handle log IO")
	}
	p := e.Params()
	if p.K == 0 || !p.Enabled {
		t.Fatalf("defaults: %+v", p)
	}
	if e.CreateTopic("t", 2) != core.OK || len(e.Topics()) != 1 {
		t.Fatal(e.Topics())
	}
	e.EnterRecovery()
	e.OnCrash(nil, core.NodeRef{})
}

func TestBridgeUnavailableWithoutCP(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	e := logengine.New(f)
	c := enginetest.New(e, 1)
	c.Crash("cp-0")
	c.Crash("cp-1")
	broker := enginetest.Ref("broker-0")
	e.Execute(c, broker, core.CPCall{Req: 1, Op: core.Lookup{TP: core.TP("t", 0)}})
	c.Run()
	if len(c.Delivered) != 1 {
		t.Fatalf("got %d %#v", len(c.Delivered), c.Delivered)
	}
	d := c.Delivered[0].Body.(core.CPDone)
	if d.Err != core.ErrUnavailable {
		t.Fatal(d)
	}
}

func TestBridgeOfferAndReply(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	f.CallerTimeout = 10 * core.Second
	e := logengine.New(f)
	c := enginetest.New(e, 2)
	broker := enginetest.Ref("broker-0")
	e.Execute(c, broker, core.CPCall{Req: 7, Op: core.DescribeTopics{}})
	c.RunUntil(sim.Time(10 * ms))
	var offer core.CPOffer
	found := false
	for _, d := range c.Delivered {
		if o, ok := d.Body.(core.CPOffer); ok {
			offer, found = o, true
			break
		}
	}
	if !found || offer.Req != 7 {
		t.Fatalf("%#v", c.Delivered)
	}
	c.Delivered = nil
	e.Execute(c, enginetest.Ref("cp-0"), core.CPReply{
		To: broker, Done: core.CPDone{Req: 7, Err: core.OK, Outcome: core.Applied},
	})
	c.RunUntil(sim.Time(20 * ms))
	ok := false
	for _, d := range c.Delivered {
		if done, is := d.Body.(core.CPDone); is && done.Req == 7 && done.Err == core.OK {
			ok = true
		}
	}
	if !ok {
		t.Fatalf("%#v", c.Delivered)
	}
}

func TestBridgeFaultCoins(t *testing.T) {
	f := faults()
	f.Unavailable503 = 1
	e := logengine.New(f)
	c := enginetest.New(e, 3)
	broker := enginetest.Ref("b")
	e.Execute(c, broker, core.CPCall{Req: 1, Op: core.Lookup{}})
	c.Run()
	d := c.Delivered[0].Body.(core.CPDone)
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
	e := logengine.New(f)
	c := enginetest.New(e, 5)
	broker := enginetest.Ref("broker-0")
	e.Execute(c, broker, core.CPCall{Req: 9, Op: core.Lookup{}})
	c.RunUntil(sim.Time(10 * ms))
	c.Delivered = nil
	e.Execute(c, enginetest.Ref("cp-0"), core.CPReply{
		To: broker, Done: core.CPDone{Req: 9, Err: core.OK, Outcome: core.Applied},
	})
	c.RunUntil(sim.Time(20 * ms))
	var got *core.CPDone
	for _, d := range c.Delivered {
		if done, ok := d.Body.(core.CPDone); ok && done.Req == 9 {
			got = &done
		}
	}
	if got == nil || got.Err != core.ErrTimeout || got.Outcome != core.MaybeApplied {
		t.Fatalf("after lost reply got %#v faults=%v delivered=%s", got, c.Faults, c.Trace())
	}
}

func TestBridgeFailoverOnUnavailable(t *testing.T) {
	f := faults()
	f.Unavailable503, f.Timeout, f.LostResponse, f.Outage.Prob = 0, 0, 0, 0
	f.CallerTimeout = 10 * core.Second
	e := logengine.New(f)
	c := enginetest.New(e, 4)
	broker := enginetest.Ref("broker-1")
	e.Execute(c, broker, core.CPCall{Req: 3, Op: core.Lookup{}})
	c.RunUntil(sim.Time(10 * ms))
	c.Delivered = nil
	e.Execute(c, enginetest.Ref("cp-0"), core.CPReply{
		To: broker, Done: core.CPDone{Req: 3, Err: core.ErrUnavailable},
	})
	c.RunUntil(sim.Time(20 * ms))
	found := false
	for _, d := range c.Delivered {
		if o, ok := d.Body.(core.CPOffer); ok && o.Req == 3 {
			found = true
			if d.To.Node != "cp-1" {
				t.Fatalf("failover to %s", d.To.Node)
			}
		}
	}
	if !found {
		t.Fatal("no failover offer", c.Delivered)
	}
}
