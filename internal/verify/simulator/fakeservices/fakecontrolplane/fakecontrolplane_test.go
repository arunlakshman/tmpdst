package fakecontrolplane_test

import (
	"slices"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/fakecontrolplane"
	"mds/internal/verify/simulator/fakeservices/internal/fakeservicetest"
)

const ms = core.Duration(1_000_000)

func base() simulator.ControlPlaneFaults {
	ts := simulator.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return simulator.ControlPlaneFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}
}

var tp = core.NewTopicPartition("t", 0)

func setup(f simulator.ControlPlaneFaults, seed uint64) (*fakecontrolplane.Engine, *fakeservicetest.Ctx, core.NodeInstance) {
	e := fakecontrolplane.New(f)
	return e, fakeservicetest.New(e, seed), fakeservicetest.Ref("a")
}

func call(e *fakecontrolplane.Engine, c *fakeservicetest.Ctx, from core.NodeInstance, request core.RequestID, operation core.ControlPlaneOperation) {
	e.Execute(c, from, core.ControlPlaneCall{Req: request, Op: operation})
}

func commit(key string) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key,
		Batches: []core.PendingBatch{{TopicPartition: tp, Records: 5, Loc: core.SegmentRef{Key: key, Len: 10}}}}
}

func last(c *fakeservicetest.Ctx) core.ControlPlaneDone {
	return c.Delivered[len(c.Delivered)-1].Body.(core.ControlPlaneDone)
}

func TestNormalAndTruthful(t *testing.T) {
	e, c, a := setup(base(), 1)
	if !e.Handles(core.ControlPlaneCall{}) || e.Handles(core.ObjectPut{}) {
		t.Fatal("Handles")
	}
	call(e, c, a, 3, commit("s1"))
	c.Run()
	d := last(c)
	if d.Err != core.OK || d.Outcome != core.Applied || d.Resp.(core.CommitSegmentResp).Assigned[0].Base != 0 {
		t.Fatal(d)
	}
	if e.HighWatermark(tp) != 5 || len(e.Committed(tp)) != 1 {
		t.Fatal("truth")
	}
	different := commit("s1")
	different.Batches[0].Records = 6
	call(e, c, a, 4, different)
	call(e, c, a, 5, core.FindSegments{TopicPartition: tp, From: -1})
	c.Run()
	if d := c.Find(4).(core.ControlPlaneDone); d.Err != core.ErrConflict {
		t.Fatal("same key, different request:", d)
	}
	if d := c.Find(5).(core.ControlPlaneDone); d.Err != core.ErrInvalid {
		t.Fatal(d)
	}
}

func TestCreateTopic(t *testing.T) {
	e, c, a := setup(base(), 1)
	if e.CreateTopic("x", 3) != core.OK || e.Gen() != 1 || e.CreateTopic("", 1) != core.ErrInvalid {
		t.Fatal("create")
	}
	call(e, c, a, 1, core.DescribeTopics{})
	c.Run()
	if r := last(c).Resp.(core.DescribeTopicsResp); len(r.Topics) != 1 || r.Topics[0].Partitions != 3 {
		t.Fatal(r)
	}
}

func TestFaultFamilies(t *testing.T) {
	a := fakeservicetest.Ref("a")
	cases := []struct {
		name    string
		f       func(*simulator.ControlPlaneFaults)
		fault   string
		err     core.ErrCode
		outcome core.Outcome
		applied bool
		at      simulator.Time
	}{
		{"503", func(f *simulator.ControlPlaneFaults) { f.Unavailable503 = 1 }, "cp_503", core.ErrUnavailable, core.NotApplied, false, 0},
		{"lost", func(f *simulator.ControlPlaneFaults) { f.LostResponse = 1 }, "cp_lost_response", core.ErrTimeout, core.MaybeApplied, true, simulator.Time(1000 * ms)},
		{"outage", func(f *simulator.ControlPlaneFaults) {
			f.Outage = simulator.ServiceOutage{Prob: 1, Dur: [2]core.Duration{ms, 2 * ms}}
		}, "cp_outage", core.ErrUnavailable, core.NotApplied, false, 0},
	}
	for _, tc := range cases {
		f := base()
		tc.f(&f)
		e, c, _ := setup(f, 1)
		call(e, c, a, 1, commit("s1"))
		c.Run()
		d := last(c)
		if d.Err != tc.err || d.Outcome != tc.outcome || len(c.Delivered) != 1 {
			t.Fatal(tc.name, d)
		}
		if tc.at != 0 && c.Delivered[0].At != tc.at {
			t.Fatal(tc.name, "at", c.Delivered[0].At)
		}
		if !slices.Contains(c.Faults, tc.fault) {
			t.Fatal(tc.name, c.Faults)
		}
		if got := e.Gen() == 1; got != tc.applied {
			t.Fatal(tc.name, "applied", got)
		}
	}
	// Timeout: applies or not.
	f := base()
	f.Timeout = 1
	yes, no := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, c, _ := setup(f, seed)
		call(e, c, a, 1, commit("s1"))
		c.Run()
		if d := last(c); d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
			t.Fatal(d)
		}
		if e.Gen() == 1 {
			yes++
		} else {
			no++
		}
	}
	if yes == 0 || no == 0 {
		t.Fatal(yes, no)
	}
}

func TestIdempotentRetryAfterLostResponse(t *testing.T) {
	f := base()
	f.LostResponse = 1
	e, c, a := setup(f, 1)
	call(e, c, a, 1, commit("s1"))
	c.Run() // applied, caller saw timeout
	e.EnterRecovery()
	call(e, c, a, 2, commit("s1")) // the identical retry
	c.Run()
	// Other commits would shift offsets; the retry must not.
	call(e, c, a, 3, commit("s2"))
	c.Run()
	call(e, c, a, 4, commit("s1"))
	c.Run()
	d := c.Delivered[len(c.Delivered)-3].Body.(core.ControlPlaneDone)
	r := last(c)
	if d.Err != core.OK || r.Err != core.OK {
		t.Fatal(d, r)
	}
	if !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, r.Resp.(core.CommitSegmentResp).Assigned) {
		t.Fatal("retry assigned different offsets", d, r)
	}
	if e.HighWatermark(tp) != 10 {
		t.Fatal("retry double-committed")
	}
}

func TestLostCommitThenRetryInSim(t *testing.T) {
	// Commit response lost (applied), retry in recovery returns the same offsets.
	f := base()
	f.LostResponse = 1
	e2, c2, a2 := setup(f, 1)
	call(e2, c2, a2, 2, commit("s")) // lost but applied
	c2.Run()
	if d := c2.Delivered[0].Body.(core.ControlPlaneDone); d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
		t.Fatal(d)
	}
	want := e2.Committed(tp)
	e2.EnterRecovery()
	call(e2, c2, a2, 3, commit("s"))
	c2.Run()
	got := last(c2).Resp.(core.CommitSegmentResp).Assigned
	if len(want) != 1 || !slices.Equal(got, want) {
		t.Fatal(got, want)
	}
}

func TestLandsAfterCrash(t *testing.T) {
	e, c, a := setup(base(), 1)
	call(e, c, a, 1, commit("s"))
	c.Crash("a")
	c.Run()
	if len(c.Delivered) != 0 || e.Gen() != 1 {
		t.Fatal("commit must land, silently")
	}
}

func TestRecoveryStopsFaults(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 1, 1, 1
	f.Outage = simulator.ServiceOutage{Prob: 1, Dur: [2]core.Duration{ms, 2 * ms}}
	e, c, a := setup(f, 1)
	call(e, c, a, 1, commit("s"))
	e.EnterRecovery()
	call(e, c, a, 2, commit("s"))
	c.Run()
	if d := c.Find(1).(core.ControlPlaneDone); d.Err != core.ErrUnavailable {
		t.Fatal("in-flight plan must stand", d)
	}
	if d := c.Find(2).(core.ControlPlaneDone); d.Err != core.OK || d.Resp.(core.CommitSegmentResp).Assigned[0].Base != 0 {
		t.Fatal(d)
	}
}

func TestDeterminism(t *testing.T) {
	f := base()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = simulator.ServiceOutage{Prob: 0.05, Dur: [2]core.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, c, a := setup(f, seed)
		for i := range 60 {
			c.T += simulator.Time(ms)
			switch i % 2 {
			case 0:
				call(e, c, a, core.RequestID(i), commit(string(rune('a'+i%7))))
			default:
				call(e, c, a, core.RequestID(i), core.FindSegments{TopicPartition: tp})
			}
		}
		mid := e.StateHash()
		c.Run()
		return c.Trace(), e.StateHash(), mid
	}
	t1, h1, m1 := run(7)
	t2, h2, m2 := run(7)
	if t1 != t2 || h1 != h2 || m1 != m2 {
		t.Fatal("same seed diverged")
	}
	if t3, _, _ := run(8); t3 == t1 {
		t.Fatal("different seeds gave one trace")
	}
	if m1 == h1 {
		t.Fatal("pending plans not hashed")
	}
}

func TestCommitSizeLimit(t *testing.T) {
	f := base()
	f.MaxCommitItems = 4
	e, c, a := setup(f, 1)
	e.CreateTopic("t", 4)
	many := func(key string, n int) core.CommitSegment {
		operation := commit(key)
		for i := 1; i < n; i++ {
			operation.Batches = append(operation.Batches, operation.Batches[0])
		}
		return operation
	}
	parts := func(key string, n int) core.CommitSegment {
		operation := many(key, n)
		for i := range operation.Batches {
			operation.Batches[i].TopicPartition.Partition = int32(i)
		}
		return operation
	}
	// One item for the segment and one run per distinct partition, so the
	// number of batches does not count: 1 + 1 = 2 for 4 batches or 400.
	call(e, c, a, 2, many("ok", 4))
	call(e, c, a, 3, many("many", 400))
	c.Run()
	for _, request := range []core.RequestID{2, 3} {
		if d := c.Find(request).(core.ControlPlaneDone); d.Err != core.OK {
			t.Fatal(request, d)
		}
	}
	// 3 partitions = 1 + 3 = 4 fits; 4 partitions = 5 is refused and changes nothing.
	call(e, c, a, 4, parts("p3", 3))
	c.Run()
	if d := last(c); d.Err != core.OK {
		t.Fatal(d)
	}
	end, gen := e.HighWatermark(tp), e.Gen()
	call(e, c, a, 5, parts("p4", 4))
	c.Run()
	if d := last(c); d.Err != core.ErrTooLarge || d.Outcome != core.NotApplied || e.HighWatermark(tp) != end || e.Gen() != gen {
		t.Fatal(d)
	}
	// A grown topic counts: partition 5 of a 4-partition topic is 1 + 1 + 1 = 3
	// (fits), but one batch each on "u" (new) and "t" partitions 0..1 is 1 + 3 + 1 = 5.
	grow := commit("g")
	grow.Batches[0].TopicPartition = core.NewTopicPartition("t", 5)
	call(e, c, a, 6, grow)
	c.Run()
	if d := last(c); d.Err != core.OK {
		t.Fatal(d)
	}
	mixed := parts("m", 2)
	mixed.Batches = append(mixed.Batches, core.PendingBatch{TopicPartition: core.NewTopicPartition("u", 0), Records: 1})
	call(e, c, a, 7, mixed)
	c.Run()
	if d := last(c); d.Err != core.ErrTooLarge {
		t.Fatal("grown topic not counted:", d)
	}
}
