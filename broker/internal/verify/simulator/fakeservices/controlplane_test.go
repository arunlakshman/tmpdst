package fakeservices_test

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/internal/servicetest"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

func planeBase() fakeservices.ControlPlaneFaults {
	ts := kernel.FastWithRareSlow{FastMin: ms, FastMax: 2 * ms, SlowMin: 50 * ms, SlowMax: 60 * ms}
	return fakeservices.ControlPlaneFaults{Faults: faulty.Faults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 1000 * ms}}
}

var tp = contract.NewTopicPartition("t", 0)

// planeSetup makes a world with one caller "a" and the fake control plane.
func planeSetup(t *testing.T, f fakeservices.ControlPlaneFaults, seed uint64) (*fakeservices.ControlPlane, *servicetest.Harness) {
	t.Helper()
	h := servicetest.New(t, seed, "a")
	e := fakeservices.NewControlPlane(h.W, f)
	svc := kernel.AddService[core.ControlPlaneCall, core.EventData](h.W, e)
	h.Route = func(r *servicetest.Router, out core.Action) {
		if c, ok := out.(core.ControlPlaneCall); ok {
			kernel.Call(r, svc, c)
		}
	}
	return e, h
}

func call(h *servicetest.Harness, request machine.RequestID, operation contract.Operation) {
	h.Do("a", core.ControlPlaneCall{Req: request, Op: operation})
}

func commit(key string) contract.CommitSegment {
	return contract.CommitSegment{SegmentKey: key,
		Batches: []contract.PendingBatch{{TopicPartition: tp, Records: 5, Loc: core.SegmentRef{Key: key, Len: 10}}}}
}

func last(h *servicetest.Harness) core.ControlPlaneDone {
	return h.Delivered[len(h.Delivered)-1].Body.(core.ControlPlaneDone)
}

func TestControlPlaneNormalAndTruthful(t *testing.T) {
	e, h := planeSetup(t, planeBase(), 1)
	if e.Name() != "fakecontrolplane" {
		t.Fatal(e.Name())
	}
	call(h, 3, commit("s1"))
	h.Run()
	d := last(h)
	if d.Err != core.OK || d.Outcome != core.Applied || d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 0 {
		t.Fatal(d)
	}
	if e.HighWatermark(tp) != 5 || len(e.Committed(tp)) != 1 {
		t.Fatal("truth")
	}
	different := commit("s1")
	different.Batches[0].Records = 6
	call(h, 4, different)
	call(h, 5, contract.FindSegments{TopicPartition: tp, From: -1})
	h.Run()
	if d := h.Find(4).(core.ControlPlaneDone); d.Err != core.ErrConflict {
		t.Fatal("same key, different request:", d)
	}
	if d := h.Find(5).(core.ControlPlaneDone); d.Err != core.ErrInvalid {
		t.Fatal(d)
	}
}

func TestControlPlaneCreateTopic(t *testing.T) {
	e, h := planeSetup(t, planeBase(), 1)
	if e.CreateTopic("x", 3) != core.OK || e.Gen() != 1 || e.CreateTopic("", 1) != core.ErrInvalid {
		t.Fatal("create")
	}
	call(h, 1, contract.DescribeTopics{})
	h.Run()
	if r := last(h).Resp.(contract.DescribeTopicsResp); len(r.Topics) != 1 || r.Topics[0].Partitions != 3 {
		t.Fatal(r)
	}
}

func TestControlPlaneFaultFamilies(t *testing.T) {
	cases := []struct {
		name    string
		f       func(*fakeservices.ControlPlaneFaults)
		fault   string
		err     core.ErrCode
		outcome core.Outcome
		applied bool
		at      kernel.Time
	}{
		{"503", func(f *fakeservices.ControlPlaneFaults) { f.Unavailable503 = 1 }, "cp_503", core.ErrUnavailable, core.NotApplied, false, 0},
		{"lost", func(f *fakeservices.ControlPlaneFaults) { f.LostResponse = 1 }, "cp_lost_response", core.ErrTimeout, core.MaybeApplied, true, kernel.Time(1000 * ms)},
		{"outage", func(f *fakeservices.ControlPlaneFaults) {
			f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{ms, 2 * ms}}
		}, "cp_outage", core.ErrUnavailable, core.NotApplied, false, 0},
	}
	for _, tc := range cases {
		f := planeBase()
		tc.f(&f)
		e, h := planeSetup(t, f, 1)
		call(h, 1, commit("s1"))
		h.Run()
		d := last(h)
		if d.Err != tc.err || d.Outcome != tc.outcome || len(h.Delivered) != 1 {
			t.Fatal(tc.name, d)
		}
		if tc.at != 0 && h.Delivered[0].At != tc.at {
			t.Fatal(tc.name, "at", h.Delivered[0].At)
		}
		if !slices.Contains(h.Faults(), tc.fault) {
			t.Fatal(tc.name, h.Faults())
		}
		if got := e.Gen() == 1; got != tc.applied {
			t.Fatal(tc.name, "applied", got)
		}
	}
	// Timeout: applies or not.
	f := planeBase()
	f.Timeout = 1
	yes, no := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		e, h := planeSetup(t, f, seed)
		call(h, 1, commit("s1"))
		h.Run()
		if d := last(h); d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
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

func TestControlPlaneIdempotentRetryAfterLostResponse(t *testing.T) {
	f := planeBase()
	f.LostResponse = 1
	e, h := planeSetup(t, f, 1)
	call(h, 1, commit("s1"))
	h.Run() // applied, caller saw timeout
	h.Recover()
	call(h, 2, commit("s1")) // the identical retry
	h.Run()
	// Other commits would shift offsets; the retry must not.
	call(h, 3, commit("s2"))
	h.Run()
	call(h, 4, commit("s1"))
	h.Run()
	d := h.Delivered[len(h.Delivered)-3].Body.(core.ControlPlaneDone)
	r := last(h)
	if d.Err != core.OK || r.Err != core.OK {
		t.Fatal(d, r)
	}
	if !slices.Equal(d.Resp.(contract.CommitSegmentResp).Assigned, r.Resp.(contract.CommitSegmentResp).Assigned) {
		t.Fatal("retry assigned different offsets", d, r)
	}
	if e.HighWatermark(tp) != 10 {
		t.Fatal("retry double-committed")
	}
}

func TestControlPlaneLostCommitThenRetry(t *testing.T) {
	// Commit response lost (applied), retry in recovery returns the same offsets.
	f := planeBase()
	f.LostResponse = 1
	e, h := planeSetup(t, f, 1)
	call(h, 2, commit("s")) // lost but applied
	h.Run()
	if d := h.Delivered[0].Body.(core.ControlPlaneDone); d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
		t.Fatal(d)
	}
	want := e.Committed(tp)
	h.Recover()
	call(h, 3, commit("s"))
	h.Run()
	got := last(h).Resp.(contract.CommitSegmentResp).Assigned
	if len(want) != 1 || !slices.Equal(got, want) {
		t.Fatal(got, want)
	}
}

func TestControlPlaneLandsAfterCrash(t *testing.T) {
	e, h := planeSetup(t, planeBase(), 1)
	call(h, 1, commit("s"))
	h.Step() // the caller issues the call
	h.Crash("a")
	h.Run()
	if len(h.Delivered) != 0 || e.Gen() != 1 {
		t.Fatal("commit must land, silently")
	}
}

func TestControlPlaneRecoveryStopsFaults(t *testing.T) {
	f := planeBase()
	f.Unavailable503, f.Timeout, f.LostResponse = 1, 1, 1
	f.Outage = faulty.Outage{Prob: 1, Dur: [2]machine.Duration{ms, 2 * ms}}
	_, h := planeSetup(t, f, 1)
	call(h, 1, commit("s"))
	h.Step() // issued: the plan is a 503
	h.Recover()
	call(h, 2, commit("s"))
	h.Run()
	if d := h.Find(1).(core.ControlPlaneDone); d.Err != core.ErrUnavailable {
		t.Fatal("in-flight plan must stand", d)
	}
	if d := h.Find(2).(core.ControlPlaneDone); d.Err != core.OK || d.Resp.(contract.CommitSegmentResp).Assigned[0].Base != 0 {
		t.Fatal(d)
	}
}

func TestControlPlaneDeterminism(t *testing.T) {
	f := planeBase()
	f.Unavailable503, f.Timeout, f.LostResponse = 0.2, 0.2, 0.2
	f.Outage = faulty.Outage{Prob: 0.05, Dur: [2]machine.Duration{10 * ms, 20 * ms}}
	run := func(seed uint64) (string, uint64, uint64) {
		e, h := planeSetup(t, f, seed)
		for i := range 60 {
			after := machine.Duration(i) * ms
			switch i % 2 {
			case 0:
				h.DoAfter(after, "a", core.ControlPlaneCall{Req: machine.RequestID(i), Op: commit(string(rune('a' + i%7)))})
			default:
				h.DoAfter(after, "a", core.ControlPlaneCall{Req: machine.RequestID(i), Op: contract.FindSegments{TopicPartition: tp}})
			}
		}
		h.RunTo(30 * kernel.Time(ms))
		mid := e.StateHash()
		h.Run()
		return h.Trace(), e.StateHash(), mid
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

func TestControlPlaneCommitSizeLimit(t *testing.T) {
	f := planeBase()
	f.MaxCommitItems = 4
	e, h := planeSetup(t, f, 1)
	e.CreateTopic("t", 4)
	many := func(key string, n int) contract.CommitSegment {
		operation := commit(key)
		for i := 1; i < n; i++ {
			operation.Batches = append(operation.Batches, operation.Batches[0])
		}
		return operation
	}
	parts := func(key string, n int) contract.CommitSegment {
		operation := many(key, n)
		for i := range operation.Batches {
			operation.Batches[i].TopicPartition.Partition = int32(i)
		}
		return operation
	}
	// One item for the segment and one run per distinct partition, so the
	// number of batches does not count: 1 + 1 = 2 for 4 batches or 400.
	call(h, 2, many("ok", 4))
	call(h, 3, many("many", 400))
	h.Run()
	for _, request := range []machine.RequestID{2, 3} {
		if d := h.Find(request).(core.ControlPlaneDone); d.Err != core.OK {
			t.Fatal(request, d)
		}
	}
	// 3 partitions = 1 + 3 = 4 fits; 4 partitions = 5 is refused and changes nothing.
	call(h, 4, parts("p3", 3))
	h.Run()
	if d := last(h); d.Err != core.OK {
		t.Fatal(d)
	}
	end, gen := e.HighWatermark(tp), e.Gen()
	call(h, 5, parts("p4", 4))
	h.Run()
	if d := last(h); d.Err != core.ErrTooLarge || d.Outcome != core.NotApplied || e.HighWatermark(tp) != end || e.Gen() != gen {
		t.Fatal(d)
	}
	// A grown topic counts: partition 5 of a 4-partition topic is 1 + 1 + 1 = 3
	// (fits), but one batch each on "u" (new) and "t" partitions 0..1 is 1 + 3 + 1 = 5.
	grow := commit("g")
	grow.Batches[0].TopicPartition = contract.NewTopicPartition("t", 5)
	call(h, 6, grow)
	h.Run()
	if d := last(h); d.Err != core.OK {
		t.Fatal(d)
	}
	mixed := parts("m", 2)
	mixed.Batches = append(mixed.Batches, contract.PendingBatch{TopicPartition: contract.NewTopicPartition("u", 0), Records: 1})
	call(h, 7, mixed)
	h.Run()
	if d := last(h); d.Err != core.ErrTooLarge {
		t.Fatal("grown topic not counted:", d)
	}
}
