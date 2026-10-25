package logengine

import (
	"slices"
	"testing"

	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/internal/enginetest"
)

const ms = core.Duration(1_000_000)

var tp = core.TopicPartition{Topic: "t", Partition: 0}

func faults(l sim.LogParams) sim.ControlPlaneFaults {
	ts := sim.TwoSpeed{FastMin: ms, FastMax: 3 * ms, SlowMin: 40 * ms, SlowMax: 60 * ms}
	l.Enabled = true
	return sim.ControlPlaneFaults{ApplyLatency: ts, RespondLatency: ts, CallerTimeout: 3000 * ms, Log: l}
}

func rig(l sim.LogParams, seed uint64) (*Engine, *enginetest.Ctx, core.NodeRef) {
	e := New(faults(l))
	e.CreateTopic("t", 4)
	return e, enginetest.New(e, seed), enginetest.Ref("a")
}

func commit(key string, records int64) core.CommitSegment {
	return core.CommitSegment{SegmentKey: key, Batches: []core.PendingBatch{{TP: tp, Records: records, Loc: core.SegmentRef{Key: key, Len: 10}}}}
}

func send(e *Engine, c *enginetest.Ctx, from core.NodeRef, req core.ReqID, op core.CPOp) {
	e.Execute(c, from, core.CPCall{Req: req, Op: op})
}

func done(t *testing.T, c *enginetest.Ctx, req core.ReqID) core.CPDone {
	t.Helper()
	d, ok := c.Find(req).(core.CPDone)
	if !ok {
		t.Fatalf("req %d: no completion (violations %v)", req, c.Violations)
	}
	return d
}

func at(n int64) sim.Time { return sim.Time(core.Duration(n) * ms) }

func reached(c *enginetest.Ctx, p string) bool { return slices.Contains(c.Reached, p) }

func TestCommitReplayConflictAndReads(t *testing.T) {
	e, c, a := rig(sim.LogParams{}, 1)
	send(e, c, a, 1, commit("s1", 5))
	c.RunUntil(at(500))
	d := done(t, c, 1)
	if d.Err != core.OK || d.Outcome != core.Applied || d.Resp.(core.CommitSegmentResp).Assigned[0].Base != 0 {
		t.Fatal(d)
	}
	send(e, c, a, 2, commit("s2", 3))
	send(e, c, a, 3, commit("s1", 5)) // replay
	send(e, c, a, 4, commit("s1", 6)) // same key, different request
	send(e, c, a, 5, core.Lookup{TP: tp, From: 0})
	send(e, c, a, 6, core.DescribeTopics{})
	send(e, c, a, 7, core.Lookup{TP: tp, From: -1})
	send(e, c, a, 8, core.CommitSegment{})
	c.RunUntil(at(1500))
	if r := done(t, c, 2).Resp.(core.CommitSegmentResp).Assigned[0]; r.Base != 5 || r.Count != 3 {
		t.Fatal(r)
	}
	if r := done(t, c, 3).Resp.(core.CommitSegmentResp).Assigned[0]; r.Base != 0 || r.Count != 5 {
		t.Fatal("replay", r)
	}
	if d := done(t, c, 4); d.Err != core.ErrConflict || d.Outcome != core.NotApplied {
		t.Fatal(d)
	}
	if done(t, c, 7).Err != core.ErrInvalid || done(t, c, 8).Err != core.ErrInvalid {
		t.Fatal("invalid")
	}
	// the read was issued with the commits, so it sees at least the landed slots: s1 (s2 may be in flight)
	if lr := done(t, c, 5).Resp.(core.LookupResp); len(lr.Ranges) < 1 || lr.End < 5 {
		t.Fatal(lr)
	}
	if tr := done(t, c, 6).Resp.(core.DescribeTopicsResp); len(tr.Topics) != 1 || tr.Topics[0].Partitions != 4 {
		t.Fatal(tr)
	}
	if e.HighWatermark(tp) != 8 || len(e.Committed(tp)) != 2 {
		t.Fatal("truth", e.HighWatermark(tp), e.Committed(tp))
	}
	if e.Applied(0) != e.Applied(1) && e.Applied(1) < 0 {
		t.Fatal("standby never applied")
	}
	if len(c.Violations) > 0 {
		t.Fatal(c.Violations)
	}
	if !reached(c, "logcp-sim: replayed key answered from the window") {
		t.Fatal("replay not reached")
	}
}

// A slot taken before the writer's Put lands: the entry goes to a later slot.
func TestLostSlotRetriedLater(t *testing.T) {
	e, c, a := rig(sim.LogParams{}, 2)
	e.slots = append(e.slots, slotRec{kind: slotEntry, writer: 1, id: 999})
	e.landed(c, 0)
	send(e, c, a, 1, commit("s1", 5))
	c.RunUntil(at(1000))
	if d := done(t, c, 1); d.Err != core.OK {
		t.Fatal(d)
	}
	if !reached(c, "logcp-sim: lost slot retried at a later slot") || e.slots[1].kind != slotEntry {
		t.Fatal("not retried at a later slot", c.Reached)
	}
	if len(c.Violations) > 0 {
		t.Fatal(c.Violations)
	}
}

// Slot 2 landed, 0 and 1 did not: any replica fills them with no-ops after HoleAfter.
func TestHoleFilled(t *testing.T) {
	e, c, a := rig(sim.LogParams{Heartbeat: 1000 * 1000 * ms}, 3)
	e.extend(2)
	e.slots[2] = slotRec{kind: slotEntry, writer: 0, id: 77}
	e.landed(c, 2)
	send(e, c, a, 1, core.DescribeTopics{}) // starts the tick chain
	c.RunUntil(at(30))
	if e.Applied(0) != -1 {
		t.Fatal("applied across a hole before HoleAfter")
	}
	c.RunUntil(at(600))
	if e.Applied(0) != 2 || e.slots[0].kind != slotEntry || e.slots[1].kind != slotEntry {
		t.Fatal("hole not filled", e.Applied(0))
	}
	if !reached(c, "logcp-sim: hole filled by a no-op") || len(c.Violations) > 0 {
		t.Fatal(c.Reached, c.Violations)
	}
}

// The active node stalls; the standby takes over, serves the retry of a key the
// first node applied (replay after takeover), and the first node steps back in.
func TestTakeoverReplayAndFailback(t *testing.T) {
	e, c, a := rig(sim.LogParams{TakeoverAfter: 300 * ms}, 4)
	send(e, c, a, 1, commit("s1", 5))
	c.RunUntil(at(400))
	first := done(t, c, 1).Resp.(core.CommitSegmentResp).Assigned
	e.reps[0].stallUntil = at(2000)
	send(e, c, a, 2, commit("s1", 5))
	c.RunUntil(at(1500))
	if d := done(t, c, 2); d.Err != core.ErrUnavailable && d.Err != core.OK {
		t.Fatal(d)
	}
	send(e, c, a, 3, commit("s1", 5))
	c.RunUntil(at(1800))
	d := done(t, c, 3)
	if d.Err != core.OK || !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, first) {
		t.Fatal("replay after takeover", d)
	}
	c.RunUntil(at(4000))
	for _, p := range []string{"logcp-sim: standby took over", "logcp-sim: idempotent replay after takeover", "logcp-sim: failback, the standby stepped down"} {
		if !reached(c, p) {
			t.Fatal("not reached:", p)
		}
	}
	if len(c.Violations) > 0 {
		t.Fatal(c.Violations)
	}
}

// Checkpoints trim the log with tombstones; a replay of a trimmed key is answered
// from the window, and a restarted replica restores from the checkpoint.
func TestTrimWindowAndRestore(t *testing.T) {
	e, c, a := rig(sim.LogParams{CkptEvery: 4, KeepSlots: 2, TrimStride: 4}, 5)
	var first []core.BatchRange
	for i := 0; i < 20; i++ {
		send(e, c, a, core.ReqID(i+1), commit("s"+string(rune('a'+i)), 2))
		c.RunUntil(at(int64(100 * (i + 1))))
	}
	first = done(t, c, 1).Resp.(core.CommitSegmentResp).Assigned
	if e.trimmed == 0 || e.slots[0].kind != slotTomb {
		t.Fatal("log not trimmed", e.trimmed)
	}
	send(e, c, a, 100, commit("sa", 2))
	e.crash(c, e.reps[1])
	c.RunUntil(at(4000))
	if d := done(t, c, 100); !slices.Equal(d.Resp.(core.CommitSegmentResp).Assigned, first) {
		t.Fatal("replay of a trimmed key", d)
	}
	if e.Applied(1) != e.Applied(0) || e.Digest(1) != e.RefDigest(e.Applied(1)) {
		t.Fatal("restarted replica diverged")
	}
	for _, p := range []string{"logcp-sim: replay of a key whose slot was trimmed", "logcp-sim: replica restored from a checkpoint", "logcp-sim: log trimmed below a checkpoint"} {
		if !reached(c, p) {
			t.Fatal("not reached:", p)
		}
	}
	if len(c.Violations) > 0 {
		t.Fatal(c.Violations)
	}
}

// Lost and refused Puts: every command is still committed exactly once.
func TestLossyPuts(t *testing.T) {
	e, c, a := rig(sim.LogParams{PutLost: 0.4, PutFail: 0.3, K: 4, GroupMax: 2}, 6)
	for i := 0; i < 30; i++ {
		send(e, c, a, core.ReqID(i+1), commit("k"+string(rune('a'+i)), 1))
	}
	c.RunUntil(at(20000))
	for i := 0; i < 30; i++ {
		if d := done(t, c, core.ReqID(i+1)); d.Err != core.OK {
			t.Fatal(i, d)
		}
	}
	if e.HighWatermark(tp) != 30 || len(c.Violations) > 0 {
		t.Fatal(e.HighWatermark(tp), c.Violations)
	}
}

func TestDeterministic(t *testing.T) {
	run := func() string {
		e, c, a := rig(sim.LogParams{PutLost: 0.2, PutFail: 0.2, StallProb: 0.01, CrashProb: 0.01}, 9)
		for i := 0; i < 20; i++ {
			send(e, c, a, core.ReqID(i+1), commit("k"+string(rune('a'+i)), 1))
			c.RunUntil(at(int64(50 * (i + 1))))
		}
		c.RunUntil(at(15000))
		return c.Trace()
	}
	a, b := run(), run()
	if a != b {
		t.Fatal("same seed, different trace")
	}
}

func TestBugsAreVisible(t *testing.T) {
	cases := []struct {
		name string
		bug  uint8
		l    sim.LogParams
	}{
		{"hole skip", BugHoleSkip, sim.LogParams{Heartbeat: 1000 * 1000 * ms}},
		{"writer offsets", BugWriterOffsets, sim.LogParams{K: 4, GroupMax: 1}},
		{"per replica window", BugPerReplicaWindow, sim.LogParams{WindowSlots: 4}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { bugRun(t, tc.bug, tc.l) })
	}
}

// bugRun drives a mutant until a property or the truth shows it.
func bugRun(t *testing.T, bug uint8, l sim.LogParams) {
	l.Bug = bug
	for seed := uint64(1); seed <= 40; seed++ {
		e, c, a := rig(l, seed)
		if bug == BugHoleSkip { // slot 2 landed, slots 0 and 1 are a hole
			e.extend(2)
			e.slots[2] = slotRec{kind: slotEntry, writer: 1, id: 5}
			e.landed(c, 2)
			send(e, c, a, 1, core.DescribeTopics{})
		}
		for i := 0; bug != BugHoleSkip && i < 12; i++ {
			k := "k" + string(rune('a'+i))
			send(e, c, a, core.ReqID(2*i+1), commit(k, 2))
			send(e, c, a, core.ReqID(2*i+2), commit(k, 2)) // a retry of the same key
		}
		c.RunUntil(at(6000))
		if len(c.Violations) > 0 || badLog(e) {
			return
		}
	}
	t.Fatal("mutant not caught")
}

// badLog reports an overlap or a gap in the reference log's offsets.
func badLog(e *Engine) bool {
	next := core.Offset(0)
	for _, r := range e.Committed(tp) {
		if r.Base != next {
			return true
		}
		next = r.Base + core.Offset(r.Count)
	}
	return false
}

func TestBoundaryFaults(t *testing.T) {
	cases := []struct {
		name string
		f    func(*sim.ControlPlaneFaults)
		err  core.ErrCode
		out  core.Outcome
		hwm  core.Offset // 0 = the commit must not have been applied
	}{
		{"503", func(f *sim.ControlPlaneFaults) { f.Unavailable503 = 1 }, core.ErrUnavailable, core.NotApplied, 0},
		{"outage", func(f *sim.ControlPlaneFaults) {
			f.Outage = sim.Outage{Prob: 1, Dur: [2]core.Duration{500 * ms, 900 * ms}}
		}, core.ErrUnavailable, core.NotApplied, 0},
		{"lost response", func(f *sim.ControlPlaneFaults) { f.LostResponse = 1 }, core.ErrTimeout, core.MaybeApplied, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := faults(sim.LogParams{})
			tc.f(&f)
			e := New(f)
			e.CreateTopic("t", 1)
			c, a := enginetest.New(e, 1), enginetest.Ref("a")
			send(e, c, a, 1, commit("s", 5))
			c.RunUntil(at(5000))
			if d := done(t, c, 1); d.Err != tc.err || d.Outcome != tc.out {
				t.Fatal(d)
			}
			if e.HighWatermark(tp) != tc.hwm {
				t.Fatal("applied:", e.HighWatermark(tp))
			}
		})
	}
	t.Run("timeout coin", func(t *testing.T) {
		applied := 0
		for seed := uint64(1); seed <= 20; seed++ {
			f := faults(sim.LogParams{})
			f.Timeout = 1
			e := New(f)
			e.CreateTopic("t", 1)
			c, a := enginetest.New(e, seed), enginetest.Ref("a")
			send(e, c, a, 1, commit("s", 5))
			c.RunUntil(at(5000))
			if d := done(t, c, 1); d.Err != core.ErrTimeout || d.Outcome != core.MaybeApplied {
				t.Fatal(d)
			}
			if e.HighWatermark(tp) == 5 {
				applied++
			}
		}
		if applied == 0 || applied == 20 {
			t.Fatalf("a timed-out commit applied in %d of 20 runs, want some", applied)
		}
	})
}

func TestAccessorsRecoveryAndStepDown(t *testing.T) {
	f := faults(sim.LogParams{Heartbeat: 1000 * 1000 * ms, TakeoverAfter: 200 * ms})
	f.Unavailable503 = 1
	e := New(f)
	e.CreateTopic("t", 1)
	if e.Name() != "logengine" || !e.Handles(core.CPCall{}) || e.Handles(core.ObjPut{}) || e.CreateTopic("", 1) != core.ErrInvalid {
		t.Fatal("identity")
	}
	e.EnterRecovery() // no more faults: the 503 coin is off
	c, a := enginetest.New(e, 1), enginetest.Ref("a")
	h0, g0 := e.StateHash(), e.Gen()
	send(e, c, a, 1, commit("s", 5))
	c.RunUntil(at(300))
	if d := done(t, c, 1); d.Err != core.OK || e.Gen() == g0 || e.StateHash() == h0 || e.Tail() < 0 {
		t.Fatal(d)
	}
	e.OnCrash(c, a)
	// take the active node away, let the standby take over and queue work on it, then wake the first
	e.reps[0].stallUntil = at(5000)
	c.RunUntil(at(900))
	if !e.reps[1].active {
		t.Fatal("standby did not take over")
	}
	send(e, c, a, 2, commit("s2", 1))
	e.reps[1].q = append(e.reps[1].q, qcmd{c: cmd{key: "never"}, req: 2})
	e.reps[1].sawLower = true
	c.RunUntil(at(920))
	if !reached(c, "logcp-sim: failback, the standby stepped down") || len(e.reps[1].q) != 0 {
		t.Fatal("standby did not step down")
	}
}
