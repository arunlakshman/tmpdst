package kafka

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"mds/internal/controlplane/statemachine"
	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/fakelogstore"
	"mds/internal/verify/simulator/fakeservices/fakeobjectstore"
)

// A healthy log store that is merely slow: every log call takes about 19 ms of
// simulated time (the measured DynamoDB tail check) and nothing fails. Catch-up
// asks for the tail every 5 ms, so a tail check is in flight most of the time.
// The checkpoint timer must still write a checkpoint every few periods.
const (
	slowLogCatchUpEvery   = 5 * core.Millisecond
	slowLogCheckpointGap  = core.Second
	slowLogHeartbeat      = 50 * core.Millisecond
	slowLogRunLength      = 20 * core.Second
	slowLogSilentFiresMax = 2  // a write within 3 periods: at most 2 silent fires in a row
	slowLogPendingShare   = 10 // at most 1 in 10 timer fires may skip only because the node was busy
)

// slowLatency is the log store's delay per call: about 19 ms, jittered 18 to 20.
var slowLatency = simulator.FastWithRareSlow{FastMin: 18 * core.Millisecond, FastMax: 20 * core.Millisecond}

// checkpointTally counts what the checkpoint timer did in one seed.
type checkpointTally struct {
	fires        int
	written      int
	reasons      map[string]int
	pendingKinds map[string]int // in-flight I/O behind each control_io_pending skip
	silentRun    int            // skipped fires since the last write
	longestRun   int            // longest run of skipped fires after the first write
	firstWrite   bool
}

func (t *checkpointTally) record(d statemachine.CheckpointDecision) {
	t.fires++
	t.reasons[d.Reason]++
	if d.Written {
		t.written++
		t.firstWrite = true
		t.silentRun = 0
		return
	}
	if d.Reason == "control_io_pending" {
		t.pendingKinds[d.PendingKind]++
	}
	t.silentRun++
	if t.firstWrite {
		t.longestRun = max(t.longestRun, t.silentRun)
	}
}

// busySkips counts fires skipped only because the node was busy, not because a
// checkpoint was not due.
func (t *checkpointTally) busySkips() int {
	return t.reasons["control_io_pending"] + t.reasons["checkpoint_in_flight"] + t.reasons["behind_tail"]
}

// reasonLine lists the decision reasons, most common first.
func (t *checkpointTally) reasonLine() string {
	keys := make([]string, 0, len(t.reasons))
	for k := range t.reasons {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if t.reasons[keys[i]] != t.reasons[keys[j]] {
			return t.reasons[keys[i]] > t.reasons[keys[j]]
		}
		return keys[i] < keys[j]
	})
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, t.reasons[k]))
	}
	return strings.Join(parts, " ")
}

// slowLogSeed runs one seed of a single CP replica (rank 0, so active) with the
// given log store latency and returns what its checkpoint timer did.
func slowLogSeed(seed uint64, logLatency simulator.FastWithRareSlow) (simulator.SeedResult, *checkpointTally) {
	tally := &checkpointTally{reasons: map[string]int{}, pendingKinds: map[string]int{}}
	machineConfig := statemachine.Config{
		CatchUpEvery:       slowLogCatchUpEvery,
		ReadLimit:          64,
		Writer:             "cp-0",
		K:                  1,
		Rank:               0,
		Heartbeat:          slowLogHeartbeat,
		CheckpointEvery:    slowLogCheckpointGap,
		CheckpointMinSlots: 1,
		OnCheckpointDecision: func(d statemachine.CheckpointDecision) {
			tally.record(d)
		},
	}
	config := simulator.Config{
		Processes: []simulator.Process{{
			Name: "cp", Count: 1, NoAttrition: true,
			FactoryFor: func(simulator.SeedConfig) core.MachineFactory {
				return statemachine.New(machineConfig)
			},
		}},
		Engines: func(sc simulator.SeedConfig) []simulator.FakeService {
			return []simulator.FakeService{fakeobjectstore.New(sc.ObjectStore), fakelogstore.New(sc.LogStore)}
		},
		ObjectStore: simulator.ObjectStoreFaults{
			ApplyLatency:   simulator.FastWithRareSlow{FastMin: core.Millisecond, FastMax: core.Millisecond},
			RespondLatency: simulator.FastWithRareSlow{FastMin: core.Millisecond, FastMax: core.Millisecond},
			CallerTimeout:  core.Second,
		},
		LogStore: simulator.LogStoreFaults{
			Enabled:        true,
			ApplyLatency:   logLatency,
			RespondLatency: simulator.FastWithRareSlow{},
			CallerTimeout:  core.Second,
		},
		Phases: simulator.Phases{Settle: slowLogRunLength},
	}
	return simulator.RunSeed(config, seed), tally
}

// TestSlowLogStoreStillCheckpoints fails today. The checkpoint timer skips for
// control_io_pending while a tail check is in flight, and each skip waits a
// whole checkpoint period. With a slow but healthy log store, the node must still
// write a checkpoint within 3 periods every time, and such skips must be rare.
func TestSlowLogStoreStillCheckpoints(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		res, tally := slowLogSeed(seed, slowLatency)
		for _, f := range res.Failures {
			t.Errorf("seed %d: %s", seed, f.Msg)
		}
		t.Logf("seed %d: %d timer fires, %d wrote a checkpoint; reasons: %s; skips by reason and request in flight: %v",
			seed, tally.fires, tally.written, tally.reasonLine(), tally.pendingKinds)
		if tally.written == 0 {
			t.Errorf("seed %d: only 0 of %d timer fires wrote a checkpoint", seed, tally.fires)
			continue
		}
		if tally.longestRun > slowLogSilentFiresMax {
			t.Errorf("seed %d: only %d of %d timer fires wrote a checkpoint; %d fires in a row wrote none (limit %d, so a write within 3 periods): %s",
				seed, tally.written, tally.fires, tally.longestRun, slowLogSilentFiresMax, tally.reasonLine())
		}
		if limit := tally.fires / slowLogPendingShare; tally.busySkips() > limit {
			t.Errorf("seed %d: only %d of %d timer fires wrote a checkpoint; %d were skipped only because the node was busy (limit %d)",
				seed, tally.written, tally.fires, tally.busySkips(), limit)
		}
	}
}

// TestFastLogStoreCheckpointControl is the control for the test above: the same
// node with a fast log store must pass, so the failure comes from the slow log.
func TestFastLogStoreCheckpointControl(t *testing.T) {
	fast := simulator.FastWithRareSlow{FastMin: core.Millisecond, FastMax: core.Millisecond}
	for seed := uint64(1); seed <= 5; seed++ {
		res, tally := slowLogSeed(seed, fast)
		if len(res.Failures) != 0 {
			t.Fatalf("seed %d: %+v", seed, res.Failures)
		}
		if tally.written == 0 {
			t.Errorf("seed %d: fast log store wrote no checkpoint in %d fires", seed, tally.fires)
		}
		if tally.longestRun > slowLogSilentFiresMax || tally.busySkips() > tally.fires/slowLogPendingShare {
			t.Errorf("seed %d: fast log store: %s", seed, tally.reasonLine())
		}
	}
}
