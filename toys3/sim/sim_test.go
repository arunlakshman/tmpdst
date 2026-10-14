package sim

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"
)

var (
	flagSeeds    = flag.Int("seeds", 200, "TestSeeds, TestC2...: number of seeds to run (from 1)")
	flagSeed     = flag.Int64("seed", -1, "TestSeeds, TestC2...: replay this one seed (TestSeeds prints the event log on failure)")
	flagDuration = flag.Duration("duration", 0, "TestSeeds, TestC2...: run seeds 1, 2, ... until this much wall-clock time has passed (overrides -seeds)")
	flagLog      = flag.Bool("log", false, "TestSeeds: with -seed, print the full event log even when the seed passes")
)

func repro(seed uint64) string {
	return fmt.Sprintf("go test ./toys3/sim -run TestSeeds -seed=%d -v", seed)
}

// TestDeterminismCanary runs several seeds twice each: any difference in
// the trace hash, the number of random draws or the number of steps means
// nondeterminism leaked into the simulator or the server.
func TestDeterminismCanary(t *testing.T) {
	for seed := uint64(1); seed <= 12; seed++ {
		a := Run(DefaultConfig(seed))
		b := Run(DefaultConfig(seed))
		if a.TraceHash != b.TraceHash || a.RandCalls != b.RandCalls || a.Steps != b.Steps {
			t.Fatalf("seed %d diverged: hash %x/%x rand %d/%d steps %d/%d",
				seed, a.TraceHash, b.TraceHash, a.RandCalls, b.RandCalls, a.Steps, b.Steps)
		}
		// Log mode only records; it must not change the run.
		cfg := DefaultConfig(seed)
		cfg.Log = true
		if c := Run(cfg); c.TraceHash != a.TraceHash || len(c.Log) == 0 {
			t.Fatalf("seed %d: Log mode changed the trace (%x vs %x)", seed, c.TraceHash, a.TraceHash)
		}
		if seed > 1 && a.TraceHash == Run(DefaultConfig(seed-1)).TraceHash {
			t.Fatalf("seeds %d and %d produced the same trace", seed, seed-1)
		}
	}
}

// TestSeeds runs the correct server against many seeds. Flags: -seeds N,
// -seed S (replay one seed, printing its event log on failure), -duration D.
func TestSeeds(t *testing.T) {
	if *flagSeed >= 0 {
		seed := uint64(*flagSeed)
		cfg := DefaultConfig(seed)
		cfg.Log = true
		r := Run(cfg)
		t.Logf("seed %d: steps=%d crashes=%d trace=%016x\n%s", seed, r.Steps, r.Crashes, r.TraceHash, r.Stats)
		if *flagLog || len(r.Failures) > 0 {
			fmt.Fprint(os.Stderr, FormatLog(r.Log))
		}
		if len(r.Failures) > 0 {
			for _, f := range r.Failures {
				t.Errorf("FAIL %s at step %d (t=%s): %s", f.Invariant, f.Step, fmtTime(f.At), f.Msg)
			}
		}
		return
	}
	start := time.Now()
	var agg Stats
	n := 0
	for seed := uint64(1); ; seed++ {
		if *flagDuration > 0 {
			if time.Since(start) >= *flagDuration {
				break
			}
		} else if seed > uint64(*flagSeeds) {
			break
		}
		r := Run(DefaultConfig(seed))
		agg.Add(r.Stats)
		n++
		if len(r.Failures) > 0 {
			f := r.Failures[0]
			t.Errorf("seed %d FAILED %s at step %d: %s\nrepro: %s", seed, f.Invariant, f.Step, f.Msg, repro(seed))
		}
	}
	t.Logf("%d seeds in %v\n%s", n, time.Since(start).Round(time.Millisecond), agg)
}

// TestCoverage checks that the fault model actually exercises what it
// claims to across 200 seeds: a passing run that never crashed mid-PUT
// proves nothing.
func TestCoverage(t *testing.T) {
	var agg Stats
	for seed := uint64(1); seed <= 200; seed++ {
		agg.Add(Run(DefaultConfig(seed)).Stats)
	}
	t.Log(agg)
	checks := []struct {
		name string
		n    int
	}{
		{"crashes", agg.Crashes},
		{"crashes mid-PUT/COPY", agg.CrashesMidPut},
		{"indeterminate (abandoned) requests", agg.Indeterminate},
		{"torn files", agg.TornFiles},
		{"lost un-synced changes", agg.LostChanges},
		{"kept un-synced changes", agg.KeptChanges},
		{"EIOs", agg.EIOs},
		{"read corruptions", agg.ReadCorruptions},
		{"200 replies", agg.ByStatus[0]},
		{"400 replies", agg.ByStatus[1]},
		{"404 replies", agg.ByStatus[2]},
		{"500 replies", agg.ByStatus[3]},
		{"Trips", agg.Trips},
		{"CorruptionDetected telemetry", agg.CorruptionsDetected},
		{"temps removed by recovery", agg.TempsRemoved},
		{"linearizability windows", agg.Windows},
	}
	for _, c := range checks {
		if c.n == 0 {
			t.Errorf("coverage: no %s in 200 seeds", c.name)
		}
	}
}

// TestCorruptionUnreachableWithoutBitFlips documents that for the
// correct server the CRC check is only a safety net: with no read
// corruption injected, torn temp files never reach obj/, so no read ever
// fails its CRC.
func TestCorruptionUnreachableWithoutBitFlips(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		cfg := DefaultConfig(seed)
		cfg.Disturbances.ReadCorruptProb = 0
		r := Run(cfg)
		if len(r.Failures) > 0 || r.Stats.CorruptionsDetected > 0 {
			t.Fatalf("seed %d: failures=%v corrupt=%d", seed, r.Failures, r.Stats.CorruptionsDetected)
		}
	}
}
