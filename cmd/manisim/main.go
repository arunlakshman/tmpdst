// Command manisim runs a scenario campaign (the Kafka broker by default, or
// the echo smoke test) through the deterministic simulator. It is not a
// deterministic package: it reads flags and the clock, and it shards a
// campaign's seeds over goroutines (seeds are independent; see merge.go).
package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"time"

	"manifester/sim"
	"manifester/sim/echo"
	"manifester/sim/kafka"
)

// coverageMin is the campaign size from which a never-satisfied Sometimes
// property counts as a failure (smaller runs cannot be expected to reach it).
const coverageMin = 100

func main() { os.Exit(run()) }

func run() int {
	seeds := flag.Int("seeds", 100, "number of seeds to run")
	start := flag.Uint64("start", 1, "first seed")
	one := flag.Int64("seed", -1, "replay this one seed with its log printed")
	verbose := flag.Bool("v", false, "print one line per seed")
	scenario := flag.String("scenario", "kafka", "scenario: kafka or echo")
	jobs := flag.Int("j", runtime.NumCPU(), "seed shards to run in parallel")
	flag.Parse()

	var config func([]uint64) sim.Config
	switch *scenario {
	case "kafka":
		config = kafka.Config
	case "echo":
		config = echo.Config
	default:
		fmt.Fprintf(os.Stderr, "manisim: unknown scenario %q (want kafka or echo)\n", *scenario)
		return 2
	}
	if *one >= 0 {
		return replay(config, uint64(*one))
	}
	list := make([]uint64, *seeds)
	for i := range list {
		list[i] = *start + uint64(i)
	}
	t0 := time.Now()
	rep, err := runSharded(config, list, *jobs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manisim:", err)
		return 2
	}
	bad := 0
	var steps uint64
	for _, s := range rep.Seeds {
		steps += s.Steps
		if *verbose {
			fmt.Printf("seed %d: steps=%d rand=%d hash=%016x crashes=%d exits=%d failures=%d\n",
				s.Seed, s.Steps, s.RandCalls, s.TraceHash, s.Crashes, s.Exits, len(s.Failures))
		}
		if len(s.Failures) > 0 {
			bad++
			for _, f := range s.Failures {
				fmt.Printf("FAIL seed %d: %s (%s) step=%d at=%v: %s\n", s.Seed, kind(f.Kind), f.Prop, f.Step, time.Duration(f.At), f.Msg)
			}
			fmt.Printf("  repro: go run ./cmd/manisim -scenario %s -seed %d\n", *scenario, s.Seed)
		}
	}
	for _, a := range rep.Always {
		fmt.Printf("VIOLATED %q first at seed %d step %d (%d seeds)\n", a.Prop, a.FirstSeed, a.FirstStep, len(a.Seeds))
	}
	for _, a := range rep.Coverage {
		fmt.Printf("coverage gap: %q (%s) never satisfied\n", a.Prop, a.Kind)
	}
	for _, s := range rep.Nondeterminism {
		fmt.Printf("NONDETERMINISM seed %d: two runs diverged\n  repro: go run ./cmd/manisim -scenario %s -seed %d\n", s, *scenario, s)
	}
	fmt.Printf("%d seeds from %d: %d failing, %d steps, %v\n", len(rep.Seeds), *start, bad, steps, time.Since(t0).Round(time.Millisecond))
	if bad > 0 || len(rep.Always) > 0 || len(rep.Nondeterminism) > 0 || (len(rep.Coverage) > 0 && len(rep.Seeds) >= coverageMin) {
		return 1
	}
	return 0
}

func replay(config func([]uint64) sim.Config, seed uint64) int {
	res, err := sim.NewRunner().Replay(config([]uint64{seed}), seed)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manisim:", err)
		return 2
	}
	for _, l := range res.Log {
		fmt.Printf("%6d %12v  %s\n", l.Step, time.Duration(l.At), l.Text)
	}
	fmt.Printf("seed %d: steps=%d rand=%d hash=%016x crashes=%d trips=%d exits=%d\n",
		seed, res.Steps, res.RandCalls, res.TraceHash, res.Crashes, res.Trips, res.Exits)
	for _, f := range res.Failures {
		fmt.Printf("FAIL %s (%s) step=%d at=%v: %s\n", kind(f.Kind), f.Prop, f.Step, time.Duration(f.At), f.Msg)
	}
	if len(res.Failures) > 0 {
		return 1
	}
	return 0
}

func kind(k sim.FailKind) string {
	switch k {
	case sim.FailViolation:
		return "violation"
	case sim.FailInconclusive:
		return "inconclusive"
	case sim.FailProtocol:
		return "protocol"
	case sim.FailPanic:
		return "panic"
	case sim.FailMaxSteps:
		return "max_steps"
	case sim.FailNondeterminism:
		return "nondeterminism"
	}
	return fmt.Sprintf("kind(%d)", k)
}
