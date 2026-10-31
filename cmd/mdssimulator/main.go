// Command mdssimulator runs a scenario campaign (the Kafka broker by default, or
// the echo smoke test) through the deterministic simulator. It is not a
// deterministic package: it reads flags and the clock, and it shards a
// campaign's seeds over goroutines (seeds are independent; see merge.go).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/echo"
	"mds/internal/verify/simulator/kafka"
)

// minimumCoverage is the campaign size from which a never-satisfied Sometimes
// property counts as a failure (smaller runs cannot be expected to reach it).
const minimumCoverage = 100

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// options are the parsed command-line flags.
type options struct {
	seeds    int
	start    uint64
	one      int64
	verbose  bool
	scenario string
	jobs     int
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	var o options
	fs := flag.NewFlagSet("mdssimulator", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.IntVar(&o.seeds, "seeds", 100, "number of seeds to run")
	fs.Uint64Var(&o.start, "start", 1, "first seed")
	fs.Int64Var(&o.one, "seed", -1, "replay this one seed with its log printed")
	fs.BoolVar(&o.verbose, "v", false, "print one line per seed")
	fs.StringVar(&o.scenario, "scenario", "kafka", "scenario: kafka, kafka-log (log-backed control plane) or echo")
	fs.IntVar(&o.jobs, "j", runtime.NumCPU(), "seed shards to run in parallel")
	err := fs.Parse(args)
	return o, err
}

// scenarioConfig maps a -scenario name to its config builder.
func scenarioConfig(name string) (func([]uint64) simulator.Config, bool) {
	switch name {
	case "kafka":
		return kafka.Config, true
	case "kafka-log":
		return kafka.ConfigLog, true
	case "echo":
		return echo.Config, true
	}
	return nil, false
}

func run(args []string, stdout, stderr io.Writer) int {
	o, err := parseOptions(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	config, ok := scenarioConfig(o.scenario)
	if !ok {
		_, _ = fmt.Fprintf(stderr, "mdssimulator: unknown scenario %q (want kafka, kafka-log or echo)\n", o.scenario)
		return 2
	}
	if o.one >= 0 {
		return rerunSeed(stdout, stderr, config, uint64(o.one))
	}
	list := make([]uint64, o.seeds)
	for i := range list {
		list[i] = o.start + uint64(i)
	}
	t0 := time.Now()
	rep, err := runInParts(config, list, o.jobs)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "mdssimulator:", err)
		return 2
	}
	bad, steps := printSeeds(stdout, rep, o)
	printCampaign(stdout, rep, o.scenario)
	_, _ = fmt.Fprintf(stdout, "%d seeds from %d: %d failing, %d steps, %v\n", len(rep.Seeds), o.start, bad, steps, time.Since(t0).Round(time.Millisecond))
	if failed(rep, bad) {
		return 1
	}
	return 0
}

// failed is the campaign verdict: any failing seed, violated Always property,
// nondeterminism, or (in a big enough campaign) a coverage gap.
func failed(rep simulator.Report, bad int) bool {
	return bad > 0 || len(rep.Always) > 0 || len(rep.Nondeterminism) > 0 || (len(rep.Coverage) > 0 && len(rep.Seeds) >= minimumCoverage)
}

// printSeeds prints the per-seed lines and failures; it returns the number of
// failing seeds and the total steps.
func printSeeds(w io.Writer, rep simulator.Report, o options) (int, uint64) {
	var bad int
	var steps uint64
	for _, s := range rep.Seeds {
		steps += s.Steps
		if o.verbose {
			_, _ = fmt.Fprintf(w, "seed %d: steps=%d rand=%d hash=%016x crashes=%d exits=%d failures=%d\n",
				s.Seed, s.Steps, s.RandCalls, s.TraceHash, s.Crashes, s.Exits, len(s.Failures))
		}
		if len(s.Failures) == 0 {
			continue
		}
		bad++
		for _, f := range s.Failures {
			_, _ = fmt.Fprintf(w, "FAIL seed %d: %s (%s) step=%d at=%v: %s\n", s.Seed, kind(f.Kind), f.Prop, f.Step, time.Duration(f.At), f.Msg)
		}
		_, _ = fmt.Fprintf(w, "  repro: go run ./cmd/mdssimulator -scenario %s -seed %d\n", o.scenario, s.Seed)
	}
	return bad, steps
}

// printCampaign prints the campaign-wide findings.
func printCampaign(w io.Writer, rep simulator.Report, scenario string) {
	for _, a := range rep.Always {
		_, _ = fmt.Fprintf(w, "VIOLATED %q first at seed %d step %d (%d seeds)\n", a.Prop, a.FirstSeed, a.FirstStep, len(a.Seeds))
	}
	for _, a := range rep.Coverage {
		_, _ = fmt.Fprintf(w, "coverage gap: %q (%s) never satisfied\n", a.Prop, a.Kind)
	}
	for _, s := range rep.Nondeterminism {
		_, _ = fmt.Fprintf(w, "NONDETERMINISM seed %d: two runs diverged\n  repro: go run ./cmd/mdssimulator -scenario %s -seed %d\n", s, scenario, s)
	}
}

func rerunSeed(stdout, stderr io.Writer, config func([]uint64) simulator.Config, seed uint64) int {
	res, err := simulator.NewRunner().Replay(config([]uint64{seed}), seed)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "mdssimulator:", err)
		return 2
	}
	for _, l := range res.Log {
		_, _ = fmt.Fprintf(stdout, "%6d %12v  %s\n", l.Step, time.Duration(l.At), l.Text)
	}
	_, _ = fmt.Fprintf(stdout, "seed %d: steps=%d rand=%d hash=%016x crashes=%d trips=%d exits=%d\n",
		seed, res.Steps, res.RandCalls, res.TraceHash, res.Crashes, res.Trips, res.Exits)
	for _, f := range res.Failures {
		_, _ = fmt.Fprintf(stdout, "FAIL %s (%s) step=%d at=%v: %s\n", kind(f.Kind), f.Prop, f.Step, time.Duration(f.At), f.Msg)
	}
	if len(res.Failures) > 0 {
		return 1
	}
	return 0
}

var failureKindNames = map[simulator.FailKind]string{
	simulator.FailViolation:     "violation",
	simulator.FailUndecided:     "inconclusive",
	simulator.FailProtocol:      "protocol",
	simulator.FailPanic:         "panic",
	simulator.FailMaxSteps:      "max_steps",
	simulator.FailNotRepeatable: "nondeterminism",
}

func kind(k simulator.FailKind) string {
	if n, ok := failureKindNames[k]; ok {
		return n
	}
	return fmt.Sprintf("kind(%d)", k)
}
