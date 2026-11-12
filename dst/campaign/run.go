package campaign

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"strings"
	"time"
)

// batchSize is how many seed numbers a batched campaign (until-stable, or
// limited by time) runs before it looks again.
const batchSize = 50

// runner holds what one campaign needs.
type runner struct {
	reg      *Registry
	o        Options
	names    []string
	cat      catalog
	now      func() time.Time
	deadline time.Time
}

// state is what the batches of a campaign add up to.
type state struct {
	total  Summary
	seen   [][]string // seen[i]: assertions that seed first+i reached, in any scenario
	failed []FailedRun
	nondet []FailedRun
	runs   int
}

// RunCampaign runs the campaign that o describes and returns its report.
// Seeds run in parallel; the results are folded in seed order, so the hash of
// a campaign does not depend on the number of workers.
//
// Every o.CheckEvery-th seed runs a second time with Log flipped. A
// difference in (Hash, RandCalls, Steps) fails that seed with the kind
// "nondeterminism". This also proves that logging cannot change a hash.
//
// If o.Out is set, the progress text goes there as the campaign runs.
func RunCampaign(reg *Registry, o Options) (Report, error) {
	return runCampaign(reg, o, declaredCatalog(), time.Now)
}

func runCampaign(reg *Registry, o Options, cat catalog, now func() time.Time) (Report, error) {
	names, err := reg.Resolve(o.Scenarios)
	if err != nil {
		return Report{}, err
	}
	c := &runner{reg: reg, o: o, names: names, cat: cat, now: now}
	var st state
	hitLimit := c.loop(&st)
	rep := buildReport(campaignData{
		first: o.First, seen: st.seen, total: st.total, failed: st.failed, nondet: st.nondet,
		runs: st.runs, minGapSeeds: o.MinSeedsForCoverage,
	}, cat)
	if o.UntilStable {
		rep.UntilStable = &StableResult{StableAfter: o.StableAfter, MaxSeeds: o.MaxSeeds, HitLimit: hitLimit}
	}
	return rep, nil
}

// loop runs the batches. It reports whether the seed limit of an
// until-stable campaign came first.
func (c *runner) loop(st *state) bool {
	o := c.o
	limit := o.Seeds
	timed := o.Duration > 0
	switch {
	case o.UntilStable:
		limit = o.MaxSeeds
	case timed:
		limit = math.MaxInt
	}
	batched := o.UntilStable || timed
	if timed {
		c.deadline = c.now().Add(o.Duration)
	}
	for done := 0; ; {
		n := limit
		if batched {
			n = min(batchSize, limit-done)
		}
		c.runBatch(st, o.First+uint64(done), n)
		done += n
		if !batched || len(st.failed) > 0 || c.stableNow(st) || c.timeUp() {
			return false
		}
		if done >= limit {
			return true
		}
	}
}

func (c *runner) stableNow(st *state) bool {
	return c.o.UntilStable && quietSeeds(c.o.First, st.seen) >= c.o.StableAfter
}

func (c *runner) timeUp() bool {
	return c.o.Duration > 0 && !c.now().Before(c.deadline)
}

// runBatch runs seeds first .. first+n-1 of every scenario and adds them to st.
func (c *runner) runBatch(st *state, first uint64, n int) {
	union := make([]map[string]bool, n)
	for _, name := range c.names {
		results, sum := c.runScenario(name, first, n)
		c.print(name, n, results, sum)
		for i, r := range results {
			if union[i] == nil {
				union[i] = map[string]bool{}
			}
			for k, v := range r.Coverage {
				if v.Checks > 0 {
					union[i][k] = true
				}
			}
			if r.OK {
				continue
			}
			fr := FailedRun{Scenario: name, Seed: r.Seed, Replay: r.Reproduce, Failures: r.Failures}
			st.failed = append(st.failed, fr)
			if isNotRepeatable(r) {
				st.nondet = append(st.nondet, fr)
			}
		}
		st.runs += n
		st.total.Merge(sum)
	}
	for _, m := range union {
		list := make([]string, 0, len(m))
		for k := range m {
			list = append(list, k)
		}
		slices.Sort(list)
		st.seen = append(st.seen, list)
	}
}

func isNotRepeatable(r Result) bool {
	return slices.ContainsFunc(r.Failures, func(f Failure) bool { return f.Kind == KindNotRepeatable })
}

// runScenario runs seeds first .. first+n-1 of one scenario on several
// goroutines. Each run is single threaded inside. It returns the results in
// seed order, and their fold.
func (c *runner) runScenario(name string, first uint64, n int) ([]Result, Summary) {
	sc, _ := c.reg.Get(name)
	results := make([]Result, max(n, 0))
	sum := RunSeeds(first, n, c.o.Workers, func(seed uint64) Result {
		r := c.runSeed(sc, seed)
		results[int(seed-first)] = r
		return r
	})
	return results, sum
}

// runSeed runs one seed, and the determinism re-run when the seed is due.
func (c *runner) runSeed(sc Scenario, seed uint64) Result {
	r := sc.Run(seed, RunOptions{})
	r.Seed = seed
	if r.Reproduce == "" && c.o.Tool != "" {
		r.Reproduce = fmt.Sprintf("go run ./cmd/%s -scenario %s -seed %d", c.o.Tool, sc.Name, seed)
	}
	every := c.o.CheckEvery
	if every <= 0 || (seed-c.o.First)%uint64(every) != 0 {
		return r
	}
	again := sc.Run(seed, RunOptions{Log: true})
	if again.Hash != r.Hash || again.RandCalls != r.RandCalls || again.Steps != r.Steps {
		r.OK = false
		r.Failures = append(slices.Clone(r.Failures), Failure{
			Kind: KindNotRepeatable, Property: "determinism", Step: r.Steps,
			Detail: "second run of the seed diverged",
		})
	}
	return r
}

// print writes the progress text of one scenario and batch: the per-seed
// lines (verbose), the summary line, and the details of each failed seed.
func (c *runner) print(name string, n int, results []Result, sum Summary) {
	w := c.o.Out
	if w == nil {
		return
	}
	if c.o.Verbose {
		for _, r := range results {
			_, _ = fmt.Fprintf(w, "%s seed %d: steps=%d rand=%d hash=%016x failures=%d\n",
				name, r.Seed, r.Steps, r.RandCalls, r.Hash, len(r.Failures))
		}
	}
	if !c.o.UntilStable { // the lines that make build prints
		_, _ = fmt.Fprintln(w, summaryLine(name, n, sum))
	}
	for _, r := range results {
		if !r.OK {
			writeFailure(w, name, r)
		}
	}
}

// summaryLine is the one-line summary of the batch of one scenario. After the
// steps it lists the other counters, by name.
func summaryLine(name string, seeds int, sum Summary) string {
	status := "ok"
	if sum.Counts.Failed > 0 {
		status = fmt.Sprintf("FAILED %d of %d seeds", sum.Counts.Failed, seeds)
	}
	var line strings.Builder
	fmt.Fprintf(&line, "%-15s %4d seeds  %9d steps", name, seeds, sum.Counters["steps"])
	others := make([]string, 0, len(sum.Counters))
	for k := range sum.Counters {
		if k != "steps" {
			others = append(others, k)
		}
	}
	slices.Sort(others)
	for _, k := range others {
		fmt.Fprintf(&line, "  %7d %s", sum.Counters[k], k)
	}
	fmt.Fprintf(&line, "  %5.1fs  hash %08x  %s", sum.Elapsed.Seconds(), uint32(sum.Hash), status)
	return line.String()
}

// Replay runs one seed with Log and HaltOnFirst, prints its step lines and a
// last line with the result, and returns the exit code: 0 for a seed that
// passes, 1 for a seed that fails, 2 for an unknown scenario.
//
// For a seed that passes, neither option changes the hash. A failing seed
// stops at its first failure, so its hash covers the steps up to there.
func Replay(reg *Registry, scenario string, seed uint64, w io.Writer) int {
	sc, ok := reg.Get(scenario)
	if !ok {
		_, _ = fmt.Fprintf(w, "unknown scenario %q (have %v)\n", scenario, reg.Names())
		return 2
	}
	r := sc.Run(seed, RunOptions{Log: true, HaltOnFirst: true})
	for _, l := range r.Log {
		_, _ = fmt.Fprintln(w, l)
	}
	_, _ = fmt.Fprintf(w, "seed %d: steps=%d rand=%d hash=%016x\n", seed, r.Steps, r.RandCalls, r.Hash)
	for _, f := range r.Failures {
		_, _ = fmt.Fprintf(w, "FAIL %s (%s) step=%d at=%v: %s\n", f.Kind, f.Property, f.Step,
			time.Duration(f.AtMs)*time.Millisecond, f.Detail)
	}
	if !r.OK {
		return 1
	}
	return 0
}

// Main is the whole main function of a product simulator command:
//
//	func main() { os.Exit(campaign.Main(config, registry, os.Args[1:], os.Stdout, os.Stderr)) }
//
// It returns the exit code: 0 when every seed passed, 1 when the campaign
// failed (see Report.HasFailed), 2 for a bad flag, variable or scenario name.
func Main(c Config, reg *Registry, args []string, stdout, stderr io.Writer) int {
	return mainWith(c, reg, args, os.Getenv, stdout, stderr)
}

func mainWith(c Config, reg *Registry, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	o, err := ParseOptions(c, args, getenv, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		if _, isEnv := errors.AsType[envError](err); isEnv { // the flag package has printed its own errors
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Tool, err)
		}
		return 2
	}
	names, err := reg.Resolve(o.Scenarios)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Tool, err)
		return 2
	}
	if o.HasSeed {
		if len(names) != 1 {
			_, _ = fmt.Fprintf(stderr, "%s: -seed needs one -scenario\n", c.Tool)
			return 2
		}
		return Replay(reg, names[0], o.Seed, stdout)
	}
	return mainCampaign(c, reg, o, stdout, stderr)
}

func mainCampaign(c Config, reg *Registry, o Options, stdout, stderr io.Writer) int {
	if !o.JSON {
		o.Out = stdout
	}
	rep, err := RunCampaign(reg, o)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Tool, err)
		return 2
	}
	code := 0
	if rep.HasFailed() {
		code = 1
	}
	if o.JSON {
		if err := rep.WriteJSON(stdout); err != nil {
			_, _ = fmt.Fprintf(stderr, "%s: %v\n", c.Tool, err)
			return 2
		}
		return code
	}
	_, _ = fmt.Fprintln(stdout, rep.coverageText())
	if o.Report {
		rep.WriteText(stdout)
	}
	if code != 0 {
		_, _ = fmt.Fprintf(stdout, "%s: FAILED\n", c.Tool)
	}
	return code
}
