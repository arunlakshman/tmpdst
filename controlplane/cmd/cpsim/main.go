// Command cpsim runs the deterministic simulation of the replicated control plane.
//
//	go run ./cmd/cpsim -seeds 300                         # all scenarios, 300 seeds each
//	go run ./cmd/cpsim -scenario chaos -seed 17           # one seed, with a step trace
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	dstcampaign "github.com/manifesterio/mds/dst/campaign"
	"io"
	"os"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/verify/simulator"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("cpsim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seeds := fs.Int("seeds", 100, "seeds to run for each scenario")
	first := fs.Uint64("first", 1, "first seed of a campaign")
	seed := fs.Uint64("seed", 0, "run this one seed with a full step trace (0: run a campaign)")
	scenario := fs.String("scenario", "all", "scenario name, or all: "+strings.Join(simulator.ScenarioNames(), ", "))
	nodes := fs.Int("nodes", 3, "number of replicas")
	steps := fs.Int("steps", simulator.DefaultSteps, "scheduler events in the fault phase of a run")
	bug := fs.String("bug", "", "plant a bug to see the simulator find it: writer-offsets, log-overwrite, claim-always-wins")
	var opt options
	fs.BoolVar(&opt.report, "report", false, "after the campaign, print a report: failed seeds, assertion counts, coverage curve")
	fs.BoolVar(&opt.json, "json", false, "print the report as JSON and nothing else (implies -report)")
	fs.BoolVar(&opt.untilStable, "until-stable", false, "run seeds until no new assertion appears for -stable-after seeds, up to -max-seeds (implies -report)")
	fs.IntVar(&opt.stableAfter, "stable-after", 200, "with -until-stable: seeds in a row without a new assertion")
	fs.IntVar(&opt.maxSeeds, "max-seeds", 5000, "with -until-stable: the most seeds to run for each scenario")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	base := simulator.Config{Nodes: *nodes, Steps: *steps, Bug: simulator.Bug(*bug)}
	if *seed != 0 {
		return runOne(base, *scenario, *seed, stdout, stderr)
	}
	names := simulator.ScenarioNames()
	if *scenario != "all" {
		names = []string{*scenario}
	}
	return runCampaign(base, names, *first, *seeds, opt, stdout, stderr)
}

func runOne(base simulator.Config, scenario string, seed uint64, stdout, stderr io.Writer) int {
	if scenario == "all" {
		_, _ = fmt.Fprintln(stderr, "cpsim: -seed needs one -scenario")
		return 2
	}
	base.Scenario, base.Seed = scenario, seed
	base.Trace = func(line string) { _, _ = fmt.Fprintln(stdout, line) }
	res, err := simulator.Run(base)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "cpsim:", err)
		return 2
	}
	_, _ = fmt.Fprintln(stdout, simulator.Report(res))
	if !res.OK() {
		return 1
	}
	return 0
}

// options are the report flags. -until-stable and -json imply -report.
type options struct {
	report      bool
	json        bool
	untilStable bool
	stableAfter int // seed numbers in a row without a new assertion
	maxSeeds    int // limit of seed numbers for -until-stable
}

// batchSize is how many seed numbers -until-stable runs before it looks again.
const batchSize = 50

// campaignState is what the batches of a campaign add up to.
type campaignState struct {
	total  dstcampaign.Summary
	seen   [][]string // seen[i]: assertions that seed first+i reached, in any scenario
	failed []FailedRun
	runs   int
	code   int
}

// runBatch runs seeds first .. first+n-1 of every scenario and adds them to st.
// It returns an exit code that is not 0 when the campaign cannot go on.
func (st *campaignState) runBatch(base simulator.Config, names []string, first uint64, n int, opt options, stdout, stderr io.Writer) int {
	union := make([]map[string]bool, n)
	for _, name := range names {
		c := base
		c.Scenario = name
		out, err := runSeeds(c, first, n)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "cpsim:", err)
			return 2
		}
		if !opt.json && !opt.untilStable { // the lines that make build prints
			_, _ = fmt.Fprintln(stdout, out.line())
		}
		for _, f := range out.failed {
			if !opt.json {
				_, _ = fmt.Fprintln(stdout, simulator.Report(f))
			}
			st.code = 1
		}
		for _, f := range out.sum.Failed {
			st.failed = append(st.failed, FailedRun{Scenario: name, Seed: f.Seed, Replay: f.Reproduce})
		}
		for i, found := range out.seen {
			if union[i] == nil {
				union[i] = map[string]bool{}
			}
			for _, k := range found {
				union[i][k] = true
			}
		}
		st.runs += n
		st.total.Merge(out.sum)
	}
	for _, m := range union {
		list := make([]string, 0, len(m))
		for k := range m {
			list = append(list, k)
		}
		st.seen = append(st.seen, list)
	}
	return 0
}

func runCampaign(base simulator.Config, names []string, first uint64, seeds int, opt options, stdout, stderr io.Writer) int {
	var st campaignState
	limit := seeds
	if opt.untilStable {
		limit = opt.maxSeeds
	}
	hitLimit := false
	for done := 0; ; {
		n := limit
		if opt.untilStable {
			n = min(batchSize, limit-done)
		}
		if rc := st.runBatch(base, names, first+uint64(done), n, opt, stdout, stderr); rc != 0 {
			return rc
		}
		done += n
		if !opt.untilStable || st.code != 0 || quietSeeds(first, st.seen) >= opt.stableAfter {
			break
		}
		if done >= limit {
			hitLimit = true
			break
		}
	}
	var rep Report
	if opt.report || opt.json || opt.untilStable {
		rep = buildReport(first, st.seen, st.total, st.failed, st.runs)
		if opt.untilStable {
			rep.UntilStable = &StableResult{StableAfter: opt.stableAfter, MaxSeeds: opt.maxSeeds, HitLimit: hitLimit}
		}
	}
	if opt.json {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
		return st.code
	}
	_, _ = fmt.Fprintln(stdout, coverageText(st.total))
	if opt.report || opt.untilStable {
		rep.writeText(stdout)
	}
	if st.code != 0 {
		_, _ = fmt.Fprintln(stdout, "cpsim: FAILED")
	}
	return st.code
}
