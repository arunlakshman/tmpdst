// Package campaign runs many independent seeds in parallel, folds the results
// and prints one report. A product registers its scenarios in a Registry and
// calls Main. The standard flags, the environment variables, the report, the
// until-stable mode and the determinism re-run are all here.
//
// Files:
//
//   - campaign.go: Result, Summary and RunSeeds (the parallel fold)
//   - registry.go: Scenario, RunOptions and Registry
//   - kernel.go: FromKernel
//   - options.go: Config, Options and ParseOptions (flags and variables)
//   - run.go: RunCampaign, Replay and Main
//   - report.go: Report, its text and JSON
package campaign

import (
	goruntime "runtime"
	"slices"
	"sync"
	"time"

	"github.com/manifesterio/mds/dst/fault"
	"github.com/manifesterio/mds/dst/trace"
)

// Coverage counts evaluations of one property across seeds.
type Coverage struct {
	Kind   string
	Checks uint64
	Passes uint64
}

// SiteTotal counts one Maybe site over many seeds: in how many runs it was
// asked (Reached), in how many it was switched on (On), and the sums of
// calls and fires.
type SiteTotal struct {
	Reached uint64
	On      uint64
	Calls   uint64
	Fired   uint64
}

// Failure is one failed property of one seed.
type Failure struct {
	Kind     string `json:"kind"` // violation, protocol, panic, max_steps, nondeterminism
	Property string `json:"property"`
	Detail   string `json:"detail"`
	Step     uint64 `json:"step"`
	AtMs     int64  `json:"at_ms"` // virtual time of the failure, in milliseconds
	Repeats  int    `json:"repeats,omitempty"`
}

// Result is the outcome of one seed.
type Result struct {
	Seed uint64
	Hash uint64
	// Steps and RandCalls, with Hash, are what the determinism re-run compares.
	Steps, RandCalls uint64
	OK               bool
	// Reproduce is the command that runs this seed again. When it is empty,
	// the campaign makes one from Options.Tool.
	Reproduce string
	Failures  []Failure
	Coverage  map[string]Coverage
	// Counters are named totals of the run (steps, writes, ...). RunSeeds sums them.
	Counters map[string]uint64
	// Sites are the counts of the run's Maybe sites (fault.Injector.Sites).
	Sites map[string]fault.SiteCount
	// Tail holds the last step lines of a failed run. Log holds every step
	// line, only when RunOptions.Log was set.
	Tail []string
	Log  []string
}

// Counts summarizes how many seeds ran and how many failed.
type Counts struct {
	Seeds  int
	OK     int
	Failed int
}

// Summary is the outcome of many seeds of one scenario.
type Summary struct {
	Failed   []Result
	Hash     uint64
	Coverage map[string]Coverage
	Counters map[string]uint64
	Sites    map[string]SiteTotal
	Elapsed  time.Duration
	Counts   Counts
}

// Merge adds the coverage, counters and sites of o into s, for a total over
// several campaigns. Hash, Elapsed, Counts and Failed are not touched.
func (s *Summary) Merge(o Summary) {
	if s.Coverage == nil {
		s.Coverage = map[string]Coverage{}
	}
	addCoverage(s.Coverage, o.Coverage)
	if s.Sites == nil {
		s.Sites = map[string]SiteTotal{}
	}
	for k, v := range o.Sites {
		t := s.Sites[k]
		t.Reached += v.Reached
		t.On += v.On
		t.Calls += v.Calls
		t.Fired += v.Fired
		s.Sites[k] = t
	}
	if s.Counters == nil {
		s.Counters = map[string]uint64{}
	}
	for k, v := range o.Counters {
		s.Counters[k] += v
	}
}

// Missing returns the sorted names in expected that no seed passed: they have
// no coverage entry or zero Passes.
func Missing(cov map[string]Coverage, expected []string) []string {
	var out []string
	for _, name := range expected {
		if cov[name].Passes == 0 {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// NeverFired returns the sorted names of the sites that did not fire in any
// run: sites in sites with Fired 0, and names in expected that no run reached.
func NeverFired(sites map[string]SiteTotal, expected []string) []string {
	var out []string
	for name, t := range sites {
		if t.Fired == 0 {
			out = append(out, name)
		}
	}
	for _, name := range expected {
		if sites[name].Fired == 0 && !slices.Contains(out, name) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// Run is the old name of RunSeeds. It stays until the control plane command
// moves to Main (plan task T1.11). Then it is deleted.
func Run(first uint64, seeds int, workers int, run func(seed uint64) Result) Summary {
	return RunSeeds(first, seeds, workers, run)
}

// RunSeeds executes seeds first, first+1, ... for seeds count, using workers goroutines.
// When workers <= 0, GOMAXPROCS is used. Each run is independent; results are
// folded in seed order. Coverage maps are merged by summing Checks and Passes.
func RunSeeds(first uint64, seeds int, workers int, run func(seed uint64) Result) Summary {
	if seeds <= 0 {
		return Summary{Coverage: map[string]Coverage{}, Counters: map[string]uint64{}, Sites: map[string]SiteTotal{}}
	}
	if workers <= 0 {
		workers = goruntime.GOMAXPROCS(0)
	}
	workers = min(workers, seeds)

	start := time.Now()
	results := make([]Result, seeds)
	var wg sync.WaitGroup
	jobs := make(chan int)
	for range workers {
		wg.Go(func() {
			for i := range jobs {
				seed := first + uint64(i)
				r := run(seed)
				r.Seed = seed
				results[i] = r
			}
		})
	}
	for i := range seeds {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	out := Summary{
		Coverage: map[string]Coverage{},
		Counters: map[string]uint64{},
		Sites:    map[string]SiteTotal{},
		Counts:   Counts{Seeds: seeds},
	}
	h := trace.New()
	for _, r := range results {
		h.Uint64(r.Hash)
		addCoverage(out.Coverage, r.Coverage)
		for k, v := range r.Counters {
			out.Counters[k] += v
		}
		for k, v := range r.Sites {
			t := out.Sites[k]
			t.Reached++
			if v.On {
				t.On++
			}
			t.Calls += v.Calls
			t.Fired += v.Fired
			out.Sites[k] = t
		}
		if r.OK {
			out.Counts.OK++
			continue
		}
		out.Failed = append(out.Failed, r)
		out.Counts.Failed++
	}
	out.Hash = h.Sum()
	out.Elapsed = time.Since(start)
	return out
}

func addCoverage(into, from map[string]Coverage) {
	for k, c := range from {
		t := into[k]
		t.Kind = c.Kind
		t.Checks += c.Checks
		t.Passes += c.Passes
		into[k] = t
	}
}
