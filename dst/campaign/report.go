package campaign

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/fault"
)

// Report is the end-of-campaign picture: what ran, what failed, which
// assertions held, and whether more seeds still find new assertions.
type Report struct {
	Seeds        int           `json:"seeds"`  // seed numbers run (each runs once per scenario)
	Runs         int           `json:"runs"`   // seeds times scenarios
	Passed       int           `json:"passed"` // runs that kept every property
	Failed       int           `json:"failed"` // runs that broke a property
	FailedRuns   []FailedRun   `json:"failed_runs"`
	Assertions   []AssertRow   `json:"assertions"`    // every assertion, by name
	NeverReached []string      `json:"never_reached"` // declared Sometimes or Reachable paths that no seed passed
	NeverChecked []string      `json:"never_checked,omitempty"`
	Sites        []SiteRow     `json:"sites"`       // buggify sites, by name
	NeverFired   []string      `json:"never_fired"` // declared buggify sites that fired in no run
	Curve        []CurvePoint  `json:"curve"`       // seed numbers where a new assertion appeared
	LastGrowth   uint64        `json:"last_growth"` // seed number of the last new assertion (0: none)
	LastSeed     uint64        `json:"last_seed"`   // last seed number run
	UntilStable  *StableResult `json:"until_stable,omitempty"`
	// Nondeterminism lists the seeds whose second run gave another result.
	// They are also in FailedRuns.
	Nondeterminism []FailedRun `json:"nondeterminism,omitempty"`

	// expectedPaths is how many Sometimes and Reachable paths are declared.
	// minGapSeeds is Options.MinSeedsForCoverage. They are not in the JSON.
	expectedPaths int
	minGapSeeds   int
}

// FailedRun is a run that broke a property, with the command that replays it.
type FailedRun struct {
	Scenario string    `json:"scenario"`
	Seed     uint64    `json:"seed"`
	Replay   string    `json:"replay"`
	Failures []Failure `json:"failures,omitempty"`
}

// AssertRow is the count of one assertion over the whole campaign.
type AssertRow struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Checks uint64 `json:"checks"`
	Passes uint64 `json:"passes"`
}

// SiteRow is the count of one buggify site over the whole campaign.
type SiteRow struct {
	Name    string `json:"name"`
	Reached uint64 `json:"reached"` // runs that asked the site
	On      uint64 `json:"on"`      // runs in which it was switched on
	Calls   uint64 `json:"calls"`
	Fired   uint64 `json:"fired"`
}

// CurvePoint says that at seed number Seed the campaign had seen Total
// different assertions.
type CurvePoint struct {
	Seed  uint64 `json:"seed"`
	Total int    `json:"total"`
}

// StableResult is set when -until-stable was used.
type StableResult struct {
	StableAfter int  `json:"stable_after"`
	MaxSeeds    int  `json:"max_seeds"`
	HitLimit    bool `json:"hit_limit"` // the limit came first: the campaign was not stable yet
}

// catalog is the list of declared assertions and buggify sites that a report
// compares the campaign with. A real campaign uses the catalog of the
// program (assert.Catalog and fault.Sites). A test can give its own.
type catalog struct {
	asserts []assert.Declared
	sites   []string
}

func declaredCatalog() catalog {
	return catalog{asserts: assert.Catalog(), sites: fault.Sites()}
}

// campaignData is what buildReport folds.
type campaignData struct {
	first       uint64
	seen        [][]string // seen[i]: assertions that seed first+i reached, in any scenario
	total       Summary
	failed      []FailedRun
	nondet      []FailedRun
	runs        int
	minGapSeeds int
}

// buildReport folds the campaign data and the catalog into a Report.
func buildReport(d campaignData, cat catalog) Report {
	r := Report{
		Seeds: len(d.seen), Runs: d.runs, Failed: len(d.failed), Passed: d.runs - len(d.failed),
		FailedRuns: d.failed, Nondeterminism: d.nondet, minGapSeeds: d.minGapSeeds,
	}
	if r.FailedRuns == nil {
		r.FailedRuns = []FailedRun{}
	}
	r.Curve, r.LastGrowth = coverageCurve(d.first, d.seen)
	if len(d.seen) > 0 {
		r.LastSeed = d.first + uint64(len(d.seen)) - 1
	}
	r.Assertions = assertRows(d.total.Coverage)
	r.NeverReached = neverReached(d.total.Coverage, cat.asserts)
	r.NeverChecked = neverChecked(d.total.Coverage, cat.asserts)
	for _, a := range cat.asserts {
		if (a.Kind == assert.KindSometimes || a.Kind == assert.KindReachable) && !a.NotExpected {
			r.expectedPaths++
		}
	}
	r.Sites, r.NeverFired = siteRows(d.total.Sites, cat.sites)
	return r
}

func assertRows(cov map[string]Coverage) []AssertRow {
	names := make([]string, 0, len(cov))
	for n := range cov {
		names = append(names, n)
	}
	slices.Sort(names)
	rows := []AssertRow{}
	for _, n := range names {
		c := cov[n]
		rows = append(rows, AssertRow{Name: n, Kind: c.Kind, Checks: c.Checks, Passes: c.Passes})
	}
	return rows
}

// neverReached lists the paths that no seed passed. A path that never ran
// has no coverage entry at all, so the declared catalog is the only thing
// that can name it. A path declared as not expected (Declared.NotExpected) is
// left out.
func neverReached(cov map[string]Coverage, declared []assert.Declared) []string {
	out := []string{}
	for n, c := range cov {
		if c.Kind == string(assert.KindSometimes) && c.Passes == 0 {
			out = append(out, n)
		}
	}
	for _, d := range declared {
		isPath := d.Kind == assert.KindSometimes || d.Kind == assert.KindReachable
		if isPath && !d.NotExpected && cov[d.Name].Passes == 0 && !slices.Contains(out, d.Name) {
			out = append(out, d.Name)
		}
	}
	slices.Sort(out)
	return out
}

// neverChecked lists the declared Always properties that no seed checked. An
// Always that is recorded only on failure (Declared.FailureOnly) is left out.
func neverChecked(cov map[string]Coverage, declared []assert.Declared) []string {
	var out []string
	for _, d := range declared {
		if d.Kind == assert.KindAlways && !d.FailureOnly && cov[d.Name].Checks == 0 {
			out = append(out, d.Name)
		}
	}
	slices.Sort(out)
	return out
}

// siteRows lists the buggify sites by name, and the declared ones that never
// fired. A site that no run asked leaves no entry, so the catalog is used to
// find it.
func siteRows(sites map[string]SiteTotal, declared []string) ([]SiteRow, []string) {
	names := make([]string, 0, len(sites))
	for n := range sites {
		names = append(names, n)
	}
	slices.Sort(names)
	rows := []SiteRow{}
	for _, n := range names {
		t := sites[n]
		rows = append(rows, SiteRow{Name: n, Reached: t.Reached, On: t.On, Calls: t.Calls, Fired: t.Fired})
	}
	never := NeverFired(sites, declared)
	if never == nil {
		never = []string{}
	}
	return rows, never
}

// coverageCurve returns one point for each seed number that saw an assertion
// for the first time, and the last such seed number.
func coverageCurve(first uint64, seen [][]string) ([]CurvePoint, uint64) {
	have := map[string]bool{}
	curve := []CurvePoint{}
	var last uint64
	for i, names := range seen {
		grew := false
		for _, n := range names {
			if !have[n] {
				have[n] = true
				grew = true
			}
		}
		if grew {
			last = first + uint64(i)
			curve = append(curve, CurvePoint{Seed: last, Total: len(have)})
		}
	}
	return curve, last
}

// quietSeeds is how many seed numbers at the end of seen added no new assertion.
func quietSeeds(first uint64, seen [][]string) int {
	_, last := coverageCurve(first, seen)
	if last == 0 {
		return len(seen)
	}
	return int(first + uint64(len(seen)) - 1 - last)
}

// HasFailed is the verdict of the campaign: a seed broke a property, a seed
// did not repeat, or (in a campaign of MinSeedsForCoverage seeds or more) a
// path was never reached or an Always was never checked.
func (r Report) HasFailed() bool {
	if r.Failed > 0 || len(r.Nondeterminism) > 0 {
		return true
	}
	bigEnough := r.minGapSeeds > 0 && r.Seeds >= r.minGapSeeds
	return bigEnough && (len(r.NeverReached) > 0 || len(r.NeverChecked) > 0)
}

// WriteJSON writes the report as indented JSON.
func (r Report) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// WriteText prints the report. Failed seeds come first.
func (r Report) WriteText(w io.Writer) {
	p := func(format string, a ...any) { _, _ = fmt.Fprintf(w, format+"\n", a...) }
	p("")
	p("report: %d seeds, %d runs, %d passed, %d failed", r.Seeds, r.Runs, r.Passed, r.Failed)
	for _, f := range r.FailedRuns {
		p("  FAILED %s seed %d: %s", f.Scenario, f.Seed, f.Replay)
	}
	if len(r.Nondeterminism) > 0 {
		p("nondeterminism (two runs of one seed gave different results):")
		for _, f := range r.Nondeterminism {
			p("  %s seed %d: %s", f.Scenario, f.Seed, f.Replay)
		}
	}
	p("assertions (%d):", len(r.Assertions))
	for _, a := range r.Assertions {
		p("  %-10s %-60s checks %9d  passes %9d", a.Kind, a.Name, a.Checks, a.Passes)
	}
	writeList(p, "never reached", r.NeverReached, true)
	writeList(p, "never checked", r.NeverChecked, false)
	p("buggify sites (%d):", len(r.Sites))
	for _, s := range r.Sites {
		p("  %-30s reached %6d runs  on %6d runs  calls %9d  fired %8d", s.Name, s.Reached, s.On, s.Calls, s.Fired)
	}
	writeList(p, "never fired", r.NeverFired, true)
	p("coverage curve (seed: assertions seen so far):")
	for _, c := range r.Curve {
		p("  seed %6d: %d", c.Seed, c.Total)
	}
	if r.LastGrowth == 0 {
		p("no assertion was seen")
	} else {
		p("stopped growing at seed %d (last seed run: %d)", r.LastGrowth, r.LastSeed)
	}
	if u := r.UntilStable; u != nil && u.HitLimit {
		p("WARNING: the limit of %d seeds came first. No %d seeds in a row without a new assertion was reached, so the campaign is not known to be stable.", u.MaxSeeds, u.StableAfter)
	}
}

// writeList prints "title: none" or the title and the names. A list that is
// not always printed (always false) is left out when it is empty.
func writeList(p func(string, ...any), title string, names []string, always bool) {
	switch {
	case len(names) > 0:
		p("%s:\n  %s", title, strings.Join(names, "\n  "))
	case always:
		p("%s: none", title)
	}
}

// writeFailure prints the details of one failed seed: one line for each
// failure, the replay command, and the last step lines of the run.
func writeFailure(w io.Writer, scenario string, r Result) {
	if r.Text != "" {
		_, _ = fmt.Fprintln(w, r.Text)
		return
	}
	for _, f := range r.Failures {
		more := ""
		if f.Repeats > 0 {
			more = fmt.Sprintf(" (and %d more times)", f.Repeats)
		}
		_, _ = fmt.Fprintf(w, "FAIL %s seed %d: %s (%s) step=%d at=%v: %s%s\n", scenario, r.Seed,
			f.Kind, f.Property, f.Step, time.Duration(f.AtMs)*time.Millisecond, f.Detail, more)
	}
	_, _ = fmt.Fprintf(w, "  repro: %s\n", r.Reproduce)
	if len(r.Tail) > 0 {
		_, _ = fmt.Fprintln(w, "  last steps:")
		for _, l := range r.Tail {
			_, _ = fmt.Fprintf(w, "    %s\n", l)
		}
	}
}

// coverageText is the short coverage verdict that follows the campaign. It
// names the declared paths that no seed reached and the Always properties
// that no seed checked.
func (r Report) coverageText() string {
	var b strings.Builder
	if len(r.NeverReached) == 0 {
		fmt.Fprintf(&b, "coverage: all %d expected paths were reached", r.expectedPaths)
	} else {
		b.WriteString("coverage: not reached by any seed:\n  " + strings.Join(r.NeverReached, "\n  "))
	}
	if len(r.NeverChecked) > 0 {
		b.WriteString("\ncoverage: not checked by any seed:\n  " + strings.Join(r.NeverChecked, "\n  "))
	}
	return b.String()
}
