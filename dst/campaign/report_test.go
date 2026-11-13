package campaign

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
)

// These tests are the tests of controlplane/cmd/cpsim/report_test.go, moved to
// the shared report. The hand-kept lists of the control plane are replaced by a
// catalog that the test gives.

var testCatalog = catalog{
	asserts: []assert.Declared{
		{Name: "t: epoch claimed", Kind: assert.KindReachable},
		{Name: "t: standby took over", Kind: assert.KindReachable},
		{Name: "t: replica crashed", Kind: assert.KindReachable},
		{Name: "t: write dropped", Kind: assert.KindSometimes},
		{Name: "t: one writer", Kind: assert.KindAlways},
		{Name: "t: never in code", Kind: assert.KindUnreachable},
		{Name: "t: maybe never", Kind: assert.KindAlwaysOrUnreachable},
	},
	sites: []string{"t: site a", "t: site b", "t: site c"},
}

func TestCoverageCurve(t *testing.T) {
	seen := [][]string{{"a"}, {"a"}, {"a", "b"}, {"b"}, {"b"}}
	curve, last := coverageCurve(10, seen)
	if last != 12 || len(curve) != 2 || curve[0] != (CurvePoint{10, 1}) || curve[1] != (CurvePoint{12, 2}) {
		t.Fatalf("curve %v last %d", curve, last)
	}
	if q := quietSeeds(10, seen); q != 2 {
		t.Fatalf("quiet seeds %d, want 2", q)
	}
}

// TestReportNamesLostPath: when a path that the campaign used to reach is no
// longer reached, the report lists it and the coverage line says so.
func TestReportNamesLostPath(t *testing.T) {
	cov := map[string]Coverage{}
	for _, d := range testCatalog.asserts {
		cov[d.Name] = Coverage{Kind: string(d.Kind), Checks: 1, Passes: 1}
	}
	lost := "t: standby took over"
	delete(cov, lost)
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, total: Summary{Coverage: cov}, runs: 1}, testCatalog)
	if len(rep.NeverReached) != 1 || rep.NeverReached[0] != lost {
		t.Fatalf("never reached %v, want [%s]", rep.NeverReached, lost)
	}
	if text := rep.coverageText(); !strings.Contains(text, lost) {
		t.Fatalf("coverage text does not name %q: %s", lost, text)
	}
}

func TestCoverageTextWhenAllPathsWereReached(t *testing.T) {
	cov := map[string]Coverage{}
	for _, d := range testCatalog.asserts {
		cov[d.Name] = Coverage{Kind: string(d.Kind), Checks: 1, Passes: 1}
	}
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, total: Summary{Coverage: cov}, runs: 1}, testCatalog)
	// Four declared paths: three Reachable and one Sometimes.
	if got, want := rep.coverageText(), "coverage: all 4 expected paths were reached"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestReportListsSitesThatNeverFired: a site that was asked but never fired,
// and a site that no run asked at all, are both listed. A site that fired is not.
func TestReportListsSitesThatNeverFired(t *testing.T) {
	names := testCatalog.sites
	sites := map[string]SiteTotal{
		names[0]: {Reached: 5, On: 2, Calls: 40, Fired: 3},
		names[1]: {Reached: 5, On: 0, Calls: 40},
	}
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, total: Summary{Sites: sites}, runs: 1}, testCatalog)
	want := append([]string{names[1]}, names[2:]...)
	slices.Sort(want)
	if !slices.Equal(rep.NeverFired, want) {
		t.Fatalf("never fired %v, want %v", rep.NeverFired, want)
	}
	if len(rep.Sites) != 2 || rep.Sites[0].Fired+rep.Sites[1].Fired != 3 {
		t.Fatalf("sites %+v", rep.Sites)
	}
	var b bytes.Buffer
	rep.WriteText(&b)
	if !strings.Contains(b.String(), "never fired:\n  "+want[0]) {
		t.Fatalf("text does not list the sites:\n%s", b.String())
	}
}

func TestReportFailedSeedsFirst(t *testing.T) {
	failed := []FailedRun{{Scenario: "chaos", Seed: 2, Replay: "go run ./cmd/cpsim -scenario chaos -seed 2"}}
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}, {"x"}}, failed: failed, runs: 2}, catalog{})
	var b bytes.Buffer
	rep.WriteText(&b)
	lines := strings.Split(b.String(), "\n")
	if !strings.Contains(lines[1], "1 failed") || !strings.Contains(lines[2], "FAILED chaos seed 2") {
		t.Fatalf("failed seeds are not first:\n%s", b.String())
	}
}

// TestNeverReachedAndNeverCheckedComeFromTheCatalog: a declared path that left
// no coverage entry is named, a Sometimes with zero passes is named, and an
// Always with zero checks is named as never checked. The kinds that may stay
// silent (Unreachable, AlwaysOrUnreachable) are not.
func TestNeverReachedAndNeverCheckedComeFromTheCatalog(t *testing.T) {
	cov := map[string]Coverage{
		"t: epoch claimed":     {Kind: "reachable", Checks: 3, Passes: 3},
		"t: write dropped":     {Kind: "sometimes", Checks: 9, Passes: 0},
		"t: undeclared maybe":  {Kind: "sometimes", Checks: 4, Passes: 0},
		"t: undeclared always": {Kind: "always", Checks: 4, Passes: 4},
	}
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, total: Summary{Coverage: cov}, runs: 1}, testCatalog)
	wantReached := []string{"t: replica crashed", "t: standby took over", "t: undeclared maybe", "t: write dropped"}
	if !slices.Equal(rep.NeverReached, wantReached) {
		t.Fatalf("never reached %v, want %v", rep.NeverReached, wantReached)
	}
	if want := []string{"t: one writer"}; !slices.Equal(rep.NeverChecked, want) {
		t.Fatalf("never checked %v, want %v", rep.NeverChecked, want)
	}
	if rep.expectedPaths != 4 {
		t.Fatalf("expected paths %d, want 4", rep.expectedPaths)
	}
	var b bytes.Buffer
	rep.WriteText(&b)
	if !strings.Contains(b.String(), "never checked:\n  t: one writer\n") {
		t.Fatalf("text does not list the Always property:\n%s", b.String())
	}
}

// TestReportTextFormat pins the text of a small report. It is the format of
// cpsim: change it only with a reason.
func TestReportTextFormat(t *testing.T) {
	cov := map[string]Coverage{
		"t: one writer":  {Kind: "always", Checks: 7, Passes: 7},
		"t: epoch claim": {Kind: "reachable", Checks: 2, Passes: 2},
	}
	sites := map[string]SiteTotal{"t: site a": {Reached: 3, On: 1, Calls: 30, Fired: 2}}
	data := campaignData{
		first: 5, seen: [][]string{{"t: one writer"}, {"t: one writer", "t: epoch claim"}},
		total:  Summary{Coverage: cov, Sites: sites},
		failed: []FailedRun{{Scenario: "chaos", Seed: 6, Replay: "replay-6"}},
		runs:   2,
	}
	rep := buildReport(data, catalog{sites: []string{"t: site a", "t: site z"}})
	var b bytes.Buffer
	rep.WriteText(&b)
	want := `
report: 2 seeds, 2 runs, 1 passed, 1 failed
  FAILED chaos seed 6: replay-6
assertions (2):
  reachable  t: epoch claim                                               checks         2  passes         2
  always     t: one writer                                                checks         7  passes         7
never reached: none
buggify sites (1):
  t: site a                      reached      3 runs  on      1 runs  calls        30  fired        2
never fired:
  t: site z
coverage curve (seed: assertions seen so far):
  seed      5: 1
  seed      6: 2
stopped growing at seed 6 (last seed run: 6)
`
	if b.String() != want {
		t.Fatalf("text changed:\n%s\nwant:\n%s", b.String(), want)
	}
}

func TestReportJSON(t *testing.T) {
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, runs: 1}, catalog{})
	var b bytes.Buffer
	if err := rep.WriteJSON(&b); err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(b.Bytes(), &back); err != nil {
		t.Fatalf("not json: %v\n%s", err, b.String())
	}
	for _, key := range []string{"seeds", "runs", "passed", "failed", "failed_runs", "assertions",
		"never_reached", "sites", "never_fired", "curve", "last_growth", "last_seed"} {
		if _, ok := back[key]; !ok {
			t.Errorf("JSON has no key %q:\n%s", key, b.String())
		}
	}
	for _, key := range []string{"never_checked", "nondeterminism", "until_stable"} {
		if _, ok := back[key]; ok {
			t.Errorf("JSON has the key %q, but it should be left out when empty", key)
		}
	}
}

func TestHasFailed(t *testing.T) {
	gap := Report{Seeds: 100, NeverReached: []string{"x"}}
	cases := []struct {
		name string
		rep  Report
		min  int
		want bool
	}{
		{"clean", Report{Seeds: 100}, 100, false},
		{"failed run", Report{Failed: 1}, 0, true},
		{"nondeterminism", Report{Nondeterminism: []FailedRun{{Seed: 3}}}, 0, true},
		{"gap, no minimum", gap, 0, false},
		{"gap, small campaign", gap, 101, false},
		{"gap, big campaign", gap, 100, true},
		{"never checked, big campaign", Report{Seeds: 100, NeverChecked: []string{"x"}}, 100, true},
	}
	for _, c := range cases {
		c.rep.minGapSeeds = c.min
		if got := c.rep.HasFailed(); got != c.want {
			t.Errorf("%s: HasFailed = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestWriteFailure(t *testing.T) {
	r := Result{
		Seed: 9, Reproduce: "go run ./cmd/x -scenario s -seed 9",
		Failures: []Failure{{Kind: "violation", Property: "p", Detail: "d", Step: 12, AtMs: 1500, Repeats: 2}},
		Tail:     []string{"line one", "line two"},
	}
	var b bytes.Buffer
	writeFailure(&b, "s", r)
	want := "FAIL s seed 9: violation (p) step=12 at=1.5s: d (and 2 more times)\n" +
		"  repro: go run ./cmd/x -scenario s -seed 9\n" +
		"  last steps:\n    line one\n    line two\n"
	if b.String() != want {
		t.Fatalf("got:\n%s\nwant:\n%s", b.String(), want)
	}
}

// TestCatalogFlags: an Always that is recorded only on failure is not "never
// checked", and a Reachable that is not expected is not "never reached" and is
// not counted as expected.
func TestCatalogFlags(t *testing.T) {
	cat := catalog{asserts: []assert.Declared{
		{Name: "t: reached", Kind: assert.KindReachable},
		{Name: "t: optional", Kind: assert.KindReachable, NotExpected: true},
		{Name: "t: violation", Kind: assert.KindAlways, FailureOnly: true},
		{Name: "t: checked always", Kind: assert.KindAlways},
	}}
	cov := map[string]Coverage{
		"t: reached":        {Kind: "reachable", Checks: 1, Passes: 1},
		"t: checked always": {Kind: "always", Checks: 2, Passes: 2},
	}
	rep := buildReport(campaignData{first: 1, seen: [][]string{{"x"}}, total: Summary{Coverage: cov}, runs: 1}, cat)
	if len(rep.NeverReached) != 0 || len(rep.NeverChecked) != 0 {
		t.Fatalf("never reached %v, never checked %v", rep.NeverReached, rep.NeverChecked)
	}
	if rep.expectedPaths != 1 {
		t.Fatalf("expected paths %d, want 1", rep.expectedPaths)
	}
	if text := rep.coverageText(); text != "coverage: all 1 expected paths were reached" {
		t.Fatalf("coverage text %q", text)
	}
}
