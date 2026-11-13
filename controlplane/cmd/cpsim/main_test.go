package main

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	dstcampaign "github.com/manifesterio/mds/dst/campaign"
)

// TestCampaignReachesReplicaPaths: over a campaign of four scenarios the replica
// and the rebalance machine reach every path that the catalog expects.
func TestCampaignReachesReplicaPaths(t *testing.T) {
	var p product
	p.nodes, p.steps = 3, 4000
	rep, err := dstcampaign.RunCampaign(registry(&p), dstcampaign.Options{
		Scenarios: []string{"crash-restart", "leader-fencing", "chaos", "log-faults"}, First: 1, Seeds: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed > 0 {
		t.Fatalf("%d runs failed: %+v", rep.Failed, rep.FailedRuns)
	}
	if len(rep.NeverReached) > 0 {
		t.Fatalf("paths not reached by any seed: %v", rep.NeverReached)
	}
	if len(rep.NeverChecked) > 0 || len(rep.NeverFired) > 0 {
		t.Fatalf("never checked %v, never fired %v", rep.NeverChecked, rep.NeverFired)
	}
}

// TestSummaryLineAndCoverageLineKeepTheirForm: the lines that make build prints.
func TestSummaryLineAndCoverageLineKeepTheirForm(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-seeds", "3", "-scenario", "basic"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s\n%s", code, errOut.String(), out.String())
	}
	line := regexp.MustCompile(`^basic +3 seeds +\d+ steps +\d+ slots +\d+ acked writes +[0-9.]+s +hash [0-9a-f]{8} +ok\n`)
	if !line.MatchString(out.String()) {
		t.Fatalf("summary line:\n%s", out.String())
	}
	// Three seeds of one scenario may miss a path; the line is either of the two forms.
	if !strings.Contains(out.String(), "coverage: ") {
		t.Fatalf("no coverage line:\n%s", out.String())
	}
}

func TestRunJSONAndUntilStable(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-seeds", "3", "-scenario", "basic", "-json"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s", code, errOut.String())
	}
	var rep dstcampaign.Report
	if err := json.Unmarshal(out.Bytes(), &rep); err != nil {
		t.Fatalf("not json: %v\n%s", err, out.String())
	}
	if rep.Seeds != 3 || rep.Runs != 3 || rep.Passed != 3 || len(rep.Assertions) == 0 {
		t.Fatalf("report %+v", rep)
	}

	out.Reset()
	if code := run([]string{"-until-stable", "-scenario", "basic", "-stable-after", "50", "-max-seeds", "50"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "WARNING: the limit of 50 seeds came first") {
		t.Fatalf("no warning:\n%s", out.String())
	}
}

// TestSeedReplayPrintsTheReport: -seed runs one seed with the step lines, then
// the report of the simulator.
func TestSeedReplayPrintsTheReport(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-scenario", "basic", "-seed", "3"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s\n%s", code, errOut.String(), out.String())
	}
	if !strings.Contains(out.String(), "scenario=basic seed=3") || !strings.HasSuffix(strings.TrimSpace(out.String()), "PASS") {
		t.Fatalf("replay output:\n%s", out.String())
	}
	out.Reset()
	if code := run([]string{"-seed", "3"}, &out, &errOut); code != 2 {
		t.Fatalf("-seed without one scenario: code %d", code)
	}
}

// TestProductFlags: -nodes and -steps reach the simulator, and -bug plants a bug.
func TestProductFlags(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-seeds", "2", "-scenario", "crash-restart", "-nodes", "5", "-steps", "1500"}, &out, &errOut); code != 0 {
		t.Fatalf("code %d: %s\n%s", code, errOut.String(), out.String())
	}
	out.Reset()
	code := run([]string{"-seeds", "300", "-scenario", "chaos", "-bug", "log-overwrite"}, &out, &errOut)
	if code != 1 || !strings.Contains(out.String(), "cpsim: FAILED") {
		t.Fatalf("a planted bug was not found: code %d\n%.2000s", code, out.String())
	}
}

func TestUnknownScenario(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-scenario", "nope"}, &out, &errOut); code != 2 || !strings.Contains(errOut.String(), "unknown scenario") {
		t.Fatalf("code %d: %s", code, errOut.String())
	}
}
