package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/campaign"
	"github.com/manifesterio/mds/dst/examples/v2/kv"
	"github.com/manifesterio/mds/dst/examples/v2/kvchaos"
)

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := campaign.Main(config(), newRegistry(), args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// TestCampaignEndToEnd runs both scenarios through the standard tool, with the
// determinism re-run on, and reads the report.
func TestCampaignEndToEnd(t *testing.T) {
	code, out, errOut := run(t, "-seeds", "100", "-report", "-check-every", "5")
	if code != 0 {
		t.Fatalf("code %d\nstdout:\n%s\nstderr:\n%s", code, out, errOut)
	}
	for _, want := range []string{
		"kv ", "kvchaos ", "100 seeds", "ok",
		"coverage: all 3 expected paths were reached", // from the catalog of the program
		"report: 100 seeds, 200 runs, 200 passed, 0 failed",
		"kvchaos v2: write dropped", "kv v2: read matches",
		"never reached: none", "never fired: none",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output has no %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "never checked") {
		t.Errorf("every Always property of the examples is checked:\n%s", out)
	}
}

func TestJSONReport(t *testing.T) {
	code, out, errOut := run(t, "-seeds", "20", "-json", "-scenario", "kvchaos")
	if code != 0 {
		t.Fatalf("code %d: %s", code, errOut)
	}
	var rep campaign.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("not json: %v\n%s", err, out)
	}
	if rep.Seeds != 20 || rep.Runs != 20 || rep.Failed != 0 || len(rep.Sites) != 1 || rep.Sites[0].Name != "kvchaos v2: node crash" {
		t.Fatalf("report %+v", rep)
	}
}

func TestUntilStable(t *testing.T) {
	code, out, errOut := run(t, "-until-stable", "-scenario", "kv", "-stable-after", "20", "-max-seeds", "500")
	if code != 0 {
		t.Fatalf("code %d: %s", code, errOut)
	}
	if strings.Contains(out, "WARNING") || !strings.Contains(out, "stopped growing at seed") {
		t.Fatalf("the kv example should become stable in one batch:\n%s", out)
	}
}

// TestReplayHashIsTheCampaignHash: the log and HaltOnFirst of a replay do not
// change the hash of a passing seed.
func TestReplayHashIsTheCampaignHash(t *testing.T) {
	code, out, errOut := run(t, "-scenario", "kv", "-seed", "7")
	if code != 0 {
		t.Fatalf("code %d: %s", code, errOut)
	}
	if want := fmt.Sprintf("hash=%016x", kv.Run(7).Hash); !strings.Contains(out, want) {
		t.Fatalf("replay output has no %q:\n%s", want, out)
	}
	if !strings.Contains(out, "begin") && !strings.Contains(out, "putRequest") {
		t.Fatalf("replay output has no step lines:\n%s", out)
	}

	code, out, _ = run(t, "-scenario", "kvchaos", "-seed", "3")
	if want := fmt.Sprintf("hash=%016x", kvchaos.Run(3).Hash); code != 0 || !strings.Contains(out, want) {
		t.Fatalf("code %d, want %q in:\n%s", code, want, out)
	}
}

func TestSeedNeedsOneScenario(t *testing.T) {
	code, _, errOut := run(t, "-seed", "3")
	if code != 2 || !strings.Contains(errOut, "-seed needs one -scenario") {
		t.Fatalf("code %d stderr %q", code, errOut)
	}
}
