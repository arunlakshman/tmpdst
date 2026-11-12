package main

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/verify/simulator"
	dstcampaign "github.com/manifesterio/mds/dst/campaign"
)

// TestCampaignReachesReplicaPaths: over a campaign of four scenarios the
// replica and the rebalance machine reach every path that the coverage line
// expects.
func TestCampaignReachesReplicaPaths(t *testing.T) {
	var total dstcampaign.Summary
	for _, sc := range []string{"crash-restart", "leader-fencing", "chaos", "log-faults"} {
		c, err := runSeeds(simulator.Config{Scenario: sc}, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(c.failed) > 0 {
			t.Fatal(simulator.Report(c.failed[0]))
		}
		total.Merge(c.sum)
	}
	if text := coverageText(total); text != fmt.Sprintf("coverage: all %d expected paths were reached", len(expectedReachable)) {
		t.Fatal(text)
	}
}
