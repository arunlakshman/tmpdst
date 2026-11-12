package main

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/verify/simulator"
	dstcampaign "github.com/manifesterio/mds/dst/campaign"
)

// campaignOut is the outcome of many seeds of one scenario.
type campaignOut struct {
	scenario string
	seeds    int
	failed   []simulator.Result // runs that broke a property, in seed order
	sum      dstcampaign.Summary
	seen     [][]string // seen[i]: assertions that seed first+i reached
}

// runSeeds runs seeds first, first+1, ... of one scenario on several
// goroutines (dst/campaign). Each run is single threaded inside.
func runSeeds(cfg simulator.Config, first uint64, seeds int) (campaignOut, error) {
	if _, ok := simulator.ScenarioByName(cfg.Scenario); !ok {
		return campaignOut{}, fmt.Errorf("unknown scenario %q (have %v)", cfg.Scenario, simulator.ScenarioNames())
	}
	results := make([]simulator.Result, max(seeds, 0))
	sum := dstcampaign.Run(first, seeds, 0, func(seed uint64) dstcampaign.Result {
		c := cfg
		c.Seed = seed
		r, _ := simulator.Run(c)
		results[int(seed-first)] = r
		cov := make(map[string]dstcampaign.Coverage, len(r.Coverage))
		for k, v := range r.Coverage {
			cov[k] = dstcampaign.Coverage{Kind: string(v.Kind), Checks: v.Checks, Passes: v.Passes}
		}
		return dstcampaign.Result{
			Hash: r.Hash, OK: r.OK(), Reproduce: r.Reproduce(), Coverage: cov, Sites: r.Sites,
			Counters: map[string]uint64{"steps": uint64(r.Steps), "slots": uint64(r.Stats.LogSlots), "acked": uint64(r.Stats.Acked)},
		}
	})
	out := campaignOut{scenario: cfg.Scenario, seeds: seeds, sum: sum}
	for _, r := range results {
		var names []string
		for k, v := range r.Coverage {
			if v.Checks > 0 {
				names = append(names, k)
			}
		}
		out.seen = append(out.seen, names)
		if !r.OK() {
			out.failed = append(out.failed, r)
		}
	}
	return out, nil
}

// line is the one-line summary of the campaign.
func (c campaignOut) line() string {
	status := "ok"
	if len(c.failed) > 0 {
		status = fmt.Sprintf("FAILED %d of %d seeds", len(c.failed), c.seeds)
	}
	return fmt.Sprintf("%-15s %4d seeds  %9d steps  %7d slots  %6d acked writes  %5.1fs  hash %08x  %s",
		c.scenario, c.seeds, c.sum.Counters["steps"], c.sum.Counters["slots"], c.sum.Counters["acked"],
		c.sum.Elapsed.Seconds(), uint32(c.sum.Hash), status)
}

// coverageText lists the replica and rebalance paths that no run reached. The
// replica says which paths matter with Reachable and Sometimes, and the
// simulator does it for the rebalance machine; a path that no seed reaches is
// not tested, so it is worth a look.
func coverageText(total dstcampaign.Summary) string {
	missing := dstcampaign.Missing(total.Coverage, expectedReachable)
	if len(missing) == 0 {
		return fmt.Sprintf("coverage: all %d expected paths were reached", len(expectedReachable))
	}
	return "coverage: not reached by any seed:\n  " + strings.Join(missing, "\n  ")
}

// expectedReachable are the Reachable paths of the replica, and of the
// rebalance machine as its host in the simulator sees them, that a long
// campaign should reach. A short campaign may miss some.
var expectedReachable = []string{
	"logcp-sim: epoch claimed",
	"logcp-sim: standby took over",
	"logcp-sim: writer fenced, now read-only",
	"logcp-sim: replica restored from a checkpoint",
	"logcp-sim: log trimmed below a checkpoint",
	"logcp-sim: lost slot retried at a later slot",
	"logcp-sim: Put not answered, same slot repeated",
	"logcp-sim: replica hit a trimmed slot",
	"logcp-sim: checkpoint read had a bad CRC, read again",
	"logcp-sim: replica crashed",
	"logcp-sim: replica stalled",
	"rebalance-sim: a join round completes",
	"rebalance-sim: a round completes with many members",
	"rebalance-sim: a member gets its assignment",
	"rebalance-sim: an offset commit is acked",
	"rebalance-sim: a leave removes a member",
	"rebalance-sim: a heartbeat is told to join again",
	"rebalance-sim: a silent member is removed",
	"rebalance-sim: a member that never synced is removed",
	"rebalance-sim: a takeover loads a group with members",
	"rebalance-sim: a failed write ends the term",
	"rebalance-sim: the write timer ends the term",
	"rebalance-sim: groups are read again from a fresh tail",
	"rebalance-sim: a refused load read is sent again",
	"rebalance-sim: the role is lost while calls wait",
	"rebalance-sim: a call is abandoned",
	"rebalance-sim: a late reply to an abandoned call is dropped",
}
