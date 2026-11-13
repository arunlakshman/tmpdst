// Command mdssimulator runs a scenario campaign (the Kafka broker by default, or
// the echo smoke test) through the deterministic simulator. It is a thin main
// over dst/campaign: the flags, the environment variables (MDS_SEEDS, MDS_SEED,
// MDS_CAMPAIGN), the report and the determinism check come from there. It reads
// flags and the clock and runs seeds on many goroutines, so it is not a
// deterministic package.
//
//	go run ./cmd/mdssimulator -scenario kafka -seeds 300   # a campaign
//	go run ./cmd/mdssimulator -scenario echo -seed 17      # one seed, with a step log
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/manifesterio/mds/broker/internal/verify/simulator/echo"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/kafka"
	"github.com/manifesterio/mds/dst/campaign"
	"github.com/manifesterio/mds/dst/kernel"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	return campaign.Main(config(), registry(), args, stdout, stderr)
}

// checkEvery is the determinism check: every Nth seed runs twice, and the two
// runs must give the same hash, steps and random calls.
const checkEvery = 10

// minimumCoverage is the campaign size from which a path that no seed reached
// counts as a failure (a smaller campaign cannot be expected to reach it).
const minimumCoverage = 100

// config is what mdssimulator gives the standard main. The default scenario is
// kafka.
func config() campaign.Config {
	return campaign.Config{
		Tool:      "mdssimulator",
		EnvPrefix: "MDS",
		Defaults: campaign.Options{
			Scenarios:           []string{"kafka"},
			CheckEvery:          checkEvery,
			MinSeedsForCoverage: minimumCoverage,
		},
	}
}

// registry has the two scenarios: kafka (the broker under a Kafka workload) and
// echo (a small smoke test).
func registry() *campaign.Registry {
	var reg campaign.Registry
	add(&reg, "kafka", "the broker under a Kafka workload", ownsKafka,
		func(seed uint64, o campaign.RunOptions) kernel.Result {
			return kafka.Run(kafka.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst})
		})
	add(&reg, "echo", "the echo smoke test", ownsEcho,
		func(seed uint64, o campaign.RunOptions) kernel.Result {
			return echo.Run(echo.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst})
		})
	return &reg
}

// add registers a scenario that runs one seed with run.
func add(reg *campaign.Registry, name, detail string, owns func(string) bool, run func(uint64, campaign.RunOptions) kernel.Result) {
	reg.Add(campaign.Scenario{
		Name:   name,
		Detail: detail,
		Owns:   owns,
		Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
			return fromKernel(name, run(seed, o))
		},
	})
}

// fromKernel turns the result of one seed into a campaign result. It adds the
// replay command and the two counters (crashes and exits) that the old tool
// printed for each seed.
func fromKernel(scenario string, r kernel.Result) campaign.Result {
	out := campaign.FromKernel(r, fmt.Sprintf("go run ./cmd/mdssimulator -scenario %s -seed %d", scenario, r.Seed))
	out.Counters["crashes"] = uint64(r.Crashes)
	out.Counters["exits"] = uint64(r.Exits)
	return out
}

// The assertion catalog is for the whole program, and the two scenarios are
// linked into it together. Without a rule, a campaign of one scenario would
// call the names of the other "never reached" or "never checked". A name that
// starts with "echo: " belongs to echo; a name that starts with "kafka: " or
// "broker: " belongs to kafka. The other names are the fake services (object
// store, control plane, network), which both scenarios use.
// TestEveryDeclaredNameHasAnOwner keeps these rules honest.
func ownsKafka(name string) bool { return !strings.HasPrefix(name, "echo: ") }

func ownsEcho(name string) bool {
	return !strings.HasPrefix(name, "kafka: ") && !strings.HasPrefix(name, "broker: ")
}
