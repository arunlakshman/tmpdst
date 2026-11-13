// Command kvsim runs the two examples (kv and kvchaos) through the standard
// campaign tool of dst/campaign. It shows how small a product simulator command
// is: build a registry, give a Config, call campaign.Main.
//
//	go run ./examples/kvsim -seeds 200 -report      # both scenarios, 200 seeds each
//	go run ./examples/kvsim -scenario kvchaos -seed 7 # one seed, with its step log
//	go run ./examples/kvsim -until-stable -scenario kv
//	MDS_CAMPAIGN=5s go run ./examples/kvsim          # seeds until 5 seconds have passed
package main

import (
	"fmt"
	"os"

	"github.com/manifesterio/mds/dst/campaign"
	"github.com/manifesterio/mds/dst/examples/kv"
	"github.com/manifesterio/mds/dst/examples/kvchaos"
	"github.com/manifesterio/mds/dst/kernel"
)

// tailLines is how many step lines a failed run keeps.
const tailLines = 20

func main() {
	os.Exit(campaign.Main(config(), newRegistry(), os.Args[1:], os.Stdout, os.Stderr))
}

// config is what the standard main needs. Every 10th seed runs twice to check
// that it repeats.
func config() campaign.Config {
	return campaign.Config{
		Tool:      "kvsim",
		EnvPrefix: "MDS",
		Defaults:  campaign.Options{Seeds: 100, CheckEvery: 10},
	}
}

// newRegistry registers the scenarios.
func newRegistry() *campaign.Registry {
	reg := &campaign.Registry{}
	reg.Add(campaign.Scenario{
		Name:   "kv",
		Detail: "a client and a key-value store, no faults",
		Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
			r := kv.RunWith(kernel.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst, Tail: tailLines})
			return campaign.FromKernel(r, reproduce("kv", seed))
		},
	})
	reg.Add(campaign.Scenario{
		Name:   "kvchaos",
		Detail: "the same store with dropped writes and a node that crashes",
		Run: func(seed uint64, o campaign.RunOptions) campaign.Result {
			r := kvchaos.RunWith(kernel.Config{Seed: seed, Log: o.Log, HaltOnFirst: o.HaltOnFirst, Tail: tailLines})
			return campaign.FromKernel(r.Result, reproduce("kvchaos", seed))
		},
	})
	return reg
}

func reproduce(scenario string, seed uint64) string {
	return fmt.Sprintf("go run ./examples/kvsim -scenario %s -seed %d", scenario, seed)
}
