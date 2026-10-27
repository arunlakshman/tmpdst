package echo

import (
	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/cpengine"
	"manifester/sim/engines/netsim"
	"manifester/sim/engines/objstore"
	"manifester/sim/internal/campaign"
)

// Config is the echo campaign preset: 3 brokers, 1 workload of 4 clients, all
// three engines, attrition and a swarm over the fault families.
func Config(seeds []uint64) sim.Config { return config(seeds, NewBroker) }

func config(seeds []uint64, brokers core.Factory) sim.Config {
	p := campaign.New()
	p.Network.PartialIOMax = 64
	return sim.Config{
		Seeds:     seeds,
		Processes: []sim.Process{{Name: "broker", Count: 3, Factory: brokers}},
		Workloads: []sim.WorkloadFactory{func(env core.Env, self core.NodeRef, bs []core.NodeRef) sim.Workload {
			return NewWorkload(env, self, bs, Opts{Clients: 4})
		}},
		Invariants: []func() sim.Invariant{
			func() sim.Invariant { return &ackedIsCommitted{} },
			func() sim.Invariant { return &readYourAck{acked: map[int64][]byte{}} },
			func() sim.Invariant { return &liveness{} },
			func() sim.Invariant { return campaign.FaultCoverage("echo:") },
		},
		Engines: func(sc sim.SeedConfig) []sim.Engine {
			cp := cpengine.New(sc.ControlPlane)
			cp.CreateTopic(Topic, 1)
			return []sim.Engine{objstore.New(sc.ObjectStore), cp, netsim.New(sc.Network)}
		},
		ObjectStore:      p.ObjectStore,
		ControlPlane:     p.ControlPlane,
		Network:          p.Network,
		Attrition:        p.Attrition,
		Cost:             p.Cost,
		ExpectReachable:  expectReachable(),
		Swarm:            swarm,
		Phases:           p.Phases,
		QuiesceEvery:     p.QuiesceEvery,
		CheckDeterminism: p.CheckDeterminism,
	}
}

func swarm(_ uint64, r sim.CountedRand, c *sim.SeedConfig) {
	campaign.Swarm(r, c, nil)
}
