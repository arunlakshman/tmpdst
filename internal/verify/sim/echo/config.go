package echo

import (
	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/cpengine"
	"manifester/internal/verify/sim/engines/netsim"
	"manifester/internal/verify/sim/engines/objstore"
	"manifester/internal/verify/sim/internal/campaign"
)

// Config is the echo campaign preset: 3 brokers, 1 workload of 4 clients, all
// three engines, attrition and a swarm over the fault families.
func Config(seeds []uint64) sim.Config { return config(seeds, NewBroker) }

func config(seeds []uint64, brokers core.MachineFactory) sim.Config {
	p := campaign.New()
	p.Network.PartialIOMax = 64
	return sim.Config{
		Seeds:     seeds,
		Processes: []sim.Process{{Name: "broker", Count: 3, Factory: brokers}},
		Workloads: []sim.WorkloadFactory{func(env core.Environment, self core.NodeInstance, bs []core.NodeInstance) sim.Workload {
			return NewWorkload(env, self, bs, Opts{Clients: 4})
		}},
		Invariants: []func() sim.SafetyRule{
			func() sim.SafetyRule { return &ackedMeansCommitted{} },
			func() sim.SafetyRule { return &readWhatWasAcked{acked: map[int64][]byte{}} },
			func() sim.SafetyRule { return &liveness{} },
			func() sim.SafetyRule { return campaign.FaultCoverageCheck("echo:") },
		},
		Engines: func(sc sim.SeedConfig) []sim.FakeService {
			cp := cpengine.New(sc.ControlPlane)
			cp.CreateTopic(Topic, 1)
			return []sim.FakeService{objstore.New(sc.ObjectStore), cp, netsim.New(sc.Network)}
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
