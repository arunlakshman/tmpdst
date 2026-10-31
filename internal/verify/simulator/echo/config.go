package echo

import (
	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/fakeservices/fakecontrolplane"
	"mds/internal/verify/simulator/fakeservices/fakenetwork"
	"mds/internal/verify/simulator/fakeservices/fakeobjectstore"
	"mds/internal/verify/simulator/internal/campaign"
)

// Config is the echo campaign preset: 3 brokers, 1 workload of 4 clients, all
// three engines, attrition and a swarm over the fault families.
func Config(seeds []uint64) simulator.Config { return config(seeds, NewBroker) }

func config(seeds []uint64, brokers core.MachineFactory) simulator.Config {
	p := campaign.New()
	p.Network.PartialIOMax = 64
	return simulator.Config{
		Seeds:     seeds,
		Processes: []simulator.Process{{Name: "broker", Count: 3, Factory: brokers}},
		Workloads: []simulator.WorkloadFactory{func(env core.Environment, self core.NodeInstance, bs []core.NodeInstance) simulator.Workload {
			return NewWorkload(env, self, bs, Opts{Clients: 4})
		}},
		Invariants: []func() simulator.SafetyRule{
			func() simulator.SafetyRule { return &ackedMeansCommitted{} },
			func() simulator.SafetyRule { return &readWhatWasAcked{acked: map[int64][]byte{}} },
			func() simulator.SafetyRule { return &liveness{} },
			func() simulator.SafetyRule { return campaign.FaultCoverageCheck("echo:") },
		},
		Engines: func(sc simulator.SeedConfig) []simulator.FakeService {
			cp := fakecontrolplane.New(sc.ControlPlane)
			cp.CreateTopic(Topic, 1)
			return []simulator.FakeService{fakeobjectstore.New(sc.ObjectStore), cp, fakenetwork.New(sc.Network)}
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

func swarm(_ uint64, r simulator.CountedRand, c *simulator.SeedConfig) {
	campaign.Swarm(r, c, nil)
}
