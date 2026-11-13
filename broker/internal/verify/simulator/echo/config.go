package echo

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/preset"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// Config says how to run one seed of the echo scenario: 3 brokers, 1 workload of
// 4 clients, all three fake services, attrition and a swarm over the fault
// families. A zero Config is the scenario as the campaign runs it.
type Config struct {
	Seed uint64
	// Log keeps every step line. HaltOnFirst stops at the first failure.
	// VerifyHashes recomputes every cached state hash at each step.
	Log, HaltOnFirst, VerifyHashes bool
	// Brokers builds the broker machines. Nil: NewBroker. A test gives a broker
	// with a planted bug.
	Brokers core.MachineFactory
	// ExtraRules, when it is set, gives more rules. They run after the rules of
	// the scenario. A test uses them to count what a campaign did.
	ExtraRules func(c *cluster.Cluster, h *History) []kernel.Rule
}

// Run builds and runs one seed of the echo scenario.
func Run(cfg Config) kernel.Result {
	brokers := cfg.Brokers
	if brokers == nil {
		brokers = NewBroker
	}
	p := preset.New()
	p.Settings.Network.PartialIOMax = 64
	preset.Swarm(rng.New(cfg.Seed).Derive("swarm"), &p.Settings, nil)
	c := cluster.New(cluster.Plan{
		Seed: cfg.Seed, Settings: p.Settings, Phases: p.Phases, QuiesceEvery: p.QuiesceEvery,
		Log: cfg.Log, HaltOnFirst: cfg.HaltOnFirst, VerifyHashes: cfg.VerifyHashes,
	})
	c.ControlPlane.CreateTopic(Topic, 1)
	c.AddBrokers(3, brokers)
	w := c.World
	h := history.New[Op, string](w.Now, func(msg string) { w.Fail(kernel.FailProtocol, "history", msg) })
	c.AddWorkload(func(env core.Environment, self machine.NodeInstance, bs []machine.NodeID) cluster.Workload {
		return NewWorkload(env, self, bs, h, Opts{Clients: 4})
	})
	w.AddRule(&ackedMeansCommitted{objs: c.Objects, cp: c.ControlPlane})
	w.AddRule(&readWhatWasAcked{hist: h, acked: map[int64][]byte{}})
	w.AddRule(&liveness{hist: h})
	w.AddRule(preset.FaultCoverageCheck(preset.EchoFaultCoverage))
	if cfg.ExtraRules != nil {
		for _, r := range cfg.ExtraRules(c, h) {
			w.AddRule(r)
		}
	}
	return c.Run()
}
