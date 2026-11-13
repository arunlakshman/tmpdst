package kafka

import (
	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/preset"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// brokerConfig is the sim-friendly broker: a short flush window so commits
// are frequent, and a small buffer so backpressure engages. The segment size is
// pinned to the pre-decision-35 default (4 MiB) and the batch cap to 8; a seed's
// swarm overrides the segment knobs (see swarmBroker).
func brokerConfig(bug broker.InjectedBug) broker.Config {
	return broker.Config{BatchWindow: 5 * machine.Millisecond, MaxSegmentBytes: 4 << 20, MaxSegmentBatches: 8,
		MaxBufferedBytes: 3 << 10, ResumeBufferedBytes: 1 << 10, InjectedBug: bug}
}

// Settings are the knobs of one seed after the swarm: the knobs of the cluster
// and the configuration of the broker.
type Settings struct {
	cluster.Settings
	Broker broker.Config
}

// Config says how to run one seed of the Kafka scenario: 3 brokers, 1 workload of
// 3 at-least-once producers and 2 consumers over 2 topics x 3 partitions, all
// three fake services, attrition and a swarm over the fault families. A zero
// Config is the scenario as the campaign runs it.
type Config struct {
	Seed uint64
	// Log keeps every step line. HaltOnFirst stops at the first failure.
	// VerifyHashes recomputes every cached state hash at each step.
	Log, HaltOnFirst, VerifyHashes bool
	// Bug plants a bug in the broker (a test proves that the rules catch it).
	Bug broker.InjectedBug
	// Opts is the workload. A zero Opts is DefaultOpts.
	Opts Opts
	// Tune, when it is set, changes the knobs of the seed after the swarm.
	Tune func(s *Settings)
	// ExtraRules, when it is set, gives more rules. They run after the rules of
	// the scenario. A test uses them to count what a campaign did.
	ExtraRules func(c *cluster.Cluster, h *History) []kernel.Rule
}

// Run builds and runs one seed of the Kafka scenario.
func Run(cfg Config) kernel.Result {
	opts := cfg.Opts
	if opts.Producers+opts.Consumers == 0 {
		opts = DefaultOpts()
	}
	p := preset.New()
	p.Settings.Network.PartialIOMax = 1024
	p.Settings.ControlPlane.MaxCommitItems = fakeservices.DynamoMaxCommitItems
	set := Settings{Settings: p.Settings, Broker: brokerConfig(cfg.Bug)}
	swarm(rng.New(cfg.Seed).Derive("swarm"), &set)
	if cfg.Tune != nil {
		cfg.Tune(&set)
	}
	c := cluster.New(cluster.Plan{
		Seed: cfg.Seed, Settings: set.Settings, Phases: p.Phases, QuiesceEvery: p.QuiesceEvery,
		Log: cfg.Log, HaltOnFirst: cfg.HaltOnFirst, VerifyHashes: cfg.VerifyHashes,
	})
	for _, t := range opts.Topics {
		c.ControlPlane.CreateTopic(t, opts.Partitions)
	}
	c.AddBrokers(3, broker.New(set.Broker))
	w := c.World
	h := history.New[Op, Res](w.Now, func(msg string) { w.Fail(kernel.FailProtocol, "history", msg) })
	c.AddWorkload(func(env core.Environment, self machine.NodeInstance, brokers []machine.NodeID) cluster.Workload {
		return NewWorkload(env, self, brokers, h, opts)
	})
	w.AddRule(newOracle(opts, h, c.Objects, c.ControlPlane))
	w.AddRule(&progressCheck{hist: h})
	w.AddRule(preset.FaultCoverageCheck(preset.KafkaFaultCoverage))
	w.AddRule(newSegmentCaps(set.Broker, set.ControlPlane.MaxCommitItems, opts.topicPartitions(), c.ControlPlane))
	w.AddRule(&segmentTiming{})
	if cfg.ExtraRules != nil {
		for _, r := range cfg.ExtraRules(c, h) {
			w.AddRule(r)
		}
	}
	return c.Run()
}

// swarm draws the knobs of one seed from the swarm stream of the seed.
func swarm(r *rng.Rand, s *Settings) {
	preset.Swarm(r, &s.Settings, func() {
		switch x := r.Intn(20); {
		case x < 2:
			s.Network.PartialIOMax = 48
		case x < 8:
			s.Network.PartialIOMax = 256
		}
	})
	swarmBroker(r, s)
}

func swarmBroker(r *rng.Rand, s *Settings) {
	bc := s.Broker
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) machine.Duration { return machine.Duration(pick(menu...)) * machine.Millisecond }
	d := broker.DefaultConfig()
	switch x := r.Float64(); {
	case x < 0.2:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 6<<10, 8<<10), window(20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(16, 32, 256), pick(12, 24, 96)
	case x < 0.4:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(256<<10, 4<<20, 16<<20), window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 3, 4), pick(12, 24, 96)
	case x < 0.55:
		bc.MaxSegmentBytes, bc.BatchWindow = d.MaxSegmentBytes, d.BatchWindow
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = d.MaxSegmentBatches, d.MaxSegmentPartitions
	case x < 0.75:
		bc.MaxSegmentBytes, bc.BatchWindow = 16<<20, window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = 256, pick(2, 3, 4, 5, 6)
	default:
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 16<<10, 64<<10, 256<<10, 4<<20, 16<<20), window(5, 20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 4, 8, 32, 256), pick(3, 6, 12, 24, 96)
	}
	bc.MaxBufferedBytes = pick(3<<10, 3<<10, 16<<10, 64<<10, 1<<20)
	bc.ResumeBufferedBytes = bc.MaxBufferedBytes / 3
	if r.Chance(0.5) {
		s.ControlPlane.MaxCommitItems = min(1+bc.MaxSegmentPartitions, fakeservices.DynamoMaxCommitItems)
	}
	s.Broker = bc
}
