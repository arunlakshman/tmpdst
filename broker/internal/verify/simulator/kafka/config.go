package kafka

import (
	"github.com/manifesterio/mds/broker/internal/broker"
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakenetwork"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakeobjectstore"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/campaign"
	"github.com/manifesterio/mds/dst/machine"
)

// brokerConfig is the sim-friendly broker: a short flush window so commits
// are frequent, and a small buffer so backpressure engages. The segment size is
// pinned to the pre-decision-35 default (4 MiB) and the batch cap to 8; a seed's
// swarm overrides the segment knobs (see swarmBroker).
func brokerConfig(bug broker.InjectedBug) broker.Config {
	return broker.Config{BatchWindow: 5 * machine.Millisecond, MaxSegmentBytes: 4 << 20, MaxSegmentBatches: 8,
		MaxBufferedBytes: 3 << 10, ResumeBufferedBytes: 1 << 10, InjectedBug: bug}
}

// Config is the Kafka campaign preset: 3 brokers, 1 workload of 3 at-least-once
// producers and 2 consumers over 2 topics x 3 partitions, all three engines,
// attrition and a swarm over the fault families.
func Config(seeds []uint64) simulator.Config { return config(seeds, broker.InjectedBugNone) }

func config(seeds []uint64, bug broker.InjectedBug) simulator.Config {
	return configWith(seeds, bug, DefaultOpts())
}

func configWith(seeds []uint64, bug broker.InjectedBug, opts Opts) simulator.Config {
	p := campaign.New()
	p.Network.PartialIOMax = 1024
	p.ControlPlane.MaxCommitItems = simulator.DynamoMaxCommitItems
	return simulator.Config{
		Seeds:  seeds,
		Params: brokerConfig(bug),
		Processes: []simulator.Process{{Name: "broker", Count: 3, FactoryFor: func(sc simulator.SeedConfig) core.MachineFactory {
			return broker.New(sc.Params.(broker.Config))
		}}},
		InvariantsFor: func(sc simulator.SeedConfig) []simulator.SafetyRule {
			return []simulator.SafetyRule{newSegmentCaps(sc.Params.(broker.Config), sc.ControlPlane.MaxCommitItems, opts.topicPartitions()), &segmentTiming{}}
		},
		Workloads: []simulator.WorkloadFactory{func(env core.Environment, self machine.NodeInstance, bs []machine.NodeInstance) simulator.Workload {
			return NewWorkload(env, self, bs, opts)
		}},
		Invariants: []func() simulator.SafetyRule{
			func() simulator.SafetyRule { return newOracle(opts) },
			func() simulator.SafetyRule { return &progressCheck{} },
			func() simulator.SafetyRule { return campaign.FaultCoverageCheck(campaign.KafkaFaultCoverage) },
		},
		Engines: func(sc simulator.SeedConfig) []simulator.FakeService {
			return []simulator.FakeService{fakeobjectstore.New(sc.ObjectStore), controlPlane(sc, opts), fakenetwork.New(sc.Network)}
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

// topicMaker is a control plane engine that topics can be created in before the run.
type topicMaker interface {
	simulator.FakeService
	CreateTopic(name string, partitions int32) core.ErrCode
}

// controlPlane builds this seed's control plane engine with the workload's topics.
// Log-backed kafka-log (replica + fakelogstore) moved to mds-controlplane; see docs/cp-extraction.md.
func controlPlane(sc simulator.SeedConfig, opts Opts) simulator.FakeService {
	cp := topicMaker(fakecontrolplane.New(sc.ControlPlane))
	for _, t := range opts.Topics {
		cp.CreateTopic(t, opts.Partitions)
	}
	return cp
}

func swarm(_ uint64, r simulator.CountedRand, c *simulator.SeedConfig) {
	campaign.Swarm(r, c, func() {
		switch x := r.Intn(20); {
		case x < 2:
			c.Network.PartialIOMax = 48
		case x < 8:
			c.Network.PartialIOMax = 256
		}
	})
	swarmBroker(r, c)
}

func swarmBroker(r simulator.CountedRand, c *simulator.SeedConfig) {
	bc := c.Params.(broker.Config)
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) machine.Duration { return campaign.Millis(int64(pick(menu...))) }
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
		c.ControlPlane.MaxCommitItems = min(1+bc.MaxSegmentPartitions, simulator.DynamoMaxCommitItems)
	}
	c.Params = bc
}
