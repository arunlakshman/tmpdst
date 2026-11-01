package kafka

import (
	"manifester/internal/broker"
	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/verify/simulator/fakeservices/fakecontrolplane"
	"manifester/internal/verify/simulator/fakeservices/fakelogservice"
	"manifester/internal/verify/simulator/fakeservices/fakelogstore"
	"manifester/internal/verify/simulator/fakeservices/fakenetwork"
	"manifester/internal/verify/simulator/fakeservices/fakeobjectstore"
	"manifester/internal/verify/simulator/internal/campaign"
)

// brokerConfig is the sim-friendly broker: a short flush window so commits
// are frequent, and a small buffer so backpressure engages. The segment size is
// pinned to the pre-decision-35 default (4 MiB) and the batch cap to 8; a seed's
// swarm overrides the segment knobs (see swarmBroker).
func brokerConfig(bug broker.InjectedBug) broker.Config {
	return broker.Config{BatchWindow: 5 * core.Millisecond, MaxSegmentBytes: 4 << 20, MaxSegmentBatches: 8,
		MaxBufferedBytes: 3 << 10, ResumeBufferedBytes: 1 << 10, InjectedBug: bug}
}

// Config is the Kafka campaign preset: 3 brokers, 1 workload of 3 at-least-once
// producers and 2 consumers over 2 topics x 3 partitions, all three engines,
// attrition and a swarm over the fault families.
func Config(seeds []uint64) simulator.Config { return config(seeds, broker.InjectedBugNone) }

func config(seeds []uint64, bug broker.InjectedBug) simulator.Config {
	return configWith(seeds, bug, DefaultOpts())
}

// ConfigLog is Config against CP replica machines plus logstore (CP-DSM-8,
// docs/sim-log-engine.md) instead of the speccheck-backed fakecontrolplane: the same
// workload, brokers, oracle and fault families. Brokers still emit ControlPlaneCall; the
// fakelogservice bridge offers them to cp-* processes. The broker keeps its default
// segment cap (4096): the log has no commit item limit.
func ConfigLog(seeds []uint64) simulator.Config { return configLog(seeds, broker.InjectedBugNone, 0) }

// configLog builds the log preset with a broker mutant and a log engine mutant.
func configLog(seeds []uint64, bug broker.InjectedBug, logBug uint8) simulator.Config {
	opts := DefaultOpts()
	// Stacked bridge + logstore retries need more produce sends than speccheck.
	opts.MaxAttempts = 16
	return configLogWith(seeds, bug, logBug, opts)
}

func configLogWith(seeds []uint64, bug broker.InjectedBug, logBug uint8, opts Opts) simulator.Config {
	cfg := configWith(seeds, bug, opts)
	cfg.ControlPlane.Log = fakelogservice.DefaultParams()
	cfg.ControlPlane.Log.Bug = logBug
	// A replica stall or crash is rolled on each role tick of the active node.
	// Raise the defaults so a short campaign (20 seeds) still reaches both.
	cfg.ControlPlane.Log.StallProb, cfg.ControlPlane.Log.CrashProb = 0.004, 0.002
	cfg.ControlPlane.MaxCommitItems = 0
	lp := cfg.ControlPlane.Log
	cfg.LogStore = simulator.LogStoreFaults{
		Enabled:        true,
		ApplyLatency:   cfg.ControlPlane.ApplyLatency,
		RespondLatency: cfg.ControlPlane.RespondLatency,
		Unavailable503: lp.PutFail,
		Timeout:        0,
		LostResponse:   lp.PutLost,
		Outage:         simulator.ServiceOutage{},
		CallerTimeout:  cfg.ControlPlane.CallerTimeout,
		Bug:            logBug,
		WindowSlots:    lp.WindowSlots,
		TrimStride:     lp.TrimStride,
	}
	cfg.Processes = append(cfg.Processes, simulator.Process{
		Name: "cp", Count: 2,
		FactoryFor: func(sc simulator.SeedConfig) core.MachineFactory { return cpFactory(sc, opts) },
	})
	// Bridge deadline covers several logstore Put attempts. Per-append timeout
	// must clear ApplyLatency SlowMax (200ms) so a slow Put is not a false
	// MaybeApplied; keep it below the bridge so PutFail can still retry inside
	// one CommitSegment.
	cfg.ControlPlane.CallerTimeout = 1500 * core.Millisecond
	cfg.LogStore.CallerTimeout = 500 * core.Millisecond
	ph := cfg.Phases
	ph.Settling = 25 * core.Second
	ph.Settle = 20 * core.Second
	cfg.Phases = ph
	cfg.ExpectReachable = append(cfg.ExpectReachable, logReachables()...)
	return cfg
}

// logReachables are the log engine's paths a campaign must reach.
func logReachables() []core.PropertyID {
	return []core.PropertyID{
		"logcp-sim: lost slot retried at a later slot",
		"logcp-sim: hole filled by a no-op",
		"logcp-sim: Put not answered, same slot repeated",
		"logcp-sim: replayed key answered from the window",
		"logcp-sim: log trimmed below a checkpoint",
		"logcp-sim: standby took over",
		"logcp-sim: failback, the standby stepped down",
		"logcp-sim: replica stalled",
		"logcp-sim: replica crashed",
		"logcp-sim: replica restored from a checkpoint",
	}
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
			return []simulator.SafetyRule{newSegmentCaps(sc.Params.(broker.Config), sc.ControlPlane.MaxCommitItems, opts.tps()), &segmentTiming{}}
		},
		Workloads: []simulator.WorkloadFactory{func(env core.Environment, self core.NodeInstance, bs []core.NodeInstance) simulator.Workload {
			return NewWorkload(env, self, bs, opts)
		}},
		Invariants: []func() simulator.SafetyRule{
			func() simulator.SafetyRule { return newOracle(opts) },
			func() simulator.SafetyRule { return &progressCheck{} },
			func() simulator.SafetyRule { return campaign.FaultCoverageCheck("kafka:") },
		},
		Engines: func(sc simulator.SeedConfig) []simulator.FakeService {
			es := []simulator.FakeService{fakeobjectstore.New(sc.ObjectStore), controlPlane(sc, opts), fakenetwork.New(sc.Network)}
			if sc.LogStore.Enabled {
				ls := fakelogstore.New(sc.LogStore)
				for _, t := range opts.Topics {
					ls.CreateTopic(t, opts.Partitions)
				}
				es = append(es, ls)
			}
			return es
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
func controlPlane(sc simulator.SeedConfig, opts Opts) simulator.FakeService {
	var cp topicMaker = fakecontrolplane.New(sc.ControlPlane)
	if sc.ControlPlane.Log.Enabled {
		cp = fakelogservice.New(sc.ControlPlane)
	}
	for _, t := range opts.Topics {
		cp.CreateTopic(t, opts.Partitions)
	}
	return cp
}

// swarmLog picks this seed's log control plane knobs. It draws after every
// other swarm decision, and only for the log preset, so the speccheck-backed
// preset keeps its draws.
func swarmLog(r simulator.CountedRand, c *simulator.SeedConfig) {
	l := &c.ControlPlane.Log
	scale := func(p *float64) {
		menu := [...]float64{0, 0.5, 1, 2, 4}
		*p = min(*p*menu[r.Intn(len(menu))], 0.4)
	}
	scale(&l.PutFail)
	scale(&l.PutLost)
	scale(&l.ReadFail)
	scale(&l.StallProb)
	scale(&l.CrashProb)
	l.K = []int{2, 4, 8}[r.Intn(3)]
	l.GroupMax = []int{1, 2, 4, 16}[r.Intn(4)]
	l.HoleAfter = campaign.Millis([]int64{20, 60, 200}[r.Intn(3)])
	l.TakeoverAfter = campaign.Millis([]int64{600, 900, 1500}[r.Intn(3)])
	if r.Chance(0.4) {
		l.CkptEvery, l.KeepSlots, l.TrimStride = []int{8, 16, 24}[r.Intn(3)], []int{16, 32}[r.Intn(2)], 4
	}
	c.LogStore.Unavailable503 = l.PutFail
	c.LogStore.LostResponse = l.PutLost
	c.LogStore.Bug = l.Bug
	c.LogStore.WindowSlots = l.WindowSlots
	c.LogStore.TrimStride = l.TrimStride
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
	if c.ControlPlane.Log.Enabled {
		swarmLog(r, c)
	}
}

func swarmBroker(r simulator.CountedRand, c *simulator.SeedConfig) {
	bc := c.Params.(broker.Config)
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) core.Duration { return campaign.Millis(int64(pick(menu...))) }
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
