package kafka

import (
	"manifester/broker"
	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/cpengine"
	"manifester/sim/engines/logengine"
	"manifester/sim/engines/logstore"
	"manifester/sim/engines/netsim"
	"manifester/sim/engines/objstore"
)

// Phases of the preset campaign.
func phases() sim.Phases {
	return sim.Phases{Chaos: 30 * core.Second, Settling: 10 * core.Second, Settle: 5 * core.Second,
		MaxQuietLatency: 3 * core.Second}
}

func ms(n int64) core.Duration { return core.Duration(n) * core.Millisecond }

// brokerConfig is the sim-friendly broker: a short flush window so commits
// are frequent, and a small buffer so backpressure engages. The segment size is
// pinned to the pre-decision-35 default (4 MiB) and the batch cap to 8; a seed's
// swarm overrides the segment knobs (see swarmBroker).
func brokerConfig(bug broker.Bug) broker.Config {
	return broker.Config{BatchWindow: 5 * core.Millisecond, MaxSegmentBytes: 4 << 20, MaxSegmentBatches: 8,
		MaxBufferedBytes: 3 << 10, ResumeBufferedBytes: 1 << 10, Bug: bug}
}

// Config is the Kafka campaign preset: 3 brokers, 1 workload of 3 at-least-once
// producers and 2 consumers over 2 topics x 3 partitions, all three engines,
// attrition and a swarm over the fault families.
func Config(seeds []uint64) sim.Config { return config(seeds, broker.BugNone) }

func config(seeds []uint64, bug broker.Bug) sim.Config { return configWith(seeds, bug, DefaultOpts()) }

// ConfigLog is Config against CP replica machines plus logstore (CP-DSM-8,
// docs/sim-log-engine.md) instead of the veritas-backed cpengine: the same
// workload, brokers, oracle and fault families. Brokers still emit CPCall; the
// logengine bridge offers them to cp-* processes. The broker keeps its default
// segment cap (4096): the log has no commit item limit.
func ConfigLog(seeds []uint64) sim.Config { return configLog(seeds, broker.BugNone, 0) }

// configLog builds the log preset with a broker mutant and a log engine mutant.
func configLog(seeds []uint64, bug broker.Bug, logBug uint8) sim.Config {
	opts := DefaultOpts()
	// Stacked bridge + logstore retries need more produce sends than veritas.
	opts.MaxAttempts = 16
	return configLogWith(seeds, bug, logBug, opts)
}

func configLogWith(seeds []uint64, bug broker.Bug, logBug uint8, opts Opts) sim.Config {
	cfg := configWith(seeds, bug, opts)
	cfg.ControlPlane.Log = logengine.DefaultParams()
	cfg.ControlPlane.Log.Bug = logBug
	cfg.ControlPlane.MaxCommitItems = 0
	lp := cfg.ControlPlane.Log
	cfg.LogStore = sim.LogStoreFaults{
		Enabled:        true,
		ApplyLatency:   cfg.ControlPlane.ApplyLatency,
		RespondLatency: cfg.ControlPlane.RespondLatency,
		Unavailable503: lp.PutFail,
		Timeout:        0,
		LostResponse:   lp.PutLost,
		Outage:         sim.Outage{},
		CallerTimeout:  cfg.ControlPlane.CallerTimeout,
		Bug:            logBug,
		WindowSlots:    lp.WindowSlots,
		TrimStride:     lp.TrimStride,
	}
	cfg.Processes = append(cfg.Processes, sim.Process{
		Name: "cp", Count: 2, NoAttrition: true,
		FactoryFor: func(sc sim.SeedConfig) core.Factory { return cpFactory(sc, opts) },
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
func logReachables() []core.Prop {
	return []core.Prop{
		"logcp-sim: lost slot retried at a later slot",
		"logcp-sim: hole filled by a no-op",
		"logcp-sim: Put not answered, same slot repeated",
		"logcp-sim: replayed key answered from the window",
		"logcp-sim: log trimmed below a checkpoint",
	}
}

func configWith(seeds []uint64, bug broker.Bug, opts Opts) sim.Config {
	ph := phases()
	slow := func(fmin, fmax, smin, smax int64, p float64) sim.TwoSpeed {
		return sim.TwoSpeed{FastMin: ms(fmin), FastMax: ms(fmax), SlowMin: ms(smin), SlowMax: ms(smax), SlowProb: p}
	}
	return sim.Config{
		Seeds:  seeds,
		Params: brokerConfig(bug),
		Processes: []sim.Process{{Name: "broker", Count: 3, FactoryFor: func(sc sim.SeedConfig) core.Factory {
			return broker.New(sc.Params.(broker.Config))
		}}},
		InvariantsFor: func(sc sim.SeedConfig) []sim.Invariant {
			return []sim.Invariant{newSegmentCaps(sc.Params.(broker.Config), sc.ControlPlane.MaxCommitItems, opts.tps()), &segmentTiming{}}
		},
		Workloads: []sim.WorkloadFactory{func(env core.Env, self core.NodeRef, bs []core.NodeRef) sim.Workload {
			return NewWorkload(env, self, bs, opts)
		}},
		Invariants: []func() sim.Invariant{
			func() sim.Invariant { return newOracle(opts) },
			func() sim.Invariant { return &liveness{} },
			func() sim.Invariant { return &faultCoverage{} },
		},
		Engines: func(sc sim.SeedConfig) []sim.Engine {
			es := []sim.Engine{objstore.New(sc.ObjectStore), controlPlane(sc, opts), netsim.New(sc.Network)}
			if sc.LogStore.Enabled {
				ls := logstore.New(sc.LogStore)
				for _, t := range opts.Topics {
					ls.CreateTopic(t, opts.Partitions)
				}
				es = append(es, ls)
			}
			return es
		},
		ObjectStore: sim.ObjectStoreFaults{
			ApplyLatency:   slow(1, 5, 50, 200, 0.05),
			RespondLatency: slow(1, 3, 20, 100, 0.05),
			Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.02, GetCorruptProb: 0.03,
			Outage:        sim.Outage{Prob: 0.002, Dur: [2]core.Duration{ms(200), 2 * core.Second}},
			CallerTimeout: ms(500),
		},
		ControlPlane: sim.ControlPlaneFaults{
			ApplyLatency:   slow(1, 5, 50, 200, 0.05),
			RespondLatency: slow(1, 3, 20, 100, 0.05),
			Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.03,
			MaxCommitItems: sim.DynamoMaxCommitItems,
			Outage:         sim.Outage{Prob: 0.002, Dur: [2]core.Duration{ms(200), 2 * core.Second}},
			CallerTimeout:  ms(500),
		},
		Network: sim.NetworkFaults{
			Latency:       sim.TwoSpeed{FastMin: 200 * core.Microsecond, FastMax: 2 * core.Millisecond, SlowMin: ms(20), SlowMax: ms(100), SlowProb: 0.03},
			PartialIOMax:  1024,
			PartitionProb: 0.002, PartitionDur: [2]core.Duration{ms(300), 3 * core.Second},
			ClogProb: 0.003, ClogDur: [2]core.Duration{ms(100), ms(500)},
			RandomCloseProb: 0.002, BlackHoleProb: 0.002,
			PairLatencyMax: ms(5), ClockSkewMax: ms(100),
		},
		Attrition: sim.Attrition{MaxDead: 1, CrashProb: 0.0005, CrashWeight: 2, GracefulWeight: 1,
			RecoveryDelay: [2]core.Duration{ms(200), ms(1500)}},
		Cost:             sim.CostParams{PerEvent: 50 * core.Microsecond, PauseProb: 0.001, Pause: slow(1, 5, 20, 100, 0.1)},
		ExpectReachable:  expectReachable(),
		Swarm:            swarm,
		Phases:           ph,
		QuiesceEvery:     core.Second,
		CheckDeterminism: 10,
	}
}

// topicMaker is a control plane engine that topics can be created in before the run.
type topicMaker interface {
	sim.Engine
	CreateTopic(name string, partitions int32) core.ErrCode
}

// controlPlane builds this seed's control plane engine with the workload's topics.
func controlPlane(sc sim.SeedConfig, opts Opts) sim.Engine {
	var cp topicMaker = cpengine.New(sc.ControlPlane)
	if sc.ControlPlane.Log.Enabled {
		cp = logengine.New(sc.ControlPlane)
	}
	for _, t := range opts.Topics {
		cp.CreateTopic(t, opts.Partitions)
	}
	return cp
}

// swarmLog picks this seed's log control plane knobs. It draws after every
// other swarm decision, and only for the log preset, so the veritas-backed
// preset keeps its draws.
func swarmLog(r sim.CountedRand, c *sim.SeedConfig) {
	l := &c.ControlPlane.Log
	scale := func(p *float64) {
		menu := [...]float64{0, 0.5, 1, 2, 4}
		*p = min(*p*menu[r.Intn(len(menu))], 0.4)
	}
	scale(&l.PutFail)
	scale(&l.PutLost)
	scale(&l.ReadFail)
	l.StallProb, l.CrashProb = 0, 0 // in-process stall/crash follow-up
	// Floor K above 1: one in-flight PutLost otherwise stalls every commit
	// behind it for the whole bridge CallerTimeout.
	l.K = []int{2, 4, 8}[r.Intn(3)]
	l.GroupMax = []int{1, 2, 4, 16}[r.Intn(4)]
	l.HoleAfter = ms([]int64{20, 60, 200}[r.Intn(3)])
	// Floor takeover above heartbeat so a healthy primary is not displaced.
	l.TakeoverAfter = ms([]int64{600, 900, 1500}[r.Intn(3)])
	if r.Chance(0.4) { // checkpoint often; keep enough slots for PutFail lag
		l.CkptEvery, l.KeepSlots, l.TrimStride = []int{8, 16, 24}[r.Intn(3)], []int{16, 32}[r.Intn(2)], 4
	}
	c.LogStore.Unavailable503 = l.PutFail
	c.LogStore.LostResponse = l.PutLost
	c.LogStore.Bug = l.Bug
	c.LogStore.WindowSlots = l.WindowSlots
	c.LogStore.TrimStride = l.TrimStride
}

// swarm switches each fault family fully off with probability 0.4 and scales
// the rest from a menu (lesson 9). Draws come only from r (Derive("swarm")).
func swarm(_ uint64, r sim.CountedRand, c *sim.SeedConfig) {
	menu := [...]float64{0.5, 1, 2, 4}
	knob := func(p *float64) {
		if r.Chance(0.4) {
			*p = 0
			return
		}
		*p = min(*p*menu[r.Intn(len(menu))], 0.4)
	}
	o, cp, n := &c.ObjectStore, &c.ControlPlane, &c.Network
	knob(&o.Unavailable503)
	knob(&o.Timeout)
	knob(&o.LostResponse)
	knob(&o.GetCorruptProb)
	knob(&o.Outage.Prob)
	knob(&cp.Unavailable503)
	knob(&cp.Timeout)
	knob(&cp.LostResponse)
	knob(&cp.Outage.Prob)
	knob(&n.PartitionProb)
	knob(&n.ClogProb)
	knob(&n.RandomCloseProb)
	knob(&n.BlackHoleProb)
	knob(&c.Attrition.CrashProb)
	switch x := r.Intn(20); { // short transfers: tiny chunks are costly (every chunk is a Step)
	case x < 2:
		n.PartialIOMax = 48
	case x < 8:
		n.PartialIOMax = 256
	}
	if r.Chance(0.3) {
		o.ApplyLatency.SlowProb, cp.ApplyLatency.SlowProb = 0.3, 0.3
	}
	if r.Chance(0.3) {
		n.Latency.SlowProb = 0.2
	}
	swarmBroker(r, c) // last, so the fault knobs above keep their draws
	if c.ControlPlane.Log.Enabled {
		swarmLog(r, c)
	}
}

// The 3 producers send at most 2 batches each and wait for the ack, so a
// segment rarely holds more than 6 batches or partitions: the batch cap binds
// only when it is 2-4, and the partition cap only when it is 2-6.
//
// swarmBroker picks this seed's segment tuning (decision 17) from a profile:
//
//	tiny (0.2)        a few KiB: the size rule binds
//	accumulate (0.2)  long windows, big byte cap, small batch cap: the batch cap binds
//	defaults (0.15)   the production defaults: 16 MiB, 200 ms, 256 batches, 96 partitions
//	wide (0.2)        long windows, nothing else binds, a partition cap of 2-6: the partition cap
//	mixed (0.25)      every knob from its own menu
//
// Buffer caps are swarmed too. Half the seeds tighten the control plane's commit
// limit to exactly what the seed's partition cap allows (1 + partitions: the
// segment item and one run item per partition, decisions 39, 41, 44; batches do
// not count), so a broker that ignores the cap is refused; the preset's own
// limit is 100.
func swarmBroker(r sim.CountedRand, c *sim.SeedConfig) {
	bc := c.Params.(broker.Config)
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) core.Duration { return ms(int64(pick(menu...))) }
	d := broker.DefaultConfig()
	switch x := r.Float64(); {
	case x < 0.2: // tiny
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 6<<10, 8<<10), window(20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(16, 32, 256), pick(12, 24, 96)
	case x < 0.4: // accumulate
		bc.MaxSegmentBytes, bc.BatchWindow = pick(256<<10, 4<<20, 16<<20), window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 3, 4), pick(12, 24, 96)
	case x < 0.55: // defaults
		bc.MaxSegmentBytes, bc.BatchWindow = d.MaxSegmentBytes, d.BatchWindow
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = d.MaxSegmentBatches, d.MaxSegmentPartitions
	case x < 0.75: // wide
		bc.MaxSegmentBytes, bc.BatchWindow = 16<<20, window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = 256, pick(2, 3, 4, 5, 6)
	default: // mixed
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 16<<10, 64<<10, 256<<10, 4<<20, 16<<20), window(5, 20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 4, 8, 32, 256), pick(3, 6, 12, 24, 96)
	}
	bc.MaxBufferedBytes = pick(3<<10, 3<<10, 16<<10, 64<<10, 1<<20)
	bc.ResumeBufferedBytes = bc.MaxBufferedBytes / 3
	if r.Chance(0.5) {
		c.ControlPlane.MaxCommitItems = min(1+bc.MaxSegmentPartitions, sim.DynamoMaxCommitItems)
	}
	c.Params = bc
}
