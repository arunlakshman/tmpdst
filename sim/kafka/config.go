package kafka

import (
	"manifester/broker"
	"manifester/core"
	"manifester/sim"
	"manifester/sim/engines/cpengine"
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

// Config is the Kafka campaign preset: 3 brokers, 1 workload of 3 idempotent
// producers and 2 consumers over 2 topics x 3 partitions, all three engines,
// attrition and a swarm over the fault families.
func Config(seeds []uint64) sim.Config { return config(seeds, broker.BugNone) }

func config(seeds []uint64, bug broker.Bug) sim.Config { return configWith(seeds, bug, DefaultOpts()) }

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
			return []sim.Invariant{newSegmentCaps(sc.Params.(broker.Config)), &segmentTiming{}}
		},
		Workloads: []sim.WorkloadFactory{func(env core.Env, self core.NodeRef, bs []core.NodeRef) sim.Workload {
			return NewWorkload(env, self, bs, opts)
		}},
		Invariants: []func() sim.Invariant{
			func() sim.Invariant { return newOracle(opts) },
			func() sim.Invariant { return &trippedNeverAcks{dead: map[core.NodeRef]bool{}} },
			func() sim.Invariant { return &liveness{} },
			func() sim.Invariant { return &faultCoverage{} },
		},
		Engines: func(sc sim.SeedConfig) []sim.Engine {
			cp := cpengine.New(sc.ControlPlane)
			for _, t := range opts.Topics {
				cp.CreateTopic(t, opts.Partitions)
			}
			return []sim.Engine{objstore.New(sc.ObjectStore), cp, netsim.New(sc.Network)}
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
	if r.Chance(fenceModeProb) {
		fenceMode(cp)
	}
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
}

// fenceModeProb is the share of seeds that run the fencing scenario.
const fenceModeProb = 0.3

// fenceMode makes a Register that the broker gave up on land after its retry
// has already succeeded, so the control plane holds a newer epoch than the
// broker and its next CommitSegment is fenced (the fail-stop path, which must
// end in Trip). Three knobs together: a caller timeout shorter than the slow
// apply latency (the broker retries before the first Register lands), frequent
// control-plane timeouts (the first Register is a timed-out one, applied or
// not by a coin at landing) and frequent slow applies. Outside Chaos no
// timeout is drawn, so Settling is unaffected.
func fenceMode(cp *sim.ControlPlaneFaults) {
	cp.CallerTimeout = ms(30)
	cp.Timeout = 0.4
	cp.ApplyLatency.SlowProb = 0.5
}

// The 3 producers send at most 2 batches each and wait for the ack, so a
// segment rarely holds more than 6: the batch cap binds only when it is 2-4.
//
// swarmBroker picks this seed's segment tuning (decision 17) from a profile:
//
//	tiny (0.25)       a few KiB: the size rule binds
//	accumulate (0.25) long windows, big byte cap, small batch cap: the batch cap binds
//	defaults (0.15)   the production defaults: 16 MiB, 200 ms, 32 batches, 24 partitions
//	wide (0.1)        2-3 partitions, nothing else binds: the partition cap
//	mixed (0.25)      every knob from its own menu
//
// Buffer caps are swarmed too. Half the seeds tighten the control plane's commit
// limit to exactly what the seed's caps allow (2 + partitions + 2 batches), so a
// broker that ignores a cap is refused; the preset's own limit is 100.
func swarmBroker(r sim.CountedRand, c *sim.SeedConfig) {
	bc := c.Params.(broker.Config)
	pick := func(menu ...int) int { return menu[r.Intn(len(menu))] }
	window := func(menu ...int) core.Duration { return ms(int64(pick(menu...))) }
	switch x := r.Float64(); {
	case x < 0.25: // tiny
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 6<<10, 8<<10), window(20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(16, 32), pick(12, 24)
	case x < 0.5: // accumulate
		bc.MaxSegmentBytes, bc.BatchWindow = pick(256<<10, 4<<20, 16<<20), window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 3, 4), pick(12, 24)
	case x < 0.65: // defaults
		d := broker.DefaultConfig()
		bc.MaxSegmentBytes, bc.BatchWindow = d.MaxSegmentBytes, d.BatchWindow
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = d.MaxSegmentBatches, d.MaxSegmentPartitions
	case x < 0.75: // wide
		bc.MaxSegmentBytes, bc.BatchWindow = 16<<20, window(50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = 32, pick(2, 3)
	default: // mixed
		bc.MaxSegmentBytes, bc.BatchWindow = pick(4<<10, 16<<10, 64<<10, 256<<10, 4<<20, 16<<20), window(5, 20, 50, 200)
		bc.MaxSegmentBatches, bc.MaxSegmentPartitions = pick(2, 4, 8, 32), pick(3, 6, 12, 24)
	}
	bc.MaxBufferedBytes = pick(3<<10, 3<<10, 16<<10, 64<<10, 1<<20)
	bc.ResumeBufferedBytes = bc.MaxBufferedBytes / 3
	if r.Chance(0.5) {
		c.ControlPlane.MaxCommitItems = 2 + bc.MaxSegmentPartitions + 2*bc.MaxSegmentBatches
	}
	c.Params = bc
}
