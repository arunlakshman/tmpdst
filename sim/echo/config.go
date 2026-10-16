package echo

import (
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

// Config is the echo campaign preset: 3 brokers, 1 workload of 4 clients, all
// three engines, attrition and a swarm over the fault families.
func Config(seeds []uint64) sim.Config { return config(seeds, NewBroker) }

func config(seeds []uint64, brokers core.Factory) sim.Config {
	ph := phases()
	slow := func(fmin, fmax, smin, smax int64, p float64) sim.TwoSpeed {
		return sim.TwoSpeed{FastMin: ms(fmin), FastMax: ms(fmax), SlowMin: ms(smin), SlowMax: ms(smax), SlowProb: p}
	}
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
			func() sim.Invariant { return &faultCoverage{} },
		},
		Engines: func(sc sim.SeedConfig) []sim.Engine {
			cp := cpengine.New(sc.ControlPlane)
			cp.CreateTopic(Topic, 1)
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
			Outage:        sim.Outage{Prob: 0.002, Dur: [2]core.Duration{ms(200), 2 * core.Second}},
			CallerTimeout: ms(500),
		},
		Network: sim.NetworkFaults{
			Latency:       sim.TwoSpeed{FastMin: 200 * core.Microsecond, FastMax: 2 * core.Millisecond, SlowMin: ms(20), SlowMax: ms(100), SlowProb: 0.03},
			PartialIOMax:  64,
			PartitionProb: 0.002, PartitionDur: [2]core.Duration{ms(300), 3 * core.Second},
			ClogProb: 0.003, ClogDur: [2]core.Duration{ms(100), ms(500)},
			RandomCloseProb: 0.002, BlackHoleProb: 0.002,
			PairLatencyMax: ms(5), ClockSkewMax: ms(100),
		},
		Attrition: sim.Attrition{MaxDead: 1, CrashProb: 0.0005, CrashWeight: 2, GracefulWeight: 1,
			RecoveryDelay: [2]core.Duration{ms(200), ms(1500)}},
		Cost:             sim.CostParams{PerEvent: 50 * core.Microsecond, PauseProb: 0.001, Pause: slow(1, 5, 20, 100, 0.1)},
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
	if r.Chance(0.3) {
		o.ApplyLatency.SlowProb, cp.ApplyLatency.SlowProb = 0.3, 0.3
	}
	if r.Chance(0.3) {
		n.Latency.SlowProb = 0.2
	}
}
