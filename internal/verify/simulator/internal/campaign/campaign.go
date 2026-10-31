// Package campaign holds the preset schedule, fault defaults, and kernel-fact
// checks shared by the echo and kafka campaigns.
package campaign

import (
	"manifester/internal/core"
	"manifester/internal/verify/simulator"
)

// ScenarioPreset is the fault-family defaults and phase schedule shared by campaigns.
type ScenarioPreset struct {
	ObjectStore      simulator.ObjectStoreFaults
	ControlPlane     simulator.ControlPlaneFaults
	Network          simulator.NetworkFaults
	Attrition        simulator.NodeKills
	Cost             simulator.CostParams
	Phases           simulator.Phases
	QuiesceEvery     core.Duration
	CheckDeterminism uint32
}

// Millis is n as a core.Duration in milliseconds.
func Millis(n int64) core.Duration { return core.Duration(n) * core.Millisecond }

func isSlowCampaign(fmin, fmax, smin, smax int64, p float64) simulator.FastWithRareSlow {
	return simulator.FastWithRareSlow{
		FastMin: Millis(fmin), FastMax: Millis(fmax),
		SlowMin: Millis(smin), SlowMax: Millis(smax), SlowProb: p,
	}
}

// Phases is Chaos 30s, Settling 10s, Settle 5s, MaxQuietLatency 3s.
func Phases() simulator.Phases {
	return simulator.Phases{
		Chaos: 30 * core.Second, Settling: 10 * core.Second, Settle: 5 * core.Second,
		MaxQuietLatency: 3 * core.Second,
	}
}

// New returns a ScenarioPreset. PartialIOMax and MaxCommitItems stay zero.
func New() ScenarioPreset {
	return ScenarioPreset{
		ObjectStore: simulator.ObjectStoreFaults{
			ApplyLatency:   isSlowCampaign(1, 5, 50, 200, 0.05),
			RespondLatency: isSlowCampaign(1, 3, 20, 100, 0.05),
			Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.02, GetCorruptProb: 0.03,
			Outage:        simulator.ServiceOutage{Prob: 0.002, Dur: [2]core.Duration{Millis(200), 2 * core.Second}},
			CallerTimeout: Millis(500),
		},
		ControlPlane: simulator.ControlPlaneFaults{
			ApplyLatency:   isSlowCampaign(1, 5, 50, 200, 0.05),
			RespondLatency: isSlowCampaign(1, 3, 20, 100, 0.05),
			Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.03,
			Outage:        simulator.ServiceOutage{Prob: 0.002, Dur: [2]core.Duration{Millis(200), 2 * core.Second}},
			CallerTimeout: Millis(500),
		},
		Network: simulator.NetworkFaults{
			Latency: simulator.FastWithRareSlow{
				FastMin: 200 * core.Microsecond, FastMax: 2 * core.Millisecond,
				SlowMin: Millis(20), SlowMax: Millis(100), SlowProb: 0.03,
			},
			PartitionProb: 0.002, PartitionDur: [2]core.Duration{Millis(300), 3 * core.Second},
			ClogProb: 0.003, ClogDur: [2]core.Duration{Millis(100), Millis(500)},
			RandomCloseProb: 0.002, BlackHoleProb: 0.002,
			PairLatencyMax: Millis(5), ClockSkewMax: Millis(100),
		},
		Attrition: simulator.NodeKills{MaxDead: 1, CrashProb: 0.0005, CrashWeight: 2, GracefulWeight: 1,
			RecoveryDelay: [2]core.Duration{Millis(200), Millis(1500)}},
		Cost:             simulator.CostParams{PerEvent: 50 * core.Microsecond, PauseProb: 0.001, Pause: isSlowCampaign(1, 5, 20, 100, 0.1)},
		Phases:           Phases(),
		QuiesceEvery:     core.Second,
		CheckDeterminism: 10,
	}
}

// ProgressGraceTime is the Settling offset used by campaign liveness invariants.
const ProgressGraceTime = 6 * core.Second

// PhaseStart reports when the kernel announced phase p, from its sim_phase fact.
func PhaseStart(v simulator.View, p simulator.Phase) (simulator.Time, bool) {
	for _, tf := range v.Timeline().Snapshot("sim_phase") {
		if ph, ok := tf.Fact.Field("phase").(core.Uint64Value); ok && simulator.Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}

// FaultCoverageCheck reports a hard crash and a graceful stop from kernel facts.
// The prefix is the scenario label and includes its trailing colon, as in "echo:".
func FaultCoverageCheck(prefix string) simulator.SafetyRule {
	return &faultCoverage{prefix: prefix}
}

type faultCoverage struct{ prefix string }

func (*faultCoverage) Name() string           { return "FaultCoverage" }
func (*faultCoverage) Observe(simulator.View) {}

func (f *faultCoverage) AtEnd(v simulator.View) {
	has := func(name, key, val string) bool {
		for _, tf := range v.Timeline().Snapshot(name) {
			if key == "" {
				return true
			}
			if s, ok := tf.Fact.Field(key).(core.StringValue); ok && string(s) == val {
				return true
			}
		}
		return false
	}
	v.Assert().Sometimes(has("sim_crashed", "cause", "attrition"), core.PropertyID(f.prefix+" fault fired: hard crash"))
	v.Assert().Sometimes(has("sim_exited", "", ""), core.PropertyID(f.prefix+" fault fired: graceful stop"))
}

// Swarm draws the shared fault knobs from r.
// afterKnobs runs after those knobs and before the two slow-probability chances.
// A nil afterKnobs draws nothing.
func Swarm(r simulator.CountedRand, c *simulator.SeedConfig, afterKnobs func()) {
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
	if afterKnobs != nil {
		afterKnobs()
	}
	if r.Chance(0.3) {
		o.ApplyLatency.SlowProb, cp.ApplyLatency.SlowProb = 0.3, 0.3
	}
	if r.Chance(0.3) {
		n.Latency.SlowProb = 0.2
	}
}
