// Package preset holds the default schedule and fault knobs, the swarm and the
// fault coverage rule that the echo and kafka scenarios share.
package preset

import (
	"github.com/manifesterio/mds/broker/internal/verify/simulator/cluster"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakecontrolplane"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakenetwork"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/fakeservices/fakeobjectstore"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// Preset is the fault defaults and the phase schedule shared by the scenarios.
type Preset struct {
	Settings     cluster.Settings
	Phases       kernel.Phases
	QuiesceEvery machine.Duration
}

func ms(n int64) machine.Duration { return machine.Duration(n) * machine.Millisecond }

func slowTail(fmin, fmax, smin, smax int64, p float64) kernel.FastWithRareSlow {
	return kernel.FastWithRareSlow{
		FastMin: ms(fmin), FastMax: ms(fmax),
		SlowMin: ms(smin), SlowMax: ms(smax), SlowProb: p,
	}
}

// Phases is Chaos 30s, Settling 10s, Settle 5s.
func Phases() kernel.Phases {
	return kernel.Phases{Chaos: 30 * machine.Second, Settling: 10 * machine.Second, Settle: 5 * machine.Second}
}

// New returns a Preset. PartialIOMax and MaxCommitItems stay zero.
func New() Preset {
	return Preset{
		Settings: cluster.Settings{
			ObjectStore: fakeobjectstore.Faults{
				ApplyLatency:   slowTail(1, 5, 50, 200, 0.05),
				RespondLatency: slowTail(1, 3, 20, 100, 0.05),
				Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.02, GetCorruptProb: 0.03,
				Outage:        fakeobjectstore.ServiceOutage{Prob: 0.002, Dur: [2]machine.Duration{ms(200), 2 * machine.Second}},
				CallerTimeout: ms(500),
			},
			ControlPlane: fakecontrolplane.Faults{
				ApplyLatency:   slowTail(1, 5, 50, 200, 0.05),
				RespondLatency: slowTail(1, 3, 20, 100, 0.05),
				Unavailable503: 0.03, Timeout: 0.02, LostResponse: 0.03,
				Outage:        fakecontrolplane.ServiceOutage{Prob: 0.002, Dur: [2]machine.Duration{ms(200), 2 * machine.Second}},
				CallerTimeout: ms(500),
			},
			Network: fakenetwork.Faults{
				Latency: kernel.FastWithRareSlow{
					FastMin: 200 * machine.Microsecond, FastMax: 2 * machine.Millisecond,
					SlowMin: ms(20), SlowMax: ms(100), SlowProb: 0.03,
				},
				PartitionProb: 0.002, PartitionDur: [2]machine.Duration{ms(300), 3 * machine.Second},
				ClogProb: 0.003, ClogDur: [2]machine.Duration{ms(100), ms(500)},
				RandomCloseProb: 0.002, BlackHoleProb: 0.002,
				PairLatencyMax: ms(5),
			},
			ClockSkewMax:  ms(100),
			Attrition:     kernel.Attrition{MaxDead: 1, CrashProb: 0.0005, CrashWeight: 2, GracefulWeight: 1, StopGrace: 5 * machine.Second},
			RecoveryDelay: [2]machine.Duration{ms(200), ms(1500)},
			Cost:          kernel.CostParams{PerEvent: 50 * machine.Microsecond, PauseProb: 0.001, Pause: slowTail(1, 5, 20, 100, 0.1)},
		},
		Phases:       Phases(),
		QuiesceEvery: machine.Second,
	}
}

// ProgressGraceTime is the Settling offset used by the liveness rules.
const ProgressGraceTime = 6 * machine.Second

// PhaseStart reports when the kernel announced phase p, from its sim_phase fact.
func PhaseStart(v kernel.View, p kernel.Phase) (kernel.Time, bool) {
	for _, tf := range v.Timeline().Snapshot("sim_phase") {
		if ph, ok := tf.Fact.Field("phase").(machine.Uint64Value); ok && kernel.Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}

// FaultCoverage names the two assertions that FaultCoverageCheck records for one
// scenario.
type FaultCoverage struct {
	HardCrash    assert.Sometimes
	GracefulStop assert.Sometimes
}

// The fault coverage assertions of the two scenarios.
var (
	EchoFaultCoverage = FaultCoverage{ //determinismcheck:ok catalog handles, set once at start
		HardCrash:    assert.NewSometimes("echo: fault fired: hard crash"),
		GracefulStop: assert.NewSometimes("echo: fault fired: graceful stop"),
	}
	KafkaFaultCoverage = FaultCoverage{ //determinismcheck:ok catalog handles, set once at start
		HardCrash:    assert.NewSometimes("kafka: fault fired: hard crash"),
		GracefulStop: assert.NewSometimes("kafka: fault fired: graceful stop"),
	}
)

// FaultCoverageCheck reports a hard crash and a graceful stop from kernel facts.
func FaultCoverageCheck(names FaultCoverage) kernel.Rule {
	return &faultCoverage{names: names}
}

type faultCoverage struct{ names FaultCoverage }

func (*faultCoverage) Name() string        { return "FaultCoverage" }
func (*faultCoverage) Observe(kernel.View) {}

func (f *faultCoverage) AtEnd(v kernel.View) {
	has := func(name, key, val string) bool {
		for _, tf := range v.Timeline().Snapshot(name) {
			if key == "" {
				return true
			}
			if s, ok := tf.Fact.Field(key).(machine.StringValue); ok && string(s) == val {
				return true
			}
		}
		return false
	}
	v.Assert().Sometimes(has("sim_crashed", "cause", "attrition"), f.names.HardCrash)
	v.Assert().Sometimes(has("sim_exited", "", ""), f.names.GracefulStop)
}

// Swarm draws the shared fault knobs from r.
// afterKnobs runs after those knobs and before the two slow-probability chances.
// A nil afterKnobs draws nothing.
func Swarm(r *rng.Rand, s *cluster.Settings, afterKnobs func()) {
	menu := [...]float64{0.5, 1, 2, 4}
	knob := func(p *float64) {
		if r.Chance(0.4) {
			*p = 0
			return
		}
		*p = min(*p*menu[r.Intn(len(menu))], 0.4)
	}
	o, cp, n := &s.ObjectStore, &s.ControlPlane, &s.Network
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
	knob(&s.Attrition.CrashProb)
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
