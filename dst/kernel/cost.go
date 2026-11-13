package kernel

import (
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/rng"
)

// CostParams is the CPU model of a node: how long the node is busy after one
// Step. While a node is busy, its inputs wait in its inbox. Zero params cost
// nothing and draw nothing, so every input is one Step at once.
type CostParams struct {
	PerEvent  machine.Duration // the base cost of any Step
	PauseProb float64          // stands in for GC pauses and CPU contention
	Pause     FastWithRareSlow // the extra cost of a pause
}

// Sample draws the cost of one Step: PerEvent, plus a Pause sample with
// probability PauseProb.
func (p CostParams) Sample(r *rng.Rand) machine.Duration {
	d := p.PerEvent
	if r.Chance(p.PauseProb) {
		d += p.Pause.Sample(r)
	}
	return d
}

// FastWithRareSlow is a delay that is mostly fast, with a rare slow tail.
type FastWithRareSlow struct {
	FastMin, FastMax machine.Duration
	SlowMin, SlowMax machine.Duration
	SlowProb         float64
}

// Sample draws one delay: a coin for the slow tail, then a value in the chosen
// range, both ends included. A range with Max <= Min gives Min and draws
// nothing.
func (t FastWithRareSlow) Sample(r *rng.Rand) machine.Duration {
	lo, hi := t.FastMin, t.FastMax
	if r.Chance(t.SlowProb) {
		lo, hi = t.SlowMin, t.SlowMax
	}
	return machine.Duration(r.Between(int64(lo), int64(hi)))
}
