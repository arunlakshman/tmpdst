package kafka

import (
	"sync/atomic"
	"testing"

	"manifester/internal/broker"
	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

// sealStats counts sealed segments by reason across a campaign.
type sealStats struct {
	segs, parts, byCap, bySize, byTimer, byBatches atomic.Int64
}

type sealInv struct {
	st   *sealStats
	seal sim.Cursor
}

func (*sealInv) Name() string   { return "SealStats" }
func (*sealInv) AtEnd(sim.View) {}
func (i *sealInv) Observe(v sim.View) {
	for _, tf := range v.Timeline().Since("segment_sealed", &i.seal) {
		n, _ := tf.Fact.Field("parts").(core.I64)
		why, _ := tf.Fact.Field("reason").(core.Str)
		i.st.segs.Add(1)
		i.st.parts.Add(int64(n))
		switch string(why) {
		case "partitions":
			i.st.byCap.Add(1)
		case "size":
			i.st.bySize.Add(1)
		case "timer":
			i.st.byTimer.Add(1)
		case "batches":
			i.st.byBatches.Add(1)
		}
	}
}

// capCampaign runs the wide workload on the veritas-backed control plane with
// no item limit (so only the broker's cap differs) at the given partition cap.
func capCampaign(t *testing.T, capParts int) *sealStats {
	t.Helper()
	prod := broker.DefaultConfig()
	prod.MaxSegmentPartitions, prod.MaxSegmentBatches = capParts, 1024
	prod.MaxBufferedBytes, prod.ResumeBufferedBytes = 64<<20, 32<<20
	opts := Opts{Producers: 128, Consumers: 1, Topics: []string{"a", "b"}, Partitions: 128}
	cfg := configWith(seeds(1, 2), broker.InjectedBugNone, opts)
	inner := cfg.Swarm
	cfg.Swarm = func(seed uint64, r sim.CountedRand, c *sim.SeedConfig) {
		inner(seed, r, c)
		c.Params, c.ControlPlane.MaxCommitItems = prod, 0
	}
	st := &sealStats{}
	cfg.Invariants = append(cfg.Invariants, func() sim.SafetyRule { return &sealInv{st: st} })
	rep, err := sim.NewRunner().Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	rep.Coverage = nil
	report(t, rep)
	return st
}

// Raising the cap from 96 to 4096 changes the run only where the cap was
// binding (decision 52): the 96-partition seals disappear (the segments they cut
// run on until the timer). In this small simulated workload the cap binds on
// about 1% of segments, so the two runs differ in a handful of segments.
func TestCapChangeIsExplainedBySealReasons(t *testing.T) {
	old, now := capCampaign(t, 96), capCampaign(t, 4096)
	t.Logf("cap 96:   %d segments, %.1f partitions each, sealed by cap %d size %d timer %d batches %d",
		old.segs.Load(), float64(old.parts.Load())/float64(old.segs.Load()), old.byCap.Load(), old.bySize.Load(), old.byTimer.Load(), old.byBatches.Load())
	t.Logf("cap 4096: %d segments, %.1f partitions each, sealed by cap %d size %d timer %d batches %d",
		now.segs.Load(), float64(now.parts.Load())/float64(now.segs.Load()), now.byCap.Load(), now.bySize.Load(), now.byTimer.Load(), now.byBatches.Load())
	if old.byCap.Load() == 0 {
		t.Error("the 96 cap never sealed a segment: the workload does not exercise it")
	}
	if now.byCap.Load() != 0 {
		t.Errorf("%d segments still sealed by the partition cap at 4096", now.byCap.Load())
	}
}
