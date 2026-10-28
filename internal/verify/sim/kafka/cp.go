package kafka

import (
	"manifester/internal/controlplane/machine"
	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

// cpFactory builds one CP replica machine for kafka-log, seeding workload topics.
func cpFactory(sc sim.SeedConfig, opts Opts) core.MachineFactory {
	lp := sc.ControlPlane.Log
	topics := opts.Topics
	parts := opts.Partitions
	return func(env core.Environment, self core.NodeInstance) core.Machine {
		rank := 0
		if self.Node == "cp-1" {
			rank = 1
		}
		cfg := machine.Config{
			CatchUpEvery:   lp.Tick,
			ReadLimit:      64,
			HoleAfter:      lp.HoleAfter,
			Writer:         string(self.Node),
			K:              lp.K,
			MaxCmdsPerSlot: lp.GroupMax,
			Rank:           rank,
			Heartbeat:      lp.Heartbeat,
			TakeoverAfter:  lp.TakeoverAfter,
			CkptEvery:      core.Duration(lp.CkptEvery) * lp.Tick,
			CkptMinSlots:   1,
			KeepSlots:      int64(lp.KeepSlots),
			InjectedBug:    lp.Bug,
			WindowSlots:    int64(lp.WindowSlots),
			TrimStride:     int64(lp.TrimStride),
			StallProb:      0, // TODO: re-enable with coordinated dual-replica faults
			CrashProb:      0,
			StallDur:       lp.StallDur,
			DownDur:        lp.DownDur,
		}
		m := machine.New(cfg)(env, self).(*machine.Machine)
		for _, t := range topics {
			_ = m.SeedTopic(t, parts)
		}
		return m
	}
}
