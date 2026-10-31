package kafka

import (
	"mds/internal/controlplane/statemachine"
	"mds/internal/core"
	"mds/internal/verify/simulator"
)

// controlPlaneFactory builds one CP replica machine for kafka-log, seeding workload topics.
func controlPlaneFactory(sc simulator.SeedConfig, opts Opts) core.MachineFactory {
	lp := sc.ControlPlane.Log
	topics := opts.Topics
	parts := opts.Partitions
	return func(env core.Environment, self core.NodeInstance) core.Machine {
		rank := 0
		if self.Node == "cp-1" {
			rank = 1
		}
		config := statemachine.Config{
			CatchUpEvery:       lp.Tick,
			ReadLimit:          64,
			GapAfter:           lp.GapAfter,
			Writer:             string(self.Node),
			K:                  lp.K,
			MaxCmdsPerSlot:     lp.GroupMax,
			Rank:               rank,
			Heartbeat:          lp.Heartbeat,
			TakeoverAfter:      lp.TakeoverAfter,
			CheckpointEvery:    core.Duration(lp.CheckpointEvery) * lp.Tick,
			CheckpointMinSlots: 1,
			KeepSlots:          int64(lp.KeepSlots),
			InjectedBug:        lp.Bug,
			WindowSlots:        int64(lp.WindowSlots),
			TrimStride:         int64(lp.TrimStride),
			StallProb:          lp.StallProb,
			CrashProb:          lp.CrashProb,
			StallDur:           lp.StallDur,
			DownDur:            lp.DownDur,
		}
		m := statemachine.New(config)(env, self).(*statemachine.Machine)
		for _, t := range topics {
			_ = m.SeedTopic(t, parts)
		}
		return m
	}
}
