package kafka

import (
	"manifester/core"
	"manifester/sim"
	"manifester/sim/internal/campaign"
)

// liveness: at the end no op is pending, and nothing that finishes more than
// campaign.LivenessGrace into Settling ends in failure. A produce still
// unresolved then is acked, never abandoned or indeterminate, except the batch
// that claims a producer id and is rejected. A fetch answers without error.
// Indeterminate produces are otherwise allowed (decision 15).
type liveness struct{}

func (*liveness) Name() string     { return "Liveness" }
func (*liveness) Observe(sim.View) {}

func (*liveness) AtEnd(v sim.View) {
	settling, ok := campaign.PhaseStart(v, sim.PhaseSettling)
	v.Assert().Always(ok, "kafka: the kernel announced Settling")
	after := settling + sim.Time(campaign.LivenessGrace)
	var produced, reads int
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != sim.OpPending, "kafka: no op pending at end", core.Detail{Key: "op", Value: op.Op})
		if !ok || op.Outcome == sim.OpPending {
			continue
		}
		switch r := op.Result.(type) {
		case ProduceRes:
			produced++
			if op.Return >= after && r.Why != "rejected" { // a deliberately idempotent batch is rejected, never acked
				v.Assert().Always(op.Outcome == sim.OpDefinite && r.Base >= 0, "kafka: a produce still running in Settling is acked",
					core.Detail{Key: "op", Value: op.Op}, core.Detail{Key: "result", Value: r}, core.Detail{Key: "return", Value: op.Return})
			}
		case ReadRes:
			reads++
			if op.Invoke >= after {
				good := op.Outcome == sim.OpDefinite
				for _, p := range r.Parts {
					good = good && p.Code == 0
				}
				v.Assert().Always(good, "kafka: a fetch invoked in Settling succeeds",
					core.Detail{Key: "op", Value: op.Op}, core.Detail{Key: "result", Value: r}, core.Detail{Key: "invoke", Value: op.Invoke})
			}
		}
	}
	v.Assert().Sometimes(produced > 0, "kafka: workload issued produces")
	v.Assert().Sometimes(reads > 0, "kafka: workload issued fetches")
}

// expectReachable is the campaign's coverage contract (Config.ExpectReachable):
// every fault family and recovery path must fire somewhere in the campaign.
// The engines and the broker declare these props where the event happens; the
// list lives here because only the campaign knows what it must see.
func expectReachable() []core.Prop {
	return []core.Prop{
		"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
		"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
		"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
		"broker: commit retried", "broker: commit succeeded after the old retry budget",
		"broker: corrupt GET detected",
		"broker: reads paused by backpressure",
		"broker: reads resumed after backpressure",
		"broker: segment sealed by size", "broker: segment sealed by timer",
		"broker: segment sealed by batch cap", "broker: segment sealed by partition cap",
		"broker: oversize batch sealed alone",
		"broker: idempotent batch rejected",
		"broker: corrupt produce batch rejected",
		"broker: gzip batch admitted", "broker: snappy batch admitted",
		"broker: lz4 batch admitted", "broker: zstd batch admitted",
	}
}
