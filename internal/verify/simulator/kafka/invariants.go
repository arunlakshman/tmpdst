package kafka

import (
	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/verify/simulator/internal/campaign"
)

type progressCheck struct{}

func (*progressCheck) Name() string           { return "Liveness" }
func (*progressCheck) Observe(simulator.View) {}

func (*progressCheck) AtEnd(v simulator.View) {
	settling, ok := campaign.PhaseStart(v, simulator.PhaseRecovering)
	v.Assert().Always(ok, "kafka: the kernel announced Settling")
	after := settling + simulator.Time(campaign.ProgressGraceTime)
	var produced, reads int
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != simulator.OpPending, "kafka: no op pending at end", core.ContextPair{Key: "op", Value: op.Op})
		if !ok || op.Outcome == simulator.OpPending {
			continue
		}
		switch r := op.Result.(type) {
		case ProduceRes:
			produced++
			if op.Return >= after && r.Why != "rejected" {
				v.Assert().Always(op.Outcome == simulator.OpResultKnown && r.Base >= 0, "kafka: a produce still running in Settling is acked",
					core.ContextPair{Key: "op", Value: op.Op}, core.ContextPair{Key: "result", Value: r}, core.ContextPair{Key: "return", Value: op.Return})
			}
		case ReadRes:
			reads++
			if op.Invoke >= after {
				good := op.Outcome == simulator.OpResultKnown
				for _, p := range r.Parts {
					good = good && p.Code == 0
				}
				v.Assert().Always(good, "kafka: a fetch invoked in Settling succeeds",
					core.ContextPair{Key: "op", Value: op.Op}, core.ContextPair{Key: "result", Value: r}, core.ContextPair{Key: "invoke", Value: op.Invoke})
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
func expectReachable() []core.PropertyID {
	return []core.PropertyID{
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
