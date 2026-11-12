package kafka

import (
	"github.com/manifesterio/mds/broker/internal/verify/simulator"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/internal/campaign"
	"github.com/manifesterio/mds/dst/assert"
)

type progressCheck struct{}

func (*progressCheck) Name() string           { return "Liveness" }
func (*progressCheck) Observe(simulator.View) {}

func (*progressCheck) AtEnd(v simulator.View) {
	settling, ok := campaign.PhaseStart(v, simulator.PhaseRecovering)
	v.Assert().Always(ok, kernelAnnouncedSettling)
	after := settling + simulator.Time(campaign.ProgressGraceTime)
	var produced, reads int
	for _, operation := range v.History().Ops() {
		v.Assert().Always(operation.Outcome != simulator.OpPending, noOpPendingAtEnd, assert.Pair{Key: "op", Value: operation.Op})
		if !ok || operation.Outcome == simulator.OpPending {
			continue
		}
		switch r := operation.Result.(type) {
		case ProduceRes:
			produced++
			if operation.Return >= after && r.Why != "rejected" {
				v.Assert().Always(operation.Outcome == simulator.OpResultKnown && r.Base >= 0, produceInSettlingIsAcked,
					assert.Pair{Key: "op", Value: operation.Op}, assert.Pair{Key: "result", Value: r}, assert.Pair{Key: "return", Value: operation.Return})
			}
		case ReadRes:
			reads++
			if operation.Invoke >= after {
				good := operation.Outcome == simulator.OpResultKnown
				for _, p := range r.Parts {
					good = good && p.Code == 0
				}
				v.Assert().Always(good, fetchInSettlingSucceeds,
					assert.Pair{Key: "op", Value: operation.Op}, assert.Pair{Key: "result", Value: r}, assert.Pair{Key: "invoke", Value: operation.Invoke})
			}
		}
	}
	v.Assert().Sometimes(produced > 0, workloadIssuedProduces)
	v.Assert().Sometimes(reads > 0, workloadIssuedFetches)
}

// expectReachable is the campaign's coverage contract (Config.ExpectReachable):
// every fault family and recovery path must fire somewhere in the campaign.
// The engines and the broker declare these props where the event happens; the
// list lives here because only the campaign knows what it must see.
func expectReachable() []string {
	return []string{
		"obj 503 injected", "obj timeout injected", "obj lost response injected", "obj outage started", "obj get corrupted",
		"cp 503 injected", "cp timeout injected", "cp lost response injected", "cp outage started",
		"net: partition injected", "net: clog injected", "net: random_close injected", "net: black_hole injected",
		"broker: commit retried", "broker: commit succeeded after the old retry budget",
		"broker: corrupt GET detected",
		"broker: fetch parked for a poll",
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
