package kafka

import (
	"github.com/manifesterio/mds/broker/internal/verify/simulation/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulation/internal/preset"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
)

type progressCheck struct{ hist history.Reader[Op, Res] }

func (*progressCheck) Name() string        { return "Liveness" }
func (*progressCheck) Observe(kernel.View) {}

func (p *progressCheck) AtEnd(v kernel.View) {
	settling, ok := preset.PhaseStart(v, kernel.PhaseRecovering)
	v.Assert().Always(ok, kernelAnnouncedSettling)
	after := settling + kernel.Time(preset.ProgressGraceTime)
	var produced, reads int
	for _, operation := range p.hist.Ops() {
		v.Assert().Always(operation.Outcome != history.Pending, noOpPendingAtEnd, assert.Pair{Key: "op", Value: operation.Op})
		if !ok || operation.Outcome == history.Pending {
			continue
		}
		switch r := operation.Result.(type) {
		case ProduceRes:
			produced++
			if operation.Return >= after && r.Why != "rejected" {
				v.Assert().Always(operation.Outcome == history.ResultKnown && r.Base >= 0, produceInSettlingIsAcked,
					assert.Pair{Key: "op", Value: operation.Op}, assert.Pair{Key: "result", Value: r}, assert.Pair{Key: "return", Value: operation.Return})
			}
		case ReadRes:
			reads++
			if operation.Invoke >= after {
				good := operation.Outcome == history.ResultKnown
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
