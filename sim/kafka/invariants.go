package kafka

import (
	"manifester/core"
	"manifester/sim"
)

// livenessGrace is how long into Settling a request may still fail: faults
// planned in Chaos can land late (an engine op planned at the end of Chaos, a
// partition that outlives it), and a node stopped at the end of Chaos is killed
// at its Stop deadline and then restarts.
const livenessGrace = 6 * core.Second

// phaseStart is when the kernel announced phase p, from its sim_phase fact.
func phaseStart(v sim.View, p sim.Phase) (sim.Time, bool) {
	for _, tf := range v.Timeline().Snapshot("sim_phase") {
		if ph, ok := fieldOf(tf.Fact, "phase").(core.U64); ok && sim.Phase(ph) == p {
			return tf.At, true
		}
	}
	return 0, false
}

// liveness: at the end no op is pending, and nothing that finishes more than
// livenessGrace into Settling ends in failure: a produce still unresolved then
// is acked (never abandoned or indeterminate), a fetch answers without error.
// Indeterminate produces are otherwise allowed (decision 15).
type liveness struct{}

func (*liveness) Name() string     { return "Liveness" }
func (*liveness) Observe(sim.View) {}

func (*liveness) AtEnd(v sim.View) {
	settling, ok := phaseStart(v, sim.PhaseSettling)
	v.Assert().Always(ok, "kafka: the kernel announced Settling")
	after := settling + sim.Time(livenessGrace)
	var produced, reads int
	for _, op := range v.History().Ops() {
		v.Assert().Always(op.Outcome != sim.OpPending, "kafka: no op pending at end", core.Detail{Key: "op", Value: op.Op})
		if !ok || op.Outcome == sim.OpPending {
			continue
		}
		switch r := op.Result.(type) {
		case ProduceRes:
			produced++
			if op.Return >= after {
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

// trippedNeverAcks: once an incarnation reported being fenced it acks nothing
// more. (Trip kills the incarnation, so a later ack from the same Source means
// the broker kept serving with a stale epoch.)
type trippedNeverAcks struct {
	acks, fenced sim.Cursor
	dead         map[core.NodeRef]bool
}

func (*trippedNeverAcks) Name() string   { return "TrippedNeverAcks" }
func (*trippedNeverAcks) AtEnd(sim.View) {}

func (i *trippedNeverAcks) Observe(v sim.View) {
	// Fenced facts first: the fact and a same-step ack share one Step, and
	// the fact is appended before any later ack.
	for _, tf := range v.Timeline().Since("broker_fenced", &i.fenced) {
		i.dead[tf.Source] = true
	}
	for _, tf := range v.Timeline().Since("produce_acked", &i.acks) {
		v.Assert().Always(!i.dead[tf.Source], "kafka: a fenced broker never acks", core.Detail{Key: "node", Value: string(tf.Source.Node)})
	}
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
		"broker: commit retried",
		"broker: corrupt GET detected",
		"broker: tripped on fencing",
		"broker: reads paused by backpressure",
		"broker: reads resumed after backpressure",
		"broker: segment sealed by size", "broker: segment sealed by timer",
		"broker: segment sealed by batch cap", "broker: segment sealed by partition cap",
		"broker: oversize batch sealed alone",
	}
}

// faultCoverage covers what only the kernel's facts show: a hard crash by
// attrition and a graceful stop. (Everything else is in expectReachable.)
type faultCoverage struct{}

func (*faultCoverage) Name() string     { return "FaultCoverage" }
func (*faultCoverage) Observe(sim.View) {}

func (*faultCoverage) AtEnd(v sim.View) {
	has := func(name, key, val string) bool {
		for _, tf := range v.Timeline().Snapshot(name) {
			if key == "" {
				return true
			}
			if s, ok := fieldOf(tf.Fact, key).(core.Str); ok && string(s) == val {
				return true
			}
		}
		return false
	}
	v.Assert().Sometimes(has("sim_crashed", "cause", "attrition"), "kafka: fault fired: hard crash")
	v.Assert().Sometimes(has("sim_exited", "", ""), "kafka: fault fired: graceful stop")
}
