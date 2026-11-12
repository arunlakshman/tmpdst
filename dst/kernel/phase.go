package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Phase is the fault regime of the run.
type Phase uint8

// The phases, in run order.
const (
	PhaseFaults     Phase = iota // faults on
	PhaseRecovering              // faults off, nodes alive; liveness is checked
	PhaseRecover                 // draining the rest
)

// Phases is a schedule: Chaos, then Settling, then Settle.
type Phases struct{ Chaos, Settling, Settle machine.Duration }

// phaseBody is the body of a phase change entry.
type phaseBody struct{ to Phase }

// Fold writes tag 902 and the new phase.
func (b phaseBody) Fold(h *trace.Hash) {
	h.Uint64(tagPhase)
	h.Uint64(uint64(b.to))
}

// SchedulePhases queues the two phase changes on HostLane (PhaseRecovering
// after Chaos, PhaseRecover after Chaos+Settling) and sets Config.End to
// Chaos+Settling+Settle.
func SchedulePhases(w *World, p Phases) {
	at := Time(p.Chaos)
	w.queuePhase(at, PhaseRecovering)
	at += Time(p.Settling)
	w.queuePhase(at, PhaseRecover)
	w.cfg.End = Time(p.Chaos + p.Settling + p.Settle)
}

func (w *World) queuePhase(at Time, to Phase) {
	e := &entry{at: at, lane: HostLane, kind: kindPhase, node: -1, body: phaseBody{to}}
	e.run = func() {
		w.counters[countKernelSteps]++
		w.SetPhase(to)
	}
	w.q.add(e)
}

// SetPhase changes the phase. It appends the "sim_phase" fact, makes buggify
// Fault quiet outside PhaseFaults, calls EnterRecovery on every service when
// PhaseRecovering starts, then gives each live driver NodeSpec.OnPhase(p), in
// node order, as one Step that takes no node time and skips the inbox.
func (w *World) SetPhase(p Phase) {
	w.phase = p
	w.logMsg(fmt.Sprintf("phase %d", p), nil)
	w.kernelFact("sim_phase", machine.Field{Key: "phase", Value: machine.Uint64Value(p)})
	w.faults.SetQuiet(p != PhaseFaults)
	if p == PhaseRecovering {
		for _, s := range w.services {
			s.base().ok = false
			if !w.guard("EnterRecovery "+s.base().name, s.enterRecovery) {
				w.halted = true
			}
		}
	}
	for _, s := range w.nodes {
		if w.halted {
			return
		}
		if s.base().driver {
			s.phaseStep(p)
		}
	}
}
