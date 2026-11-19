package kernel

import (
	"fmt"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Phase is the fault regime of the run.
type Phase uint8

// The phases, in run order. Numeric values are frozen (0, 1, 2): they are folded
// into the trace hash (phaseBody.Fold and nodeHost.phaseStep), so changing them
// breaks seed determinism and golden pins.
const (
	PhaseFaults Phase = 0 // faults on
	PhaseHeal   Phase = 1 // faults off, nodes alive; liveness is checked
	PhaseDrain  Phase = 2 // draining remaining in-flight messages
)

// String returns a short name for debugging. It is not folded into the trace hash.
func (p Phase) String() string {
	switch p {
	case PhaseFaults:
		return "faults"
	case PhaseHeal:
		return "heal"
	case PhaseDrain:
		return "drain"
	default:
		return fmt.Sprintf("Phase(%d)", p)
	}
}

// Phases is the duration schedule: Faults, then Heal, then Drain.
type Phases struct {
	Faults machine.Duration
	Heal   machine.Duration
	Drain  machine.Duration
}

// phaseBody is the body of a phase change entry.
type phaseBody struct{ to Phase }

// Fold writes tag 902 and the new phase.
func (b phaseBody) Fold(h *trace.Hash) {
	h.Uint64(tagPhase)
	h.Uint64(uint64(b.to))
}

// SchedulePhases queues the two phase changes on HostLane (PhaseHeal after
// Faults, PhaseDrain after Faults+Heal) and sets Config.End to
// Faults+Heal+Drain.
func SchedulePhases(w *World, p Phases) {
	at := Time(p.Faults)
	w.queuePhase(at, PhaseHeal)
	at += Time(p.Heal)
	w.queuePhase(at, PhaseDrain)
	w.cfg.End = Time(p.Faults + p.Heal + p.Drain)
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
// PhaseHeal starts, then gives each live driver NodeSpec.OnPhase(p), in
// node order, as one Step that takes no node time and skips the inbox.
func (w *World) SetPhase(p Phase) {
	w.phase = p
	w.logMsg(fmt.Sprintf("phase %s", p), nil)
	w.kernelFact("sim_phase", machine.Field{Key: "phase", Value: machine.Uint64Value(p)})
	w.faults.SetQuiet(p != PhaseFaults)
	if p == PhaseHeal {
		for _, s := range w.services {
			s.base().invalidateHash()
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
