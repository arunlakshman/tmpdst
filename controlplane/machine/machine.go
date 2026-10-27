package machine

import "manifester/core"

const timerCatchUp = core.TimerID(1)

// Machine is the control-plane DSM. Build it with New.
type Machine struct {
	env  core.Env
	self core.NodeRef
	cfg  Config

	started  bool
	stopping bool
	exited   bool

	timerGen    core.TimerGen
	nextReq     core.ReqID
	nextSlot    core.LogSlot // next slot to LogRead; apply is CP-DSM-4
	goalTail    core.LogSlot // CheckTail answer for the current round
	pending     core.ReqID   // in-flight log ReqID, or 0
	pendingKind pendingKind
}

type pendingKind uint8

const (
	pendingNone pendingKind = iota
	pendingTail
	pendingRead
)

// New returns the core.Factory of the control-plane machine.
func New(cfg Config) core.Factory {
	return func(env core.Env, self core.NodeRef) core.Machine {
		return &Machine{env: env, self: self, cfg: cfg}
	}
}

var _ core.Machine = (*Machine)(nil)

// Step implements core.StateMachine. Unknown completions and unhandled bodies
// leave state unchanged. After Exit, every event is a no-op.
func (m *Machine) Step(ev core.Event) []core.Action {
	if m.exited {
		return nil
	}
	switch body := ev.Body.(type) {
	case core.Started:
		return m.onStarted()
	case core.Stop:
		return m.stop()
	case core.TimerFired:
		return m.onTimer(body)
	case core.LogCheckTailDone:
		return m.onCheckTailDone(body)
	case core.LogReadDone:
		return m.onReadDone(body)
	default:
		return nil
	}
}

func (m *Machine) onStarted() []core.Action {
	m.started = true
	if m.cfg.CatchUpEvery <= 0 {
		return nil
	}
	return m.armCatchUp()
}

func (m *Machine) onTimer(e core.TimerFired) []core.Action {
	if e.ID != timerCatchUp || e.Gen != m.timerGen || m.pending != 0 || m.cfg.CatchUpEvery <= 0 {
		return nil
	}
	return m.emitCheckTail()
}

func (m *Machine) onCheckTailDone(d core.LogCheckTailDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTail {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	m.goalTail = d.Tail
	if d.Tail > m.nextSlot {
		return m.emitRead()
	}
	return m.armCatchUp()
}

func (m *Machine) onReadDone(d core.LogReadDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingRead {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK || d.Outcome != core.Applied {
		return m.armCatchUp()
	}
	// Catch-up sketch only advances the read cursor. Apply is CP-DSM-4.
	for _, e := range d.Entries {
		if e.Slot >= m.nextSlot {
			m.nextSlot = e.Slot + 1
		}
	}
	if len(d.Entries) >= int(m.cfg.readLimit()) && m.nextSlot < m.goalTail {
		return m.emitRead()
	}
	// Short page: holes or tombs above nextSlot. Jump to the known tail;
	// hole-fill is CP-DSM-4.
	if m.nextSlot < m.goalTail {
		m.nextSlot = m.goalTail
	}
	return m.armCatchUp()
}

func (m *Machine) emitCheckTail() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTail
	return []core.Action{core.LogCheckTail{Req: m.pending}}
}

func (m *Machine) emitRead() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingRead
	return []core.Action{core.LogRead{Req: m.pending, From: m.nextSlot, Limit: m.cfg.readLimit()}}
}

func (m *Machine) armCatchUp() []core.Action {
	m.timerGen++
	return []core.Action{core.SetTimer{ID: timerCatchUp, Gen: m.timerGen, After: m.cfg.CatchUpEvery}}
}

// stop marks the machine shutting down and emits Exit. With no in-flight work
// yet that must drain, Exit is immediate (decision 8).
func (m *Machine) stop() []core.Action {
	if m.stopping {
		return nil
	}
	m.stopping = true
	m.exited = true
	return []core.Action{core.Exit{}}
}

// StateHash folds every field that influences behaviour, in a fixed order.
func (m *Machine) StateHash() uint64 {
	h := uint64(14695981039346656037) // FNV-1a offset basis
	h = core.FnvU64(h, bool64(m.started))
	h = core.FnvU64(h, bool64(m.stopping))
	h = core.FnvU64(h, bool64(m.exited))
	h = core.FnvU64(h, uint64(m.timerGen))
	h = core.FnvU64(h, uint64(m.nextReq))
	h = core.FnvU64(h, uint64(m.nextSlot))
	h = core.FnvU64(h, uint64(m.goalTail))
	h = core.FnvU64(h, uint64(m.pending))
	h = core.FnvU64(h, uint64(m.pendingKind))
	h = core.FnvU64(h, uint64(m.cfg.CatchUpEvery))
	h = core.FnvU64(h, uint64(m.cfg.ReadLimit))
	return h
}

func bool64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
