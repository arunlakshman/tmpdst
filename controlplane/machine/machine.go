package machine

import "manifester/core"

// Machine is the control-plane DSM. Build it with New.
type Machine struct {
	env  core.Env
	self core.NodeRef
	cfg  Config

	started  bool
	stopping bool
	exited   bool
}

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
	switch ev.Body.(type) {
	case core.Started:
		m.started = true
		return nil
	case core.Stop:
		return m.stop()
	default:
		return nil
	}
}

// stop marks the machine shutting down and emits Exit. With no in-flight work
// yet, Exit is immediate (decision 8).
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
	return h
}

func bool64(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
