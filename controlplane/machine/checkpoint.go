package machine

import (
	"bytes"

	"manifester/core"
)

const (
	timerCkpt = core.TimerID(4)

	defaultCkptKey = "ckpt/latest"
)

func (m *Machine) ckptKey() string {
	if m.cfg.CkptKey != "" {
		return m.cfg.CkptKey
	}
	return defaultCkptKey
}

func (m *Machine) ckptEnabled() bool {
	return m.cfg.CkptEvery > 0 || m.cfg.CkptKey != ""
}

func (m *Machine) armCkpt() []core.Action {
	if m.cfg.CkptEvery <= 0 || m.exited {
		return nil
	}
	m.ckptGen++
	return []core.Action{core.SetTimer{ID: timerCkpt, Gen: m.ckptGen, After: m.cfg.CkptEvery}}
}

func (m *Machine) onCkptTimer(e core.TimerFired) []core.Action {
	if e.Gen != m.ckptGen || m.cfg.CkptEvery <= 0 {
		return nil
	}
	acts := m.maybeCheckpoint()
	return append(acts, m.armCkpt()...)
}

func (m *Machine) maybeCheckpoint() []core.Action {
	if m.pending != 0 || !m.isAppender() || !m.ready {
		return nil
	}
	slot := m.applied
	if slot == 0 {
		return nil
	}
	minSlots := m.cfg.CkptMinSlots
	if minSlots <= 0 {
		minSlots = 1
	}
	if int64(slot-m.ckptSlot) < minSlots {
		return nil
	}
	var buf bytes.Buffer
	if err := m.fold.Snapshot(&buf, int64(slot)); err != nil {
		return m.trip(err)
	}
	m.ckptData = buf.Bytes()
	m.ckptPending = slot
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjPut
	return []core.Action{core.ObjPut{Req: m.pending, Key: m.ckptKey(), Data: m.ckptData}}
}

func (m *Machine) emitCkptGet() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjGet
	return []core.Action{core.ObjGet{Req: m.pending, Ref: core.SegmentRef{Key: m.ckptKey()}}}
}

func (m *Machine) onObjGetDone(d core.ObjGetDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjGet {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err == core.OK && len(d.Data) > 0 {
		slot, err := m.fold.Restore(bytes.NewReader(d.Data))
		if err != nil {
			return m.trip(err)
		}
		m.startAt(core.LogSlot(slot))
		m.ckptSlot = core.LogSlot(slot)
	}
	return m.afterRestore()
}

func (m *Machine) onObjPutDone(d core.ObjPutDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjPut {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	slot := m.ckptPending
	m.ckptPending = 0
	m.ckptData = nil
	if d.Err != core.OK || d.Outcome != core.Applied {
		return nil
	}
	m.ckptSlot = max(m.ckptSlot, slot)
	m.ckptAt = m.now
	return m.emitTrim(slot)
}

func (m *Machine) emitTrim(ckptSlot core.LogSlot) []core.Action {
	keep := m.cfg.KeepSlots
	if keep <= 0 {
		return nil
	}
	below := ckptSlot - core.LogSlot(keep)
	if below <= 0 {
		return nil
	}
	through := below - 1
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTrim
	return []core.Action{core.LogTrim{Req: m.pending, Through: through}}
}

func (m *Machine) onTrimDone(d core.LogTrimDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTrim {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	return nil
}

func (m *Machine) startAt(slot core.LogSlot) {
	m.applied = slot
	m.high = max(m.high, slot)
	m.goalTail = max(m.goalTail, slot)
	clear(m.buf)
}
