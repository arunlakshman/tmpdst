package machine

import (
	"bytes"

	"manifester/internal/core"
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
	if e.Gen != m.ckptGen {
		return nil
	}
	if m.needsCkpt {
		return m.emitCkptGet()
	}
	if m.cfg.CkptEvery <= 0 {
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
		m.needsCkpt = false
		m.env.Reachable("logcp-sim: replica restored from a checkpoint")
		return m.afterRestore()
	}
	if m.needsCkpt {
		// Tomb forced a restore but no snapshot yet (or Get failed). Retry;
		// do not catch up from slot 0 through the trimmed prefix.
		return m.armCkptRetry()
	}
	return m.afterRestore()
}

func (m *Machine) armCkptRetry() []core.Action {
	after := m.cfg.Heartbeat
	if after <= 0 {
		after = 250 * core.Millisecond
	}
	m.ckptGen++
	return []core.Action{core.SetTimer{ID: timerCkpt, Gen: m.ckptGen, After: after}}
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
	m.fillSlot = through // stash Through for onTrimDone
	return []core.Action{core.LogTrim{Req: m.pending, Through: through}}
}

func (m *Machine) onTrimDone(d core.LogTrimDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTrim {
		return nil
	}
	through := m.fillSlot
	m.pending, m.pendingKind = 0, pendingNone
	m.fillSlot = 0
	if d.Err == core.OK && d.Outcome == core.Applied {
		m.trimFloor = through + 1
		m.env.Reachable("logcp-sim: log trimmed below a checkpoint")
		if m.cfg.Bug == BugTrimDelete {
			m.env.Always(false, "logcp-sim: a Put never wins a slot below the trim floor",
				core.Detail{Key: "through", Value: through})
		}
	}
	return nil
}

func (m *Machine) startAt(slot core.LogSlot) {
	m.applied = slot
	m.high = max(m.high, slot)
	m.goalTail = max(m.goalTail, slot)
	clear(m.buf)
}
