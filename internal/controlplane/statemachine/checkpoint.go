package statemachine

import (
	"bytes"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

const (
	timerCheckpoint = core.TimerID(4)

	defaultCheckpointKey = "ckpt/latest"
)

func (m *Machine) checkpointKey() string {
	if m.config.CheckpointKey != "" {
		return m.config.CheckpointKey
	}
	return defaultCheckpointKey
}

func (m *Machine) checkpointEnabled() bool {
	return m.config.CheckpointEvery > 0 || m.config.CheckpointKey != ""
}

func (m *Machine) armCheckpoint() []core.Action {
	if m.config.CheckpointEvery <= 0 || m.exited {
		return nil
	}
	m.checkpointGen++
	return []core.Action{core.SetTimer{ID: timerCheckpoint, Gen: m.checkpointGen, After: m.config.CheckpointEvery}}
}

func (m *Machine) onCheckpointTimer(e core.TimerFired) []core.Action {
	if e.Gen != m.checkpointGen {
		return nil
	}
	if m.needsCheckpoint || m.checkpointRetry {
		return m.emitCheckpointGet()
	}
	if m.config.CheckpointEvery <= 0 {
		return nil
	}
	acts := m.maybeCheckpoint()
	return append(acts, m.armCheckpoint()...)
}

func (m *Machine) maybeCheckpoint() []core.Action {
	if m.pending != 0 || !m.isAppender() || !m.ready {
		return nil
	}
	slot := m.applied
	if slot == 0 {
		return nil
	}
	// A node behind the log tail must not write: its snapshot could replace a
	// newer one from the other node, and the log below that newer one may
	// already be trimmed, so a restart could never recover.
	if m.applied < m.goalTail {
		return nil
	}
	minSlots := m.config.CheckpointMinSlots
	if minSlots <= 0 {
		minSlots = 1
	}
	if int64(slot-m.checkpointSlot) < minSlots {
		return nil
	}
	var buf bytes.Buffer
	if err := m.fold.Snapshot(&buf, int64(slot)); err != nil {
		return m.trip(err)
	}
	m.checkpointData = buf.Bytes()
	m.checkpointPending = slot
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjPut
	return []core.Action{core.ObjectPut{Req: m.pending, Key: m.checkpointKey(), Data: m.checkpointData}}
}

func (m *Machine) emitCheckpointGet() []core.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjGet
	return []core.Action{core.ObjectGet{Req: m.pending, Ref: core.SegmentRef{Key: m.checkpointKey()}}}
}

func (m *Machine) onObjGetDone(d core.ObjectGetDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjGet {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != core.OK && d.Err != core.ErrNotFound {
		// The read failed, so we do not know whether a checkpoint exists. The log
		// may be trimmed, so never catch up from slot 0 on a guess: try again.
		m.checkpointRetry = true
		return m.armCheckpointRetry()
	}
	m.checkpointRetry = false
	if d.Err == core.OK && len(d.Data) > 0 && stateupdater.CheckSnapshot(d.Data) != nil {
		// A changed byte in the read: the stored object may be fine. Read it again.
		m.env.Reachable("logcp-sim: checkpoint read had a bad CRC, read again")
		return m.emitCheckpointGet()
	}
	if d.Err == core.OK && len(d.Data) > 0 {
		slot, err := m.fold.Restore(bytes.NewReader(d.Data))
		if err != nil {
			return m.trip(err)
		}
		m.startAt(core.LogPosition(slot))
		m.checkpointSlot = core.LogPosition(slot)
		m.needsCheckpoint = false
		m.env.Reachable("logcp-sim: replica restored from a checkpoint")
		return m.afterRestore()
	}
	if m.needsCheckpoint {
		// Tomb forced a restore but no snapshot yet (or Get failed). Retry;
		// do not catch up from slot 0 through the trimmed prefix.
		return m.armCheckpointRetry()
	}
	return m.afterRestore()
}

func (m *Machine) armCheckpointRetry() []core.Action {
	after := m.config.Heartbeat
	if after <= 0 {
		after = 250 * core.Millisecond
	}
	m.checkpointGen++
	return []core.Action{core.SetTimer{ID: timerCheckpoint, Gen: m.checkpointGen, After: after}}
}

func (m *Machine) onObjPutDone(d core.ObjectPutDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjPut {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	slot := m.checkpointPending
	m.checkpointPending = 0
	m.checkpointData = nil
	if d.Err != core.OK || d.Outcome != core.Applied {
		return nil
	}
	m.checkpointSlot = max(m.checkpointSlot, slot)
	m.checkpointAt = m.now
	return m.emitTrim(slot)
}

func (m *Machine) emitTrim(checkpointSlot core.LogPosition) []core.Action {
	keep := m.config.KeepSlots
	if keep <= 0 {
		return nil
	}
	below := checkpointSlot - core.LogPosition(keep)
	if below <= 0 {
		return nil
	}
	through := below - 1
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingTrim
	m.fillSlot = through // stash Through for onTrimDone
	return []core.Action{core.LogDeleteOld{Req: m.pending, Through: through}}
}

func (m *Machine) onTrimDone(d core.LogDeleteOldDone) []core.Action {
	if d.Req != m.pending || m.pendingKind != pendingTrim {
		return nil
	}
	through := m.fillSlot
	m.pending, m.pendingKind = 0, pendingNone
	m.fillSlot = 0
	if d.Err == core.OK && d.Outcome == core.Applied {
		m.trimFloor = through + 1
		m.env.Reachable("logcp-sim: log trimmed below a checkpoint")
		if m.config.InjectedBug == InjectedBugTrimDelete {
			m.env.Always(false, "logcp-sim: a Put never wins a slot below the trim floor",
				core.ContextPair{Key: "through", Value: through})
		}
	}
	return nil
}

func (m *Machine) startAt(slot core.LogPosition) {
	m.applied = slot
	m.high = max(m.high, slot)
	m.goalTail = max(m.goalTail, slot)
	clear(m.buf)
}
