package replica

import (
	"bytes"
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

const (
	timerCheckpoint = machine.TimerID(4)

	defaultCheckpointPrefix = "checkpoint/"
)

// checkpointKeyFor names the object that holds the snapshot taken at slot.
// Every checkpoint has its own key, so a late or repeated upload can only
// rewrite the same bytes and never replaces a newer snapshot. The slot has a
// fixed width, so the last key in a sorted list is the newest checkpoint.
func (m *Replica) checkpointKeyFor(slot runtime.LogPosition) string {
	return fmt.Sprintf("%s%020d", m.checkpointPrefix(), slot)
}

func (m *Replica) checkpointPrefix() string {
	if m.config.CheckpointPrefix != "" {
		return m.config.CheckpointPrefix
	}
	return defaultCheckpointPrefix
}

func (m *Replica) checkpointEnabled() bool {
	return m.config.CheckpointEvery > 0 || m.config.CheckpointPrefix != ""
}

func (m *Replica) armCheckpoint() []runtime.Action {
	if m.config.CheckpointEvery <= 0 || m.exited {
		return nil
	}
	m.checkpointGen++
	return []runtime.Action{arm(timerCheckpoint, m.checkpointGen, m.config.CheckpointEvery)}
}

func (m *Replica) onCheckpointTimer(e runtime.TimerFired) []runtime.Action {
	if e.Gen != m.checkpointGen {
		return nil
	}
	if m.needsCheckpoint || m.checkpointRetry {
		return m.emitCheckpointList()
	}
	if m.config.CheckpointEvery <= 0 {
		return nil
	}
	acts := m.maybeCheckpoint()
	return append(acts, m.armCheckpoint()...)
}

// CheckpointDecision says what the checkpoint timer did and why.
type CheckpointDecision struct {
	Written        bool   // true: a checkpoint write was started
	Reason         string // why it was skipped; "ok" when written
	Applied        int64  // slots folded so far
	GoalTail       int64  // newest slot seen in the log
	LastCheckpoint int64  // slot of the last checkpoint written by this node
	MinSlots       int64  // slots needed since the last checkpoint
	PendingKind    string // the control I/O in flight, if any
}

func (k pendingKind) String() string {
	switch k {
	case pendingTail:
		return "tail_check"
	case pendingRead:
		return "log_read"
	case pendingObjList:
		return "checkpoint_list"
	case pendingObjGet:
		return "checkpoint_get"
	}
	return "none"
}

// decide reports one checkpoint decision to the optional observer.
func (m *Replica) decide(written bool, reason string) {
	if m.config.OnCheckpointDecision == nil {
		return
	}
	m.config.OnCheckpointDecision(CheckpointDecision{
		Written: written, Reason: reason,
		Applied: int64(m.applied), GoalTail: int64(m.goalTail), LastCheckpoint: int64(m.checkpointSlot),
		MinSlots: m.config.CheckpointMinSlots, PendingKind: m.pendingKind.String(),
	})
}

func (m *Replica) maybeCheckpoint() []runtime.Action {
	switch {
	case m.checkpointRequest != 0:
		m.decide(false, "checkpoint_in_flight")
		return nil
	case !m.isAppender():
		m.decide(false, "not_appender")
		return nil
	case !m.ready:
		m.decide(false, "not_ready")
		return nil
	}
	slot := m.applied
	if slot == 0 {
		m.decide(false, "nothing_applied")
		return nil
	}
	// A node behind the log tail must not write: its snapshot could replace a
	// newer one from the other node, and the log below that newer one may
	// already be trimmed, so a restart could never recover.
	if m.applied < m.goalTail {
		m.decide(false, "behind_tail")
		return nil
	}
	minSlots := m.config.CheckpointMinSlots
	if minSlots <= 0 {
		minSlots = 1
	}
	if int64(slot-m.checkpointSlot) < minSlots {
		m.decide(false, "too_few_new_slots")
		return nil
	}
	var buf bytes.Buffer
	if err := m.meta.Snapshot(&buf, int64(slot)); err != nil {
		return m.trip(err)
	}
	m.decide(true, "ok")
	m.checkpointData = buf.Bytes()
	m.checkpointPending = slot
	m.nextReq++
	m.checkpointRequest, m.checkpointStage = m.nextReq, stageUpload
	return []runtime.Action{runtime.ObjectPut{Req: m.checkpointRequest, Key: m.checkpointKeyFor(slot), Data: m.checkpointData}}
}

// emitCheckpointList starts a restore: find the newest checkpoint object.
func (m *Replica) emitCheckpointList() []runtime.Action {
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjList
	return []runtime.Action{runtime.ObjectList{Req: m.pending, Prefix: m.checkpointPrefix()}}
}

func (m *Replica) emitCheckpointGet(key string) []runtime.Action {
	m.restoreKey = key
	m.nextReq++
	m.pending = m.nextReq
	m.pendingKind = pendingObjGet
	return []runtime.Action{runtime.ObjectGet{Req: m.pending, Ref: runtime.SegmentRef{Key: key}}}
}

func (m *Replica) onObjListDone(d runtime.ObjectListDone) []runtime.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjList {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err != runtime.OK {
		// The list failed, so we do not know whether a checkpoint exists. The
		// log may be trimmed, so never catch up from slot 0 on a guess: try again.
		m.checkpointRetry = true
		return m.armCheckpointRetry()
	}
	if len(d.Keys) == 0 {
		return m.noCheckpointFound()
	}
	return m.emitCheckpointGet(d.Keys[len(d.Keys)-1])
}

// noCheckpointFound handles a restore that found no checkpoint object.
func (m *Replica) noCheckpointFound() []runtime.Action {
	m.checkpointRetry = false
	if m.needsCheckpoint {
		// The log was trimmed under us but no snapshot is there (yet). Retry;
		// do not catch up from slot 0 through the trimmed prefix.
		return m.armCheckpointRetry()
	}
	return m.afterRestore()
}

func (m *Replica) onObjGetDone(d runtime.ObjectGetDone) []runtime.Action {
	if d.Req != m.pending || m.pendingKind != pendingObjGet {
		return nil
	}
	m.pending, m.pendingKind = 0, pendingNone
	if d.Err == runtime.ErrNotFound {
		// Listed a moment ago and gone now: a newer checkpoint replaced it.
		return m.emitCheckpointList()
	}
	if d.Err != runtime.OK {
		m.checkpointRetry = true
		return m.armCheckpointRetry()
	}
	m.checkpointRetry = false
	if len(d.Data) > 0 && metadb.CheckSnapshot(d.Data) != nil {
		// A changed byte in the read: the stored object may be fine. Read it again.
		m.env.Reachable(checkpointReadBadCRC)
		return m.emitCheckpointGet(m.restoreKey)
	}
	if len(d.Data) > 0 {
		slot, err := m.meta.Restore(bytes.NewReader(d.Data))
		if err != nil {
			return m.trip(err)
		}
		m.startAt(runtime.LogPosition(slot))
		m.checkpointSlot = runtime.LogPosition(slot)
		m.needsCheckpoint = false
		m.env.Reachable(replicaRestoredFromCheckpoint)
		return m.afterRestore()
	}
	return m.noCheckpointFound()
}

func (m *Replica) armCheckpointRetry() []runtime.Action {
	after := m.config.Heartbeat
	if after <= 0 {
		after = 250 * machine.Millisecond
	}
	m.checkpointGen++
	return []runtime.Action{arm(timerCheckpoint, m.checkpointGen, after)}
}

func (m *Replica) onObjPutDone(d runtime.ObjectPutDone) []runtime.Action {
	if m.checkpointRequest == 0 || d.Req != m.checkpointRequest || m.checkpointStage != stageUpload {
		return nil
	}
	m.checkpointRequest, m.checkpointStage = 0, 0
	slot := m.checkpointPending
	m.checkpointPending = 0
	m.checkpointData = nil
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		return nil
	}
	previous := m.checkpointSlot
	m.checkpointSlot = max(m.checkpointSlot, slot)
	m.checkpointAt = m.now
	acts := m.emitTrim(slot)
	if previous > 0 && previous < slot {
		// The older snapshot is no longer needed. Fire and forget: a failed
		// delete only leaves one extra object, and restore picks the newest.
		m.nextReq++
		acts = append(acts, runtime.ObjectDelete{Req: m.nextReq, Key: m.checkpointKeyFor(previous)})
	}
	return acts
}

func (m *Replica) emitTrim(checkpointSlot runtime.LogPosition) []runtime.Action {
	keep := m.config.KeepSlots
	if keep <= 0 {
		return nil
	}
	below := checkpointSlot - runtime.LogPosition(keep)
	if below <= 0 {
		return nil
	}
	through := below - 1
	m.nextReq++
	m.checkpointRequest, m.checkpointStage = m.nextReq, stageTrim
	m.trimThrough = through
	return []runtime.Action{runtime.LogDeleteOld{Req: m.checkpointRequest, Through: through}}
}

func (m *Replica) onTrimDone(d runtime.LogDeleteOldDone) []runtime.Action {
	if m.checkpointRequest == 0 || d.Req != m.checkpointRequest || m.checkpointStage != stageTrim {
		return nil
	}
	through := m.trimThrough
	m.checkpointRequest, m.checkpointStage, m.trimThrough = 0, 0, 0
	if d.Err == runtime.OK && d.Outcome == runtime.Applied {
		m.trimFloor = through + 1
		m.env.Reachable(logTrimmedBelowCheckpoint)
	}
	return nil
}

func (m *Replica) startAt(slot runtime.LogPosition) {
	m.applied = slot
	m.high = max(m.high, slot)
	m.goalTail = max(m.goalTail, slot)
	clear(m.buf)
}
