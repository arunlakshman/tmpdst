package statemachine

import (
	"fmt"
	"slices"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

const (
	timerRole = core.TimerID(3)
)

// Roles are advisory (decision 51): the slot put remains the fence. Rank 0
// becomes active at Ready; a standby promotes after TakeoverAfter of silence
// from other writers while its own reads succeed; an active node steps down
// when a lower-rank writer appends at or after promoSlot.

// Ready reports that restore (if any) and the first catch-up round finished.
func (m *Machine) Ready() bool { return m.ready }

// Booting reports restore or catch-up in progress (not yet Ready).
func (m *Machine) Booting() bool { return m.booting }

// IsActive reports the advisory appender role.
func (m *Machine) IsActive() bool { return m.active }

// Promote makes this node the advisory appender (tests / operator).
func (m *Machine) Promote() { m.setActive(true) }

// Demote makes this node a standby; in-flight appends still finish.
func (m *Machine) Demote() { m.setActive(false) }

func (m *Machine) setActive(on bool) {
	if m.active == on {
		return
	}
	was := m.active
	m.active = on
	if on {
		m.promoSlot = max(m.high, m.applied)
		m.lowerSeen = false
		m.lastOtherAt = m.now
		if !was && m.ready {
			m.env.Reachable("logcp-sim: standby took over")
		}
	} else if was {
		m.env.Reachable("logcp-sim: failback, the standby stepped down")
	}
	m.lowerSeen = false
}

func (m *Machine) rolesArmed() bool {
	return m.config.Heartbeat > 0 || m.config.TakeoverAfter > 0
}

func (m *Machine) isAppender() bool {
	if !m.rolesArmed() {
		return true
	}
	return m.active
}

func (m *Machine) canCommit() bool {
	if m.booting || m.crashed || m.now < m.stallUntil {
		return false
	}
	if m.rolesArmed() {
		return m.ready && m.active
	}
	return true
}

func (m *Machine) roleEvery() core.Duration {
	a, b := m.config.Heartbeat, m.config.TakeoverAfter
	switch {
	case a > 0 && b > 0:
		return min(a, b)
	case a > 0:
		return a
	default:
		return b
	}
}

func (m *Machine) armRole() []core.Action {
	every := m.roleEvery()
	if every <= 0 || m.exited {
		return nil
	}
	m.roleGen++
	return []core.Action{core.SetTimer{ID: timerRole, Gen: m.roleGen, After: every}}
}

func (m *Machine) onRoleTimer(e core.TimerFired) []core.Action {
	if e.Gen != m.roleGen || !m.rolesArmed() {
		return nil
	}
	acts := m.roleTick()
	return append(acts, m.armRole()...)
}

func (m *Machine) roleTick() []core.Action {
	if m.crashed {
		if m.now < m.downUntil {
			return nil
		}
		m.crashed = false
		return m.afterCrashRestart()
	}
	if m.now < m.stallUntil {
		return nil
	}
	if acts := m.maybeReplicaFault(); acts != nil || m.crashed || m.now < m.stallUntil {
		return acts
	}
	switch {
	case m.failbackDue():
		m.setActive(false)
		return nil
	case m.takeoverDue():
		m.setActive(true)
		return nil
	case m.heartbeatDue():
		return m.emitHeartbeat()
	}
	return nil
}

func (m *Machine) failbackDue() bool {
	return m.active && m.lowerSeen
}

func (m *Machine) takeoverDue() bool {
	take := m.config.TakeoverAfter
	if m.active || take <= 0 {
		return false
	}
	return m.now-m.lastOtherAt > core.NodeTime(take) &&
		m.now-m.lastPullOK < core.NodeTime(take)
}

func (m *Machine) heartbeatDue() bool {
	hb := m.config.Heartbeat
	if !m.active || hb <= 0 {
		return false
	}
	if m.now-m.lastOwnAt <= core.NodeTime(hb) {
		return false
	}
	return len(m.queue) == 0 && len(m.inFlightAppends) == 0 && m.pending == 0
}

func (m *Machine) maybeReplicaFault() []core.Action {
	// Only impair the appender so a hot standby can take over (old fakelogservice
	// never impaired both replicas at once).
	if !m.active || !m.ready {
		return nil
	}
	if m.config.StallProb > 0 && m.env.Fault("logcp_stall", m.config.StallProb) {
		m.stallUntil = m.now + core.NodeTime(betweenDur(m.env, m.config.StallDur))
		m.env.Reachable("logcp-sim: replica stalled")
		return nil
	}
	if m.config.CrashProb > 0 && m.env.Fault("logcp_crash", m.config.CrashProb) {
		return m.softCrash()
	}
	return nil
}

func betweenDur(r interface{ Intn(int) int }, d [2]core.Duration) core.Duration {
	if d[1] <= d[0] {
		return d[0]
	}
	return d[0] + core.Duration(r.Intn(int(d[1]-d[0])+1))
}

func (m *Machine) softCrash() []core.Action {
	m.env.Reachable("logcp-sim: replica crashed")
	m.setActive(false)
	m.ready = false
	m.booting = true
	m.crashed = true
	m.timerGen++ // no catch-up while down
	m.downUntil = m.now + core.NodeTime(betweenDur(m.env, m.config.DownDur))
	m.answers = nil
	m.clearLocal()
	// Refuse client work until afterCrashRestart restores fold + catch-up.
	return m.refuseReads()
}

// isDeleted reports a LogAppend into a trimmed slot (tombstone): Conflict, Taken,
// and no Existing bytes (store.go). Empty-delete mutants look like free slots.
func isDeleted(d core.LogAppendDone) bool {
	return d.Err == core.ErrConflict && d.Taken && len(d.Existing) == 0
}

// hitTrimmed handles append/fill against a tombstone: refuse clients, drop
// local log state, and restore from checkpoint (docs/log-cp-service.md).
// Catch-up from slot 0 after a tomb re-enters the trimmed prefix; needsCheckpoint
// blocks that until a snapshot lands.
func (m *Machine) hitTrimmed() []core.Action {
	m.env.Reachable("logcp-sim: replica hit a trimmed slot")
	m.refuseClients(core.ErrUnavailable, core.MaybeApplied)
	m.setActive(false)
	m.ready = false
	m.booting = true
	m.needsCheckpoint = true
	m.clearLocal()
	acts := m.refuseReads()
	return append(acts, m.afterCrashRestart()...)
}

func (m *Machine) clearLocal() {
	m.queue = nil
	m.inFlightAppends = map[core.RequestID]*inFlightAppend{}
	m.waiters = map[string][]waitingCommand{}
	m.buf = map[core.LogPosition][]byte{}
	m.pending, m.pendingKind = 0, pendingNone
	m.proj = nil
	m.fillSlot, m.fillData = 0, nil
	m.gapArmed = false
}

func (m *Machine) refuseClients(err core.ErrCode, o core.Outcome) {
	refuse := func(c waitingCommand) {
		m.answers = append(m.answers, ReplyToClient{
			Req: c.request, From: c.from,
			Done: core.ControlPlaneDone{Req: c.request, Err: err, Outcome: o},
		})
	}
	// Sort map keys so answer order is stable across runs (TraceHash).
	reqs := make([]core.RequestID, 0, len(m.inFlightAppends))
	for request := range m.inFlightAppends { //determinismcheck:ok keys are collected, then sorted before use
		reqs = append(reqs, request)
	}
	slices.Sort(reqs)
	seen := map[string]bool{}
	for _, request := range reqs {
		f := m.inFlightAppends[request]
		seen[f.id] = true
		for _, c := range f.cmds {
			refuse(c)
		}
	}
	for _, c := range m.queue {
		refuse(c)
	}
	ids := make([]string, 0, len(m.waiters))
	for id := range m.waiters { //determinismcheck:ok keys are collected, then sorted before use
		if !seen[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, c := range m.waiters[id] {
			refuse(c)
		}
	}
}

func (m *Machine) afterCrashRestart() []core.Action {
	// An old catch-up timer must not start a tail check while the restore waits:
	// it would make the node Ready at slot 0 over a trimmed log.
	m.timerGen++
	// Close the old store first: a database may need its directory back.
	_ = m.fold.Close() // the old state is dropped whether or not close succeeds
	fold, err := m.config.openFold()
	if err != nil {
		return m.trip(err)
	}
	m.fold = fold
	m.reseedTopics()
	m.applied, m.high, m.goalTail = 0, 0, 0
	m.seq = 0
	if m.checkpointEnabled() {
		return m.emitCheckpointGet()
	}
	return m.afterRestore()
}

func (m *Machine) emitHeartbeat() []core.Action {
	m.seq++
	id := fmt.Sprintf("%s/%d", m.config.writer(), m.seq)
	inner := stateupdater.Encode(stateupdater.Entry{ID: id, Kind: stateupdater.KindNoop})
	data := wrap(m.config.Rank, m.config.writer(), inner)
	m.nextReq++
	request := m.nextReq
	slot := m.alloc()
	m.inFlightAppends[request] = &inFlightAppend{request: request, slot: slot, id: id, data: data, cmds: nil}
	return []core.Action{core.LogAppend{Req: request, Slot: slot, Data: data}}
}

func (m *Machine) observeWriter(slot core.LogPosition, rank int, writer string, enveloped bool) {
	if !enveloped || writer == "" {
		return
	}
	if writer == m.config.writer() {
		m.lastOwnAt = m.now
		return
	}
	m.lastOtherAt = m.now
	if slot >= m.promoSlot && rank < m.config.Rank {
		m.lowerSeen = true
	}
}

func (m *Machine) notePullOK() {
	m.lastPullOK = m.now
}
