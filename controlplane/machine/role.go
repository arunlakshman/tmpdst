package machine

import (
	"fmt"

	"manifester/controlplane/applier"
	"manifester/core"
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
	return m.cfg.Heartbeat > 0 || m.cfg.TakeoverAfter > 0
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
	a, b := m.cfg.Heartbeat, m.cfg.TakeoverAfter
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
	take := m.cfg.TakeoverAfter
	switch {
	case m.active && m.lowerSeen:
		m.setActive(false)
		return nil
	case !m.active && take > 0 &&
		m.now-m.lastOtherAt > core.Time(take) &&
		m.now-m.lastPullOK < core.Time(take):
		m.setActive(true)
		return nil
	case m.active && m.cfg.Heartbeat > 0 &&
		m.now-m.lastOwnAt > core.Time(m.cfg.Heartbeat) &&
		len(m.queue) == 0 && len(m.flights) == 0 && m.pending == 0:
		return m.emitHeartbeat()
	}
	return nil
}

func (m *Machine) maybeReplicaFault() []core.Action {
	// Only impair the appender so a hot standby can take over (old logengine
	// never impaired both replicas at once).
	if !m.active || !m.ready {
		return nil
	}
	if m.cfg.StallProb > 0 && m.env.Fault("logcp_stall", m.cfg.StallProb) {
		m.stallUntil = m.now + core.Time(betweenDur(m.env, m.cfg.StallDur))
		m.env.Reachable("logcp-sim: replica stalled")
		return nil
	}
	if m.cfg.CrashProb > 0 && m.env.Fault("logcp_crash", m.cfg.CrashProb) {
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
	m.downUntil = m.now + core.Time(betweenDur(m.env, m.cfg.DownDur))
	m.queue = nil
	m.flights = map[core.ReqID]*flight{}
	m.waiters = map[string][]pendingCmd{}
	m.answers = nil
	m.buf = map[core.LogSlot][]byte{}
	m.pending, m.pendingKind = 0, pendingNone
	m.proj = nil
	// Refuse client work until afterCrashRestart restores fold + catch-up.
	return nil
}

func (m *Machine) afterCrashRestart() []core.Action {
	m.fold = applier.NewMem()
	m.reseedTopics()
	m.applied, m.high, m.goalTail = 0, 0, 0
	m.seq = 0
	if m.ckptEnabled() {
		return m.emitCkptGet()
	}
	return m.afterRestore()
}

func (m *Machine) emitHeartbeat() []core.Action {
	m.seq++
	id := fmt.Sprintf("%s/%d", m.cfg.writer(), m.seq)
	inner := applier.Encode(applier.Entry{ID: id, Kind: applier.KindNoop})
	data := wrap(m.cfg.Rank, m.cfg.writer(), inner)
	m.nextReq++
	req := m.nextReq
	slot := m.alloc()
	m.flights[req] = &flight{req: req, slot: slot, id: id, data: data, cmds: nil}
	return []core.Action{core.LogAppend{Req: req, Slot: slot, Data: data}}
}

func (m *Machine) observeWriter(slot core.LogSlot, rank int, writer string, enveloped bool) {
	if !enveloped || writer == "" {
		return
	}
	if writer == m.cfg.writer() {
		m.lastOwnAt = m.now
		return
	}
	m.lastOtherAt = m.now
	if slot >= m.promoSlot && rank < m.cfg.Rank {
		m.lowerSeen = true
	}
}

func (m *Machine) notePullOK() {
	m.lastPullOK = m.now
}
