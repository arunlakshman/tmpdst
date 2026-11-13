package replica

import (
	"fmt"
	"slices"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
	"github.com/manifesterio/mds/dst/machine"
)

const (
	timerRole = machine.TimerID(3)
)

// One node holds the write role at a time (docs/writer-fencing-design.md). A
// node becomes the writer by claiming a new epoch in the log (a compare-and-swap
// on one item), and then it writes a fence entry at the tail. Every entry it
// writes carries the epoch. A node that loses a slot to a newer epoch, or that
// reads an entry from a newer epoch, stops writing and stays a standby: it keeps
// reading and serving reads. The first node (rank 0) claims at once on an empty
// log. Every other claim waits for TakeoverAfter of silence from other writers
// while the node's own reads still work.

// Ready reports that restore (if any) and the first catch-up round finished.
func (m *Replica) Ready() bool { return m.ready }

// IsActive reports the advisory appender role.
func (m *Replica) IsActive() bool { return m.active }

// Epoch is the writer epoch this node holds. It is zero when the node is not the writer.
func (m *Replica) Epoch() uint64 { return m.epoch }

// StepDowns is how many times this node stopped being the writer because it saw
// a newer epoch.
func (m *Replica) StepDowns() uint64 { return m.stepDowns }

// Promote makes this node the writer at once, without a claim in the log. It is
// a test and operator hook: it does not fence another writer.
func (m *Replica) Promote() {
	m.epoch = m.maxEpochSeen + 1
	m.maxEpochSeen = m.epoch
	m.active = true
}

// Demote makes this node a standby; in-flight appends still finish.
func (m *Replica) Demote() { m.dropRole() }

// dropRole clears everything that makes this node the writer.
func (m *Replica) dropRole() {
	m.epoch, m.active, m.claimReq, m.fencedFlag = 0, false, 0, false
}

func (m *Replica) rolesArmed() bool {
	return m.config.Heartbeat > 0 || m.config.TakeoverAfter > 0
}

func (m *Replica) isAppender() bool {
	if !m.rolesArmed() {
		return true
	}
	return m.active
}

func (m *Replica) canCommit() bool {
	if m.booting || m.crashed || m.now < m.stallUntil {
		return false
	}
	if m.rolesArmed() {
		return m.ready && m.active
	}
	return true
}

func (m *Replica) roleEvery() machine.Duration {
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

func (m *Replica) armRole() []runtime.Action {
	every := m.roleEvery()
	if every <= 0 || m.exited {
		return nil
	}
	m.roleGen++
	return []runtime.Action{arm(timerRole, m.roleGen, every)}
}

func (m *Replica) onRoleTimer(e runtime.TimerFired) []runtime.Action {
	if e.Gen != m.roleGen || !m.rolesArmed() {
		return nil
	}
	acts := m.roleTick()
	return append(acts, m.armRole()...)
}

func (m *Replica) roleTick() []runtime.Action {
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
	case m.claimDue():
		return m.emitClaim()
	case m.heartbeatDue():
		return m.emitHeartbeat()
	}
	return nil
}

// canClaim reports that this node is fully caught up and may ask for the write
// role. A node that is restoring or catching up never claims and never writes.
func (m *Replica) canClaim() bool {
	return m.rolesArmed() && !m.active && m.epoch == 0 && m.claimReq == 0 &&
		m.ready && !m.booting && !m.crashed && m.now >= m.stallUntil &&
		m.pending == 0 && !m.checkpointRetry && !m.needsCheckpoint && m.applied >= m.goalTail
}

// claimDue reports that the silence rule says to claim the write role now.
func (m *Replica) claimDue() bool {
	take := m.config.TakeoverAfter
	if take <= 0 || !m.canClaim() {
		return false
	}
	wait := machine.NodeTime(take) + machine.NodeTime(m.config.Rank)*machine.NodeTime(m.config.TakeoverRankStep)
	return m.now-m.lastOtherAt > wait && m.now-m.lastPullOK < machine.NodeTime(take)
}

// claimNow claims at boot for rank 0 when the log has no writer epoch yet.
func (m *Replica) claimNow() []runtime.Action {
	if m.config.Rank != 0 || m.maxEpochSeen != 0 || !m.canClaim() {
		return nil
	}
	return m.emitClaim()
}

func (m *Replica) emitClaim() []runtime.Action {
	m.nextReq++
	m.claimReq = m.nextReq
	m.claimExpect = m.maxEpochSeen
	return []runtime.Action{runtime.LogClaimEpoch{Req: m.claimReq, Expect: m.claimExpect, Owner: m.config.writer()}}
}

// onClaimDone handles the answer to a claim. A won claim makes this node the
// writer once its fence entry is applied. A lost claim only teaches the node
// the epoch that is stored now, and it waits a full silence period again.
func (m *Replica) onClaimDone(d runtime.LogClaimEpochDone) []runtime.Action {
	if m.claimReq == 0 || d.Req != m.claimReq {
		return nil
	}
	m.claimReq = 0
	if d.Err != runtime.OK {
		m.lastOtherAt = m.now
		return nil
	}
	m.maxEpochSeen = max(m.maxEpochSeen, d.Epoch)
	// A claim that timed out may have landed: the log then shows our own name.
	if !d.Won && (d.Owner != m.config.writer() || d.Epoch != m.claimExpect+1) {
		m.lastOtherAt = m.now
		return nil
	}
	m.epoch = d.Epoch
	m.env.Reachable(epochClaimed)
	return m.emitHeartbeat() // the fence entry: a no-op at the tail with the new epoch
}

// stepDown ends the write role after a newer epoch is seen. The node refuses
// the clients it was serving, keeps reading, and waits before it claims again.
func (m *Replica) stepDown() []runtime.Action {
	m.env.Reachable(writerFencedNowReadOnly)
	m.stepDowns++
	m.refuseClients(runtime.ErrUnavailable, runtime.MaybeApplied)
	m.dropRole()
	m.queue = nil
	m.inFlightAppends = map[machine.RequestID]*inFlightAppend{}
	m.waiters = map[string][]waitingCommand{}
	m.lastOtherAt = m.now
	return nil
}

func (m *Replica) heartbeatDue() bool {
	hb := m.config.Heartbeat
	if !m.active || !m.ready || m.booting || hb <= 0 {
		return false
	}
	if m.now-m.lastOwnAt <= machine.NodeTime(hb) {
		return false
	}
	return len(m.queue) == 0 && len(m.inFlightAppends) == 0 && m.pending == 0
}

func (m *Replica) maybeReplicaFault() []runtime.Action {
	// Only impair the appender so a hot standby can take over (old fakelogservice
	// never impaired both replicas at once).
	if !m.active || !m.ready {
		return nil
	}
	if m.config.StallProb > 0 && m.env.Fault(siteStall, m.config.StallProb) {
		m.stallUntil = m.now + machine.NodeTime(betweenDur(m.env, m.config.StallDur))
		m.env.Reachable(replicaStalled)
		return nil
	}
	if m.config.CrashProb > 0 && m.env.Fault(siteCrash, m.config.CrashProb) {
		return m.softCrash()
	}
	return nil
}

func betweenDur(r interface{ Intn(int) int }, d [2]machine.Duration) machine.Duration {
	if d[1] <= d[0] {
		return d[0]
	}
	return d[0] + machine.Duration(r.Intn(int(d[1]-d[0])+1))
}

func (m *Replica) softCrash() []runtime.Action {
	m.env.Reachable(replicaCrashed)
	m.dropRole()
	m.ready = false
	m.booting = true
	m.crashed = true
	m.timerGen++ // no catch-up while down
	m.downUntil = m.now + machine.NodeTime(betweenDur(m.env, m.config.DownDur))
	m.answers = nil
	m.clearLocal()
	// Refuse client work until afterCrashRestart restores fold + catch-up.
	return m.refuseReads()
}

// isBelowFloor reports a LogAppend that the log refused because its slot is at
// or below the floor (the trimmed part of the log). The put is not a commit.
// The log answers it as a conflict with no existing entry; a slot that is taken
// by a real entry always carries its bytes.
func isBelowFloor(d runtime.LogAppendDone) bool {
	return d.Err == runtime.ErrConflict && d.Taken && len(d.Existing) == 0
}

// hitTrimmed handles a read that found its next slot trimmed: refuse clients,
// drop local log state, and restore from the checkpoint. Catch-up from slot 0
// would re-enter the trimmed prefix, so needsCheckpoint blocks that until a
// snapshot is found.
func (m *Replica) hitTrimmed() []runtime.Action {
	m.env.Reachable(replicaHitTrimmedSlot)
	m.refuseClients(runtime.ErrUnavailable, runtime.MaybeApplied)
	m.dropRole()
	m.ready = false
	m.booting = true
	m.needsCheckpoint = true
	m.clearLocal()
	acts := m.refuseReads()
	return append(acts, m.afterCrashRestart()...)
}

func (m *Replica) clearLocal() {
	m.queue = nil
	m.inFlightAppends = map[machine.RequestID]*inFlightAppend{}
	m.waiters = map[string][]waitingCommand{}
	m.buf = map[runtime.LogPosition][]byte{}
	m.pending, m.pendingKind = 0, pendingNone
	m.checkpointRequest, m.checkpointStage, m.trimThrough = 0, 0, 0
	m.checkpointPending, m.checkpointData = 0, nil
}

func (m *Replica) refuseClients(err runtime.ErrCode, o runtime.Outcome) {
	refuse := func(c waitingCommand) {
		m.answers = append(m.answers, ReplyToClient{
			Req: c.request, From: c.from,
			Done: runtime.ControlPlaneDone{Req: c.request, Err: err, Outcome: o},
		})
	}
	// Sort map keys so answer order is stable across runs (TraceHash).
	reqs := make([]machine.RequestID, 0, len(m.inFlightAppends))
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

func (m *Replica) afterCrashRestart() []runtime.Action {
	// An old catch-up timer must not start a tail check while the restore waits:
	// it would make the node Ready at slot 0 over a trimmed log.
	m.timerGen++
	// Close the old store first: a database may need its directory back.
	_ = m.meta.Close() // the old state is dropped whether or not close succeeds
	meta, err := m.config.openMetadata()
	if err != nil {
		return m.trip(err)
	}
	m.meta = meta
	m.reseedTopics()
	m.applied, m.high, m.goalTail = 0, 0, 0
	m.tailChecked = false
	m.seq = 0
	if m.checkpointEnabled() {
		return m.emitCheckpointList()
	}
	return m.afterRestore()
}

func (m *Replica) emitHeartbeat() []runtime.Action {
	m.seq++
	id := fmt.Sprintf("%s/e%d/%d", m.config.writer(), m.epoch, m.seq)
	inner := metadb.Encode(metadb.Entry{ID: id, Kind: metadb.KindNoop})
	data := wrap(m.epoch, m.config.writer(), inner)
	m.nextReq++
	request := m.nextReq
	slot := m.alloc()
	m.inFlightAppends[request] = &inFlightAppend{request: request, slot: slot, id: id, data: data, cmds: nil}
	return []runtime.Action{runtime.LogAppend{Req: request, Slot: slot, Data: data}}
}

func (m *Replica) observeWriter(epoch uint64, writer string, enveloped bool) {
	if !enveloped || writer == "" {
		return
	}
	m.maxEpochSeen = max(m.maxEpochSeen, epoch)
	if writer == m.config.writer() {
		m.lastOwnAt = m.now
		return
	}
	m.lastOtherAt = m.now
	if m.epoch > 0 && epoch > m.epoch {
		m.fencedFlag = true
	}
}

func (m *Replica) notePullOK() {
	m.lastPullOK = m.now
}
