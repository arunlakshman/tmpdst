package statemachine

import (
	"encoding/binary"
	"fmt"
	"slices"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/core"
)

// groupMarker prefixes a multi-command commit entry so drain can tell it from a
// single stateupdater.Encode payload (kinds 0..2).
const groupMarker byte = 0xC5

// ClientOperation is one client command for the commit queue. Prod injects via
// controlplane/server; sim delivers core.ControlPlaneDeliver which becomes Offer here.
type ClientOperation struct {
	Req  core.RequestID
	Cmd  stateupdater.Entry // ID is assigned when the group is built
	From core.NodeInstance  // broker; empty skips ControlPlaneReply (unit tests)
}

// ReplyToClient is the apply-time result for one ClientOperation.Req. TakeAnswers drains them.
type ReplyToClient struct {
	Req  core.RequestID
	From core.NodeInstance
	Done core.ControlPlaneDone
}

// waitingCommand is a queued or in-flight client command.
type waitingCommand struct {
	req  core.RequestID
	from core.NodeInstance // broker; namespaces Req across brokers
	cmd  stateupdater.Entry
}

// inFlightAppend is one in-inFlightAppend commit LogAppend (one of at most K).
type inFlightAppend struct {
	req  core.RequestID
	slot core.LogPosition
	id   string
	data []byte
	cmds []waitingCommand
}

// Offer enqueues a client command and flushes when an in-flight slot is free.
// Answers arrive via TakeAnswers only after the winning slot applies.
// Standby or not-yet-Ready nodes refuse without emitting LogAppend.
//
// A CommitSegment whose key is already in the fold is answered immediately
// (no LogAppend). A retry for a key still queued or in flight retargets that
// waiter to the new Req/From so a bridge timeout cannot orphan the apply
// answer onto a dead broker RequestID.
func (m *Machine) Offer(op ClientOperation) []core.Action {
	if m.exited {
		return nil
	}
	if m.cfg.K <= 0 || !m.canCommit() {
		m.answers = append(m.answers, ReplyToClient{
			Req: op.Req, From: op.From,
			Done: core.ControlPlaneDone{Req: op.Req, Err: core.ErrUnavailable},
		})
		return nil
	}
	if op.Cmd.Kind == stateupdater.KindCommit {
		if d, ok, err := m.fold.KnownCommit(op.Cmd.Commit); err == nil && ok {
			d.Req = op.Req
			m.env.Reachable("logcp-sim: replayed key answered from the window")
			m.answers = append(m.answers, ReplyToClient{Req: op.Req, From: op.From, Done: d})
			return nil
		}
		if m.retargetCommit(op) {
			return nil
		}
	}
	if len(m.queue) >= m.cfg.queueMax() {
		m.answers = append(m.answers, ReplyToClient{
			Req: op.Req, From: op.From,
			Done: core.ControlPlaneDone{Req: op.Req, Err: core.ErrUnavailable},
		})
		return nil
	}
	cmd := op.Cmd
	cmd.ID = ""
	m.queue = append(m.queue, waitingCommand{req: op.Req, from: op.From, cmd: cmd})
	return m.flushCommits()
}

// retargetCommit points an in-flight or queued waiter for the same SegmentKey
// at this Offer. The prior broker Req timed out at the bridge; the apply must
// answer the live one. Flight ReqIDs are sorted so map walk order cannot
// diverge two runs of the same seed.
func (m *Machine) retargetCommit(op ClientOperation) bool {
	key := op.Cmd.Commit.SegmentKey
	retarget := func(c *waitingCommand) bool {
		if c.cmd.Kind != stateupdater.KindCommit || c.cmd.Commit.SegmentKey != key {
			return false
		}
		c.req, c.from = op.Req, op.From
		return true
	}
	for i := range m.queue {
		if retarget(&m.queue[i]) {
			return true
		}
	}
	reqs := make([]core.RequestID, 0, len(m.flights))
	for req := range m.flights { //detcheck:ok keys are collected, then sorted before use
		reqs = append(reqs, req)
	}
	slices.Sort(reqs)
	for _, req := range reqs {
		f := m.flights[req]
		for i := range f.cmds {
			if retarget(&f.cmds[i]) {
				return true
			}
		}
	}
	return false
}

// TakeAnswers returns and clears apply-time answers for prior Offers.
func (m *Machine) TakeAnswers() []ReplyToClient {
	out := m.answers
	m.answers = nil
	return out
}

// InFlightCommits is the number of live commit LogAppend ReqIDs (at most K).
func (m *Machine) InFlightCommits() int { return len(m.flights) }

// QueueDepth is the number of client ops waiting for a commit slot.
func (m *Machine) QueueDepth() int { return len(m.queue) }

func (m *Machine) flushCommits() []core.Action {
	if m.cfg.K <= 0 || m.exited || m.booting {
		return nil
	}
	// Demote still drains a non-empty queue; Offer already refused new work.
	if !m.isAppender() && len(m.queue) == 0 {
		return nil
	}
	var acts []core.Action
	for len(m.flights) < m.cfg.K && len(m.queue) > 0 {
		acts = append(acts, m.emitCommitGroup()...)
	}
	return acts
}

func (m *Machine) emitCommitGroup() []core.Action {
	n := min(m.cfg.maxCmds(), len(m.queue))
	if n <= 0 {
		return nil
	}
	cmds := append([]waitingCommand(nil), m.queue[:n]...)
	m.queue = append(m.queue[:0:0], m.queue[n:]...)

	m.seq++
	id := fmt.Sprintf("%s/%d", m.cfg.writer(), m.seq)
	entries := make([]stateupdater.Entry, len(cmds))
	for i, c := range cmds {
		e := c.cmd
		e.ID = id
		if m.cfg.InjectedBug == InjectedBugWriterOffsets && e.Kind == stateupdater.KindCommit {
			e.WriterBases = m.projectBases(e.Commit)
		}
		entries[i] = e
		cmds[i].cmd = e
	}
	inner := encodeGroup(id, entries)
	data := wrap(m.cfg.Rank, m.cfg.writer(), inner)
	slot := m.alloc()
	m.env.Always(slot >= m.trimFloor, "logcp-sim: a Put never wins a slot below the trim floor",
		core.ContextPair{Key: "slot", Value: slot})
	m.nextReq++
	req := m.nextReq
	m.flights[req] = &inFlightAppend{req: req, slot: slot, id: id, data: data, cmds: cmds}
	m.waiters[id] = cmds
	return []core.Action{core.LogAppend{Req: req, Slot: slot, Data: data}}
}

// projectBases is InjectedBugWriterOffsets: skip one offset per partition so a retry
// leaves a gap in the committed log.
func (m *Machine) projectBases(c core.CommitSegment) []core.Offset {
	if m.proj == nil {
		m.proj = map[core.TopicPartition]core.Offset{}
	}
	bases := make([]core.Offset, len(c.Batches))
	for i, b := range c.Batches {
		e, ok := m.proj[b.TP]
		if !ok {
			e = m.fold.End(b.TP) + 1 // skip one
		}
		bases[i] = e
		m.proj[b.TP] = e + core.Offset(b.Records)
	}
	return bases
}

func (m *Machine) alloc() core.LogPosition {
	n := max(m.high, m.applied)
	m.high = n + 1
	return n
}

func (m *Machine) observeSlot(n core.LogPosition) {
	m.high = max(m.high, n+1)
}

func (m *Machine) onCommitAppendDone(d core.LogAppendDone, f *inFlightAppend) []core.Action {
	if isDeleted(d) {
		// Slot was trimmed under us; refuse in-flight clients and restore.
		return m.hitTrimmed()
	}
	delete(m.flights, d.Req)
	if d.Err != core.OK || d.Outcome != core.Applied {
		// Conditional put: NotApplied and MaybeApplied both retry the same
		// slot. Allocating a later slot here used to leave a permanent hole
		// (progress was not armed), wedging drain so every later commit sat
		// in buf unapplied and brokers saw only bridge timeouts.
		if d.Outcome == core.MaybeApplied {
			m.env.Reachable("logcp-sim: Put not answered, same slot repeated")
		}
		acts := m.retrySameSlot(f)
		return append(acts, m.flushCommits()...)
	}
	m.observeSlot(f.slot)
	data := f.data
	lost := false
	if d.Taken {
		data = append([]byte(nil), d.Existing...)
		lost = !groupIDMatches(data, f.id)
	}
	if f.slot >= m.applied {
		m.buf[f.slot] = data
	}
	if err := m.drain(); err != nil {
		return m.trip(err)
	}
	var acts []core.Action
	if lost {
		acts = append(acts, m.retryFlight(f)...)
	}
	acts = append(acts, m.progress()...)
	acts = append(acts, m.flushCommits()...)
	return acts
}

// retrySameSlot repeats the Put at f.slot (conditional append; slot still ours).
func (m *Machine) retrySameSlot(f *inFlightAppend) []core.Action {
	m.nextReq++
	req := m.nextReq
	m.flights[req] = &inFlightAppend{req: req, slot: f.slot, id: f.id, data: f.data, cmds: f.cmds}
	return []core.Action{core.LogAppend{Req: req, Slot: f.slot, Data: f.data}}
}

// retryFlight appends the same entry (same id and cmds) at the next free slot
// after another writer won f.slot.
func (m *Machine) retryFlight(f *inFlightAppend) []core.Action {
	m.env.Reachable("logcp-sim: lost slot retried at a later slot")
	slot := m.alloc()
	m.nextReq++
	req := m.nextReq
	m.flights[req] = &inFlightAppend{req: req, slot: slot, id: f.id, data: f.data, cmds: f.cmds}
	return []core.Action{core.LogAppend{Req: req, Slot: slot, Data: f.data}}
}

func encodeGroup(id string, cmds []stateupdater.Entry) []byte {
	b := []byte{groupMarker}
	b = binary.AppendUvarint(b, uint64(len(id)))
	b = append(b, id...)
	b = binary.AppendUvarint(b, uint64(len(cmds)))
	for _, c := range cmds {
		enc := stateupdater.Encode(c)
		b = binary.AppendUvarint(b, uint64(len(enc)))
		b = append(b, enc...)
	}
	return b
}

func decodeGroup(data []byte) (string, []stateupdater.Entry, bool) {
	_, _, payload, ok := unwrap(data)
	if !ok {
		return "", nil, false
	}
	data = payload
	if len(data) == 0 || data[0] != groupMarker {
		return "", nil, false
	}
	rest := data[1:]
	n, rest, ok := takeUvarint(rest)
	if !ok || n > uint64(len(rest)) {
		return "", nil, false
	}
	id := string(rest[:n])
	rest = rest[n:]
	n, rest, ok = takeUvarint(rest)
	if !ok {
		return "", nil, false
	}
	cmds := make([]stateupdater.Entry, 0, n)
	for range n {
		ln, r2, ok := takeUvarint(rest)
		if !ok || ln > uint64(len(r2)) {
			return "", nil, false
		}
		e, err := stateupdater.Decode(r2[:ln])
		if err != nil {
			return "", nil, false
		}
		cmds = append(cmds, e)
		rest = r2[ln:]
	}
	if len(rest) != 0 {
		return "", nil, false
	}
	return id, cmds, true
}

func groupIDMatches(data []byte, id string) bool {
	got, _, ok := decodeGroup(data)
	return ok && got == id
}

func takeUvarint(b []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(b)
	if n <= 0 {
		return 0, b, false
	}
	return v, b[n:], true
}

func (m *Machine) applySlot(slot core.LogPosition, data []byte) error {
	rank, writer, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("statemachine: truncated log envelope at slot %d", slot)
	}
	enveloped := len(data) > 0 && data[0] == envelopeMarker
	m.observeWriter(slot, rank, writer, enveloped)

	if id, cmds, ok := decodeGroup(payload); ok {
		if ps, ok := m.waiters[id]; ok {
			for i := range cmds {
				if i < len(ps) && len(ps[i].cmd.WriterBases) > 0 {
					cmds[i].WriterBases = ps[i].cmd.WriterBases
				}
			}
		}
		dones := make([]core.ControlPlaneDone, len(cmds))
		for i, c := range cmds {
			dones[i] = m.fold.Apply(int64(slot), c)
		}
		m.deliver(id, dones)
		m.afterApply(slot)
		return nil
	}
	d, err := m.fold.ApplyBytes(int64(slot), payload)
	if err != nil {
		return err
	}
	// Single-entry catch-up / hole-fill: deliver only if we were waiting on it.
	if e, err := stateupdater.Decode(payload); err == nil {
		m.deliver(e.ID, []core.ControlPlaneDone{d})
	}
	m.afterApply(slot)
	return nil
}

func (m *Machine) afterApply(slot core.LogPosition) {
	m.env.Reachable("logcp-sim: replayed key answered from the window")
	window := m.cfg.WindowSlots
	stride := m.cfg.TrimStride
	if stride <= 0 {
		stride = 16
	}
	if m.cfg.InjectedBug == InjectedBugPerReplicaWindow && window > 0 {
		// Mutant: forget by this replica's applied clock, not log stride.
		_, _ = m.fold.Trim(int64(slot) - window + 1)
		m.env.Always(false, "logcp-sim: a replica's state equals the replay of the log at its slot",
			core.ContextPair{Key: "window", Value: window})
		return
	}
	if window > 0 && int64(slot)%stride == 0 && int64(slot) >= window {
		_, _ = m.fold.Trim(int64(slot) - window)
	}
}

func (m *Machine) deliver(id string, dones []core.ControlPlaneDone) {
	ps, ok := m.waiters[id]
	if !ok {
		return
	}
	delete(m.waiters, id)
	for i, p := range ps {
		d := core.ControlPlaneDone{Req: p.req, Err: core.ErrInvalid}
		if i < len(dones) {
			d = dones[i]
			d.Req = p.req
			if d.Err == core.OK && d.Outcome == core.Applied && !m.isAppender() {
				m.env.Reachable("logcp-sim: idempotent replay after takeover")
			}
		}
		m.answers = append(m.answers, ReplyToClient{Req: p.req, From: p.from, Done: d})
	}
}
