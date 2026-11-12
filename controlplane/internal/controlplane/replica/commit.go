package replica

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// groupMarker prefixes a multi-command commit entry so drain can tell it from a
// single metadb.Encode payload (kinds 0..2).
const groupMarker byte = 0xC5

// ClientOperation is one client command for the commit queue. Prod injects via
// controlplane/server; sim delivers runtime.ControlPlaneDeliver which becomes Offer here.
type ClientOperation struct {
	Req  runtime.RequestID
	Cmd  metadb.Entry         // ID is assigned when the group is built
	From runtime.NodeInstance // broker; empty skips ControlPlaneReply (unit tests)
}

// ReplyToClient is the apply-time result for one ClientOperation.Req. withReplies turns it
// into a ControlPlaneReply for the broker in From.
type ReplyToClient struct {
	Req  runtime.RequestID
	From runtime.NodeInstance
	Done runtime.ControlPlaneDone
}

// waitingCommand is a queued or in-flight client command.
type waitingCommand struct {
	request runtime.RequestID
	from    runtime.NodeInstance // broker; namespaces Req across brokers
	cmd     metadb.Entry
}

// inFlightAppend is one in-flight commit LogAppend (at most one).
type inFlightAppend struct {
	request runtime.RequestID
	slot    runtime.LogPosition
	id      string
	data    []byte
	cmds    []waitingCommand
}

// Offer enqueues a client command and flushes when an in-flight slot is free.
// Answers are replied (ControlPlaneReply) only after the winning slot applies.
// Standby or not-yet-Ready nodes refuse without emitting LogAppend.
//
// A CommitSegment whose key is already in the fold is answered immediately
// (no LogAppend). A retry for a key still queued or in flight retargets that
// waiter to the new Req/From so a bridge timeout cannot orphan the apply
// answer onto a dead broker RequestID.
func (m *Replica) Offer(operation ClientOperation) []any {
	if m.exited {
		return nil
	}
	if !m.canCommit() {
		m.answers = append(m.answers, ReplyToClient{
			Req: operation.Req, From: operation.From,
			Done: runtime.ControlPlaneDone{Req: operation.Req, Err: runtime.ErrUnavailable},
		})
		return nil
	}
	if commit, isCommit := operation.Cmd.CommitSegment(); isCommit {
		if d, ok, err := m.meta.KnownCommit(commit); err == nil && ok {
			d.Req = operation.Req
			m.env.Reachable("logcp-sim: replayed key answered from the window")
			m.answers = append(m.answers, ReplyToClient{Req: operation.Req, From: operation.From, Done: d})
			return nil
		}
		if m.retargetCommit(operation) {
			return nil
		}
	}
	if !m.fitsAlone(operation.Cmd) {
		m.answers = append(m.answers, ReplyToClient{
			Req: operation.Req, From: operation.From,
			Done: runtime.ControlPlaneDone{Req: operation.Req, Err: runtime.ErrTooLarge},
		})
		return nil
	}
	if len(m.queue) >= m.config.queueMax() {
		m.answers = append(m.answers, ReplyToClient{
			Req: operation.Req, From: operation.From,
			Done: runtime.ControlPlaneDone{Req: operation.Req, Err: runtime.ErrUnavailable},
		})
		return nil
	}
	cmd := operation.Cmd
	cmd.ID = ""
	m.queue = append(m.queue, waitingCommand{request: operation.Req, from: operation.From, cmd: cmd})
	return m.flushCommits()
}

// retargetCommit points an in-flight or queued waiter for the same SegmentKey
// at this Offer. The prior broker Req timed out at the bridge; the apply must
// answer the live one. In-flight ReqIDs are sorted so map walk order cannot
// diverge two runs of the same seed.
func (m *Replica) retargetCommit(operation ClientOperation) bool {
	wanted, _ := operation.Cmd.CommitSegment()
	key := wanted.SegmentKey
	retarget := func(c *waitingCommand) bool {
		if queued, ok := c.cmd.CommitSegment(); !ok || queued.SegmentKey != key {
			return false
		}
		c.request, c.from = operation.Req, operation.From
		return true
	}
	for i := range m.queue {
		if retarget(&m.queue[i]) {
			return true
		}
	}
	reqs := make([]runtime.RequestID, 0, len(m.inFlightAppends))
	for request := range m.inFlightAppends { //determinismcheck:ok keys are collected, then sorted before use
		reqs = append(reqs, request)
	}
	slices.Sort(reqs)
	for _, request := range reqs {
		f := m.inFlightAppends[request]
		for i := range f.cmds {
			if retarget(&f.cmds[i]) {
				return true
			}
		}
	}
	return false
}

func (m *Replica) flushCommits() []any {
	if m.exited || m.booting {
		return nil
	}
	// Demote still drains a non-empty queue; Offer already refused new work.
	if !m.isAppender() && len(m.queue) == 0 {
		return nil
	}
	var acts []any
	for len(m.inFlightAppends) == 0 && len(m.queue) > 0 {
		acts = append(acts, m.emitCommitGroup()...)
	}
	return acts
}

func (m *Replica) emitCommitGroup() []any {
	limit := min(m.config.maxCmds(), len(m.queue))
	if limit <= 0 {
		return nil
	}
	m.seq++
	id := fmt.Sprintf("%s/e%d/%d", m.config.writer(), m.epoch, m.seq)

	// Take commands while the entry stays under MaxEntryBytes. The first one
	// always goes: Offer already refused any command that cannot fit alone.
	encoded := make([][]byte, 0, limit)
	size := m.entryFixedBytes(id)
	for _, c := range m.queue[:limit] {
		e := c.cmd
		e.ID = id
		enc := metadb.Encode(e)
		cost := len(enc) + binary.MaxVarintLen32
		if len(encoded) > 0 && size+cost > m.config.maxEntryBytes() {
			break
		}
		size += cost
		encoded = append(encoded, enc)
	}
	n := len(encoded)
	cmds := append([]waitingCommand(nil), m.queue[:n]...)
	m.queue = append(m.queue[:0:0], m.queue[n:]...)

	for i := range cmds {
		e := cmds[i].cmd
		e.ID = id
		cmds[i].cmd = e
	}
	inner := encodeGroupParts(id, encoded)
	data := wrap(m.epoch, m.config.writer(), inner)
	slot := m.alloc()
	m.env.Always(slot >= m.trimFloor, "logcp-sim: a Put never wins a slot below the trim floor",
		runtime.ContextPair{Key: "slot", Value: slot})
	m.env.Always(len(data) <= m.config.maxEntryBytes() || n == 1, "logcp-sim: a group entry stays under the byte cap",
		runtime.ContextPair{Key: "bytes", Value: len(data)})
	m.nextReq++
	request := m.nextReq
	m.inFlightAppends[request] = &inFlightAppend{request: request, slot: slot, id: id, data: data, cmds: cmds}
	m.waiters[id] = cmds
	return []any{runtime.LogAppend{Req: request, Slot: slot, Data: data}}
}

// entryFixedBytes is the most bytes one entry adds around its commands: the
// envelope, the group marker, the id, and the two counts. Each command also
// costs a length prefix, which emitCommitGroup adds per command.
func (m *Replica) entryFixedBytes(id string) int {
	return len(wrap(math.MaxUint64, m.config.writer(), nil)) + 1 + binary.MaxVarintLen64 + len(id) + binary.MaxVarintLen64
}

// fitsAlone reports whether cmd fits in a log entry of its own. The id is
// sized for the largest sequence number, so a command that fits now fits later.
func (m *Replica) fitsAlone(cmd metadb.Entry) bool {
	id := m.config.writer() + "/" + strings.Repeat("9", 20)
	cmd.ID = id
	size := m.entryFixedBytes(id) + len(metadb.Encode(cmd)) + binary.MaxVarintLen32
	return size <= m.config.maxEntryBytes()
}

func (m *Replica) alloc() runtime.LogPosition {
	n := max(m.high, m.applied)
	m.high = n + 1
	return n
}

func (m *Replica) observeSlot(n runtime.LogPosition) {
	m.high = max(m.high, n+1)
}

func (m *Replica) onCommitAppendDone(d runtime.LogAppendDone, f *inFlightAppend) []any {
	if isBelowFloor(d) {
		// The slot is at or below the floor of the log: this node is far behind
		// and the put is not a commit. Refuse the clients and restore.
		m.env.Reachable("logcp-sim: put refused at or below the floor")
		return m.hitTrimmed()
	}
	delete(m.inFlightAppends, d.Req)
	if d.Err != runtime.OK || d.Outcome != runtime.Applied {
		// Conditional put: NotApplied and MaybeApplied both retry the same
		// slot. Allocating a later slot here would leave an empty slot, and
		// drain would stop there for good.
		if d.Outcome == runtime.MaybeApplied {
			m.env.Reachable("logcp-sim: Put not answered, same slot repeated")
		}
		acts := m.retrySameSlot(f)
		return append(acts, m.flushCommits()...)
	}
	m.observeSlot(f.slot)
	data := f.data
	lost := false
	if d.Taken {
		data = bytes.Clone(d.Existing)
		lost = !groupIDMatches(data, f.id)
	}
	if f.slot >= m.applied {
		m.buf[f.slot] = data
	}
	if err := m.drain(); err != nil {
		return m.trip(err)
	}
	var acts []any
	if m.fencedFlag {
		acts = m.stepDown()
		return append(acts, m.progress()...)
	}
	if lost {
		acts = append(acts, m.retryInFlightAppend(f)...)
	}
	acts = append(acts, m.progress()...)
	acts = append(acts, m.flushCommits()...)
	return acts
}

// retrySameSlot repeats the Put at f.slot (conditional append; slot still ours).
func (m *Replica) retrySameSlot(f *inFlightAppend) []any {
	m.nextReq++
	request := m.nextReq
	m.inFlightAppends[request] = &inFlightAppend{request: request, slot: f.slot, id: f.id, data: f.data, cmds: f.cmds}
	return []any{runtime.LogAppend{Req: request, Slot: f.slot, Data: f.data}}
}

// retryInFlightAppend appends the same entry (same id and cmds) at the next free slot
// after another writer won f.slot.
func (m *Replica) retryInFlightAppend(f *inFlightAppend) []any {
	m.env.Reachable("logcp-sim: lost slot retried at a later slot")
	slot := m.alloc()
	m.nextReq++
	request := m.nextReq
	m.inFlightAppends[request] = &inFlightAppend{request: request, slot: slot, id: f.id, data: f.data, cmds: f.cmds}
	return []any{runtime.LogAppend{Req: request, Slot: slot, Data: f.data}}
}

// encodeGroupParts packs commands that are already encoded.
func encodeGroupParts(id string, encoded [][]byte) []byte {
	b := []byte{groupMarker}
	b = binary.AppendUvarint(b, uint64(len(id)))
	b = append(b, id...)
	b = binary.AppendUvarint(b, uint64(len(encoded)))
	for _, enc := range encoded {
		b = binary.AppendUvarint(b, uint64(len(enc)))
		b = append(b, enc...)
	}
	return b
}

func decodeGroup(data []byte) (string, []metadb.Entry, bool) {
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
	cmds := make([]metadb.Entry, 0, n)
	for range n {
		ln, r2, ok := takeUvarint(rest)
		if !ok || ln > uint64(len(r2)) {
			return "", nil, false
		}
		e, err := metadb.Decode(r2[:ln])
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

func (m *Replica) applySlot(slot runtime.LogPosition, data []byte) error {
	epoch, writer, payload, ok := unwrap(data)
	if !ok {
		return fmt.Errorf("replica: truncated log envelope at slot %d", slot)
	}
	enveloped := len(data) > 0 && data[0] == envelopeMarker
	m.observeWriter(epoch, writer, enveloped)
	if m.epoch > 0 && !m.active && enveloped && writer == m.config.writer() && epoch == m.epoch {
		// Our fence entry is applied: this node is the writer now.
		m.active = true
		m.env.Reachable("logcp-sim: standby took over")
	}

	if id, cmds, ok := decodeGroup(payload); ok {
		dones := make([]runtime.ControlPlaneDone, len(cmds))
		for i, c := range cmds {
			dones[i] = m.meta.Apply(int64(slot), c)
		}
		m.deliver(id, dones)
		m.afterApply(slot)
		return nil
	}
	d, err := m.meta.ApplyBytes(int64(slot), payload)
	if err != nil {
		return err
	}
	// Single-entry catch-up: deliver only if we were waiting on it.
	if e, err := metadb.Decode(payload); err == nil {
		m.deliver(e.ID, []runtime.ControlPlaneDone{d})
	}
	m.afterApply(slot)
	return nil
}

func (m *Replica) afterApply(slot runtime.LogPosition) {
	m.env.Reachable("logcp-sim: replayed key answered from the window")
	window := m.config.WindowSlots
	stride := m.config.TrimStride
	if stride <= 0 {
		stride = 16
	}
	if window > 0 && int64(slot)%stride == 0 && int64(slot) >= window {
		_, _ = m.meta.Trim(int64(slot) - window)
	}
}

func (m *Replica) deliver(id string, dones []runtime.ControlPlaneDone) {
	ps, ok := m.waiters[id]
	if !ok {
		return
	}
	delete(m.waiters, id)
	for i, p := range ps {
		d := runtime.ControlPlaneDone{Req: p.request, Err: runtime.ErrInvalid}
		if i < len(dones) {
			d = dones[i]
			d.Req = p.request
			if d.Err == runtime.OK && d.Outcome == runtime.Applied && !m.isAppender() {
				m.env.Reachable("logcp-sim: idempotent replay after takeover")
			}
		}
		m.answers = append(m.answers, ReplyToClient{Req: p.request, From: p.from, Done: d})
	}
}
