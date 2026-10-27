package machine

import (
	"encoding/binary"
	"fmt"

	"manifester/controlplane/applier"
	"manifester/core"
)

// groupMagic prefixes a multi-command commit entry so drain can tell it from a
// single applier.Encode payload (kinds 0..2).
const groupMagic byte = 0xC5

// ClientOp is one client command for the commit queue. The gRPC adapter (later)
// and unit tests inject these via Offer; core.EventBody stays closed until the
// cutover PR adds inbound RPC events.
type ClientOp struct {
	Req core.ReqID
	Cmd applier.Entry // ID is assigned when the group is built
}

// Answer is the apply-time result for one ClientOp.Req. TakeAnswers drains them.
type Answer struct {
	Req  core.ReqID
	Done core.CPDone
}

// pendingCmd is a queued or in-flight client command.
type pendingCmd struct {
	req core.ReqID
	cmd applier.Entry
}

// flight is one in-flight commit LogAppend (one of at most K).
type flight struct {
	req  core.ReqID
	slot core.LogSlot
	id   string
	data []byte
	cmds []pendingCmd
}

// Offer enqueues a client command and flushes when an in-flight slot is free.
// Answers arrive via TakeAnswers only after the winning slot applies.
func (m *Machine) Offer(op ClientOp) []core.Action {
	if m.exited {
		return nil
	}
	if m.cfg.K <= 0 {
		m.answers = append(m.answers, Answer{
			Req:  op.Req,
			Done: core.CPDone{Req: op.Req, Err: core.ErrUnavailable},
		})
		return nil
	}
	if len(m.queue) >= m.cfg.queueMax() {
		m.answers = append(m.answers, Answer{
			Req:  op.Req,
			Done: core.CPDone{Req: op.Req, Err: core.ErrUnavailable},
		})
		return nil
	}
	cmd := op.Cmd
	cmd.ID = ""
	m.queue = append(m.queue, pendingCmd{req: op.Req, cmd: cmd})
	return m.flushCommits()
}

// TakeAnswers returns and clears apply-time answers for prior Offers.
func (m *Machine) TakeAnswers() []Answer {
	out := m.answers
	m.answers = nil
	return out
}

// InFlightCommits is the number of live commit LogAppend ReqIDs (at most K).
func (m *Machine) InFlightCommits() int { return len(m.flights) }

// QueueDepth is the number of client ops waiting for a commit slot.
func (m *Machine) QueueDepth() int { return len(m.queue) }

func (m *Machine) flushCommits() []core.Action {
	if m.cfg.K <= 0 || m.exited {
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
	cmds := append([]pendingCmd(nil), m.queue[:n]...)
	m.queue = append(m.queue[:0:0], m.queue[n:]...)

	m.seq++
	id := fmt.Sprintf("%s/%d", m.cfg.writer(), m.seq)
	entries := make([]applier.Entry, len(cmds))
	for i, c := range cmds {
		e := c.cmd
		e.ID = id
		entries[i] = e
	}
	data := encodeGroup(id, entries)
	slot := m.alloc()
	m.nextReq++
	req := m.nextReq
	m.flights[req] = &flight{req: req, slot: slot, id: id, data: data, cmds: cmds}
	m.waiters[id] = cmds
	return []core.Action{core.LogAppend{Req: req, Slot: slot, Data: data}}
}

func (m *Machine) alloc() core.LogSlot {
	n := max(m.high, m.applied)
	m.high = n + 1
	return n
}

func (m *Machine) observeSlot(n core.LogSlot) {
	m.high = max(m.high, n+1)
}

func (m *Machine) onCommitAppendDone(d core.LogAppendDone, f *flight) []core.Action {
	delete(m.flights, d.Req)
	if d.Err != core.OK || d.Outcome != core.Applied {
		// Unknown outcome: same entry, later slot (append is conditional).
		acts := m.retryFlight(f)
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

// retryFlight appends the same entry (same id and cmds) at the next free slot.
func (m *Machine) retryFlight(f *flight) []core.Action {
	slot := m.alloc()
	m.nextReq++
	req := m.nextReq
	m.flights[req] = &flight{req: req, slot: slot, id: f.id, data: f.data, cmds: f.cmds}
	return []core.Action{core.LogAppend{Req: req, Slot: slot, Data: f.data}}
}

func encodeGroup(id string, cmds []applier.Entry) []byte {
	b := []byte{groupMagic}
	b = binary.AppendUvarint(b, uint64(len(id)))
	b = append(b, id...)
	b = binary.AppendUvarint(b, uint64(len(cmds)))
	for _, c := range cmds {
		enc := applier.Encode(c)
		b = binary.AppendUvarint(b, uint64(len(enc)))
		b = append(b, enc...)
	}
	return b
}

func decodeGroup(data []byte) (string, []applier.Entry, bool) {
	if len(data) == 0 || data[0] != groupMagic {
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
	cmds := make([]applier.Entry, 0, n)
	for range n {
		ln, r2, ok := takeUvarint(rest)
		if !ok || ln > uint64(len(r2)) {
			return "", nil, false
		}
		e, err := applier.Decode(r2[:ln])
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

func (m *Machine) applySlot(slot core.LogSlot, data []byte) error {
	if id, cmds, ok := decodeGroup(data); ok {
		dones := make([]core.CPDone, len(cmds))
		for i, c := range cmds {
			dones[i] = m.fold.Apply(int64(slot), c)
		}
		m.deliver(id, dones)
		return nil
	}
	d, err := m.fold.ApplyBytes(int64(slot), data)
	if err != nil {
		return err
	}
	// Single-entry catch-up / hole-fill: deliver only if we were waiting on it.
	if e, err := applier.Decode(data); err == nil {
		m.deliver(e.ID, []core.CPDone{d})
	}
	return nil
}

func (m *Machine) deliver(id string, dones []core.CPDone) {
	ps, ok := m.waiters[id]
	if !ok {
		return
	}
	delete(m.waiters, id)
	for i, p := range ps {
		d := core.CPDone{Req: p.req, Err: core.ErrInvalid}
		if i < len(dones) {
			d = dones[i]
			d.Req = p.req
		}
		m.answers = append(m.answers, Answer{Req: p.req, Done: d})
	}
}
