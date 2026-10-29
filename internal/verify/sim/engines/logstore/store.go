package logstore

import (
	"slices"

	"manifester/internal/core"
)

// slotKind is the occupancy of one loglet slot.
type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
	slotDeleted
)

type slotRec struct {
	kind slotKind
	data []byte
}

// store is the in-memory Dynamo loglet: if-absent puts, sparse slots, trim
// tombstones. Reads skip empty and tombstoned slots and return entries in
// ascending slot order. CheckTail is 1 + the highest occupied slot (entry or
// tomb), matching DynLog.
type store struct {
	slots []slotRec
	bug   uint8 // logengine.InjectedBugTrimDelete empties instead of tombstoning
}

func (s *store) ensure(slot core.LogPosition) {
	n := int(slot) + 1
	if n > len(s.slots) {
		s.slots = append(s.slots, make([]slotRec, n-len(s.slots))...)
	}
}

func (s *store) exec(a core.IOCall) core.IOResult {
	switch a := a.(type) {
	case core.LogAppend:
		return s.append(a)
	case core.LogRead:
		return s.read(a)
	case core.LogGetEnd:
		return s.checkTail(a)
	case core.LogDeleteOld:
		return s.trim(a)
	}
	panic("logstore: not a log action")
}

func (s *store) append(a core.LogAppend) core.LogAppendDone {
	if a.Slot < 0 {
		return core.LogAppendDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	s.ensure(a.Slot)
	cur := s.slots[a.Slot]
	switch cur.kind {
	case slotEntry:
		return core.LogAppendDone{
			Req: a.Req, Slot: a.Slot, Taken: true,
			Existing: slices.Clone(cur.data),
			Err:      core.OK, Outcome: core.Applied,
		}
	case slotDeleted:
		return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Taken: true, Err: core.ErrConflict, Outcome: core.NotApplied}
	}
	s.slots[a.Slot] = slotRec{kind: slotEntry, data: slices.Clone(a.Data)}
	return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: core.OK, Outcome: core.Applied}
}

func (s *store) read(a core.LogRead) core.LogReadDone {
	if a.From < 0 || a.Limit < 0 {
		return core.LogReadDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	var out []core.LogEntry
	for i := int(a.From); i < len(s.slots) && len(out) < int(a.Limit); i++ {
		if s.slots[i].kind != slotEntry {
			continue
		}
		out = append(out, core.LogEntry{Slot: core.LogPosition(i), Data: slices.Clone(s.slots[i].data)})
	}
	return core.LogReadDone{Req: a.Req, Entries: out, Err: core.OK, Outcome: core.Applied}
}

func (s *store) checkTail(a core.LogGetEnd) core.LogGetEndDone {
	tail := core.LogPosition(0)
	for i, r := range slices.Backward(s.slots) {
		if r.kind != slotEmpty {
			tail = core.LogPosition(i) + 1
			break
		}
	}
	return core.LogGetEndDone{Req: a.Req, Tail: tail, Err: core.OK, Outcome: core.Applied}
}

func (s *store) trim(a core.LogDeleteOld) core.LogDeleteOldDone {
	if a.Through < 0 {
		return core.LogDeleteOldDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	s.ensure(a.Through)
	kind := slotDeleted
	if s.bug == 2 { // logengine.InjectedBugTrimDelete
		kind = slotEmpty
	}
	for i := range int(a.Through) + 1 {
		s.slots[i] = slotRec{kind: kind}
	}
	return core.LogDeleteOldDone{Req: a.Req, Err: core.OK, Outcome: core.Applied}
}

func (s *store) entry(slot core.LogPosition) ([]byte, bool) {
	if slot < 0 || int(slot) >= len(s.slots) || s.slots[slot].kind != slotEntry {
		return nil, false
	}
	return slices.Clone(s.slots[slot].data), true
}

func (s *store) stateHash() uint64 {
	h := core.NewHasher()
	mix := h.U64
	mix(uint64(len(s.slots)))
	for i, r := range s.slots {
		mix(uint64(i))
		mix(uint64(r.kind))
		mix(uint64(len(r.data)))
		for _, b := range r.data {
			mix(uint64(b))
		}
	}
	return h.Sum()
}
