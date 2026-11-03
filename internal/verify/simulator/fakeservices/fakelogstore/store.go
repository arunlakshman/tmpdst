package fakelogstore

import (
	"slices"

	"github.com/manifesterio/mds/internal/core"
)

// slotKind is the occupancy of one loglet slot.
type slotKind uint8

const (
	slotEmpty slotKind = iota
	slotEntry
)

type slotRec struct {
	kind slotKind
	data []byte
}

// noFloor is the floor before any LogDeleteOld. No slot is at or below it.
const noFloor core.LogPosition = -1

// store is the in-memory Dynamo loglet: if-absent puts, sparse slots, trim
// deletes. It also keeps the writer epoch (one owner name) and the floor.
//
// The floor is the highest slot that trim has covered. A put at or below the
// floor is not a commit: the row may stay in the slot, but the caller gets
// ErrConflict with no Existing bytes. Reads skip every row at or below the
// floor, so a stray row is never read as a commit. CheckTail is 1 + the
// highest occupied slot, matching DynLog.
type store struct {
	slots []slotRec
	floor core.LogPosition
	epoch uint64
	owner string
	// noFloorCheck is the InjectedBugNoFloorCheck mutant: a put at or below the
	// floor is answered as a normal success.
	noFloorCheck bool
}

func newStore(noFloorCheck bool) *store {
	return &store{floor: noFloor, noFloorCheck: noFloorCheck}
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
	case core.LogClaimEpoch:
		return s.claim(a)
	}
	panic("logstore: not a log action")
}

func (s *store) append(a core.LogAppend) core.LogAppendDone {
	if a.Slot < 0 {
		return core.LogAppendDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	s.ensure(a.Slot)
	cur := s.slots[a.Slot]
	if a.Slot <= s.floor && !s.noFloorCheck {
		// Not a commit. An empty slot still gets the row, like the real store, so a
		// stray row can stay here. An occupied slot is left as it is, and the
		// caller never sees its bytes.
		if cur.kind != slotEntry {
			s.slots[a.Slot] = slotRec{kind: slotEntry, data: slices.Clone(a.Data)}
		}
		return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Taken: true, Err: core.ErrConflict, Outcome: core.NotApplied}
	}
	if cur.kind == slotEntry {
		return core.LogAppendDone{
			Req: a.Req, Slot: a.Slot, Taken: true,
			Existing: slices.Clone(cur.data),
			Err:      core.OK, Outcome: core.Applied,
		}
	}
	s.slots[a.Slot] = slotRec{kind: slotEntry, data: slices.Clone(a.Data)}
	return core.LogAppendDone{Req: a.Req, Slot: a.Slot, Err: core.OK, Outcome: core.Applied}
}

// read returns the rows above the floor, in slot order. A row at or below the
// floor is never returned, even when a rejected put left it there.
func (s *store) read(a core.LogRead) core.LogReadDone {
	if a.From < 0 || a.Limit < 0 {
		return core.LogReadDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	var out []core.LogEntry
	for i := int(a.From); i < len(s.slots) && len(out) < int(a.Limit); i++ {
		if s.slots[i].kind != slotEntry || core.LogPosition(i) <= s.floor {
			continue
		}
		out = append(out, core.LogEntry{Slot: core.LogPosition(i), Data: slices.Clone(s.slots[i].data)})
	}
	return core.LogReadDone{Req: a.Req, Entries: out, Err: core.OK, Outcome: core.Applied}
}

func (s *store) checkTail(a core.LogGetEnd) core.LogGetEndDone {
	tail := core.LogPosition(0)
	for i, r := range slices.Backward(s.slots) {
		if r.kind == slotEntry {
			tail = core.LogPosition(i) + 1
			break
		}
	}
	return core.LogGetEndDone{Req: a.Req, Tail: tail, Err: core.OK, Outcome: core.Applied}
}

// trim raises the floor first, then empties slots 0..Through.
func (s *store) trim(a core.LogDeleteOld) core.LogDeleteOldDone {
	if a.Through < 0 {
		return core.LogDeleteOldDone{Req: a.Req, Err: core.ErrInvalid, Outcome: core.NotApplied}
	}
	s.floor = max(s.floor, a.Through)
	s.ensure(a.Through)
	for i := range int(a.Through) + 1 {
		s.slots[i] = slotRec{kind: slotEmpty}
	}
	return core.LogDeleteOldDone{Req: a.Req, Err: core.OK, Outcome: core.Applied}
}

// claim wins only when the stored epoch equals Expect. A loss answers the
// stored epoch and owner, so a caller whose claim timed out can see whether its
// own claim landed. No slot is written.
func (s *store) claim(a core.LogClaimEpoch) core.LogClaimEpochDone {
	if a.Expect == s.epoch {
		s.epoch = a.Expect + 1
		s.owner = a.Owner
		return core.LogClaimEpochDone{Req: a.Req, Epoch: s.epoch, Owner: s.owner, Won: true, Err: core.OK, Outcome: core.Applied}
	}
	return core.LogClaimEpochDone{Req: a.Req, Epoch: s.epoch, Owner: s.owner, Won: false, Err: core.OK, Outcome: core.Applied}
}

// entry returns a copy of the row at slot. A row at or below the floor counts as
// missing, the same as Read.
func (s *store) entry(slot core.LogPosition) ([]byte, bool) {
	if slot < 0 || int(slot) >= len(s.slots) || s.slots[slot].kind != slotEntry || slot <= s.floor {
		return nil, false
	}
	return slices.Clone(s.slots[slot].data), true
}

func (s *store) stateHash() uint64 {
	h := core.NewHasher()
	mix := h.Uint64Value
	mix(uint64(len(s.slots)))
	for i, r := range s.slots {
		mix(uint64(i))
		mix(uint64(r.kind))
		mix(uint64(len(r.data)))
		for _, b := range r.data {
			mix(uint64(b))
		}
	}
	mix(uint64(s.floor))
	mix(s.epoch)
	h.StringValue(s.owner)
	return h.Sum()
}
