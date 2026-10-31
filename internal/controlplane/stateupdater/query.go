package stateupdater

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"

	"mds/internal/core"
)

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func (a *StateUpdater) Trim(slot int64) (int, error) {
	hi := binary.BigEndian.AppendUint64([]byte{prefixSlot}, uint64(slot))
	var dropped int
	err := a.keyValue.Update(func(transaction Transaction) {
		var keysToDrop [][]byte
		transaction.Ascend([]byte{prefixSlot}, hi, func(k, _ []byte) bool {
			keysToDrop = append(keysToDrop, bytes.Clone(k))
			return true
		})
		for _, k := range keysToDrop {
			transaction.Delete(duplicateCheckKey(string(k[9:])))
			transaction.Delete(k)
		}
		dropped = len(keysToDrop)
	})
	return dropped, err
}

// Window is the number of segment keys currently remembered.
func (a *StateUpdater) Window() int {
	var n int
	_ = a.keyValue.View(func(transaction Transaction) {
		lo := []byte{prefixSlot}
		transaction.Ascend(lo, prefixEnd(lo), func(_, _ []byte) bool { n++; return true })
	})
	return n
}

// StateHash is a digest of the materialised state (topics, ends, runs), not of
// the idempotency window: two state updaters that applied the same commands in the
// same order agree, whatever they trimmed. It is not speccheck's StateHash.
func (a *StateUpdater) StateHash() uint64 {
	h := fnv.New64a()
	_ = a.keyValue.View(func(transaction Transaction) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			if k[0] != prefixKey && k[0] != prefixSlot {
				if _, err := h.Write(binary.AppendUvarint(nil, uint64(len(k)))); err != nil {
					return false
				}
				if _, err := h.Write(k); err != nil {
					return false
				}
				if _, err := h.Write(v); err != nil {
					return false
				}
			}
			return true
		})
	})
	return h.Sum64()
}

// AssignState says what Assigned knows about a command.
type AssignState int

// Assigned results.
const (
	// OffsetOwnerUnknown: no record of the key (invalid, or forgotten by Trim).
	OffsetOwnerUnknown AssignState = iota
	// OffsetOwnedElsewhere: the key committed at another slot, or with another
	// request: this command was a replay or a conflict and assigned nothing.
	OffsetOwnedElsewhere
	// OffsetOwnedHere: the command is the first commit of its key, at this slot.
	OffsetOwnedHere
)

// Assigned reports the ranges operation was given if it is the first commit of its
// key at slot, rebuilt from the key's record. An indexer uses it to write the
// run rows of a slot after the fact.
func (a *StateUpdater) Assigned(slot int64, operation core.CommitSegment) ([]core.BatchRange, AssignState, error) {
	var rs []core.BatchRange
	var st AssignState
	err := a.keyValue.View(func(transaction Transaction) {
		rec := transaction.Get(duplicateCheckKey(operation.SegmentKey))
		if rec == nil {
			return
		}
		st = OffsetOwnedElsewhere
		first, _ := binary.Uvarint(rec)
		d := replay(rec, checksum(operation.Batches), operation)
		if int64(first) != slot || d.Err != core.OK {
			return
		}
		rs, st = d.Resp.(core.CommitSegmentResp).Assigned, OffsetOwnedHere
	})
	return rs, st, err
}
