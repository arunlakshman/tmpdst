package applier

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"

	"manifester/internal/core"
)

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func (a *Applier) Trim(slot int64) (int, error) {
	hi := binary.BigEndian.AppendUint64([]byte{pSlot}, uint64(slot))
	var dropped int
	err := a.kv.Update(func(tx Tx) {
		var sk [][]byte
		tx.Ascend([]byte{pSlot}, hi, func(k, _ []byte) bool {
			sk = append(sk, bytes.Clone(k))
			return true
		})
		for _, k := range sk {
			tx.Delete(idemKey(string(k[9:])))
			tx.Delete(k)
		}
		dropped = len(sk)
	})
	return dropped, err
}

// Window is the number of segment keys currently remembered.
func (a *Applier) Window() int {
	var n int
	_ = a.kv.View(func(tx Tx) {
		lo := []byte{pSlot}
		tx.Ascend(lo, prefixEnd(lo), func(_, _ []byte) bool { n++; return true })
	})
	return n
}

// StateHash is a digest of the materialised state (topics, ends, runs), not of
// the idempotency window: two appliers that applied the same commands in the
// same order agree, whatever they trimmed. It is not veritas's StateHash.
func (a *Applier) StateHash() uint64 {
	h := fnv.New64a()
	_ = a.kv.View(func(tx Tx) {
		tx.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			if k[0] != pKey && k[0] != pSlot {
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
	// AssignUnknown: no record of the key (invalid, or forgotten by Trim).
	AssignUnknown AssignState = iota
	// AssignElsewhere: the key committed at another slot, or with another
	// request: this command was a replay or a conflict and assigned nothing.
	AssignElsewhere
	// AssignHere: the command is the first commit of its key, at this slot.
	AssignHere
)

// Assigned reports the ranges op was given if it is the first commit of its
// key at slot, rebuilt from the key's record. An indexer uses it to write the
// run rows of a slot after the fact.
func (a *Applier) Assigned(slot int64, op core.CommitSegment) ([]core.BatchRange, AssignState, error) {
	var rs []core.BatchRange
	var st AssignState
	err := a.kv.View(func(tx Tx) {
		rec := tx.Get(idemKey(op.SegmentKey))
		if rec == nil {
			return
		}
		st = AssignElsewhere
		first, _ := binary.Uvarint(rec)
		d := replay(rec, digest(op.Batches), op)
		if int64(first) != slot || d.Err != core.OK {
			return
		}
		rs, st = d.Resp.(core.CommitSegmentResp).Assigned, AssignHere
	})
	return rs, st, err
}
