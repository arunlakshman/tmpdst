package logsm

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"

	"manifester/core"
	"manifester/veritas"
)

// Checkpoint is a snapshot of what a replica must know to continue from slot
// Slot without the log below it: every partition's end offset and the set of
// committed segment keys (16-byte hashes). It is deliberately only what idea A
// says it carries, enough to assign offsets and to recognise a key as already
// committed. It cannot answer a replay with the original ranges, nor detect
// ErrConflict: that needs the command, so keys inside the replay window must
// stay in the log (docs/design-a-prototype.md). It is a prototype for X5: there
// is no restore into a veritas.ControlPlane.
type Checkpoint struct {
	Slot int64 // the first slot NOT covered: replay resumes here
	Hash uint64
	Ends []veritas.End
	Keys [][16]byte // sorted
}

// KeyHash is the 16-byte identity of a segment key in a checkpoint.
func KeyHash(key string) [16]byte {
	sum := sha256.Sum256([]byte(key))
	return [16]byte(sum[:16])
}

// Checkpoint snapshots the applied prefix.
func (r *Replica) Checkpoint() Checkpoint {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.cp.(*veritas.ControlPlane) // the prototype checkpoint is veritas-only
	c := Checkpoint{Slot: r.applied, Hash: v.StateHash(), Ends: v.Ends()}
	for _, k := range v.SegmentKeys() {
		c.Keys = append(c.Keys, KeyHash(k))
	}
	slices.SortFunc(c.Keys, func(a, b [16]byte) int { return bytes.Compare(a[:], b[:]) })
	return c
}

// Marshal serialises the checkpoint: slot, hash, ends grouped by topic with
// delta-coded partition numbers, then the sorted key hashes verbatim.
func (c Checkpoint) Marshal() []byte {
	b := binary.AppendVarint(nil, c.Slot)
	b = binary.LittleEndian.AppendUint64(b, c.Hash)
	var topics [][]veritas.End
	for _, e := range c.Ends {
		if n := len(topics); n == 0 || topics[n-1][0].TP.Topic != e.TP.Topic {
			topics = append(topics, nil)
		}
		topics[len(topics)-1] = append(topics[len(topics)-1], e)
	}
	b = binary.AppendUvarint(b, uint64(len(topics)))
	for _, ends := range topics {
		b = binary.AppendUvarint(b, uint64(len(ends[0].TP.Topic)))
		b = append(b, ends[0].TP.Topic...)
		b = binary.AppendUvarint(b, uint64(len(ends)))
		prev := int32(0)
		for _, e := range ends {
			b = binary.AppendUvarint(b, uint64(e.TP.Partition-prev))
			b = binary.AppendUvarint(b, uint64(e.Offset))
			prev = e.TP.Partition
		}
	}
	b = binary.AppendUvarint(b, uint64(len(c.Keys)))
	for _, k := range c.Keys {
		b = append(b, k[:]...)
	}
	return b
}

// UnmarshalCheckpoint parses Marshal's output.
func UnmarshalCheckpoint(b []byte) (Checkpoint, error) {
	d := &decoder{b: b}
	c := Checkpoint{Slot: d.i()}
	if len(d.b) < 8 {
		return Checkpoint{}, errCorrupt
	}
	c.Hash, d.b = binary.LittleEndian.Uint64(d.b), d.b[8:]
	for nt := d.u(); nt > 0 && d.err == nil; nt-- {
		topic := d.s()
		prev := int32(0)
		for n := d.u(); n > 0 && d.err == nil; n-- {
			prev += int32(d.u())
			c.Ends = append(c.Ends, veritas.End{TP: core.TP(topic, prev), Offset: core.Offset(d.u())})
		}
	}
	n := d.u()
	if d.err != nil || n > uint64(len(d.b))/16 || uint64(len(d.b)) != n*16 {
		return Checkpoint{}, errCorrupt
	}
	c.Keys = make([][16]byte, n)
	for i := range c.Keys {
		c.Keys[i] = [16]byte(d.b[:16])
		d.b = d.b[16:]
	}
	return c, nil
}

// Restored is the in-memory form a replica loads a checkpoint into.
type Restored struct {
	Ends map[core.TopicPartition]core.Offset
	Keys map[[16]byte]struct{}
}

// Restore builds the lookup structures a replica needs from the checkpoint.
func (c Checkpoint) Restore() Restored {
	r := Restored{Ends: make(map[core.TopicPartition]core.Offset, len(c.Ends)), Keys: make(map[[16]byte]struct{}, len(c.Keys))}
	for _, e := range c.Ends {
		r.Ends[e.TP] = e.Offset
	}
	for _, k := range c.Keys {
		r.Keys[k] = struct{}{}
	}
	return r
}
