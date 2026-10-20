package applier

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
)

// A checkpoint is the whole store at one log slot: replaying the log from that
// slot onwards on a restored store gives the state of the full replay. It
// carries the idempotency window (K and S records), so a replay of a recent key
// is still answered with the original ranges.
//
// Format: "APLR1", varint slot, then entries (uvarint klen, key, uvarint vlen,
// val) in key order, closed by klen 0. The slot is the first log slot NOT
// covered (the caller's Applied()).

const snapMagic = "APLR1"

// restoreChunk is how many bytes of entries one Restore transaction holds.
const restoreChunk = 8 << 20

var errSnap = errors.New("applier: bad checkpoint")

// Snapshot writes a consistent checkpoint of the store, tagged with slot.
func (a *Applier) Snapshot(w io.Writer, slot int64) error {
	bw := bufio.NewWriter(w)
	bw.WriteString(snapMagic)
	bw.Write(binary.AppendVarint(nil, slot))
	var werr error
	err := a.kv.View(func(tx Tx) {
		tx.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			werr = writeEntry(bw, k, v)
			return werr == nil
		})
	})
	if err = errors.Join(err, werr); err != nil {
		return err
	}
	bw.WriteByte(0)
	return bw.Flush()
}

func writeEntry(w *bufio.Writer, k, v []byte) error {
	w.Write(binary.AppendUvarint(nil, uint64(len(k))))
	w.Write(k)
	w.Write(binary.AppendUvarint(nil, uint64(len(v))))
	_, err := w.Write(v)
	return err
}

// Restore loads a checkpoint into this (empty) store and returns its slot:
// the next log slot to apply.
func (a *Applier) Restore(r io.Reader) (slot int64, err error) {
	br := bufio.NewReader(r)
	head := make([]byte, len(snapMagic))
	if _, err = io.ReadFull(br, head); err != nil || string(head) != snapMagic {
		return 0, fmt.Errorf("%w: header", errSnap)
	}
	if slot, err = binary.ReadVarint(br); err != nil {
		return 0, fmt.Errorf("%w: slot", errSnap)
	}
	for done := false; !done; {
		if done, err = a.restoreChunk(br); err != nil {
			return 0, err
		}
	}
	return slot, nil
}

// restoreChunk reads entries until the chunk is full or the end mark, and
// writes them in one transaction.
func (a *Applier) restoreChunk(br *bufio.Reader) (done bool, err error) {
	var ents [][2][]byte
	size := 0
	for size < restoreChunk && !done {
		k, v, end, err := readEntry(br)
		if err != nil {
			return false, err
		}
		if done = end; !end {
			ents, size = append(ents, [2][]byte{k, v}), size+len(k)+len(v)
		}
	}
	return done, a.kv.Update(func(tx Tx) {
		for _, e := range ents {
			tx.Set(e[0], e[1])
		}
	})
}

func readEntry(br *bufio.Reader) (k, v []byte, end bool, err error) {
	kl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnap)
	}
	if kl == 0 {
		return nil, nil, true, nil
	}
	if k, err = readN(br, kl); err != nil {
		return nil, nil, false, err
	}
	vl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnap)
	}
	v, err = readN(br, vl)
	return k, v, false, err
}

// readN reads exactly n bytes without trusting n for the allocation.
func readN(br *bufio.Reader, n uint64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, br, int64(n)); err != nil {
		return nil, fmt.Errorf("%w: truncated", errSnap)
	}
	return buf.Bytes(), nil
}

// Trim forgets the idempotency records of commands at slots below slot (the
// log no longer holds them) and returns how many keys it dropped. Offsets and
// runs are untouched. A key forgotten here commits again as a new segment.
func (a *Applier) Trim(slot int64) (dropped int, err error) {
	hi := binary.BigEndian.AppendUint64([]byte{pSlot}, uint64(slot))
	err = a.kv.Update(func(tx Tx) {
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
func (a *Applier) Window() (n int) {
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
				h.Write(binary.AppendUvarint(nil, uint64(len(k))))
				h.Write(k)
				h.Write(v)
			}
			return true
		})
	})
	return h.Sum64()
}
