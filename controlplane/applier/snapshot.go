package applier

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	return a.SnapshotAt(w, func() int64 { return slot })
}

// SnapshotAt is Snapshot with the slot decided once the consistent view
// exists: begin runs after the view is established and before anything is
// read, and returns the slot to tag. A replica uses it to read its applied
// counter at exactly the point the view was taken (it blocks its apply loop
// until begin returns, not for the whole copy).
func (a *Applier) SnapshotAt(w io.Writer, begin func() int64) error {
	bw := bufio.NewWriter(w)
	var werr error
	err := a.kv.View(func(tx Tx) { werr = writeSnap(bw, tx, begin) })
	if err = errors.Join(err, werr); err != nil {
		return err
	}
	_ = bw.WriteByte(0) // errors are latched and reported by Flush
	return bw.Flush()
}

func writeSnap(bw *bufio.Writer, tx Tx, begin func() int64) error {
	// bufio.Writer latches write errors; Flush below reports them.
	_, _ = bw.WriteString(snapMagic)
	_, _ = bw.Write(binary.AppendVarint(nil, begin()))
	var werr error
	tx.Ascend([]byte{0}, nil, func(k, v []byte) bool {
		werr = writeEntry(bw, k, v)
		return werr == nil
	})
	return werr
}

func writeEntry(w *bufio.Writer, k, v []byte) error {
	// bufio.Writer latches the first error, so the last Write reports it.
	_, _ = w.Write(binary.AppendUvarint(nil, uint64(len(k))))
	_, _ = w.Write(k)
	_, _ = w.Write(binary.AppendUvarint(nil, uint64(len(v))))
	_, err := w.Write(v)
	return err
}

// Restore loads a checkpoint into this (empty) store and returns its slot:
// the next log slot to apply.
func (a *Applier) Restore(r io.Reader) (int64, error) {
	var slot int64
	var err error
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
func (a *Applier) restoreChunk(br *bufio.Reader) (bool, error) {
	var ents [][2][]byte
	size := 0
	var done bool
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

func readEntry(br *bufio.Reader) ([]byte, []byte, bool, error) {
	kl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnap)
	}
	if kl == 0 {
		return nil, nil, true, nil
	}
	k, err := readN(br, kl)
	if err != nil {
		return nil, nil, false, err
	}
	vl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnap)
	}
	v, err := readN(br, vl)
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
