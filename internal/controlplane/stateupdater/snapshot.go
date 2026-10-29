package stateupdater

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// A checkpoint is the whole store at one log slot: replaying the log from that
// slot onwards on a restored store gives the state of the full replay. It
// carries the idempotency window (K and S records), so a replay of a recent key
// is still answered with the original ranges.
//
// Format: "APLR1", varint slot, then entries (uvarint klen, key, uvarint vlen,
// val) in key order, closed by klen 0. The slot is the first log slot NOT
// covered (the caller's Applied()). After the end mark come 4 bytes: the
// CRC-32 (IEEE, big endian) of every byte before them. A store that returns a
// changed byte is found by CheckSnapshot or by Restore, never restored silently.

const snapshotMarker = "APLR1"

// restoreChunk is how many bytes of entries one Restore transaction holds.
const restoreChunk = 8 << 20

var errSnapshot = errors.New("applier: bad checkpoint")

// CheckSnapshot reports whether data is a whole checkpoint with a good CRC.
// Call it before Restore: Restore writes into the store as it reads.
func CheckSnapshot(data []byte) error {
	if len(data) < len(snapshotMarker)+4 {
		return fmt.Errorf("%w: short", errSnapshot)
	}
	body, tail := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(tail) {
		return fmt.Errorf("%w: crc", errSnapshot)
	}
	return nil
}

// crcReader hashes every byte that passes through it.
type crcReader struct {
	br  *bufio.Reader
	crc uint32
}

func (c *crcReader) Read(p []byte) (int, error) {
	n, err := c.br.Read(p)
	c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	return n, err
}

func (c *crcReader) ReadByte() (byte, error) {
	b, err := c.br.ReadByte()
	if err == nil {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, []byte{b})
	}
	return b, err
}

// Snapshot writes a consistent checkpoint of the store, tagged with slot.
func (a *StateUpdater) Snapshot(w io.Writer, slot int64) error {
	return a.SnapshotAt(w, func() int64 { return slot })
}

// SnapshotAt is Snapshot with the slot decided once the consistent view
// exists: begin runs after the view is established and before anything is
// read, and returns the slot to tag. A replica uses it to read its applied
// counter at exactly the point the view was taken (it blocks its apply loop
// until begin returns, not for the whole copy).
func (a *StateUpdater) SnapshotAt(w io.Writer, begin func() int64) error {
	crc := crc32.NewIEEE()
	bw := bufio.NewWriter(io.MultiWriter(w, crc))
	var werr error
	err := a.kv.View(func(tx Transaction) { werr = writeSnapshot(bw, tx, begin) })
	if err = errors.Join(err, werr); err != nil {
		return err
	}
	_ = bw.WriteByte(0) // errors are latched and reported by Flush
	if err = bw.Flush(); err != nil {
		return err
	}
	_, err = w.Write(binary.BigEndian.AppendUint32(nil, crc.Sum32()))
	return err
}

func writeSnapshot(bw *bufio.Writer, tx Transaction, begin func() int64) error {
	// bufio.Writer latches write errors; Flush below reports them.
	_, _ = bw.WriteString(snapshotMarker)
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
func (a *StateUpdater) Restore(r io.Reader) (int64, error) {
	var slot int64
	var err error
	br := &crcReader{br: bufio.NewReader(r)}
	head := make([]byte, len(snapshotMarker))
	if _, err = io.ReadFull(br, head); err != nil || string(head) != snapshotMarker {
		return 0, fmt.Errorf("%w: header", errSnapshot)
	}
	if slot, err = binary.ReadVarint(br); err != nil {
		return 0, fmt.Errorf("%w: slot", errSnapshot)
	}
	for done := false; !done; {
		if done, err = a.restoreChunk(br); err != nil {
			return 0, err
		}
	}
	want := br.crc
	var tail [4]byte
	if _, err = io.ReadFull(br.br, tail[:]); err != nil {
		return 0, fmt.Errorf("%w: crc missing", errSnapshot)
	}
	if binary.BigEndian.Uint32(tail[:]) != want {
		return 0, fmt.Errorf("%w: crc", errSnapshot)
	}
	return slot, nil
}

// restoreChunk reads entries until the chunk is full or the end mark, and
// writes them in one transaction.
func (a *StateUpdater) restoreChunk(br *crcReader) (bool, error) {
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
	return done, a.kv.Update(func(tx Transaction) {
		for _, e := range ents {
			tx.Set(e[0], e[1])
		}
	})
}

func readEntry(br *crcReader) ([]byte, []byte, bool, error) {
	kl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnapshot)
	}
	if kl == 0 {
		return nil, nil, true, nil
	}
	k, err := readBytes(br, kl)
	if err != nil {
		return nil, nil, false, err
	}
	vl, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, nil, false, fmt.Errorf("%w: truncated", errSnapshot)
	}
	v, err := readBytes(br, vl)
	return k, v, false, err
}

// readBytes reads exactly n bytes without trusting n for the allocation.
func readBytes(br *crcReader, n uint64) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := io.CopyN(&buf, br, int64(n)); err != nil {
		return nil, fmt.Errorf("%w: truncated", errSnapshot)
	}
	return buf.Bytes(), nil
}
