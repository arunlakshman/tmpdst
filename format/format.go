// Package format is the byte layout of a segment object and the naming of
// segment keys, shared by the broker, sim invariants and veritas. It is pure
// and deterministic (detcheck-clean) and never panics, whatever the input.
//
// One segment object is one upload holding batches for many partitions (see
// core.CommitSegment). All integers are big-endian.
//
//	object = header body
//
//	header (20 bytes)
//	+--------+---------+-------+----------+----------+
//	| "MSEG" | version | flags | body len | body CRC |
//	|   4    |   u16   |  u16  |   u64    |   u32    |
//	+--------+---------+-------+----------+----------+
//
//	body = index payloads        (CRC32C, Castagnoli, over the whole body)
//
//	index
//	+-------+-----------------------------------------------------------+
//	| count | count x entry                                             |
//	|  u32  | topic len u16, topic, partition i32, records u32,         |
//	|       | off u64 (from body start), len u64, batch CRC u32         |
//	+-------+-----------------------------------------------------------+
//
//	payloads: the batches' opaque Kafka record-batch bytes, back to back, in
//	index order, starting right after the index and ending at the body end.
//
// The layout is canonical: version is 1, flags are 0, payload offsets are
// contiguous, topics are non-empty, payloads are non-empty. Anything else is
// ErrCorrupt, so one batch list has exactly one encoding.
//
// A reader that fetched only one batch with an ObjGet range (core.SegmentRef,
// see Entry.Ref) cannot check the body CRC, so each entry carries its own
// CRC32C of its payload; VerifyBatch checks the fetched bytes against it.
// Entry offsets as returned by Encode and Decode are absolute in the object
// (the wire stores them relative to the body).
//
// Corruption of every kind (truncation, bad magic, bad version, bad flags,
// length mismatch, CRC mismatch, malformed index) returns an error that
// matches ErrCorrupt; Code maps it to core.ErrCorrupt.
package format

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"manifester/core"
)

const (
	Magic      = "MSEG"
	Version    = 1
	HeaderSize = 20
)

// ErrCorrupt means the bytes are not a valid segment object or batch.
// Code maps it to core.ErrCorrupt.
var ErrCorrupt = errors.New("format: corrupt segment")

// ErrInvalid means Encode was given a batch it cannot represent.
// Code maps it to core.ErrInvalid.
var ErrInvalid = errors.New("format: invalid batch")

// Code maps an error from this package to a core.ErrCode: nil is core.OK,
// ErrCorrupt is core.ErrCorrupt, ErrInvalid is core.ErrInvalid.
func Code(err error) core.ErrCode {
	switch {
	case err == nil:
		return core.OK
	case errors.Is(err, ErrCorrupt):
		return core.ErrCorrupt
	case errors.Is(err, ErrInvalid):
		return core.ErrInvalid
	}
	return core.ErrCorrupt
}

// Checksum is CRC32C (Castagnoli).
func Checksum(b []byte) uint32 { return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) }

// Batch is one partition's record batch to be written.
type Batch struct {
	TP      core.TopicPartition
	Records uint32 // record count
	Data    []byte // opaque Kafka record-batch bytes; non-empty
}

// Entry places one batch inside an object.
type Entry struct {
	TP      core.TopicPartition
	Records uint32
	Off     int64  // absolute offset of Data in the object
	Len     int64  // len(Data)
	CRC     uint32 // CRC32C of Data
}

// Ref is the ObjGet range of the entry's bytes in the object stored at key.
func (e Entry) Ref(key string) core.SegmentRef {
	return core.SegmentRef{Key: key, Off: e.Off, Len: e.Len}
}

// Segment is a decoded object. Batches[i].Data aliases the input.
type Segment struct {
	Entries []Entry
	Batches []Batch
}

const (
	maxTopic = math.MaxUint16
	// minEntry is the smallest encoded index entry (empty topic is rejected
	// later, but this bound only guards allocation).
	minEntry = 2 + 4 + 4 + 8 + 8 + 4
)

// Encode lays out batches as one object and returns it with one Entry per
// batch, in order. It returns ErrInvalid for an empty list, an empty topic or
// payload, a topic longer than 65535 bytes, or a negative partition.
func Encode(batches []Batch) ([]byte, []Entry, error) {
	if len(batches) == 0 || uint64(len(batches)) > math.MaxUint32 {
		return nil, nil, fmt.Errorf("%w: %d batches", ErrInvalid, len(batches))
	}
	idx := 4
	total := 0
	for i, b := range batches {
		if b.TP.Topic == "" || len(b.TP.Topic) > maxTopic || b.TP.Partition < 0 || len(b.Data) == 0 {
			return nil, nil, fmt.Errorf("%w: batch %d", ErrInvalid, i)
		}
		idx += minEntry + len(b.TP.Topic)
		total += len(b.Data)
	}
	bodyLen := idx + total
	obj := make([]byte, HeaderSize+bodyLen)
	body := obj[HeaderSize:]
	binary.BigEndian.PutUint32(body, uint32(len(batches)))
	entries := make([]Entry, len(batches))
	p, off := 4, idx
	for i, b := range batches {
		p += putEntry(body[p:], b, uint64(off))
		copy(body[off:], b.Data)
		entries[i] = Entry{b.TP, b.Records, int64(HeaderSize + off), int64(len(b.Data)), Checksum(b.Data)}
		off += len(b.Data)
	}
	copy(obj, Magic)
	binary.BigEndian.PutUint16(obj[4:], Version)
	// obj[6:8] flags = 0
	binary.BigEndian.PutUint64(obj[8:], uint64(bodyLen))
	binary.BigEndian.PutUint32(obj[16:], Checksum(body))
	return obj, entries, nil
}

func putEntry(w []byte, b Batch, off uint64) int {
	binary.BigEndian.PutUint16(w, uint16(len(b.TP.Topic)))
	n := 2 + copy(w[2:], b.TP.Topic)
	binary.BigEndian.PutUint32(w[n:], uint32(b.TP.Partition))
	binary.BigEndian.PutUint32(w[n+4:], b.Records)
	binary.BigEndian.PutUint64(w[n+8:], off)
	binary.BigEndian.PutUint64(w[n+16:], uint64(len(b.Data)))
	binary.BigEndian.PutUint32(w[n+24:], Checksum(b.Data))
	return n + 28
}

func corrupt(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCorrupt}, a...)...)
}

// Decode validates a whole object (header, lengths, body CRC, index, every
// batch CRC) and returns its contents. It never panics.
func Decode(obj []byte) (Segment, error) {
	if len(obj) < HeaderSize {
		return Segment{}, corrupt("truncated header: %d bytes", len(obj))
	}
	if string(obj[:4]) != Magic {
		return Segment{}, corrupt("bad magic")
	}
	if v := binary.BigEndian.Uint16(obj[4:]); v != Version {
		return Segment{}, corrupt("unsupported version %d", v)
	}
	if f := binary.BigEndian.Uint16(obj[6:]); f != 0 {
		return Segment{}, corrupt("unknown flags %#x", f)
	}
	if bl := binary.BigEndian.Uint64(obj[8:]); bl != uint64(len(obj)-HeaderSize) {
		return Segment{}, corrupt("body length %d, have %d bytes", bl, len(obj)-HeaderSize)
	}
	body := obj[HeaderSize:]
	if binary.BigEndian.Uint32(obj[16:]) != Checksum(body) {
		return Segment{}, corrupt("body CRC mismatch")
	}
	if len(body) < 4 {
		return Segment{}, corrupt("no index")
	}
	count := uint64(binary.BigEndian.Uint32(body))
	if count == 0 || count > uint64(len(body)-4)/minEntry {
		return Segment{}, corrupt("bad batch count %d", count)
	}
	seg := Segment{Entries: make([]Entry, 0, count), Batches: make([]Batch, 0, count)}
	type raw struct{ off, n uint64 }
	raws := make([]raw, 0, count)
	p := 4
	for i := uint64(0); i < count; i++ {
		if len(body)-p < 2 {
			return Segment{}, corrupt("truncated index")
		}
		tl := int(binary.BigEndian.Uint16(body[p:]))
		if tl == 0 || len(body)-p < 2+tl+28 {
			return Segment{}, corrupt("bad index entry %d", i)
		}
		topic := string(body[p+2 : p+2+tl])
		q := p + 2 + tl
		part := int32(binary.BigEndian.Uint32(body[q:]))
		if part < 0 {
			return Segment{}, corrupt("negative partition in entry %d", i)
		}
		e := Entry{TP: core.TopicPartition{Topic: topic, Partition: part}, Records: binary.BigEndian.Uint32(body[q+4:]),
			CRC: binary.BigEndian.Uint32(body[q+24:])}
		raws = append(raws, raw{binary.BigEndian.Uint64(body[q+8:]), binary.BigEndian.Uint64(body[q+16:])})
		seg.Entries = append(seg.Entries, e)
		p = q + 28
	}
	next := uint64(p) // payloads start right after the index
	for i, r := range raws {
		if r.off != next || r.n == 0 || r.n > uint64(len(body))-r.off {
			return Segment{}, corrupt("entry %d: bad range off=%d len=%d", i, r.off, r.n)
		}
		next = r.off + r.n
		data := body[r.off:next:next]
		e := &seg.Entries[i]
		if Checksum(data) != e.CRC {
			return Segment{}, corrupt("entry %d: batch CRC mismatch", i)
		}
		e.Off, e.Len = int64(HeaderSize+r.off), int64(r.n)
		seg.Batches = append(seg.Batches, Batch{TP: e.TP, Records: e.Records, Data: data})
	}
	if next != uint64(len(body)) {
		return Segment{}, corrupt("%d trailing body bytes", uint64(len(body))-next)
	}
	return seg, nil
}

// VerifyBatch checks bytes fetched with Entry.Ref against the entry: right
// length and CRC32C. It returns b on success.
func VerifyBatch(e Entry, b []byte) ([]byte, error) {
	if int64(len(b)) != e.Len {
		return nil, corrupt("batch length %d, want %d", len(b), e.Len)
	}
	if Checksum(b) != e.CRC {
		return nil, corrupt("batch CRC mismatch")
	}
	return b, nil
}
