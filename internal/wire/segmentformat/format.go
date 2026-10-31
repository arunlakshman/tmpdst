// Package segmentformat is the byte layout of a segment object and the naming of
// segment keys, shared by the broker, sim invariants and speccheck. It is pure
// and deterministic (determinismcheck-clean) and never panics, whatever the input.
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
package segmentformat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"

	"manifester/internal/core"
)

const (
	// FileMarker is the segment header magic.
	FileMarker = "MSEG"
	// Version is the segment format version.
	Version = 1
	// HeaderSize is the segment header length in bytes.
	HeaderSize = 20
)

// ErrCorrupt means the bytes are not a valid segment object or batch.
// Code maps it to core.ErrCorrupt.
var ErrCorrupt = errors.New("format: corrupt segment")

// ErrInvalid means Encode was given a batch it cannot represent.
// Code maps it to core.ErrInvalid.
var ErrInvalid = errors.New("format: invalid batch")

// Code maps an error from this package to a core.ErrCode: nil is core.OK,
// ErrInvalid is core.ErrInvalid, anything else (ErrCorrupt) is core.ErrCorrupt.
func Code(err error) core.ErrCode {
	switch {
	case err == nil:
		return core.OK
	case errors.Is(err, ErrInvalid):
		return core.ErrInvalid
	}
	return core.ErrCorrupt
}

// CRC32CChecksum is CRC32C (Castagnoli); MakeTable returns the cached table.
func CRC32CChecksum(b []byte) uint32 { return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) }

// Batch is one partition's record batch to be written.
type Batch struct {
	TopicPartition core.TopicPartition
	Records        uint32 // record count
	Data           []byte // opaque Kafka record-batch bytes; non-empty
}

// Entry places one batch inside an object.
type Entry struct {
	TopicPartition core.TopicPartition
	Records        uint32
	Off            int64  // absolute offset of Data in the object
	Len            int64  // len(Data)
	CRC            uint32 // CRC32C of Data
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
	// entryTail is the fixed part of an index entry after the topic bytes:
	// partition, records, off, len, CRC.
	entryTail = 4 + 4 + 8 + 8 + 4
	// minEntry is the smallest encoded index entry (empty topic is rejected
	// later, but this bound only guards allocation).
	minEntry = 2 + entryTail
)

// Encode lays out batches as one object and returns it with one Entry per
// batch, in order. It returns ErrInvalid for an empty list, an empty topic or
// payload, a topic longer than 65535 bytes, or a negative partition.
func Encode(batches []Batch) ([]byte, []Entry, error) {
	idx, total, err := measureBatches(batches)
	if err != nil {
		return nil, nil, err
	}
	obj, entries := writeObject(batches, idx, total)
	return obj, entries, nil
}

func measureBatches(batches []Batch) (int, int, error) {
	if len(batches) == 0 || uint64(len(batches)) > math.MaxUint32 {
		return 0, 0, fmt.Errorf("%w: %d batches", ErrInvalid, len(batches))
	}
	idx := 4
	total := 0
	for i, b := range batches {
		if b.TopicPartition.Topic == "" || len(b.TopicPartition.Topic) > maxTopic || b.TopicPartition.Partition < 0 || len(b.Data) == 0 {
			return 0, 0, fmt.Errorf("%w: batch %d", ErrInvalid, i)
		}
		idx += minEntry + len(b.TopicPartition.Topic)
		total += len(b.Data)
	}
	return idx, total, nil
}

func writeObject(batches []Batch, idx, total int) ([]byte, []Entry) {
	bodyLen := idx + total
	obj := make([]byte, HeaderSize+bodyLen)
	body := obj[HeaderSize:]
	binary.BigEndian.PutUint32(body, uint32(len(batches)))
	entries := make([]Entry, len(batches))
	p, off := 4, idx
	for i, b := range batches {
		p += putEntry(body[p:], b, uint64(off))
		copy(body[off:], b.Data)
		entries[i] = Entry{
			TopicPartition: b.TopicPartition,
			Records:        b.Records,
			Off:            int64(HeaderSize + off),
			Len:            int64(len(b.Data)),
			CRC:            CRC32CChecksum(b.Data),
		}
		off += len(b.Data)
	}
	copy(obj, FileMarker)
	binary.BigEndian.PutUint16(obj[4:], Version)
	// obj[6:8] flags = 0
	binary.BigEndian.PutUint64(obj[8:], uint64(bodyLen))
	binary.BigEndian.PutUint32(obj[16:], CRC32CChecksum(body))
	return obj, entries
}

func putEntry(w []byte, b Batch, off uint64) int {
	binary.BigEndian.PutUint16(w, uint16(len(b.TopicPartition.Topic)))
	n := 2 + copy(w[2:], b.TopicPartition.Topic)
	binary.BigEndian.PutUint32(w[n:], uint32(b.TopicPartition.Partition))
	binary.BigEndian.PutUint32(w[n+4:], b.Records)
	binary.BigEndian.PutUint64(w[n+8:], off)
	binary.BigEndian.PutUint64(w[n+16:], uint64(len(b.Data)))
	binary.BigEndian.PutUint32(w[n+24:], CRC32CChecksum(b.Data))
	return n + entryTail
}

func corrupt(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, a...))
}

// Decode validates a whole object (header, lengths, body CRC, index, every
// batch CRC) and returns its contents. It never panics.
func Decode(obj []byte) (Segment, error) {
	body, err := decodeHeader(obj)
	if err != nil {
		return Segment{}, err
	}
	idx, payloadOff, err := decodeIndex(body)
	if err != nil {
		return Segment{}, err
	}
	return attachPayloads(body, idx, payloadOff)
}

// decodeHeader checks magic/version/flags/body-len/body-CRC and returns the body.
func decodeHeader(obj []byte) ([]byte, error) {
	if len(obj) < HeaderSize {
		return nil, corrupt("truncated header: %d bytes", len(obj))
	}
	if string(obj[:4]) != FileMarker {
		return nil, corrupt("bad magic")
	}
	if v := binary.BigEndian.Uint16(obj[4:]); v != Version {
		return nil, corrupt("unsupported version %d", v)
	}
	if f := binary.BigEndian.Uint16(obj[6:]); f != 0 {
		return nil, corrupt("unknown flags %#x", f)
	}
	if bl := binary.BigEndian.Uint64(obj[8:]); bl != uint64(len(obj)-HeaderSize) {
		return nil, corrupt("body length %d, have %d bytes", bl, len(obj)-HeaderSize)
	}
	body := obj[HeaderSize:]
	if binary.BigEndian.Uint32(obj[16:]) != CRC32CChecksum(body) {
		return nil, corrupt("body CRC mismatch")
	}
	return body, nil
}

// segmentIndexEntry is a decoded index entry with its body-relative payload range;
// Entry.Off and Entry.Len are filled once the range is validated.
type segmentIndexEntry struct {
	Entry
	off, n uint64
}

// decodeIndex parses the index and returns its entries and the body offset
// where payloads must start.
func decodeIndex(body []byte) ([]segmentIndexEntry, uint64, error) {
	if len(body) < 4 {
		return nil, 0, corrupt("no index")
	}
	count := uint64(binary.BigEndian.Uint32(body))
	if count == 0 || count > uint64(len(body)-4)/minEntry {
		return nil, 0, corrupt("bad batch count %d", count)
	}
	idx := make([]segmentIndexEntry, 0, count)
	p := 4
	for i := range count {
		ent, next, err := readIndexEntry(body, p, i)
		if err != nil {
			return nil, 0, err
		}
		idx = append(idx, ent)
		p = next
	}
	return idx, uint64(p), nil
}

func readIndexEntry(body []byte, p int, i uint64) (segmentIndexEntry, int, error) {
	if len(body)-p < 2 {
		return segmentIndexEntry{}, 0, corrupt("truncated index")
	}
	tl := int(binary.BigEndian.Uint16(body[p:]))
	if tl == 0 || len(body)-p < 2+tl+entryTail {
		return segmentIndexEntry{}, 0, corrupt("bad index entry %d", i)
	}
	topic := string(body[p+2 : p+2+tl])
	q := p + 2 + tl
	part := int32(binary.BigEndian.Uint32(body[q:]))
	if part < 0 {
		return segmentIndexEntry{}, 0, corrupt("negative partition in entry %d", i)
	}
	return segmentIndexEntry{
		Entry: Entry{
			TopicPartition: core.NewTopicPartition(topic, part),
			Records:        binary.BigEndian.Uint32(body[q+4:]),
			CRC:            binary.BigEndian.Uint32(body[q+24:]),
		},
		off: binary.BigEndian.Uint64(body[q+8:]),
		n:   binary.BigEndian.Uint64(body[q+16:]),
	}, q + entryTail, nil
}

// attachPayloads checks contiguous ranges and per-batch CRCs, fills Off/Len,
// and builds Batches that alias body slices.
func attachPayloads(body []byte, idx []segmentIndexEntry, next uint64) (Segment, error) {
	seg := Segment{Entries: make([]Entry, 0, len(idx)), Batches: make([]Batch, 0, len(idx))}
	for i, r := range idx {
		if r.off != next || r.n == 0 || r.n > uint64(len(body))-r.off {
			return Segment{}, corrupt("entry %d: bad range off=%d len=%d", i, r.off, r.n)
		}
		next = r.off + r.n
		data := body[r.off:next:next]
		if CRC32CChecksum(data) != r.CRC {
			return Segment{}, corrupt("entry %d: batch CRC mismatch", i)
		}
		r.Off, r.Len = int64(HeaderSize+r.off), int64(r.n)
		seg.Entries = append(seg.Entries, r.Entry)
		seg.Batches = append(seg.Batches, Batch{TopicPartition: r.TopicPartition, Records: r.Records, Data: data})
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
	if CRC32CChecksum(b) != e.CRC {
		return nil, corrupt("batch CRC mismatch")
	}
	return b, nil
}
