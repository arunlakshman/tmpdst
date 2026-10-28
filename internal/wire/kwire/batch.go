package kwire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Record batch v2 layout: baseOffset[0:8] length[8:12] leaderEpoch[12:16]
// magic[16] crc[17:21] attributes[21:23] ... records[61:]. The CRC covers
// bytes 21 to the end; length counts bytes after offset 12.
const (
	lengthOffset   = 8
	epochOffset    = 12 // also the size of the base offset + length prefix
	magicOffset    = 16
	crcOffset      = 17
	crcStart       = 21 // attributes; the CRC covers [crcStart:]
	batchHeaderLen = 61
	batchMinLength = batchHeaderLen - epochOffset
)

var (
	// ErrCorrupt means a batch failed structure or CRC validation.
	ErrCorrupt = errors.New("kwire: corrupt record batch")
	// ErrUnsupportedBatch means magic != 2, or Records was called on a
	// compressed batch (the broker never decompresses).
	ErrUnsupportedBatch = errors.New("kwire: unsupported record batch")
	// ErrUnknownCodec means the attributes carry a compression codec id above
	// CodecZstd. Kafka answers it with UNSUPPORTED_COMPRESSION_TYPE.
	ErrUnknownCodec = errors.New("kwire: unknown compression codec")
	// ErrTruncated means SplitBatches hit a partial trailing batch.
	ErrTruncated = errors.New("kwire: truncated record batch")
	// ErrNotSingle means a payload did not hold exactly one batch.
	ErrNotSingle = errors.New("kwire: expected exactly one record batch")
)

// Compression codec ids, the low three attribute bits of a v2 batch. The
// broker stores and serves compressed payloads as opaque bytes (decision 45).
const (
	CodecNone   = 0
	CodecGzip   = 1
	CodecSnappy = 2
	CodecLZ4    = 3
	CodecZstd   = 4
)

// crc32cChecksum is CRC32C (Castagnoli); MakeTable returns the cached table.
func crc32cChecksum(b []byte) uint32 { return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) }

// BatchParams are the caller-supplied batch header fields (no clock here).
type BatchParams struct {
	LeaderEpoch   int32
	Attributes    int16 // flag bits; BuildBatch needs compression bits 0, BuildOpaqueBatch takes them as the codec
	ProducerID    int64
	ProducerEpoch int16
	BaseSequence  int32
	FirstTimestamp,
	MaxTimestamp int64
}

// BuildBatch builds an uncompressed RecordBatch v2 with base offset 0.
// Only Key, Value, Headers and the timestamp delta of each record are used;
// offset deltas are the record indexes.
func BuildBatch(p BatchParams, recs []kmsg.Record) ([]byte, error) {
	if len(recs) == 0 {
		return nil, errors.New("kwire: empty batch")
	}
	if p.Attributes&7 != 0 {
		return nil, fmt.Errorf("%w: compression requested", ErrUnsupportedBatch)
	}
	return assembleBatch(p, int32(len(recs)), encodeRecords(recs)), nil
}

func encodeRecords(recs []kmsg.Record) []byte {
	var body []byte
	for i := range recs {
		r := recs[i]
		r.Length, r.Attributes, r.OffsetDelta = 0, 0, int32(i)
		if r.TimestampDelta64 == 0 {
			r.TimestampDelta64 = int64(r.TimestampDelta)
		}
		enc := r.AppendTo(nil)[1:] // drop the zero length varint
		body = binary.AppendVarint(body, int64(len(enc)))
		body = append(body, enc...)
	}
	return body
}

// BuildOpaqueBatch builds a v2 batch around an already-encoded (and, with a
// codec in p.Attributes, already-compressed) records payload that it never
// looks inside: numRecords goes into the header, lastOffsetDelta is
// numRecords-1 and the CRC is valid. It models what a client sends, for
// tests and the simulator; the payload need not decompress.
func BuildOpaqueBatch(p BatchParams, numRecords int32, payload []byte) ([]byte, error) {
	if numRecords <= 0 || len(payload) == 0 {
		return nil, errors.New("kwire: empty batch")
	}
	if p.Attributes&7 > CodecZstd {
		return nil, fmt.Errorf("%w: %d", ErrUnknownCodec, p.Attributes&7)
	}
	return assembleBatch(p, numRecords, payload), nil
}

func assembleBatch(p BatchParams, n int32, body []byte) []byte {
	rb := kmsg.NewRecordBatch()
	rb.Length = int32(batchMinLength + len(body))
	rb.PartitionLeaderEpoch = p.LeaderEpoch
	rb.Magic = 2
	rb.Attributes = p.Attributes
	rb.LastOffsetDelta = n - 1
	rb.FirstTimestamp = p.FirstTimestamp
	rb.MaxTimestamp = p.MaxTimestamp
	rb.ProducerID = p.ProducerID
	rb.ProducerEpoch = p.ProducerEpoch
	rb.FirstSequence = p.BaseSequence
	rb.NumRecords = n
	rb.Records = body
	out := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(out[crcOffset:], crc32cChecksum(out[crcStart:]))
	return out
}

// Batch is a parsed, CRC-verified record batch v2 header.
type Batch struct {
	BaseOffset           int64
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	FirstTimestamp       int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	NumRecords           int32
	records              []byte // aliases the input to ParseBatch
}

// IsTransactional reports the transactional attribute flag bit.
func (b Batch) IsTransactional() bool { return b.Attributes&0x10 != 0 }

// IsControl reports the control attribute flag bit.
func (b Batch) IsControl() bool { return b.Attributes&0x20 != 0 }

// Codec is the compression codec id (CodecNone .. CodecZstd).
func (b Batch) Codec() int { return int(b.Attributes & 7) }

// ParseBatch parses and verifies exactly one batch without decompressing it.
// It rejects magic != 2 with ErrUnsupportedBatch, a valid-CRC batch with a
// codec id above CodecZstd with ErrUnknownCodec, and structural or CRC
// failures with ErrCorrupt. The returned Batch aliases b.
func ParseBatch(b []byte) (Batch, error) {
	if err := checkBatchShape(b); err != nil {
		return Batch{}, err
	}
	if crc32cChecksum(b[crcStart:]) != binary.BigEndian.Uint32(b[crcOffset:]) {
		return Batch{}, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	return decodeBatch(b)
}

// ParseBatchUnverified is ParseBatch without the CRC check. It exists only so
// a broker mutant (broker.InjectedBugSkipProduceCRC) can model a missing check; real
// code must use ParseBatch.
func ParseBatchUnverified(b []byte) (Batch, error) {
	if err := checkBatchShape(b); err != nil {
		return Batch{}, err
	}
	return decodeBatch(b)
}

func checkBatchShape(b []byte) error {
	if len(b) <= magicOffset {
		return fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	if m := int8(b[magicOffset]); m != 2 {
		return fmt.Errorf("%w: magic %d", ErrUnsupportedBatch, m)
	}
	if len(b) < batchHeaderLen {
		return fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	if l := int32(binary.BigEndian.Uint32(b[lengthOffset:])); l < batchMinLength || int64(l) != int64(len(b))-epochOffset {
		return fmt.Errorf("%w: length %d for %d bytes", ErrCorrupt, l, len(b))
	}
	return nil
}

func decodeBatch(b []byte) (Batch, error) {
	var out Batch
	if c := binary.BigEndian.Uint16(b[crcStart:]) & 7; c > CodecZstd {
		return out, fmt.Errorf("%w: %d", ErrUnknownCodec, c)
	}
	var rb kmsg.RecordBatch
	if err := rb.ReadFrom(b); err != nil {
		return out, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	if !countFits(rb.Attributes&7 != 0, rb.NumRecords, len(rb.Records)) {
		return out, fmt.Errorf("%w: %d records in %d bytes", ErrCorrupt, rb.NumRecords, len(rb.Records))
	}
	return Batch{
		BaseOffset: rb.FirstOffset, PartitionLeaderEpoch: rb.PartitionLeaderEpoch,
		Magic: rb.Magic, CRC: uint32(rb.CRC), Attributes: rb.Attributes,
		LastOffsetDelta: rb.LastOffsetDelta, FirstTimestamp: rb.FirstTimestamp,
		MaxTimestamp: rb.MaxTimestamp, ProducerID: rb.ProducerID,
		ProducerEpoch: rb.ProducerEpoch, BaseSequence: rb.FirstSequence,
		NumRecords: rb.NumRecords, records: rb.Records,
	}, nil
}

// countFits reports whether the header record count is possible for a payload
// of n bytes. Every plain record takes at least one byte, so the count cannot
// exceed n; a compressed payload can be far smaller than its record count, so
// only emptiness is checked.
func countFits(compressed bool, count int32, n int) bool {
	switch {
	case count < 0:
		return false
	case compressed:
		return count == 0 || n > 0
	}
	return int64(count) <= int64(n)
}

// Records decodes the batch's records. A compressed batch yields
// ErrUnsupportedBatch: the broker never decompresses. Keys, values and header values alias
// the buffer given to ParseBatch.
func (b Batch) Records() ([]kmsg.Record, error) {
	if b.Codec() != CodecNone {
		return nil, fmt.Errorf("%w: compressed (codec %d)", ErrUnsupportedBatch, b.Codec())
	}
	return DecodeRecords(b.records, b.NumRecords)
}

// Payload is the batch's records section as stored: compressed when Codec is
// not CodecNone. It aliases the buffer given to ParseBatch.
func (b Batch) Payload() []byte { return b.records }

// DecodeRecords decodes n records from an uncompressed records section (a
// Payload of a CodecNone batch, or a client's own decompressed payload).
func DecodeRecords(rest []byte, n int32) ([]kmsg.Record, error) {
	recs := make([]kmsg.Record, 0, min(int(n), len(rest)))
	for i := range n {
		l, k := binary.Varint(rest)
		if k <= 0 || l <= 0 || l > int64(len(rest)-k) {
			return nil, fmt.Errorf("%w: record %d length", ErrCorrupt, i)
		}
		var r kmsg.Record
		if err := r.ReadFrom(rest[:k+int(l)]); err != nil {
			return nil, fmt.Errorf("%w: record %d: %w", ErrCorrupt, i, err)
		}
		recs = append(recs, r)
		rest = rest[k+int(l):]
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes", ErrCorrupt, len(rest))
	}
	return recs, nil
}

// SplitBatches splits concatenated batches by their length fields (structure
// only; no CRC check). The elements are capped subslices of b. A partial
// trailing batch yields the complete ones so far and ErrTruncated; a bad
// length yields ErrCorrupt.
func SplitBatches(b []byte) ([][]byte, error) {
	var out [][]byte
	for len(b) > 0 {
		if len(b) < epochOffset {
			return out, ErrTruncated
		}
		l := int64(int32(binary.BigEndian.Uint32(b[lengthOffset:])))
		if l < batchMinLength {
			return out, fmt.Errorf("%w: length %d", ErrCorrupt, l)
		}
		n := epochOffset + l
		if n > int64(len(b)) {
			return out, ErrTruncated
		}
		out = append(out, b[:n:n])
		b = b[n:]
	}
	return out, nil
}

// RequireSingleBatch returns b if it holds exactly one batch (Produce v3+ partition
// Records), else ErrNotSingle (wrapping the split error if any).
func RequireSingleBatch(b []byte) ([]byte, error) {
	bs, err := SplitBatches(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNotSingle, err)
	}
	if len(bs) != 1 {
		return nil, fmt.Errorf("%w: found %d", ErrNotSingle, len(bs))
	}
	return bs[0], nil
}

// ChangeBatchOffset returns a copy of b with base offset (bytes 0..7) and partition
// leader epoch (12..15) rewritten. Both lie outside the CRC, which stays
// valid. An input shorter than 16 bytes is returned as an unchanged copy.
func ChangeBatchOffset(b []byte, baseOffset int64, leaderEpoch int32) []byte {
	out := append([]byte(nil), b...)
	if len(out) >= magicOffset {
		binary.BigEndian.PutUint64(out, uint64(baseOffset))
		binary.BigEndian.PutUint32(out[epochOffset:], uint32(leaderEpoch))
	}
	return out
}
