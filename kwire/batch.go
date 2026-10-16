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
	batchHeaderLen = 61
	batchMinLength = batchHeaderLen - 12
	crcStart       = 21
)

var (
	// ErrCorrupt means a batch failed structure or CRC validation.
	ErrCorrupt = errors.New("kwire: corrupt record batch")
	// ErrUnsupportedBatch means magic != 2 or the batch is compressed.
	ErrUnsupportedBatch = errors.New("kwire: unsupported record batch")
	// ErrTruncated means SplitBatches hit a partial trailing batch.
	ErrTruncated = errors.New("kwire: truncated record batch")
	// ErrNotSingle means a payload did not hold exactly one batch.
	ErrNotSingle = errors.New("kwire: expected exactly one record batch")
)

func crc32c(b []byte) uint32 { return crc32.Checksum(b, crc32.MakeTable(crc32.Castagnoli)) }

// BatchParams are the caller-supplied batch header fields (no clock here).
type BatchParams struct {
	LeaderEpoch   int32
	Attributes    int16 // flag bits only; compression bits must be 0
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
	rb := kmsg.NewRecordBatch()
	rb.Length = int32(batchMinLength + len(body))
	rb.PartitionLeaderEpoch = p.LeaderEpoch
	rb.Magic = 2
	rb.Attributes = p.Attributes
	rb.LastOffsetDelta = int32(len(recs) - 1)
	rb.FirstTimestamp = p.FirstTimestamp
	rb.MaxTimestamp = p.MaxTimestamp
	rb.ProducerID = p.ProducerID
	rb.ProducerEpoch = p.ProducerEpoch
	rb.FirstSequence = p.BaseSequence
	rb.NumRecords = int32(len(recs))
	rb.Records = body
	out := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(out[17:], crc32c(out[crcStart:]))
	return out, nil
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

// IsTransactional and IsControl report the attribute flag bits.
func (b Batch) IsTransactional() bool { return b.Attributes&0x10 != 0 }
func (b Batch) IsControl() bool       { return b.Attributes&0x20 != 0 }

// ParseBatch parses and verifies exactly one batch. It rejects magic != 2 and
// compressed batches with ErrUnsupportedBatch, and structural or CRC failures
// with ErrCorrupt. The returned Batch aliases b.
func ParseBatch(b []byte) (Batch, error) {
	var out Batch
	if len(b) < 17 {
		return out, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	if m := int8(b[16]); m != 2 {
		return out, fmt.Errorf("%w: magic %d", ErrUnsupportedBatch, m)
	}
	if len(b) < batchHeaderLen {
		return out, fmt.Errorf("%w: %d bytes", ErrCorrupt, len(b))
	}
	if l := int32(binary.BigEndian.Uint32(b[8:])); l < batchMinLength || int64(l) != int64(len(b))-12 {
		return out, fmt.Errorf("%w: length %d for %d bytes", ErrCorrupt, l, len(b))
	}
	if binary.BigEndian.Uint16(b[21:])&7 != 0 {
		return out, fmt.Errorf("%w: compressed", ErrUnsupportedBatch)
	}
	if crc32c(b[crcStart:]) != binary.BigEndian.Uint32(b[17:]) {
		return out, fmt.Errorf("%w: crc mismatch", ErrCorrupt)
	}
	var rb kmsg.RecordBatch
	if err := rb.ReadFrom(b); err != nil {
		return out, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if rb.NumRecords < 0 || int64(rb.NumRecords) > int64(len(rb.Records)) {
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

// Records decodes the batch's records. Keys, values and header values alias
// the buffer given to ParseBatch.
func (b Batch) Records() ([]kmsg.Record, error) {
	rest := b.records
	recs := make([]kmsg.Record, 0, b.NumRecords)
	for i := int32(0); i < b.NumRecords; i++ {
		l, k := binary.Varint(rest)
		if k <= 0 || l <= 0 || l > int64(len(rest)-k) {
			return nil, fmt.Errorf("%w: record %d length", ErrCorrupt, i)
		}
		var r kmsg.Record
		if err := r.ReadFrom(rest[:k+int(l)]); err != nil {
			return nil, fmt.Errorf("%w: record %d: %v", ErrCorrupt, i, err)
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
		if len(b) < 12 {
			return out, ErrTruncated
		}
		l := int64(int32(binary.BigEndian.Uint32(b[8:])))
		if l < batchMinLength {
			return out, fmt.Errorf("%w: length %d", ErrCorrupt, l)
		}
		n := 12 + l
		if n > int64(len(b)) {
			return out, ErrTruncated
		}
		out = append(out, b[:n:n])
		b = b[n:]
	}
	return out, nil
}

// OneBatch returns b if it holds exactly one batch (Produce v3+ partition
// Records), else ErrNotSingle (wrapping the split error if any).
func OneBatch(b []byte) ([]byte, error) {
	bs, err := SplitBatches(b)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotSingle, err)
	}
	if len(bs) != 1 {
		return nil, fmt.Errorf("%w: found %d", ErrNotSingle, len(bs))
	}
	return bs[0], nil
}

// PatchBatch returns a copy of b with base offset (bytes 0..7) and partition
// leader epoch (12..15) rewritten. Both lie outside the CRC, which stays
// valid. An input shorter than 16 bytes is returned as an unchanged copy.
func PatchBatch(b []byte, baseOffset int64, leaderEpoch int32) []byte {
	out := append([]byte(nil), b...)
	if len(out) >= 16 {
		binary.BigEndian.PutUint64(out, uint64(baseOffset))
		binary.BigEndian.PutUint32(out[12:], uint32(leaderEpoch))
	}
	return out
}
