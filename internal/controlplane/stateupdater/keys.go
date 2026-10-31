package stateupdater

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"mds/internal/core"
)

const (
	prefixTopic     = 'T'
	prefixEndOffset = 'E'
	prefixRun       = 'R'
	prefixKey       = 'K'
	prefixSlot      = 'S'
	prefixGroup     = 'G'
	prefixMember    = 'M'
	prefixOffset    = 'O'

	checksumLength = 16
	runFormat      = 1
)

var errCorrupt = errors.New("stateupdater: corrupt record")

func topicKey(name string) []byte { return append([]byte{prefixTopic}, name...) }

// topicPartitionPrefix is the prefix-free key prefix of one partition for kind p.
func topicPartitionPrefix(p byte, tp core.TopicPartition) []byte {
	return appendTopicPartition([]byte{p}, tp)
}

// appendTopicPartition appends uvarint(len topic), topic, partition (4, big endian).
func appendTopicPartition(b []byte, tp core.TopicPartition) []byte {
	b = binary.AppendUvarint(b, uint64(len(tp.Topic)))
	b = append(b, tp.Topic...)
	return binary.BigEndian.AppendUint32(b, uint32(tp.Partition))
}

// splitTopicPartition reads a partition that appendTopicPartition wrote, with
// nothing after it.
func splitTopicPartition(b []byte) (core.TopicPartition, bool) {
	r := &reader{b: b, ok: true}
	topic := r.str()
	if !r.ok || len(r.b) != 4 {
		return core.TopicPartition{}, false
	}
	return core.TopicPartition{Topic: topic, Partition: int32(binary.BigEndian.Uint32(r.b))}, true
}

// groupKey is the G key of a group: its header.
func groupKey(group string) []byte { return append([]byte{prefixGroup}, group...) }

// groupPrefix is the prefix-free key prefix of one group for kind p.
func groupPrefix(p byte, group string) []byte {
	b := binary.AppendUvarint([]byte{p}, uint64(len(group)))
	return append(b, group...)
}

// memberKey is the M key of one member of a group.
func memberKey(group, member string) []byte {
	return append(groupPrefix(prefixMember, group), member...)
}

// offsetKey is the O key of one committed partition of a group.
func offsetKey(group string, tp core.TopicPartition) []byte {
	return appendTopicPartition(groupPrefix(prefixOffset, group), tp)
}

func endKey(tp core.TopicPartition) []byte { return topicPartitionPrefix(prefixEndOffset, tp) }

func batchRunKey(tp core.TopicPartition, base int64) []byte {
	return binary.BigEndian.AppendUint64(topicPartitionPrefix(prefixRun, tp), uint64(base))
}

func duplicateCheckKey(segKey string) []byte { return append([]byte{prefixKey}, segKey...) }

func slotKey(slot int64, segKey string) []byte {
	return append(binary.BigEndian.AppendUint64([]byte{prefixSlot}, uint64(slot)), segKey...)
}

func writeVarint(n int64) []byte { return binary.AppendUvarint(nil, uint64(n)) }

// readVarint decodes a stored non-negative number (0 when absent).
func readVarint(b []byte) int64 {
	v, _ := binary.Uvarint(b)
	return int64(v)
}

// checksum identifies a request's batches (not its key): a replay is the same
// request iff the digests are equal.
func checksum(bs []core.PendingBatch) [checksumLength]byte {
	sum := sha256.Sum256(checksumBytes(bs))
	return [checksumLength]byte(sum[:checksumLength])
}

func checksumBytes(bs []core.PendingBatch) []byte {
	n := binary.AppendUvarint(nil, uint64(len(bs)))
	for _, b := range bs {
		n = binary.AppendUvarint(n, uint64(len(b.TopicPartition.Topic)))
		n = append(n, b.TopicPartition.Topic...)
		n = binary.AppendVarint(n, int64(b.TopicPartition.Partition))
		n = binary.AppendVarint(n, b.Records)
		n = binary.AppendUvarint(n, uint64(len(b.Loc.Key)))
		n = append(n, b.Loc.Key...)
		n = binary.AppendVarint(n, b.Loc.Off)
		n = binary.AppendVarint(n, b.Loc.Len)
	}
	return n
}

// duplicateCheckRecord is the stored fact that a key committed: its slot, the request
// digest, and the base offset assigned to each batch, in request order.
func duplicateCheckRecord(slot int64, dig [checksumLength]byte, rs []core.BatchRange) []byte {
	b := binary.AppendUvarint(nil, uint64(slot))
	b = append(b, dig[:]...)
	for _, r := range rs {
		b = binary.AppendUvarint(b, uint64(r.Base))
	}
	return b
}

// parseDuplicateCheck splits a record into its digest and bases.
func parseDuplicateCheck(b []byte) ([checksumLength]byte, []int64, error) {
	var dig [checksumLength]byte
	var bases []int64
	_, n := binary.Uvarint(b)
	if n <= 0 || len(b) < n+checksumLength {
		return dig, nil, errCorrupt
	}
	copy(dig[:], b[n:])
	b = b[n+checksumLength:]
	for len(b) > 0 {
		v, m := binary.Uvarint(b)
		if m <= 0 {
			return dig, nil, errCorrupt
		}
		bases, b = append(bases, int64(v)), b[m:]
	}
	return dig, bases, nil
}

// encodeBatchRun packs the batches of one run (one partition, contiguous offsets,
// in order): format byte, the distinct segment keys, then per batch the key
// index, record count, offset and length.
func encodeBatchRun(rs []core.BatchRange) []byte {
	var keys []string
	idx := map[string]int{} // lookups only
	for _, r := range rs {
		if _, ok := idx[r.Loc.Key]; !ok {
			idx[r.Loc.Key] = len(keys)
			keys = append(keys, r.Loc.Key)
		}
	}
	b := []byte{runFormat}
	b = binary.AppendUvarint(b, uint64(len(keys)))
	for _, k := range keys {
		b = binary.AppendUvarint(b, uint64(len(k)))
		b = append(b, k...)
	}
	b = binary.AppendUvarint(b, uint64(len(rs)))
	for _, r := range rs {
		b = binary.AppendUvarint(b, uint64(idx[r.Loc.Key]))
		b = binary.AppendUvarint(b, uint64(r.Count))
		b = binary.AppendVarint(b, r.Loc.Off)
		b = binary.AppendVarint(b, r.Loc.Len)
	}
	return b
}

// reader is a cursor over a packed record; the first failure sticks.
type reader struct {
	b  []byte
	ok bool
}

func (r *reader) uv() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.ok = false
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) sv() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.ok = false
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) blob() []byte {
	n := r.uv()
	if n > uint64(len(r.b)) {
		r.ok = false
		return nil
	}
	v := bytes.Clone(r.b[:n])
	r.b = r.b[n:]
	return v
}

func (r *reader) str() string {
	n := r.uv()
	if n > uint64(len(r.b)) {
		r.ok = false
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

// decodeBatchRun unpacks the run of tp that starts at base.
func decodeBatchRun(tp core.TopicPartition, base int64, b []byte) ([]core.BatchRange, error) {
	if len(b) == 0 || b[0] != runFormat {
		return nil, errCorrupt
	}
	r := &reader{b: b[1:], ok: true}
	nk := r.uv()
	if nk > uint64(len(r.b)) {
		return nil, errCorrupt
	}
	keys := make([]string, 0, nk)
	for ; nk > 0 && r.ok; nk-- {
		keys = append(keys, r.str())
	}
	n := r.uv()
	if n > uint64(len(r.b)) {
		return nil, errCorrupt
	}
	out := make([]core.BatchRange, 0, n)
	for ; n > 0 && r.ok; n-- {
		ki, cnt, off, ln := r.uv(), r.uv(), r.sv(), r.sv()
		if ki >= uint64(len(keys)) {
			return nil, errCorrupt
		}
		out = append(out, core.BatchRange{TopicPartition: tp, Base: core.Offset(base), Count: int64(cnt),
			Loc: core.SegmentRef{Key: keys[ki], Off: off, Len: ln}})
		base += int64(cnt)
	}
	if !r.ok || len(r.b) != 0 {
		return nil, errCorrupt
	}
	return out, nil
}

// batchRunBase is the base offset in an R key.
func batchRunBase(key []byte) int64 {
	return int64(binary.BigEndian.Uint64(key[len(key)-8:]))
}
