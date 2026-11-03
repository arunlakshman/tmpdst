package kv

import (
	"bytes"
	"encoding/binary"

	"github.com/manifesterio/mds/internal/core"
)

// Reader is a cursor over a packed record. The first failure sticks: OK stays
// false from then on, so a caller can read a whole record and check OK or Done
// once at the end. Numbers are varints; strings and byte fields are uvarint
// length then bytes.
type Reader struct {
	b  []byte
	ok bool
}

// NewReader returns a Reader over b.
func NewReader(b []byte) *Reader { return &Reader{b: b, ok: true} }

// OK reports whether every read so far succeeded.
func (r *Reader) OK() bool { return r.ok }

// Rest is the bytes not read yet.
func (r *Reader) Rest() []byte { return r.b }

// Done reports whether every read succeeded and nothing is left over.
func (r *Reader) Done() bool { return r.ok && len(r.b) == 0 }

// Uvarint reads an unsigned varint.
func (r *Reader) Uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.ok = false
		return 0
	}
	r.b = r.b[n:]
	return v
}

// Varint reads a signed (zigzag) varint.
func (r *Reader) Varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.ok = false
		return 0
	}
	r.b = r.b[n:]
	return v
}

// Blob reads a length-prefixed byte field and returns a copy.
func (r *Reader) Blob() []byte {
	n := r.Uvarint()
	if n > uint64(len(r.b)) {
		r.ok = false
		return nil
	}
	v := bytes.Clone(r.b[:n])
	r.b = r.b[n:]
	return v
}

// String reads a length-prefixed string.
func (r *Reader) String() string {
	n := r.Uvarint()
	if n > uint64(len(r.b)) {
		r.ok = false
		return ""
	}
	s := string(r.b[:n])
	r.b = r.b[n:]
	return s
}

// AppendTopicPartition appends uvarint(len topic), topic, partition (4, big endian).
func AppendTopicPartition(b []byte, topicPartition core.TopicPartition) []byte {
	b = binary.AppendUvarint(b, uint64(len(topicPartition.Topic)))
	b = append(b, topicPartition.Topic...)
	return binary.BigEndian.AppendUint32(b, uint32(topicPartition.Partition))
}

// SplitTopicPartition reads a partition that AppendTopicPartition wrote, with
// nothing after it.
func SplitTopicPartition(b []byte) (core.TopicPartition, bool) {
	r := NewReader(b)
	topic := r.String()
	if !r.OK() || len(r.Rest()) != 4 {
		return core.TopicPartition{}, false
	}
	return core.TopicPartition{Topic: topic, Partition: int32(binary.BigEndian.Uint32(r.Rest()))}, true
}
