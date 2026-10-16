// Package echo is the milestone-1 smoke system: a toy broker and closed-loop
// workload that drive every engine (object store, control plane, network)
// through the real kernel before any Kafka logic exists.
//
// Protocol: frames are a 4-byte big-endian length then a text body.
//
//	PUT <payload>  ->  OK <offset> | ERR | ERR maybe
//	GET <offset>   ->  VAL <payload> | ERR notfound | ERR corrupt | ERR | ERR maybe
//
// "ERR" proves the request had no effect; "ERR maybe" may have committed.
// A connection carries one outstanding request at a time.
package echo

import (
	"encoding/binary"

	"manifester/core"
)

const maxFrame = 1 << 20

// Topic and partition of the one log the echo broker writes.
const Topic = "echo"

func tp() core.TopicPartition { return core.TopicPartition{Topic: Topic, Partition: 0} }

func appendFrame(dst []byte, body string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	return append(dst, body...)
}

// framer reassembles frames from arbitrarily split chunks.
type framer struct{ buf []byte }

// push returns every complete frame; bad reports an unrecoverable stream.
func (f *framer) push(d []byte) (frames []string, bad bool) {
	f.buf = append(f.buf, d...)
	for len(f.buf) >= 4 {
		n := binary.BigEndian.Uint32(f.buf)
		if n > maxFrame {
			return frames, true
		}
		if uint64(len(f.buf)) < 4+uint64(n) {
			break
		}
		frames = append(frames, string(f.buf[4:4+n]))
		f.buf = append([]byte(nil), f.buf[4+n:]...)
	}
	return frames, false
}

// hasher is FNV-1a; hash/fnv is off the deterministic allow-list.
type hasher uint64

func newHasher() hasher { return 14695981039346656037 }

func (h *hasher) byte(b byte) { *h = (*h ^ hasher(b)) * 1099511628211 }
func (h *hasher) u64(v uint64) {
	for i := 0; i < 8; i++ {
		h.byte(byte(v >> (8 * i)))
	}
}
func (h *hasher) bool(b bool) {
	if b {
		h.u64(1)
	} else {
		h.u64(0)
	}
}
func (h *hasher) bytes(b []byte) {
	h.u64(uint64(len(b)))
	for _, c := range b {
		h.byte(c)
	}
}
func (h *hasher) str(s string) {
	h.u64(uint64(len(s)))
	for i := 0; i < len(s); i++ {
		h.byte(s[i])
	}
}
