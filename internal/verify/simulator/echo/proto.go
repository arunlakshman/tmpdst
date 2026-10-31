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

	"manifester/internal/core"
)

const maxFrame = 1 << 20

// Topic and partition of the one log the echo broker writes.
const Topic = "echo"

func topicPartition() core.TopicPartition { return core.NewTopicPartition(Topic, 0) }

func appendFrame(dst []byte, body string) []byte {
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(body)))
	return append(dst, body...)
}

// framer reassembles frames from arbitrarily split chunks.
type framer struct{ buf []byte }

// push returns every complete frame; bad reports an unrecoverable stream.
func (f *framer) push(d []byte) ([]string, bool) {
	f.buf = append(f.buf, d...)
	var frames []string
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
