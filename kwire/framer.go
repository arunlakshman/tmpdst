package kwire

import (
	"encoding/binary"
	"errors"
	"fmt"

	"manifester/core"
)

// ErrBadFrame means the stream carried an invalid length; it is unrecoverable.
var ErrBadFrame = errors.New("kwire: bad frame length")

// Framer reassembles 4-byte big-endian length-prefixed frames. It implements
// core.Framer. After an error every later Push returns the same error.
type Framer struct {
	max int32
	buf []byte
	err error
}

var _ core.Framer = (*Framer)(nil)

// NewFramer returns a Framer rejecting frames longer than max bytes.
func NewFramer(max int32) *Framer { return &Framer{max: max} }

// Push appends data and returns the complete frames, without length prefix.
// Each frame is a fresh slice that is never touched again.
func (f *Framer) Push(data []byte) ([][]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.buf = append(f.buf, data...)
	var frames [][]byte
	off := 0
	for len(f.buf)-off >= 4 {
		n := int32(binary.BigEndian.Uint32(f.buf[off:]))
		if n <= 0 || n > f.max {
			f.err = fmt.Errorf("%w: %d (max %d)", ErrBadFrame, n, f.max)
			f.buf = nil
			return frames, f.err
		}
		if len(f.buf)-off-4 < int(n) {
			break
		}
		frames = append(frames, append([]byte(nil), f.buf[off+4:off+4+int(n)]...))
		off += 4 + int(n)
	}
	f.buf = f.buf[:copy(f.buf, f.buf[off:])]
	return frames, nil
}

// Buffered reports bytes held waiting for the rest of a frame.
func (f *Framer) Buffered() int { return len(f.buf) }
