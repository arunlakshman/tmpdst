package sim

import (
	"encoding/binary"

	"manifester/toys3/control"
)

// Self-describing values: the shared kernel between the stimulus (Workload,
// which writes them) and verification (the oracle and NoCorruptReads, which
// read them). Any bytes a GET returns name the PUT that wrote them.

// ValueHeader is the size of the self-describing prefix of every value:
// writer ClientReq (8 bytes LE) + total length (4 bytes LE).
const ValueHeader = 12

// MakeValue returns the unique value written by PUT req: ValueHeader followed
// by filler that is a pure function of (req, size). size >= ValueHeader.
func MakeValue(req control.ClientReq, size int) []byte {
	if size < ValueHeader {
		size = ValueHeader
	}
	b := make([]byte, size)
	binary.LittleEndian.PutUint64(b, uint64(req))
	binary.LittleEndian.PutUint32(b[8:], uint32(size))
	x := uint64(req)*0x9e3779b97f4a7c15 + uint64(size)
	for i := ValueHeader; i < size; i++ {
		x = Fold(x, uint64(i))
		b[i] = byte(x)
	}
	return b
}

// ValueWriter parses b's header and returns the PUT that wrote it. ok is
// false unless b is byte-for-byte equal to MakeValue(writer, len(b)), so a
// torn or mixed value is always rejected.
func ValueWriter(b []byte) (writer control.ClientReq, ok bool) {
	if len(b) < ValueHeader || int(binary.LittleEndian.Uint32(b[8:])) != len(b) {
		return 0, false
	}
	writer = control.ClientReq(binary.LittleEndian.Uint64(b))
	want := MakeValue(writer, len(b))
	for i := range b {
		if b[i] != want[i] {
			return 0, false
		}
	}
	return writer, true
}
