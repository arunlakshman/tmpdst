package format

import (
	"encoding/binary"
	"hash/crc32"
)

// Object file header (see the package doc for the layout).
const (
	Magic      = "TOY3"
	HeaderSize = 16
)

// Checksum is the CRC-32 (IEEE) stored in the object header.
func Checksum(payload []byte) uint32 { return crc32.ChecksumIEEE(payload) }

// EncodeObject returns the full object file for payload: header + payload.
func EncodeObject(payload []byte) []byte {
	b := make([]byte, HeaderSize+len(payload))
	copy(b, Magic)
	binary.LittleEndian.PutUint32(b[4:], Checksum(payload))
	binary.LittleEndian.PutUint64(b[8:], uint64(len(payload)))
	copy(b[HeaderSize:], payload)
	return b
}

// DecodeObject validates an object file and returns its payload (aliasing
// file). ok is false if the file is short, has the wrong magic, a length that
// does not match, or a CRC mismatch: i.e. it is torn or corrupt.
func DecodeObject(file []byte) (payload []byte, ok bool) {
	if len(file) < HeaderSize || string(file[:4]) != Magic {
		return nil, false
	}
	sum := binary.LittleEndian.Uint32(file[4:])
	n := binary.LittleEndian.Uint64(file[8:])
	if n != uint64(len(file)-HeaderSize) {
		return nil, false
	}
	payload = file[HeaderSize:]
	if Checksum(payload) != sum {
		return nil, false
	}
	return payload, true
}
