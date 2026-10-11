package core

import (
	"encoding/binary"
	"hash/crc32"
	"strconv"
	"strings"
)

// Object file header (see the package doc for the layout).
const (
	Magic      = "TOY3"
	HeaderSize = 16
	// MaxNameLen is the longest file name most filesystems accept; an escaped
	// key must fit in it.
	MaxNameLen = 255
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

// EscapeKey maps a key to a single safe file name: ASCII letters, digits, '-'
// and '_' pass through; every other byte becomes %XX (uppercase hex). The
// mapping is injective, and the result never contains '/', and is never "."
// or "..".
func EscapeKey(key string) string {
	var b strings.Builder
	for i := 0; i < len(key); i++ {
		c := key[i]
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		const hex = "0123456789ABCDEF"
		b.WriteByte('%')
		b.WriteByte(hex[c>>4])
		b.WriteByte(hex[c&15])
	}
	return b.String()
}

// UnescapeKey inverts EscapeKey. ok is false if name is not a valid escape.
func UnescapeKey(name string) (key string, ok bool) {
	var b strings.Builder
	for i := 0; i < len(name); i++ {
		c := name[i]
		if isUnreserved(c) {
			b.WriteByte(c)
			continue
		}
		if c != '%' || i+2 >= len(name) {
			return "", false
		}
		v, err := strconv.ParseUint(name[i+1:i+3], 16, 8)
		if err != nil || name[i+1:i+3] != strings.ToUpper(name[i+1:i+3]) || isUnreserved(byte(v)) {
			return "", false
		}
		b.WriteByte(byte(v))
		i += 2
	}
	return b.String(), true
}

func isUnreserved(c byte) bool {
	return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-' || c == '_'
}

// ValidKey reports whether key can be stored: non-empty and short enough that
// its escaped name fits in MaxNameLen.
func ValidKey(key string) bool {
	return key != "" && len(EscapeKey(key)) <= MaxNameLen
}

// ObjDir is the directory holding committed objects.
func ObjDir(root string) string { return root + "/obj" }

// TmpDir is the directory holding in-progress writes.
func TmpDir(root string) string { return root + "/tmp" }

// ObjPath is the file holding key's committed object.
func ObjPath(root, key string) string { return ObjDir(root) + "/" + EscapeKey(key) }

// TmpPath is the temp file for one write, named by the IOID of its FSWrite
// (16 hex digits). Names are unique within an incarnation, and recovery clears
// tmp/ before a new incarnation writes anything, so they never collide.
func TmpPath(root string, id IOID) string {
	s := strconv.FormatUint(uint64(id), 16)
	return TmpDir(root) + "/" + strings.Repeat("0", 16-len(s)) + s
}
