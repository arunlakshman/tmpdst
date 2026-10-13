package format

import (
	"strconv"
	"strings"
)

// MaxNameLen is the longest file name most filesystems accept; an escaped
// key must fit in it.
const MaxNameLen = 255

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
func TmpPath(root string, id uint64) string {
	s := strconv.FormatUint(id, 16)
	return TmpDir(root) + "/" + strings.Repeat("0", 16-len(s)) + s
}
