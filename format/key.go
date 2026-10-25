package format

import (
	"strings"

	"manifester/core"
)

// Segment keys are "seg/<id hex>": one flat name (decision 43). The ID is a
// random 128-bit value the broker draws from Env.NewID, never from a ReqID
// (decision 10), so a key is unique across nodes, boots and segments, and a
// retry of one commit reuses it. The hex is 32 lower-case digits, so the key
// has a single canonical spelling and no case aliasing on case-insensitive
// stores.

const keyPrefix = "seg/"

const hexDigits = "0123456789abcdef"

func putHex(b *strings.Builder, id [16]byte) {
	for _, c := range id {
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&15])
	}
}

func parseHex(s string) ([16]byte, bool) {
	var id [16]byte
	if len(s) != 32 {
		return id, false
	}
	for i := range id {
		var v [2]byte
		for j := 0; j < 2; j++ {
			c := s[2*i+j]
			switch {
			case '0' <= c && c <= '9':
				v[j] = c - '0'
			case 'a' <= c && c <= 'f':
				v[j] = c - 'a' + 10
			default:
				return id, false
			}
		}
		id[i] = v[0]<<4 | v[1]
	}
	return id, true
}

// SegmentKey names the object of one segment upload.
func SegmentKey(id core.ID) string {
	var b strings.Builder
	b.WriteString(keyPrefix)
	putHex(&b, id)
	return b.String()
}

// ParseSegmentKey inverts SegmentKey. ok is false for any string SegmentKey
// could not have produced, including the old nested "seg/<node>/<inc>/<id>".
func ParseSegmentKey(key string) (core.ID, bool) {
	var id core.ID
	rest, found := strings.CutPrefix(key, keyPrefix)
	if !found {
		return id, false
	}
	d, ok := parseHex(rest)
	if !ok {
		return id, false
	}
	return core.ID(d), true
}
