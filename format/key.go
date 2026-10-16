package format

import (
	"strings"

	"manifester/core"
)

// Segment keys are "seg/<escaped node>/<incarnation hex>/<id hex>". The ID
// comes from Env.NewID, never from a ReqID (decision 10), so a key is unique
// across incarnations and retries of one commit reuse it.
//
// NodeID is escaped because it is free text: unescaped, node "a/b" with
// incarnation i would collide with the path of node "a" and break parsing,
// and on case-insensitive stores (a local FS, some S3 gateways) "Node" and
// "node" would alias. Escaping is injective: only [a-z0-9_-] pass through;
// every other byte, including '/', '%', '.', and upper-case letters, becomes
// %XX with upper-case hex. The result has no '/', so the key splits into
// exactly four parts. Inc and ID are fixed-width lower-case hex.

const keyPrefix = "seg/"

const hexDigits = "0123456789abcdef"

func plain(c byte) bool {
	return 'a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '-' || c == '_'
}

// EscapeNode is the injective escaping of a NodeID used in segment keys.
func EscapeNode(n core.NodeID) string {
	var b strings.Builder
	for i := 0; i < len(n); i++ {
		c := n[i]
		if plain(c) {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte("0123456789ABCDEF"[c>>4])
		b.WriteByte("0123456789ABCDEF"[c&15])
	}
	return b.String()
}

func unescapeNode(s string) (core.NodeID, bool) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if plain(c) {
			b.WriteByte(c)
			continue
		}
		if c != '%' || i+2 >= len(s) {
			return "", false
		}
		hi, ok1 := upperNibble(s[i+1])
		lo, ok2 := upperNibble(s[i+2])
		v := hi<<4 | lo
		if !ok1 || !ok2 || plain(v) {
			return "", false // non-canonical: would not round-trip
		}
		b.WriteByte(v)
		i += 2
	}
	return core.NodeID(b.String()), true
}

func upperNibble(c byte) (byte, bool) {
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

func putHex(b *strings.Builder, id [16]byte) {
	for _, c := range id {
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&15])
	}
}

func parseHex(s string) (id [16]byte, ok bool) {
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
func SegmentKey(node core.NodeID, inc core.Incarnation, id core.ID) string {
	var b strings.Builder
	b.WriteString(keyPrefix)
	b.WriteString(EscapeNode(node))
	b.WriteByte('/')
	putHex(&b, inc)
	b.WriteByte('/')
	putHex(&b, id)
	return b.String()
}

// ParseSegmentKey inverts SegmentKey. ok is false for any string SegmentKey
// could not have produced.
func ParseSegmentKey(key string) (node core.NodeID, inc core.Incarnation, id core.ID, ok bool) {
	rest, found := strings.CutPrefix(key, keyPrefix)
	if !found {
		return
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 3 {
		return
	}
	n, ok1 := unescapeNode(parts[0])
	i, ok2 := parseHex(parts[1])
	d, ok3 := parseHex(parts[2])
	if !ok1 || !ok2 || !ok3 {
		return
	}
	return n, core.Incarnation(i), core.ID(d), true
}
