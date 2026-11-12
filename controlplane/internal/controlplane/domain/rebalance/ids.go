package rebalance

import "github.com/manifesterio/mds/dst/rng"

// base32 is the RFC 4648 base32 alphabet.
const base32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"

// newMemberID returns the id of a new member (rule 4): the client id, a dash,
// and 16 random bytes as base32 text (26 characters). With no client id it is
// the random text alone. Ids are not secrets.
func newMemberID(clientID string, r *rng.Rand) string {
	var raw [16]byte
	r.Bytes(raw[:])
	text := make([]byte, 0, 26)
	var acc uint32
	bits := 0
	for _, b := range raw {
		acc = acc<<8 | uint32(b)
		bits += 8
		for bits >= 5 {
			bits -= 5
			text = append(text, base32[acc>>bits&31])
		}
	}
	text = append(text, base32[acc<<(5-bits)&31])
	if clientID == "" {
		return string(text)
	}
	return clientID + "-" + string(text)
}
