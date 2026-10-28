package format

import (
	"encoding/hex"
	"strings"

	"manifester/internal/core"
)

// Segment keys are "seg/<id hex>": one flat name (decision 43). The ID is a
// random 128-bit value the broker draws from Env.NewID, never from a ReqID
// (decision 10), so a key is unique across nodes, boots and segments, and a
// retry of one commit reuses it. The hex is 32 lower-case digits, so the key
// has a single canonical spelling and no case aliasing on case-insensitive
// stores.

const keyPrefix = "seg/"

// SegmentKey names the object of one segment upload.
func SegmentKey(id core.ID) string {
	return keyPrefix + hex.EncodeToString(id[:])
}

// ParseSegmentKey inverts SegmentKey. ok is false for any string SegmentKey
// could not have produced, including the old nested "seg/<node>/<inc>/<id>".
func ParseSegmentKey(key string) (core.ID, bool) {
	var id core.ID
	rest, found := strings.CutPrefix(key, keyPrefix)
	if !found {
		return id, false
	}
	if _, err := hex.Decode(id[:], []byte(rest)); err != nil {
		return core.ID{}, false
	}
	// hex.Decode accepts upper case; only the canonical spelling is a key.
	if SegmentKey(id) != key {
		return core.ID{}, false
	}
	return id, true
}
