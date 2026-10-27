package machine

import "manifester/controlplane/applier"

// DecodeGroup exports group decoding for tests.
func DecodeGroup(data []byte) (string, []applier.Entry, bool) {
	return decodeGroup(data)
}

// EncodeGroupForTest builds a multi-command entry with a fixed id.
func EncodeGroupForTest(id string, cmds []applier.Entry) []byte {
	in := make([]applier.Entry, len(cmds))
	for i, c := range cmds {
		c.ID = id
		in[i] = c
	}
	return encodeGroup(id, in)
}
