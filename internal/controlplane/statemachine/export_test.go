package statemachine

import "github.com/manifesterio/mds/internal/controlplane/stateupdater"

// Timer IDs for role and checkpoint tests.
const (
	TimerRole       = timerRole
	TimerCheckpoint = timerCheckpoint
)

// DecodeGroup exports group decoding for tests (strips the machine envelope).
func DecodeGroup(data []byte) (string, []stateupdater.Entry, bool) {
	return decodeGroup(data)
}

// UnwrapPayload exports envelope stripping for tests.
func UnwrapPayload(data []byte) ([]byte, bool) {
	_, _, payload, ok := unwrap(data)
	return payload, ok
}

// EncodeGroupForTest builds a multi-command entry with a fixed id.
func EncodeGroupForTest(id string, cmds []stateupdater.Entry) []byte {
	in := make([]stateupdater.Entry, len(cmds))
	for i, c := range cmds {
		c.ID = id
		in[i] = c
	}
	return encodeGroup(id, in)
}

// EncodeEnvelopedForTest wraps an inner payload with epoch and writer.
func EncodeEnvelopedForTest(epoch uint64, writer string, payload []byte) []byte {
	return wrap(epoch, writer, payload)
}
