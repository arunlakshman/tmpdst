package machine

import "encoding/binary"

// envelopeMagic prefixes machine-emitted log payloads with advisory rank and
// writer so apply can drive takeover/failback without changing applier codecs.
const envelopeMagic byte = 0xC7

// wrap stamps rank and writer onto an inner payload (group or applier.Encode).
func wrap(rank int, writer string, payload []byte) []byte {
	b := []byte{envelopeMagic}
	b = binary.AppendUvarint(b, uint64(rank))
	b = binary.AppendUvarint(b, uint64(len(writer)))
	b = append(b, writer...)
	return append(b, payload...)
}

// unwrap strips a machine envelope when present. The ok flag is false when the
// bytes look like a truncated envelope.
func unwrap(data []byte) (int, string, []byte, bool) {
	if len(data) == 0 || data[0] != envelopeMagic {
		return 0, "", data, true
	}
	rest := data[1:]
	r, rest, ok := takeUvarint(rest)
	if !ok {
		return 0, "", nil, false
	}
	n, rest, ok := takeUvarint(rest)
	if !ok || n > uint64(len(rest)) {
		return 0, "", nil, false
	}
	return int(r), string(rest[:n]), rest[n:], true
}
