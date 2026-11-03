package replica

import "encoding/binary"

// envelopeMarker prefixes machine-emitted log payloads with the writer epoch and
// the writer name. A reader can tell which writer wrote an entry, and in which
// epoch, without changing the metadata codecs.
const envelopeMarker byte = 0xC8

// wrap stamps the epoch and the writer name onto an inner payload (group or
// metadata.Encode).
func wrap(epoch uint64, writer string, payload []byte) []byte {
	b := []byte{envelopeMarker}
	b = binary.AppendUvarint(b, epoch)
	b = binary.AppendUvarint(b, uint64(len(writer)))
	b = append(b, writer...)
	return append(b, payload...)
}

// unwrap strips a machine envelope when present. The ok flag is false when the
// bytes look like a truncated envelope.
func unwrap(data []byte) (uint64, string, []byte, bool) {
	if len(data) == 0 || data[0] != envelopeMarker {
		return 0, "", data, true
	}
	rest := data[1:]
	e, rest, ok := takeUvarint(rest)
	if !ok {
		return 0, "", nil, false
	}
	n, rest, ok := takeUvarint(rest)
	if !ok || n > uint64(len(rest)) {
		return 0, "", nil, false
	}
	return e, string(rest[:n]), rest[n:], true
}

// EntryWriter reads the epoch and the writer name from a log entry. The ok flag
// is false when the entry has no valid envelope. The simulator uses it to check
// that one epoch has one writer.
func EntryWriter(data []byte) (uint64, string, bool) {
	if len(data) == 0 || data[0] != envelopeMarker {
		return 0, "", false
	}
	epoch, writer, _, ok := unwrap(data)
	return epoch, writer, ok
}
