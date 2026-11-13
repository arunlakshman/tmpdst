package group

import (
	"bytes"
	"errors"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
)

// corruptOrValue runs a state-record decoder. These decoders panic with
// errCorrupt on purpose when a stored record is damaged (fail-stop on corrupt
// state: the node must not go on with a state it cannot read). That panic is the
// intended answer to bad bytes, so it is allowed here. Any other panic is a bug,
// and it is raised again. It reports whether the record decoded.
func corruptOrValue(t *testing.T, decode func()) bool {
	t.Helper()
	decoded := false
	func() {
		defer func() {
			if p := recover(); p != nil {
				if err, ok := p.(error); ok && errors.Is(err, errCorrupt) {
					return
				}
				panic(p)
			}
		}()
		decode()
		decoded = true
	}()
	return decoded
}

// FuzzDecodeHeader: a stored group header decodes, or panics with errCorrupt
// (the intended fail-stop). A header that decoded encodes to the same header.
func FuzzDecodeHeader(f *testing.F) {
	f.Add(Header{Generation: 3, ProtocolType: "consumer", Protocol: "range", Leader: "m", AssignedGeneration: 2}.encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var h Header
		if !corruptOrValue(t, func() { h = decodeHeader("g", b) }) {
			return
		}
		if h2 := decodeHeader("g", h.encode()); h2 != h {
			t.Fatalf("round trip: %+v, %+v", h, h2)
		}
	})
}

// FuzzDecodeMember: a stored member record decodes, or panics with errCorrupt
// (the intended fail-stop). A member that decoded re-encodes to bytes that
// decode and re-encode to the same bytes.
func FuzzDecodeMember(f *testing.F) {
	f.Add(encodeMember(contract.GroupMemberInfo{
		GroupMember:  contract.GroupMember{ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte("md")},
		JoinedTimeMs: 7, Assignment: []byte("as"),
	}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var m contract.GroupMemberInfo
		if !corruptOrValue(t, func() { m = decodeMember("g", "id", b) }) {
			return
		}
		again := encodeMember(m)
		if !bytes.Equal(encodeMember(decodeMember("g", "id", again)), again) {
			t.Fatalf("re-encoding changed the bytes")
		}
	})
}

// FuzzDecodeOffset: a stored offset record decodes, or panics with errCorrupt
// (the intended fail-stop). An offset that decoded encodes to the same offset.
func FuzzDecodeOffset(f *testing.F) {
	f.Add(offsetRecord{offset: 5, epoch: 1, meta: "md", time: 99}.encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var o offsetRecord
		if !corruptOrValue(t, func() { o = decodeOffset("g", b) }) {
			return
		}
		if o2 := decodeOffset("g", o.encode()); o2 != o {
			t.Fatalf("round trip: %+v, %+v", o, o2)
		}
	})
}
