package group

import (
	"bytes"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
)

// TestRecordBytesPinned pins the bytes of the three state records. They are
// the stored state, so they must not change when the rules move.
func TestRecordBytesPinned(t *testing.T) {
	h := Header{Generation: 3, ProtocolType: "consumer", Protocol: "range", Leader: "m1", AssignedGeneration: 2}
	wantHeader := []byte{6, 8, 99, 111, 110, 115, 117, 109, 101, 114, 5, 114, 97, 110, 103, 101, 2, 109, 49, 4}
	if got := h.encode(); !bytes.Equal(got, wantHeader) {
		t.Errorf("header: %v, want %v", got, wantHeader)
	}
	if got := decodeHeader("g", wantHeader); got != h {
		t.Errorf("header decode: %+v", got)
	}

	m := contract.GroupMemberInfo{
		GroupMember:  contract.GroupMember{ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000, Metadata: []byte("md")},
		JoinedTimeMs: 1000, Assignment: []byte("as"),
	}
	wantMember := []byte{1, 99, 1, 104, 1, 105, 160, 156, 1, 224, 212, 3, 208, 15, 2, 109, 100, 2, 97, 115}
	if got := encodeMember(m); !bytes.Equal(got, wantMember) {
		t.Errorf("member: %v, want %v", got, wantMember)
	}
	m.MemberID = "id"
	if got := decodeMember("g", "id", wantMember); !bytes.Equal(encodeMember(got), wantMember) || got.MemberID != "id" || string(got.Assignment) != "as" {
		t.Errorf("member decode: %+v", got)
	}

	o := offsetRecord{offset: 5, epoch: -1, meta: "md", time: 4000}
	wantOffset := []byte{10, 1, 2, 109, 100, 192, 62}
	if got := o.encode(); !bytes.Equal(got, wantOffset) {
		t.Errorf("offset: %v, want %v", got, wantOffset)
	}
	if got := decodeOffset("g", wantOffset); got != o {
		t.Errorf("offset decode: %+v", got)
	}
}

// A damaged record stops the node: it never decodes to a made-up value.
func TestCorruptRecordsPanic(t *testing.T) {
	for name, f := range map[string]func(){
		"header":        func() { decodeHeader("g", []byte{0x80}) },
		"header tail":   func() { decodeHeader("g", append(Header{}.encode(), 0)) },
		"member":        func() { decodeMember("g", "m", []byte{9}) },
		"offset":        func() { decodeOffset("g", []byte{0x80}) },
		"offset tail":   func() { decodeOffset("g", append(offsetRecord{}.encode(), 0)) },
		"empty header":  func() { decodeHeader("g", nil) },
		"empty offset":  func() { decodeOffset("g", nil) },
		"empty member":  func() { decodeMember("g", "m", nil) },
		"short member":  func() { decodeMember("g", "m", []byte{1, 'c'}) },
		"member suffix": func() { decodeMember("g", "m", append(encodeMember(contract.GroupMemberInfo{}), 0)) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic on a corrupt record", name)
				}
			}()
			f()
		}()
	}
}
