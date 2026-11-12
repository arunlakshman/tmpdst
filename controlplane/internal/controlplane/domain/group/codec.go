package group

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
)

// The state records of the group context, written by hand. These bytes are
// the stored state: the checkpoint and the state hash read them, so a change
// here moves the golden hashes. Numbers are zigzag varints; strings and byte
// fields are uvarint length then bytes.

var errCorrupt = errors.New("groups: corrupt record")

// offsetRecord is the O record of one committed partition.
type offsetRecord struct {
	offset int64
	epoch  int32
	meta   string
	time   int64
}

// blobOrNil copies b; an empty field reads back as nil.
func blobOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}

// corruptGroup stops the node: a stored record of group cannot be read
// (fail-stop on corrupt state).
func corruptGroup(group string) {
	panic(fmt.Errorf("groups: group %q: %w", group, errCorrupt))
}

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBlob(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func (h Header) encode() []byte {
	b := binary.AppendVarint(nil, int64(h.Generation))
	b = appendStr(b, h.ProtocolType)
	b = appendStr(b, h.Protocol)
	b = appendStr(b, h.Leader)
	return binary.AppendVarint(b, int64(h.AssignedGeneration))
}

func decodeHeader(group string, b []byte) Header {
	r := engine.NewReader(b)
	h := Header{Generation: int32(r.Varint()), ProtocolType: r.String(), Protocol: r.String(), Leader: r.String(), AssignedGeneration: int32(r.Varint())}
	if !r.Done() {
		corruptGroup(group)
	}
	return h
}

func encodeMember(m contract.GroupMemberInfo) []byte {
	b := appendStr(nil, m.ClientID)
	b = appendStr(b, m.ClientHost)
	b = appendStr(b, m.GroupInstanceID)
	b = binary.AppendVarint(b, int64(m.SessionTimeoutMs))
	b = binary.AppendVarint(b, int64(m.RebalanceTimeoutMs))
	b = binary.AppendVarint(b, m.JoinedTimeMs)
	b = appendBlob(b, m.Metadata)
	return appendBlob(b, m.Assignment)
}

// decodeMember reads a member record; id is the member id from its key.
func decodeMember(group, id string, b []byte) contract.GroupMemberInfo {
	r := engine.NewReader(b)
	m := contract.GroupMemberInfo{GroupMember: contract.GroupMember{MemberID: id}}
	m.ClientID = r.String()
	m.ClientHost = r.String()
	m.GroupInstanceID = r.String()
	m.SessionTimeoutMs = int32(r.Varint())
	m.RebalanceTimeoutMs = int32(r.Varint())
	m.JoinedTimeMs = r.Varint()
	m.Metadata = blobOrNil(r.Blob())
	m.Assignment = blobOrNil(r.Blob())
	if !r.Done() {
		corruptGroup(group)
	}
	return m
}

func (o offsetRecord) encode() []byte {
	b := binary.AppendVarint(nil, o.offset)
	b = binary.AppendVarint(b, int64(o.epoch))
	b = appendStr(b, o.meta)
	return binary.AppendVarint(b, o.time)
}

func decodeOffset(group string, b []byte) offsetRecord {
	r := engine.NewReader(b)
	o := offsetRecord{offset: r.Varint(), epoch: int32(r.Varint()), meta: r.String(), time: r.Varint()}
	if !r.Done() {
		corruptGroup(group)
	}
	return o
}
