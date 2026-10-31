package stateupdater

import (
	"encoding/binary"
	"fmt"

	"mds/internal/core"
)

// Group commands (slice 4a). The fold of GroupInstall, GroupAssign,
// GroupRemove and OffsetCommit. The rules are in internal/verify/speccheck
// (groups.go), which is the oracle. Every command is checked first and
// written after, so an error writes nothing.

// maxGroupCommand is the largest group command, by the size rule below.
const maxGroupCommand = 256 << 10

// groupHeader is the G record of a group.
type groupHeader struct {
	generation  int32
	protoType   string
	protocol    string
	leader      string
	assignedGen int32
}

// offsetRecord is the O record of one committed partition.
type offsetRecord struct {
	offset int64
	epoch  int32
	meta   string
	time   int64
}

// cmdSize is the size rule of a group command: 16 for the command, 4 + length
// for each string or byte field, 10 for each number and each list length.
// The stored bytes are never longer than this count, so a command that passes
// the rule fits the log item.
type cmdSize struct{ n int }

func (c *cmdSize) str(s string)   { c.n += 4 + len(s) }
func (c *cmdSize) bytes(b []byte) { c.n += 4 + len(b) }
func (c *cmdSize) num()           { c.n += 10 }

func installSize(operation core.GroupInstall) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.str(operation.ProtocolType)
	c.str(operation.Protocol)
	c.num()
	c.str(operation.Leader)
	c.num()
	c.num()
	for _, m := range operation.Members {
		c.str(m.MemberID)
		c.str(m.ClientID)
		c.str(m.ClientHost)
		c.str(m.GroupInstanceID)
		c.num()
		c.num()
		c.bytes(m.Metadata)
	}
	return c.n
}

func assignSize(operation core.GroupAssign) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.num()
	c.str(operation.Leader)
	c.num()
	c.num()
	for _, a := range operation.Assignments {
		c.str(a.Member)
		c.bytes(a.Bytes)
	}
	return c.n
}

func removeSize(operation core.GroupRemove) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.num()
	c.str(operation.Reason)
	c.num()
	for _, m := range operation.Members {
		c.str(m)
	}
	return c.n
}

func offsetCommitSize(operation core.OffsetCommit) int {
	c := &cmdSize{n: 16}
	c.str(operation.Group)
	c.str(operation.Member)
	c.num()
	c.num()
	c.num()
	for _, o := range operation.Offsets {
		c.str(o.TopicPartition.Topic)
		c.num()
		c.num()
		c.num()
		c.str(o.Metadata)
	}
	return c.n
}

// blobOrNil copies b; an empty field reads back as nil.
func blobOrNil(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}

func corruptGroup(group string) {
	panic(fmt.Errorf("stateupdater: group %q: %w", group, errCorrupt))
}

// Record codecs. Numbers are zigzag varints; strings and byte fields are
// uvarint length then bytes.

func appendStr(b []byte, s string) []byte {
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

func appendBlob(b, v []byte) []byte {
	b = binary.AppendUvarint(b, uint64(len(v)))
	return append(b, v...)
}

func (h groupHeader) encode() []byte {
	b := binary.AppendVarint(nil, int64(h.generation))
	b = appendStr(b, h.protoType)
	b = appendStr(b, h.protocol)
	b = appendStr(b, h.leader)
	return binary.AppendVarint(b, int64(h.assignedGen))
}

func decodeHeader(group string, b []byte) groupHeader {
	r := &reader{b: b, ok: true}
	h := groupHeader{generation: int32(r.sv()), protoType: r.str(), protocol: r.str(), leader: r.str(), assignedGen: int32(r.sv())}
	if !r.ok || len(r.b) != 0 {
		corruptGroup(group)
	}
	return h
}

func encodeMember(m core.GroupMemberInfo) []byte {
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
func decodeMember(group, id string, b []byte) core.GroupMemberInfo {
	r := &reader{b: b, ok: true}
	m := core.GroupMemberInfo{GroupMember: core.GroupMember{MemberID: id}}
	m.ClientID = r.str()
	m.ClientHost = r.str()
	m.GroupInstanceID = r.str()
	m.SessionTimeoutMs = int32(r.sv())
	m.RebalanceTimeoutMs = int32(r.sv())
	m.JoinedTimeMs = r.sv()
	m.Metadata = blobOrNil(r.blob())
	m.Assignment = blobOrNil(r.blob())
	if !r.ok || len(r.b) != 0 {
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
	r := &reader{b: b, ok: true}
	o := offsetRecord{offset: r.sv(), epoch: int32(r.sv()), meta: r.str(), time: r.sv()}
	if !r.ok || len(r.b) != 0 {
		corruptGroup(group)
	}
	return o
}

// groupTransaction runs one group write in tx.
func groupTransaction(transaction Transaction, operation core.ControlPlaneOperation) core.ControlPlaneDone {
	switch x := operation.(type) {
	case core.GroupInstall:
		return groupAnswer(installGroup(transaction, x))
	case core.GroupAssign:
		return groupAnswer(assignGroup(transaction, x))
	case core.GroupRemove:
		return groupAnswer(removeGroup(transaction, x))
	case core.OffsetCommit:
		return commitOffsets(transaction, x)
	}
	return core.ControlPlaneDone{Err: core.ErrInvalid}
}

// groupAnswer is the answer to a group write that has no response.
func groupAnswer(code core.ErrCode) core.ControlPlaneDone {
	if code != core.OK {
		return core.ControlPlaneDone{Err: code}
	}
	return core.ControlPlaneDone{Outcome: core.Applied}
}

// groupWrite runs one group write in its own transaction.
//
//nolint:dupl // same shape as the reads, but in a write transaction
func (a *StateUpdater) groupWrite(operation core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.keyValue.Update(func(transaction Transaction) { d = groupTransaction(transaction, operation) })
	return d, err
}

// readHeader returns the header of group; found is false when it was never installed.
func readHeader(transaction Transaction, group string) (groupHeader, bool) {
	v := transaction.Get(groupKey(group))
	if v == nil {
		return groupHeader{}, false
	}
	return decodeHeader(group, v), true
}

// memberKeys lists the M keys of a group, in member id order.
func memberKeys(transaction Transaction, group string) [][]byte {
	var keys [][]byte
	lo := groupPrefix(prefixMember, group)
	transaction.Ascend(lo, prefixEnd(lo), func(k, _ []byte) bool {
		keys = append(keys, append([]byte(nil), k...))
		return true
	})
	return keys
}

// readMembers lists the members of a group, sorted by member id.
func readMembers(transaction Transaction, group string) []core.GroupMemberInfo {
	var out []core.GroupMemberInfo
	lo := groupPrefix(prefixMember, group)
	transaction.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		out = append(out, decodeMember(group, string(k[len(lo):]), v))
		return true
	})
	return out
}

func installGroup(transaction Transaction, operation core.GroupInstall) core.ErrCode {
	if installSize(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	if !validInstall(operation) {
		return core.ErrInvalid
	}
	h, _ := readHeader(transaction, operation.Group)
	if operation.Generation != h.generation+1 {
		return core.ErrIllegalGeneration
	}
	for _, k := range memberKeys(transaction, operation.Group) {
		transaction.Delete(k)
	}
	for _, m := range operation.Members {
		info := core.GroupMemberInfo{GroupMember: m, JoinedTimeMs: operation.TimeMs}
		transaction.Set(memberKey(operation.Group, m.MemberID), encodeMember(info))
	}
	next := groupHeader{generation: operation.Generation, protoType: operation.ProtocolType, protocol: operation.Protocol, leader: operation.Leader}
	transaction.Set(groupKey(operation.Group), next.encode())
	return core.OK
}

// validInstall checks the shape of a join result: a group name, members with
// unique non-empty ids, and a leader that is one of them.
func validInstall(operation core.GroupInstall) bool {
	if operation.Group == "" || len(operation.Members) == 0 {
		return false
	}
	ids := map[string]bool{}
	for _, m := range operation.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return false
		}
		ids[m.MemberID] = true
	}
	return ids[operation.Leader]
}

func assignGroup(transaction Transaction, operation core.GroupAssign) core.ErrCode {
	if assignSize(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	h, found := readHeader(transaction, operation.Group)
	if operation.Generation != h.generation {
		return core.ErrIllegalGeneration
	}
	if !found || operation.Leader != h.leader || transaction.Get(memberKey(operation.Group, operation.Leader)) == nil {
		return core.ErrUnknownMember
	}
	want := map[string][]byte{}
	for _, a := range operation.Assignments {
		if transaction.Get(memberKey(operation.Group, a.Member)) == nil {
			return core.ErrUnknownMember
		}
		if _, dup := want[a.Member]; dup {
			return core.ErrInvalid
		}
		want[a.Member] = a.Bytes
	}
	for _, m := range readMembers(transaction, operation.Group) {
		m.Assignment = blobOrNil(want[m.MemberID])
		transaction.Set(memberKey(operation.Group, m.MemberID), encodeMember(m))
	}
	h.assignedGen = operation.Generation
	transaction.Set(groupKey(operation.Group), h.encode())
	return core.OK
}

func removeGroup(transaction Transaction, operation core.GroupRemove) core.ErrCode {
	if removeSize(operation) > maxGroupCommand {
		return core.ErrTooLarge
	}
	h, found := readHeader(transaction, operation.Group)
	if !found {
		return core.OK
	}
	left := len(memberKeys(transaction, operation.Group))
	for _, id := range operation.Members {
		k := memberKey(operation.Group, id)
		if transaction.Get(k) != nil {
			transaction.Delete(k)
			left--
		}
	}
	if transaction.Get(memberKey(operation.Group, h.leader)) == nil {
		h.leader = ""
	}
	if left == 0 {
		h.leader = ""
		h.assignedGen = 0
	}
	transaction.Set(groupKey(operation.Group), h.encode())
	return core.OK
}

func commitOffsets(transaction Transaction, operation core.OffsetCommit) core.ControlPlaneDone {
	if offsetCommitSize(operation) > maxGroupCommand {
		return core.ControlPlaneDone{Err: core.ErrTooLarge}
	}
	h, found := readHeader(transaction, operation.Group)
	switch {
	case operation.Generation >= 0:
		if !found || transaction.Get(memberKey(operation.Group, operation.Member)) == nil {
			return core.ControlPlaneDone{Err: core.ErrUnknownMember}
		}
		if operation.Generation != h.generation {
			return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
		}
	case operation.Generation == -1 && operation.Member == "":
		if len(memberKeys(transaction, operation.Group)) > 0 {
			return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
		}
	default:
		return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
	}
	var errs []core.ErrCode
	saved := false
	for _, o := range operation.Offsets {
		code := commitOne(transaction, operation, o)
		saved = saved || code == core.OK
		errs = append(errs, code)
	}
	done := core.ControlPlaneDone{Resp: core.OffsetCommitResp{Errs: errs}}
	if saved {
		done.Outcome = core.Applied
	}
	return done
}

// commitOne checks and stores one offset of an OffsetCommit.
func commitOne(transaction Transaction, operation core.OffsetCommit, o core.PartitionOffset) core.ErrCode {
	n := readVarint(transaction.Get(topicKey(o.TopicPartition.Topic)))
	if o.TopicPartition.Partition < 0 || int64(o.TopicPartition.Partition) >= n {
		return core.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return core.ErrInvalid
	}
	rec := offsetRecord{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata, time: operation.TimeMs}
	transaction.Set(offsetKey(operation.Group, o.TopicPartition), rec.encode())
	return core.OK
}
