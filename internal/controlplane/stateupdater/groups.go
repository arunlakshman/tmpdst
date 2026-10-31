package stateupdater

import (
	"encoding/binary"
	"fmt"

	"manifester/internal/core"
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

func installSize(op core.GroupInstall) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.str(op.ProtocolType)
	c.str(op.Protocol)
	c.num()
	c.str(op.Leader)
	c.num()
	c.num()
	for _, m := range op.Members {
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

func assignSize(op core.GroupAssign) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.num()
	c.str(op.Leader)
	c.num()
	c.num()
	for _, a := range op.Assignments {
		c.str(a.Member)
		c.bytes(a.Bytes)
	}
	return c.n
}

func removeSize(op core.GroupRemove) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.num()
	c.str(op.Reason)
	c.num()
	for _, m := range op.Members {
		c.str(m)
	}
	return c.n
}

func offsetCommitSize(op core.OffsetCommit) int {
	c := &cmdSize{n: 16}
	c.str(op.Group)
	c.str(op.Member)
	c.num()
	c.num()
	c.num()
	for _, o := range op.Offsets {
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
func groupTransaction(tx Transaction, op core.ControlPlaneOperation) core.ControlPlaneDone {
	switch x := op.(type) {
	case core.GroupInstall:
		return groupAnswer(installGroup(tx, x))
	case core.GroupAssign:
		return groupAnswer(assignGroup(tx, x))
	case core.GroupRemove:
		return groupAnswer(removeGroup(tx, x))
	case core.OffsetCommit:
		return commitOffsets(tx, x)
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
func (a *StateUpdater) groupWrite(op core.ControlPlaneOperation) (core.ControlPlaneDone, error) {
	var d core.ControlPlaneDone
	err := a.kv.Update(func(tx Transaction) { d = groupTransaction(tx, op) })
	return d, err
}

// readHeader returns the header of group; found is false when it was never installed.
func readHeader(tx Transaction, group string) (groupHeader, bool) {
	v := tx.Get(groupKey(group))
	if v == nil {
		return groupHeader{}, false
	}
	return decodeHeader(group, v), true
}

// memberKeys lists the M keys of a group, in member id order.
func memberKeys(tx Transaction, group string) [][]byte {
	var keys [][]byte
	lo := groupPrefix(prefixMember, group)
	tx.Ascend(lo, prefixEnd(lo), func(k, _ []byte) bool {
		keys = append(keys, append([]byte(nil), k...))
		return true
	})
	return keys
}

// readMembers lists the members of a group, sorted by member id.
func readMembers(tx Transaction, group string) []core.GroupMemberInfo {
	var out []core.GroupMemberInfo
	lo := groupPrefix(prefixMember, group)
	tx.Ascend(lo, prefixEnd(lo), func(k, v []byte) bool {
		out = append(out, decodeMember(group, string(k[len(lo):]), v))
		return true
	})
	return out
}

func installGroup(tx Transaction, op core.GroupInstall) core.ErrCode {
	if installSize(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	if !validInstall(op) {
		return core.ErrInvalid
	}
	h, _ := readHeader(tx, op.Group)
	if op.Generation != h.generation+1 {
		return core.ErrIllegalGeneration
	}
	for _, k := range memberKeys(tx, op.Group) {
		tx.Delete(k)
	}
	for _, m := range op.Members {
		info := core.GroupMemberInfo{GroupMember: m, JoinedTimeMs: op.TimeMs}
		tx.Set(memberKey(op.Group, m.MemberID), encodeMember(info))
	}
	next := groupHeader{generation: op.Generation, protoType: op.ProtocolType, protocol: op.Protocol, leader: op.Leader}
	tx.Set(groupKey(op.Group), next.encode())
	return core.OK
}

// validInstall checks the shape of a join result: a group name, members with
// unique non-empty ids, and a leader that is one of them.
func validInstall(op core.GroupInstall) bool {
	if op.Group == "" || len(op.Members) == 0 {
		return false
	}
	ids := map[string]bool{}
	for _, m := range op.Members {
		if m.MemberID == "" || ids[m.MemberID] {
			return false
		}
		ids[m.MemberID] = true
	}
	return ids[op.Leader]
}

func assignGroup(tx Transaction, op core.GroupAssign) core.ErrCode {
	if assignSize(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	h, found := readHeader(tx, op.Group)
	if op.Generation != h.generation {
		return core.ErrIllegalGeneration
	}
	if !found || op.Leader != h.leader || tx.Get(memberKey(op.Group, op.Leader)) == nil {
		return core.ErrUnknownMember
	}
	want := map[string][]byte{}
	for _, a := range op.Assignments {
		if tx.Get(memberKey(op.Group, a.Member)) == nil {
			return core.ErrUnknownMember
		}
		if _, dup := want[a.Member]; dup {
			return core.ErrInvalid
		}
		want[a.Member] = a.Bytes
	}
	for _, m := range readMembers(tx, op.Group) {
		m.Assignment = blobOrNil(want[m.MemberID])
		tx.Set(memberKey(op.Group, m.MemberID), encodeMember(m))
	}
	h.assignedGen = op.Generation
	tx.Set(groupKey(op.Group), h.encode())
	return core.OK
}

func removeGroup(tx Transaction, op core.GroupRemove) core.ErrCode {
	if removeSize(op) > maxGroupCommand {
		return core.ErrTooLarge
	}
	h, found := readHeader(tx, op.Group)
	if !found {
		return core.OK
	}
	left := len(memberKeys(tx, op.Group))
	for _, id := range op.Members {
		k := memberKey(op.Group, id)
		if tx.Get(k) != nil {
			tx.Delete(k)
			left--
		}
	}
	if tx.Get(memberKey(op.Group, h.leader)) == nil {
		h.leader = ""
	}
	if left == 0 {
		h.leader = ""
		h.assignedGen = 0
	}
	tx.Set(groupKey(op.Group), h.encode())
	return core.OK
}

func commitOffsets(tx Transaction, op core.OffsetCommit) core.ControlPlaneDone {
	if offsetCommitSize(op) > maxGroupCommand {
		return core.ControlPlaneDone{Err: core.ErrTooLarge}
	}
	h, found := readHeader(tx, op.Group)
	switch {
	case op.Generation >= 0:
		if !found || tx.Get(memberKey(op.Group, op.Member)) == nil {
			return core.ControlPlaneDone{Err: core.ErrUnknownMember}
		}
		if op.Generation != h.generation {
			return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
		}
	case op.Generation == -1 && op.Member == "":
		if len(memberKeys(tx, op.Group)) > 0 {
			return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
		}
	default:
		return core.ControlPlaneDone{Err: core.ErrIllegalGeneration}
	}
	var errs []core.ErrCode
	saved := false
	for _, o := range op.Offsets {
		code := commitOne(tx, op, o)
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
func commitOne(tx Transaction, op core.OffsetCommit, o core.PartitionOffset) core.ErrCode {
	n := readVarint(tx.Get(topicKey(o.TopicPartition.Topic)))
	if o.TopicPartition.Partition < 0 || int64(o.TopicPartition.Partition) >= n {
		return core.ErrUnknownPartition
	}
	if o.Offset < 0 {
		return core.ErrInvalid
	}
	rec := offsetRecord{offset: o.Offset, epoch: o.LeaderEpoch, meta: o.Metadata, time: op.TimeMs}
	tx.Set(offsetKey(op.Group, o.TopicPartition), rec.encode())
	return core.OK
}
