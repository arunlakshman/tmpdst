package core

import (
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Trace-hash tags and folds.
//
// Every case of EventData, Action, ControlPlaneOperation and ControlPlaneResponse
// has two methods: Tag, and Fold(h *trace.Hash). Fold writes the tag first, then the
// fields in a fixed order. The tag numbers and the field order are frozen: they are
// written by hand, never by %T and never by reflection, so renaming a Go type cannot
// move a golden hash. Changing a tag or a field order moves every golden hash.
// Tags are append-only.
//
// Tag ranges:
//
//	events                    1 to 99
//	actions                   101 to 199
//	control plane operations  201 to 299 (200: no operation)
//	control plane responses   301 to 399 (300: no response)
//	field values              401 to 499 (in dst/machine)
//	facts                     501        (in dst/machine)
//
// The interfaces include Fold, so a new case without it does not compile. Before,
// the simulator had a table of 600 lines (verify/simulator/tags.go) that gave the
// same numbers, and it panicked for a case without a row.

// foldInstance folds the node name and the boot number.
func foldInstance(h *trace.Hash, r machine.NodeInstance) {
	h.String(string(r.Node))
	h.Bytes(r.Inc[:])
}

// foldSegmentRef folds a byte range of an object.
func foldSegmentRef(h *trace.Hash, s SegmentRef) {
	h.String(s.Key)
	h.Int64(s.Off)
	h.Int64(s.Len)
}

// foldTopicPartition folds the topic name and the partition number.
func foldTopicPartition(h *trace.Hash, tp TopicPartition) {
	h.String(tp.Topic)
	h.Uint64(uint64(uint32(tp.Partition)))
}

// foldBatchRanges folds the number of ranges, then each range.
func foldBatchRanges(h *trace.Hash, rs []BatchRange) {
	h.Uint64(uint64(len(rs)))
	for _, b := range rs {
		foldTopicPartition(h, b.TopicPartition)
		h.Int64(int64(b.Base))
		h.Int64(b.Count)
		foldSegmentRef(h, b.Loc)
	}
}

// foldPartitionOffset folds one committed offset (a group write or read).
func foldPartitionOffset(h *trace.Hash, o PartitionOffset) {
	foldTopicPartition(h, o.TopicPartition)
	h.Int64(o.Offset)
	h.Int64(int64(o.LeaderEpoch))
	h.String(o.Metadata)
}

// foldGroupMember folds the fields of a member that a join or a describe both carry.
func foldGroupMember(h *trace.Hash, m GroupMember) {
	h.String(m.MemberID)
	h.String(m.ClientID)
	h.String(m.ClientHost)
	h.String(m.GroupInstanceID)
	h.Int64(int64(m.SessionTimeoutMs))
	h.Int64(int64(m.RebalanceTimeoutMs))
	h.Bytes(m.Metadata)
}

// foldGroupMemberInfos folds the number of members, then each member with its join
// time and its assignment.
func foldGroupMemberInfos(h *trace.Hash, ms []GroupMemberInfo) {
	h.Uint64(uint64(len(ms)))
	for _, m := range ms {
		foldGroupMember(h, m.GroupMember)
		h.Int64(m.JoinedTimeMs)
		h.Bytes(m.Assignment)
	}
}

// foldAssignments folds the number of assignments, then each one.
func foldAssignments(h *trace.Hash, as []GroupAssignment) {
	h.Uint64(uint64(len(as)))
	for _, a := range as {
		h.String(a.Member)
		h.Bytes(a.Bytes)
	}
}

// foldStrings folds the number of strings, then each string.
func foldStrings(h *trace.Hash, ss []string) {
	h.Uint64(uint64(len(ss)))
	for _, s := range ss {
		h.String(s)
	}
}

// foldErrCodes folds the number of codes, then each code.
func foldErrCodes(h *trace.Hash, cs []ErrCode) {
	h.Uint64(uint64(len(cs)))
	for _, c := range cs {
		h.Uint64(uint64(c))
	}
}

// foldOperation folds an operation. No operation (nil) is tag 200.
func foldOperation(h *trace.Hash, op ControlPlaneOperation) {
	if op == nil {
		h.Uint64(200)
		return
	}
	op.Fold(h)
}

// foldResponse folds a response. No response (nil, an error completion) is tag 300.
func foldResponse(h *trace.Hash, r ControlPlaneResponse) {
	if r == nil {
		h.Uint64(300)
		return
	}
	r.Fold(h)
}
