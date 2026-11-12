package core

import "github.com/manifesterio/mds/dst/trace"

// Tag and Fold of every ControlPlaneOperation case and every ControlPlaneResponse
// case. The operations use the tags 202 to 218, the responses 302 to 312. The
// tags 200 (no operation) and 300 (no response) are written by foldOperation and
// foldResponse (fold.go).

// ---- operations ----

// Tag returns the frozen trace-hash number of this operation.
func (CommitSegment) Tag() uint64 { return 202 }

// Fold writes the operation into the trace hash.
func (o CommitSegment) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.SegmentKey)
	h.Uint64(uint64(len(o.Batches)))
	for _, b := range o.Batches {
		foldTopicPartition(h, b.TopicPartition)
		h.Int64(b.Records)
		foldSegmentRef(h, b.Loc)
	}
}

// Tag returns the frozen trace-hash number of this operation.
func (FindSegments) Tag() uint64 { return 203 }

// Fold writes the operation into the trace hash.
func (o FindSegments) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	foldTopicPartition(h, o.TopicPartition)
	h.Int64(int64(o.From))
	h.Uint64(uint64(uint32(o.MaxBytes)))
}

// Tag returns the frozen trace-hash number of this operation.
func (DescribeTopics) Tag() uint64 { return 204 }

// Fold writes the operation into the trace hash.
func (o DescribeTopics) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(len(o.Topics)))
	for _, t := range o.Topics {
		h.String(t)
	}
}

// Tag returns the frozen trace-hash number of this operation. An exclusive create
// has its own tag; plain creates keep theirs.
func (o CreateTopic) Tag() uint64 {
	if o.FailIfExists {
		return 218
	}
	return 205
}

// Fold writes the operation into the trace hash.
func (o CreateTopic) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Name)
	h.Uint64(uint64(uint32(o.Partitions)))
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupInstall) Tag() uint64 { return 206 }

// Fold writes the operation into the trace hash.
func (o GroupInstall) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.String(o.ProtocolType)
	h.String(o.Protocol)
	h.Int64(int64(o.Generation))
	h.String(o.Leader)
	h.Int64(o.TimeMs)
	h.Uint64(uint64(len(o.Members)))
	for _, m := range o.Members {
		foldGroupMember(h, m)
	}
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupAssign) Tag() uint64 { return 207 }

// Fold writes the operation into the trace hash.
func (o GroupAssign) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.Int64(int64(o.Generation))
	h.String(o.Leader)
	h.Int64(o.TimeMs)
	foldAssignments(h, o.Assignments)
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupRemove) Tag() uint64 { return 208 }

// Fold writes the operation into the trace hash.
func (o GroupRemove) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.Int64(o.TimeMs)
	h.String(o.Reason)
	foldStrings(h, o.Members)
}

// Tag returns the frozen trace-hash number of this operation.
func (OffsetCommit) Tag() uint64 { return 209 }

// Fold writes the operation into the trace hash.
func (o OffsetCommit) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.String(o.Member)
	h.Int64(int64(o.Generation))
	h.Int64(o.TimeMs)
	h.Uint64(uint64(len(o.Offsets)))
	for _, p := range o.Offsets {
		foldPartitionOffset(h, p)
	}
}

// Tag returns the frozen trace-hash number of this operation.
func (OffsetFetch) Tag() uint64 { return 210 }

// Fold writes the operation into the trace hash.
func (o OffsetFetch) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.Uint64(uint64(len(o.Partitions)))
	for _, tp := range o.Partitions {
		foldTopicPartition(h, tp)
	}
}

// Tag returns the frozen trace-hash number of this operation.
func (DescribeGroup) Tag() uint64 { return 211 }

// Fold writes the operation into the trace hash.
func (o DescribeGroup) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
}

// Tag returns the frozen trace-hash number of this operation.
func (ListGroups) Tag() uint64 { return 212 }

// Fold writes the operation into the trace hash.
func (o ListGroups) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupJoin) Tag() uint64 { return 213 }

// Fold writes the operation into the trace hash.
func (o GroupJoin) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.String(o.MemberID)
	h.String(o.GroupInstanceID)
	h.Int64(int64(o.SessionTimeoutMs))
	h.Int64(int64(o.RebalanceTimeoutMs))
	h.String(o.ProtocolType)
	h.Uint64(uint64(len(o.Protocols)))
	for _, p := range o.Protocols {
		h.String(p.Name)
		h.Bytes(p.Metadata)
	}
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupSync) Tag() uint64 { return 214 }

// Fold writes the operation into the trace hash.
func (o GroupSync) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.String(o.MemberID)
	h.Int64(int64(o.Generation))
	foldAssignments(h, o.Assignments)
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupHeartbeat) Tag() uint64 { return 215 }

// Fold writes the operation into the trace hash.
func (o GroupHeartbeat) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	h.String(o.MemberID)
	h.Int64(int64(o.Generation))
}

// Tag returns the frozen trace-hash number of this operation.
func (GroupLeave) Tag() uint64 { return 216 }

// Fold writes the operation into the trace hash.
func (o GroupLeave) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.String(o.Group)
	foldStrings(h, o.Members)
}

// Tag returns the frozen trace-hash number of this operation.
func (DescribeGroups) Tag() uint64 { return 217 }

// Fold writes the operation into the trace hash.
func (o DescribeGroups) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	foldStrings(h, o.Groups)
}

// ---- responses ----

// Tag returns the frozen trace-hash number of this response.
func (CommitSegmentResp) Tag() uint64 { return 302 }

// Fold writes the response into the trace hash.
func (r CommitSegmentResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	foldBatchRanges(h, r.Assigned)
}

// Tag returns the frozen trace-hash number of this response.
func (FindSegmentsResp) Tag() uint64 { return 303 }

// Fold writes the response into the trace hash.
func (r FindSegmentsResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	foldBatchRanges(h, r.Ranges)
	h.Int64(int64(r.End))
}

// Tag returns the frozen trace-hash number of this response.
func (DescribeTopicsResp) Tag() uint64 { return 304 }

// Fold writes the response into the trace hash.
func (r DescribeTopicsResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Uint64(uint64(len(r.Topics)))
	for _, t := range r.Topics {
		h.String(t.Name)
		h.Uint64(uint64(uint32(t.Partitions)))
	}
}

// Tag returns the frozen trace-hash number of this response.
func (OffsetCommitResp) Tag() uint64 { return 305 }

// Fold writes the response into the trace hash.
func (r OffsetCommitResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	foldErrCodes(h, r.Errs)
}

// Tag returns the frozen trace-hash number of this response.
func (OffsetFetchResp) Tag() uint64 { return 306 }

// Fold writes the response into the trace hash.
func (r OffsetFetchResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Uint64(uint64(len(r.Offsets)))
	for _, p := range r.Offsets {
		foldPartitionOffset(h, p)
	}
}

// Tag returns the frozen trace-hash number of this response.
func (DescribeGroupResp) Tag() uint64 { return 307 }

// Fold writes the response into the trace hash.
func (r DescribeGroupResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Bool(r.Exists)
	h.Int64(int64(r.Generation))
	h.String(r.ProtocolType)
	h.String(r.Protocol)
	h.String(r.Leader)
	h.Int64(int64(r.AssignedGeneration))
	h.String(r.State)
	foldGroupMemberInfos(h, r.Members)
}

// Tag returns the frozen trace-hash number of this response.
func (ListGroupsResp) Tag() uint64 { return 308 }

// Fold writes the response into the trace hash.
func (r ListGroupsResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Uint64(uint64(len(r.Groups)))
	for _, g := range r.Groups {
		h.String(g.Name)
		h.String(g.ProtocolType)
		h.String(g.State)
	}
}

// Tag returns the frozen trace-hash number of this response.
func (GroupJoinResp) Tag() uint64 { return 309 }

// Fold writes the response into the trace hash.
func (r GroupJoinResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Int64(int64(r.Generation))
	h.String(r.Protocol)
	h.String(r.Leader)
	h.String(r.MemberID)
	h.Uint64(uint64(len(r.Members)))
	for _, m := range r.Members {
		h.String(m.MemberID)
		h.String(m.GroupInstanceID)
		h.Bytes(m.Metadata)
	}
}

// Tag returns the frozen trace-hash number of this response.
func (GroupSyncResp) Tag() uint64 { return 310 }

// Fold writes the response into the trace hash.
func (r GroupSyncResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Bytes(r.Assignment)
}

// Tag returns the frozen trace-hash number of this response.
func (GroupLeaveResp) Tag() uint64 { return 311 }

// Fold writes the response into the trace hash.
func (r GroupLeaveResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	foldErrCodes(h, r.Errs)
}

// Tag returns the frozen trace-hash number of this response.
func (DescribeGroupsResp) Tag() uint64 { return 312 }

// Fold writes the response into the trace hash.
func (r DescribeGroupsResp) Fold(h *trace.Hash) {
	h.Uint64(r.Tag())
	h.Uint64(uint64(len(r.Groups)))
	for _, g := range r.Groups {
		h.String(g.Name)
		h.Bool(g.Exists)
		h.Int64(int64(g.Generation))
		h.String(g.ProtocolType)
		h.String(g.Protocol)
		h.String(g.Leader)
		h.String(g.State)
		foldGroupMemberInfos(h, g.Members)
	}
}
