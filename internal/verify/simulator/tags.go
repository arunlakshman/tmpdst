package simulator

import (
	"fmt"

	"mds/internal/core"
)

// The trace-hash tag tables. Every core event, action, control-plane type and
// fact value gets a frozen numeric tag, written out by hand: never %T, never
// reflection, so renaming a Go type cannot move a TraceHash. Tags are
// append-only; changing one re-baselines every golden trace. Ranges: events
// 1..99, actions 101..199, CP ops 201..299, CP responses 301..399, field
// values 401..499. A type without a row panics (fail closed); the tag test
// scans core/ for types that lack one.

func (h *stateHasher) ref(r core.NodeInstance) {
	h.StringValue(string(r.Node))
	h.Bytes(r.Inc[:])
}

func (h *stateHasher) segmentRef(s core.SegmentRef) {
	h.StringValue(s.Key)
	h.Int64Value(s.Off)
	h.Int64Value(s.Len)
}

func (h *stateHasher) batchRange(b core.BatchRange) {
	h.StringValue(b.TopicPartition.Topic)
	h.Uint64Value(uint64(uint32(b.TopicPartition.Partition)))
	h.Int64Value(int64(b.Base))
	h.Int64Value(b.Count)
	h.segmentRef(b.Loc)
}

func (h *stateHasher) event(b core.EventData) {
	switch e := b.(type) {
	case core.NodeStarted:
		h.Uint64Value(1)
		h.ref(e.Self)
	case core.StopRequest:
		h.Uint64Value(2)
		h.Int64Value(int64(e.Deadline))
	case core.ConnOpened:
		h.Uint64Value(3)
		h.Uint64Value(uint64(e.Conn))
		h.StringValue(e.Peer)
	case core.BytesReceived:
		h.Uint64Value(4)
		h.Uint64Value(uint64(e.Conn))
		h.Bytes(e.Data)
	case core.ConnClosed:
		h.Uint64Value(5)
		h.Uint64Value(uint64(e.Conn))
		h.Bool(e.Abort)
	case core.SendDone:
		h.Uint64Value(6)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Conn))
		h.Uint64Value(uint64(e.Err))
	case core.ConnectDone:
		h.Uint64Value(7)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Conn))
		h.Uint64Value(uint64(e.Err))
	case core.TimerFired:
		h.Uint64Value(8)
		h.Uint64Value(uint64(e.ID))
		h.Uint64Value(uint64(e.Gen))
	default:
		h.eventDone(b)
	}
}

func (h *stateHasher) eventDone(b core.EventData) {
	switch e := b.(type) {
	case core.ObjectPutDone:
		h.Uint64Value(9)
		h.Uint64Value(uint64(e.Req))
		h.StringValue(e.Key)
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.ObjectGetDone:
		h.Uint64Value(10)
		h.Uint64Value(uint64(e.Req))
		h.Bytes(e.Data)
		h.Uint64Value(uint64(e.Err))
	case core.ObjectDeleteDone:
		h.Uint64Value(11)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.ObjectListDone:
		h.Uint64Value(12)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(len(e.Keys)))
		for _, k := range e.Keys {
			h.StringValue(k)
		}
	case core.ControlPlaneDone:
		h.Uint64Value(13)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
		h.controlPlaneResponse(e.Resp)
	case core.ControlPlaneDeliver:
		h.Uint64Value(18)
		h.ref(e.From)
		h.Uint64Value(uint64(e.Req))
		h.controlPlaneOperation(e.Op)
	default:
		h.eventLogDone(b)
	}
}

func (h *stateHasher) eventLogDone(b core.EventData) {
	switch e := b.(type) {
	case core.LogAppendDone:
		h.Uint64Value(14)
		h.Uint64Value(uint64(e.Req))
		h.Int64Value(int64(e.Slot))
		h.Bool(e.Taken)
		h.Bytes(e.Existing)
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.LogReadDone:
		h.Uint64Value(15)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(len(e.Entries)))
		for _, ent := range e.Entries {
			h.Int64Value(int64(ent.Slot))
			h.Bytes(ent.Data)
		}
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.LogGetEndDone:
		h.Uint64Value(16)
		h.Uint64Value(uint64(e.Req))
		h.Int64Value(int64(e.Tail))
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.LogDeleteOldDone:
		h.Uint64Value(17)
		h.Uint64Value(uint64(e.Req))
		h.Uint64Value(uint64(e.Err))
		h.Uint64Value(uint64(e.Outcome))
	case core.LiveBrokers:
		h.Uint64Value(19)
		h.Uint64Value(uint64(len(e.Brokers)))
		for _, bk := range e.Brokers {
			h.Int64Value(int64(bk.NodeID))
			h.StringValue(bk.Host)
			h.Int64Value(int64(bk.Port))
		}
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged event %T", b))
	}
}

func (h *stateHasher) action(a core.Action) {
	switch x := a.(type) {
	case core.SetTimer:
		h.Uint64Value(101)
		h.Uint64Value(uint64(x.ID))
		h.Uint64Value(uint64(x.Gen))
		h.Int64Value(int64(x.After))
	case core.CancelTimer:
		h.Uint64Value(102)
		h.Uint64Value(uint64(x.ID))
	case core.Send:
		h.Uint64Value(103)
		h.Uint64Value(uint64(x.Req))
		h.Uint64Value(uint64(x.Conn))
		h.Bytes(x.Data)
	case core.CloseConn:
		h.Uint64Value(104)
		h.Uint64Value(uint64(x.Conn))
		h.Bool(x.Abort)
	case core.PauseReads:
		h.Uint64Value(105)
		h.Uint64Value(uint64(x.Conn))
	case core.ResumeReads:
		h.Uint64Value(106)
		h.Uint64Value(uint64(x.Conn))
	case core.Connect:
		h.Uint64Value(107)
		h.Uint64Value(uint64(x.Req))
		h.StringValue(x.Addr)
	default:
		h.actionIO(a)
	}
}

func (h *stateHasher) actionIO(a core.Action) {
	switch x := a.(type) {
	case core.ObjectPut:
		h.Uint64Value(108)
		h.Uint64Value(uint64(x.Req))
		h.StringValue(x.Key)
		h.Bytes(x.Data)
	case core.ObjectGet:
		h.Uint64Value(109)
		h.Uint64Value(uint64(x.Req))
		h.segmentRef(x.Ref)
	case core.ObjectDelete:
		h.Uint64Value(110)
		h.Uint64Value(uint64(x.Req))
		h.StringValue(x.Key)
	case core.ObjectList:
		h.Uint64Value(111)
		h.Uint64Value(uint64(x.Req))
		h.StringValue(x.Prefix)
	case core.ControlPlaneCall:
		h.Uint64Value(112) // Timeout is not folded: the simulator never sets it
		h.Uint64Value(uint64(x.Req))
		h.controlPlaneOperation(x.Op)
	case core.ControlPlaneReply:
		h.Uint64Value(120)
		h.ref(x.To)
		h.Uint64Value(uint64(x.Done.Req))
		h.Uint64Value(uint64(x.Done.Err))
		h.Uint64Value(uint64(x.Done.Outcome))
		h.controlPlaneResponse(x.Done.Resp)
	case core.RecordFact:
		h.Uint64Value(113) // the fact itself is folded when it reaches the timeline
	case core.StopForRestart:
		h.Uint64Value(114)
		h.StringValue(x.Cause)
	case core.ShutdownDone:
		h.Uint64Value(115)
	default:
		h.actionLog(a)
	}
}

func (h *stateHasher) actionLog(a core.Action) {
	switch x := a.(type) {
	case core.LogAppend:
		h.Uint64Value(116)
		h.Uint64Value(uint64(x.Req))
		h.Int64Value(int64(x.Slot))
		h.Bytes(x.Data)
	case core.LogRead:
		h.Uint64Value(117)
		h.Uint64Value(uint64(x.Req))
		h.Int64Value(int64(x.From))
		h.Uint64Value(uint64(uint32(x.Limit)))
	case core.LogGetEnd:
		h.Uint64Value(118)
		h.Uint64Value(uint64(x.Req))
	case core.LogDeleteOld:
		h.Uint64Value(119)
		h.Uint64Value(uint64(x.Req))
		h.Int64Value(int64(x.Through))
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged action %T", a))
	}
}

// controlPlaneOperation folds an op; nil is tag 200.
func (h *stateHasher) controlPlaneOperation(operation core.ControlPlaneOperation) {
	switch x := operation.(type) {
	case nil:
		h.Uint64Value(200)
	case core.CommitSegment:
		h.Uint64Value(202)
		h.StringValue(x.SegmentKey)
		h.Uint64Value(uint64(len(x.Batches)))
		for _, b := range x.Batches {
			h.StringValue(b.TopicPartition.Topic)
			h.Uint64Value(uint64(uint32(b.TopicPartition.Partition)))
			h.Int64Value(b.Records)
			h.segmentRef(b.Loc)
		}
	case core.FindSegments:
		h.Uint64Value(203)
		h.StringValue(x.TopicPartition.Topic)
		h.Uint64Value(uint64(uint32(x.TopicPartition.Partition)))
		h.Int64Value(int64(x.From))
		h.Uint64Value(uint64(uint32(x.MaxBytes)))
	case core.DescribeTopics:
		h.Uint64Value(204)
		h.Uint64Value(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.StringValue(t)
		}
	case core.CreateTopic:
		tag := uint64(205) // an exclusive create has its own tag; plain creates keep theirs
		if x.FailIfExists {
			tag = 218
		}
		h.Uint64Value(tag)
		h.StringValue(x.Name)
		h.Uint64Value(uint64(uint32(x.Partitions)))
	default:
		if !h.controlPlaneGroupOperation(operation) {
			panic(fmt.Sprintf("sim: trace hash: untagged CP op %T", operation))
		}
	}
}

// controlPlaneResponse folds a response; nil (an error completion) is tag 300.
func (h *stateHasher) controlPlaneResponse(r core.ControlPlaneResponse) {
	switch x := r.(type) {
	case nil:
		h.Uint64Value(300)
	case core.CommitSegmentResp:
		h.Uint64Value(302)
		h.Uint64Value(uint64(len(x.Assigned)))
		for _, b := range x.Assigned {
			h.batchRange(b)
		}
	case core.FindSegmentsResp:
		h.Uint64Value(303)
		h.Uint64Value(uint64(len(x.Ranges)))
		for _, b := range x.Ranges {
			h.batchRange(b)
		}
		h.Int64Value(int64(x.End))
	case core.DescribeTopicsResp:
		h.Uint64Value(304)
		h.Uint64Value(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.StringValue(t.Name)
			h.Uint64Value(uint64(uint32(t.Partitions)))
		}
	default:
		if !h.controlPlaneGroupResponse(r) {
			panic(fmt.Sprintf("sim: trace hash: untagged CP response %T", r))
		}
	}
}

func (h *stateHasher) fieldValue(v core.FieldValue) {
	switch x := v.(type) {
	case core.Int64Value:
		h.Uint64Value(401)
		h.Int64Value(int64(x))
	case core.Uint64Value:
		h.Uint64Value(402)
		h.Uint64Value(uint64(x))
	case core.StringValue:
		h.Uint64Value(403)
		h.StringValue(string(x))
	case core.Bool:
		h.Uint64Value(404)
		h.Bool(bool(x))
	case core.Bytes:
		h.Uint64Value(405)
		h.Bytes(x)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged field value %T", v))
	}
}

func (h *stateHasher) fact(f core.Fact) {
	h.Uint64Value(501)
	h.StringValue(f.Name)
	h.Uint64Value(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.StringValue(fl.Key)
		h.fieldValue(fl.Value)
	}
}

// partitionOffset folds one committed offset (a group write or read).
func (h *stateHasher) partitionOffset(o core.PartitionOffset) {
	h.StringValue(o.TopicPartition.Topic)
	h.Uint64Value(uint64(uint32(o.TopicPartition.Partition)))
	h.Int64Value(o.Offset)
	h.Int64Value(int64(o.LeaderEpoch))
	h.StringValue(o.Metadata)
}

// groupMember folds the fields of a member that a join or a describe both carry.
func (h *stateHasher) groupMember(m core.GroupMember) {
	h.StringValue(m.MemberID)
	h.StringValue(m.ClientID)
	h.StringValue(m.ClientHost)
	h.StringValue(m.GroupInstanceID)
	h.Int64Value(int64(m.SessionTimeoutMs))
	h.Int64Value(int64(m.RebalanceTimeoutMs))
	h.StringValue(string(m.Metadata))
}

// controlPlaneGroupOperation folds a group operation. It reports false for other operations.
func (h *stateHasher) controlPlaneGroupOperation(operation core.ControlPlaneOperation) bool {
	switch x := operation.(type) {
	case core.GroupInstall:
		h.Uint64Value(206)
		h.StringValue(x.Group)
		h.StringValue(x.ProtocolType)
		h.StringValue(x.Protocol)
		h.Int64Value(int64(x.Generation))
		h.StringValue(x.Leader)
		h.Int64Value(x.TimeMs)
		h.Uint64Value(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m)
		}
	case core.GroupAssign:
		h.Uint64Value(207)
		h.StringValue(x.Group)
		h.Int64Value(int64(x.Generation))
		h.StringValue(x.Leader)
		h.Int64Value(x.TimeMs)
		h.Uint64Value(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.StringValue(a.Member)
			h.StringValue(string(a.Bytes))
		}
	case core.GroupRemove:
		h.Uint64Value(208)
		h.StringValue(x.Group)
		h.Int64Value(x.TimeMs)
		h.StringValue(x.Reason)
		h.Uint64Value(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.StringValue(m)
		}
	case core.OffsetCommit:
		h.Uint64Value(209)
		h.StringValue(x.Group)
		h.StringValue(x.Member)
		h.Int64Value(int64(x.Generation))
		h.Int64Value(x.TimeMs)
		h.Uint64Value(uint64(len(x.Offsets)))
		for _, o := range x.Offsets {
			h.partitionOffset(o)
		}
	default:
		return h.controlPlaneGroupRead(operation) || h.controlPlaneGroupCall(operation)
	}
	return true
}

// controlPlaneGroupCall folds a broker group call to the control plane (decision Q3).
// It reports false for other operations.
func (h *stateHasher) controlPlaneGroupCall(operation core.ControlPlaneOperation) bool {
	switch x := operation.(type) {
	case core.GroupJoin:
		h.Uint64Value(213)
		h.StringValue(x.Group)
		h.StringValue(x.MemberID)
		h.StringValue(x.GroupInstanceID)
		h.Int64Value(int64(x.SessionTimeoutMs))
		h.Int64Value(int64(x.RebalanceTimeoutMs))
		h.StringValue(x.ProtocolType)
		h.Uint64Value(uint64(len(x.Protocols)))
		for _, p := range x.Protocols {
			h.StringValue(p.Name)
			h.StringValue(string(p.Metadata))
		}
	case core.GroupSync:
		h.Uint64Value(214)
		h.StringValue(x.Group)
		h.StringValue(x.MemberID)
		h.Int64Value(int64(x.Generation))
		h.Uint64Value(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.StringValue(a.Member)
			h.StringValue(string(a.Bytes))
		}
	case core.GroupHeartbeat:
		h.Uint64Value(215)
		h.StringValue(x.Group)
		h.StringValue(x.MemberID)
		h.Int64Value(int64(x.Generation))
	case core.GroupLeave:
		h.Uint64Value(216)
		h.StringValue(x.Group)
		h.Uint64Value(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.StringValue(m)
		}
	case core.DescribeGroups:
		h.Uint64Value(217)
		h.Uint64Value(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.StringValue(g)
		}
	default:
		return false
	}
	return true
}

// controlPlaneGroupRead folds a group read. It reports false for other operations.
func (h *stateHasher) controlPlaneGroupRead(operation core.ControlPlaneOperation) bool {
	switch x := operation.(type) {
	case core.OffsetFetch:
		h.Uint64Value(210)
		h.StringValue(x.Group)
		h.Uint64Value(uint64(len(x.Partitions)))
		for _, topicPartition := range x.Partitions {
			h.StringValue(topicPartition.Topic)
			h.Uint64Value(uint64(uint32(topicPartition.Partition)))
		}
	case core.DescribeGroup:
		h.Uint64Value(211)
		h.StringValue(x.Group)
	case core.ListGroups:
		h.Uint64Value(212)
	default:
		return false
	}
	return true
}

// controlPlaneGroupResponse folds a group response. It reports false for other responses.
func (h *stateHasher) controlPlaneGroupResponse(r core.ControlPlaneResponse) bool {
	switch x := r.(type) {
	case core.OffsetCommitResp:
		h.Uint64Value(305)
		h.Uint64Value(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.Uint64Value(uint64(c))
		}
	case core.OffsetFetchResp:
		h.Uint64Value(306)
		h.Uint64Value(uint64(len(x.Offsets)))
		for _, o := range x.Offsets {
			h.partitionOffset(o)
		}
	case core.DescribeGroupResp:
		h.Uint64Value(307)
		h.Uint64Value(map[bool]uint64{false: 0, true: 1}[x.Exists])
		h.Int64Value(int64(x.Generation))
		h.StringValue(x.ProtocolType)
		h.StringValue(x.Protocol)
		h.StringValue(x.Leader)
		h.Int64Value(int64(x.AssignedGeneration))
		h.StringValue(x.State)
		h.Uint64Value(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m.GroupMember)
			h.Int64Value(m.JoinedTimeMs)
			h.StringValue(string(m.Assignment))
		}
	case core.ListGroupsResp:
		h.Uint64Value(308)
		h.Uint64Value(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.StringValue(g.Name)
			h.StringValue(g.ProtocolType)
			h.StringValue(g.State)
		}
	default:
		return h.controlPlaneGroupCallResponse(r)
	}
	return true
}

// controlPlaneGroupCallResponse folds the answers of the broker group calls. It reports false
// for other responses.
func (h *stateHasher) controlPlaneGroupCallResponse(r core.ControlPlaneResponse) bool {
	switch x := r.(type) {
	case core.GroupJoinResp:
		h.Uint64Value(309)
		h.Int64Value(int64(x.Generation))
		h.StringValue(x.Protocol)
		h.StringValue(x.Leader)
		h.StringValue(x.MemberID)
		h.Uint64Value(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.StringValue(m.MemberID)
			h.StringValue(m.GroupInstanceID)
			h.StringValue(string(m.Metadata))
		}
	case core.GroupSyncResp:
		h.Uint64Value(310)
		h.StringValue(string(x.Assignment))
	case core.GroupLeaveResp:
		h.Uint64Value(311)
		h.Uint64Value(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.Uint64Value(uint64(c))
		}
	case core.DescribeGroupsResp:
		h.Uint64Value(312)
		h.Uint64Value(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.StringValue(g.Name)
			h.Uint64Value(map[bool]uint64{false: 0, true: 1}[g.Exists])
			h.Int64Value(int64(g.Generation))
			h.StringValue(g.ProtocolType)
			h.StringValue(g.Protocol)
			h.StringValue(g.Leader)
			h.StringValue(g.State)
			h.Uint64Value(uint64(len(g.Members)))
			for _, m := range g.Members {
				h.groupMember(m.GroupMember)
				h.Int64Value(m.JoinedTimeMs)
				h.StringValue(string(m.Assignment))
			}
		}
	default:
		return false
	}
	return true
}
