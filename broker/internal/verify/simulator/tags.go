package simulator

import (
	"fmt"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/machine"
)

// The trace-hash tag tables. Every core event, action, control-plane type and
// fact value gets a frozen numeric tag, written out by hand: never %T, never
// reflection, so renaming a Go type cannot move a TraceHash. Tags are
// append-only; changing one re-baselines every golden trace. Ranges: events
// 1..99, actions 101..199, CP ops 201..299, CP responses 301..399, field
// values 401..499. A type without a row panics (fail closed); the tag test
// scans core/ for types that lack one.

func (h *stateHasher) ref(r machine.NodeInstance) {
	h.String(string(r.Node))
	h.Bytes(r.Inc[:])
}

func (h *stateHasher) segmentRef(s core.SegmentRef) {
	h.String(s.Key)
	h.Int64(s.Off)
	h.Int64(s.Len)
}

func (h *stateHasher) batchRange(b core.BatchRange) {
	h.String(b.TopicPartition.Topic)
	h.Uint64(uint64(uint32(b.TopicPartition.Partition)))
	h.Int64(int64(b.Base))
	h.Int64(b.Count)
	h.segmentRef(b.Loc)
}

func (h *stateHasher) event(b core.EventData) {
	switch e := b.(type) {
	case core.NodeStarted:
		h.Uint64(1)
		h.ref(e.Self)
	case core.StopRequest:
		h.Uint64(2)
		h.Int64(int64(e.Deadline))
	case core.ConnOpened:
		h.Uint64(3)
		h.Uint64(uint64(e.Conn))
		h.String(e.Peer)
	case core.BytesReceived:
		h.Uint64(4)
		h.Uint64(uint64(e.Conn))
		h.Bytes(e.Data)
	case core.ConnClosed:
		h.Uint64(5)
		h.Uint64(uint64(e.Conn))
		h.Bool(e.Abort)
	case core.SendDone:
		h.Uint64(6)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Conn))
		h.Uint64(uint64(e.Err))
	case core.ConnectDone:
		h.Uint64(7)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Conn))
		h.Uint64(uint64(e.Err))
	case core.TimerFired:
		h.Uint64(8)
		h.Uint64(uint64(e.ID))
		h.Uint64(uint64(e.Gen))
	default:
		h.eventDone(b)
	}
}

func (h *stateHasher) eventDone(b core.EventData) {
	switch e := b.(type) {
	case core.ObjectPutDone:
		h.Uint64(9)
		h.Uint64(uint64(e.Req))
		h.String(e.Key)
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.ObjectGetDone:
		h.Uint64(10)
		h.Uint64(uint64(e.Req))
		h.Bytes(e.Data)
		h.Uint64(uint64(e.Err))
	case core.ObjectDeleteDone:
		h.Uint64(11)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.ObjectListDone:
		h.Uint64(12)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(len(e.Keys)))
		for _, k := range e.Keys {
			h.String(k)
		}
	case core.ControlPlaneDone:
		h.Uint64(13)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
		h.controlPlaneResponse(e.Resp)
	case core.ControlPlaneDeliver:
		h.Uint64(18)
		h.ref(e.From)
		h.Uint64(uint64(e.Req))
		h.controlPlaneOperation(e.Op)
	default:
		h.eventLogDone(b)
	}
}

func (h *stateHasher) eventLogDone(b core.EventData) {
	switch e := b.(type) {
	case core.LogAppendDone:
		h.Uint64(14)
		h.Uint64(uint64(e.Req))
		h.Int64(int64(e.Slot))
		h.Bool(e.Taken)
		h.Bytes(e.Existing)
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.LogReadDone:
		h.Uint64(15)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(len(e.Entries)))
		for _, ent := range e.Entries {
			h.Int64(int64(ent.Slot))
			h.Bytes(ent.Data)
		}
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.LogGetEndDone:
		h.Uint64(16)
		h.Uint64(uint64(e.Req))
		h.Int64(int64(e.Tail))
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.LogDeleteOldDone:
		h.Uint64(17)
		h.Uint64(uint64(e.Req))
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.LogClaimEpochDone:
		h.Uint64(20)
		h.Uint64(uint64(e.Req))
		h.Uint64(e.Epoch)
		h.String(e.Owner)
		h.Bool(e.Won)
		h.Uint64(uint64(e.Err))
		h.Uint64(uint64(e.Outcome))
	case core.LiveBrokers:
		h.Uint64(19)
		h.Uint64(uint64(len(e.Brokers)))
		for _, bk := range e.Brokers {
			h.Int64(int64(bk.NodeID))
			h.String(bk.Host)
			h.Int64(int64(bk.Port))
		}
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged event %T", b))
	}
}

func (h *stateHasher) action(a core.Action) {
	switch x := a.(type) {
	case core.SetTimer:
		h.Uint64(101)
		h.Uint64(uint64(x.ID))
		h.Uint64(uint64(x.Gen))
		h.Int64(int64(x.After))
	case core.CancelTimer:
		h.Uint64(102)
		h.Uint64(uint64(x.ID))
	case core.Send:
		h.Uint64(103)
		h.Uint64(uint64(x.Req))
		h.Uint64(uint64(x.Conn))
		h.Bytes(x.Data)
	case core.CloseConn:
		h.Uint64(104)
		h.Uint64(uint64(x.Conn))
		h.Bool(x.Abort)
	case core.PauseReads:
		h.Uint64(105)
		h.Uint64(uint64(x.Conn))
	case core.ResumeReads:
		h.Uint64(106)
		h.Uint64(uint64(x.Conn))
	case core.Connect:
		h.Uint64(107)
		h.Uint64(uint64(x.Req))
		h.String(x.Addr)
	default:
		h.actionIO(a)
	}
}

func (h *stateHasher) actionIO(a core.Action) {
	switch x := a.(type) {
	case core.ObjectPut:
		h.Uint64(108)
		h.Uint64(uint64(x.Req))
		h.String(x.Key)
		h.Bytes(x.Data)
	case core.ObjectGet:
		h.Uint64(109)
		h.Uint64(uint64(x.Req))
		h.segmentRef(x.Ref)
	case core.ObjectDelete:
		h.Uint64(110)
		h.Uint64(uint64(x.Req))
		h.String(x.Key)
	case core.ObjectList:
		h.Uint64(111)
		h.Uint64(uint64(x.Req))
		h.String(x.Prefix)
	case core.ControlPlaneCall:
		h.Uint64(112) // Timeout is not folded: the simulator never sets it
		h.Uint64(uint64(x.Req))
		h.controlPlaneOperation(x.Op)
	case core.ControlPlaneReply:
		h.Uint64(120)
		h.ref(x.To)
		h.Uint64(uint64(x.Done.Req))
		h.Uint64(uint64(x.Done.Err))
		h.Uint64(uint64(x.Done.Outcome))
		h.controlPlaneResponse(x.Done.Resp)
	case core.RecordFact:
		h.Uint64(113) // the fact itself is folded when it reaches the timeline
	case core.StopForRestart:
		h.Uint64(114)
		h.String(x.Cause)
	case core.ShutdownDone:
		h.Uint64(115)
	default:
		h.actionLog(a)
	}
}

func (h *stateHasher) actionLog(a core.Action) {
	switch x := a.(type) {
	case core.LogAppend:
		h.Uint64(116)
		h.Uint64(uint64(x.Req))
		h.Int64(int64(x.Slot))
		h.Bytes(x.Data)
	case core.LogRead:
		h.Uint64(117)
		h.Uint64(uint64(x.Req))
		h.Int64(int64(x.From))
		h.Uint64(uint64(uint32(x.Limit)))
	case core.LogGetEnd:
		h.Uint64(118)
		h.Uint64(uint64(x.Req))
	case core.LogDeleteOld:
		h.Uint64(119)
		h.Uint64(uint64(x.Req))
		h.Int64(int64(x.Through))
	case core.LogClaimEpoch:
		h.Uint64(121)
		h.Uint64(uint64(x.Req))
		h.Uint64(x.Expect)
		h.String(x.Owner)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged action %T", a))
	}
}

// controlPlaneOperation folds an op; nil is tag 200.
func (h *stateHasher) controlPlaneOperation(operation core.ControlPlaneOperation) {
	switch x := operation.(type) {
	case nil:
		h.Uint64(200)
	case core.CommitSegment:
		h.Uint64(202)
		h.String(x.SegmentKey)
		h.Uint64(uint64(len(x.Batches)))
		for _, b := range x.Batches {
			h.String(b.TopicPartition.Topic)
			h.Uint64(uint64(uint32(b.TopicPartition.Partition)))
			h.Int64(b.Records)
			h.segmentRef(b.Loc)
		}
	case core.FindSegments:
		h.Uint64(203)
		h.String(x.TopicPartition.Topic)
		h.Uint64(uint64(uint32(x.TopicPartition.Partition)))
		h.Int64(int64(x.From))
		h.Uint64(uint64(uint32(x.MaxBytes)))
	case core.DescribeTopics:
		h.Uint64(204)
		h.Uint64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.String(t)
		}
	case core.CreateTopic:
		tag := uint64(205) // an exclusive create has its own tag; plain creates keep theirs
		if x.FailIfExists {
			tag = 218
		}
		h.Uint64(tag)
		h.String(x.Name)
		h.Uint64(uint64(uint32(x.Partitions)))
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
		h.Uint64(300)
	case core.CommitSegmentResp:
		h.Uint64(302)
		h.Uint64(uint64(len(x.Assigned)))
		for _, b := range x.Assigned {
			h.batchRange(b)
		}
	case core.FindSegmentsResp:
		h.Uint64(303)
		h.Uint64(uint64(len(x.Ranges)))
		for _, b := range x.Ranges {
			h.batchRange(b)
		}
		h.Int64(int64(x.End))
	case core.DescribeTopicsResp:
		h.Uint64(304)
		h.Uint64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.String(t.Name)
			h.Uint64(uint64(uint32(t.Partitions)))
		}
	default:
		if !h.controlPlaneGroupResponse(r) {
			panic(fmt.Sprintf("sim: trace hash: untagged CP response %T", r))
		}
	}
}

func (h *stateHasher) fieldValue(v machine.FieldValue) {
	switch x := v.(type) {
	case machine.Int64Value:
		h.Uint64(401)
		h.Int64(int64(x))
	case machine.Uint64Value:
		h.Uint64(402)
		h.Uint64(uint64(x))
	case machine.StringValue:
		h.Uint64(403)
		h.String(string(x))
	case machine.Bool:
		h.Uint64(404)
		h.Bool(bool(x))
	case machine.Bytes:
		h.Uint64(405)
		h.Bytes(x)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged field value %T", v))
	}
}

func (h *stateHasher) fact(f machine.Fact) {
	h.Uint64(501)
	h.String(f.Name)
	h.Uint64(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.String(fl.Key)
		h.fieldValue(fl.Value)
	}
}

// partitionOffset folds one committed offset (a group write or read).
func (h *stateHasher) partitionOffset(o core.PartitionOffset) {
	h.String(o.TopicPartition.Topic)
	h.Uint64(uint64(uint32(o.TopicPartition.Partition)))
	h.Int64(o.Offset)
	h.Int64(int64(o.LeaderEpoch))
	h.String(o.Metadata)
}

// groupMember folds the fields of a member that a join or a describe both carry.
func (h *stateHasher) groupMember(m core.GroupMember) {
	h.String(m.MemberID)
	h.String(m.ClientID)
	h.String(m.ClientHost)
	h.String(m.GroupInstanceID)
	h.Int64(int64(m.SessionTimeoutMs))
	h.Int64(int64(m.RebalanceTimeoutMs))
	h.String(string(m.Metadata))
}

// controlPlaneGroupOperation folds a group operation. It reports false for other operations.
func (h *stateHasher) controlPlaneGroupOperation(operation core.ControlPlaneOperation) bool {
	switch x := operation.(type) {
	case core.GroupInstall:
		h.Uint64(206)
		h.String(x.Group)
		h.String(x.ProtocolType)
		h.String(x.Protocol)
		h.Int64(int64(x.Generation))
		h.String(x.Leader)
		h.Int64(x.TimeMs)
		h.Uint64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m)
		}
	case core.GroupAssign:
		h.Uint64(207)
		h.String(x.Group)
		h.Int64(int64(x.Generation))
		h.String(x.Leader)
		h.Int64(x.TimeMs)
		h.Uint64(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.String(a.Member)
			h.String(string(a.Bytes))
		}
	case core.GroupRemove:
		h.Uint64(208)
		h.String(x.Group)
		h.Int64(x.TimeMs)
		h.String(x.Reason)
		h.Uint64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.String(m)
		}
	case core.OffsetCommit:
		h.Uint64(209)
		h.String(x.Group)
		h.String(x.Member)
		h.Int64(int64(x.Generation))
		h.Int64(x.TimeMs)
		h.Uint64(uint64(len(x.Offsets)))
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
		h.Uint64(213)
		h.String(x.Group)
		h.String(x.MemberID)
		h.String(x.GroupInstanceID)
		h.Int64(int64(x.SessionTimeoutMs))
		h.Int64(int64(x.RebalanceTimeoutMs))
		h.String(x.ProtocolType)
		h.Uint64(uint64(len(x.Protocols)))
		for _, p := range x.Protocols {
			h.String(p.Name)
			h.String(string(p.Metadata))
		}
	case core.GroupSync:
		h.Uint64(214)
		h.String(x.Group)
		h.String(x.MemberID)
		h.Int64(int64(x.Generation))
		h.Uint64(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.String(a.Member)
			h.String(string(a.Bytes))
		}
	case core.GroupHeartbeat:
		h.Uint64(215)
		h.String(x.Group)
		h.String(x.MemberID)
		h.Int64(int64(x.Generation))
	case core.GroupLeave:
		h.Uint64(216)
		h.String(x.Group)
		h.Uint64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.String(m)
		}
	case core.DescribeGroups:
		h.Uint64(217)
		h.Uint64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.String(g)
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
		h.Uint64(210)
		h.String(x.Group)
		h.Uint64(uint64(len(x.Partitions)))
		for _, topicPartition := range x.Partitions {
			h.String(topicPartition.Topic)
			h.Uint64(uint64(uint32(topicPartition.Partition)))
		}
	case core.DescribeGroup:
		h.Uint64(211)
		h.String(x.Group)
	case core.ListGroups:
		h.Uint64(212)
	default:
		return false
	}
	return true
}

// controlPlaneGroupResponse folds a group response. It reports false for other responses.
func (h *stateHasher) controlPlaneGroupResponse(r core.ControlPlaneResponse) bool {
	switch x := r.(type) {
	case core.OffsetCommitResp:
		h.Uint64(305)
		h.Uint64(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.Uint64(uint64(c))
		}
	case core.OffsetFetchResp:
		h.Uint64(306)
		h.Uint64(uint64(len(x.Offsets)))
		for _, o := range x.Offsets {
			h.partitionOffset(o)
		}
	case core.DescribeGroupResp:
		h.Uint64(307)
		h.Uint64(map[bool]uint64{false: 0, true: 1}[x.Exists])
		h.Int64(int64(x.Generation))
		h.String(x.ProtocolType)
		h.String(x.Protocol)
		h.String(x.Leader)
		h.Int64(int64(x.AssignedGeneration))
		h.String(x.State)
		h.Uint64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m.GroupMember)
			h.Int64(m.JoinedTimeMs)
			h.String(string(m.Assignment))
		}
	case core.ListGroupsResp:
		h.Uint64(308)
		h.Uint64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.String(g.Name)
			h.String(g.ProtocolType)
			h.String(g.State)
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
		h.Uint64(309)
		h.Int64(int64(x.Generation))
		h.String(x.Protocol)
		h.String(x.Leader)
		h.String(x.MemberID)
		h.Uint64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.String(m.MemberID)
			h.String(m.GroupInstanceID)
			h.String(string(m.Metadata))
		}
	case core.GroupSyncResp:
		h.Uint64(310)
		h.String(string(x.Assignment))
	case core.GroupLeaveResp:
		h.Uint64(311)
		h.Uint64(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.Uint64(uint64(c))
		}
	case core.DescribeGroupsResp:
		h.Uint64(312)
		h.Uint64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.String(g.Name)
			h.Uint64(map[bool]uint64{false: 0, true: 1}[g.Exists])
			h.Int64(int64(g.Generation))
			h.String(g.ProtocolType)
			h.String(g.Protocol)
			h.String(g.Leader)
			h.String(g.State)
			h.Uint64(uint64(len(g.Members)))
			for _, m := range g.Members {
				h.groupMember(m.GroupMember)
				h.Int64(m.JoinedTimeMs)
				h.String(string(m.Assignment))
			}
		}
	default:
		return false
	}
	return true
}
