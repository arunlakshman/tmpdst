package simulator

import (
	"fmt"

	"manifester/internal/core"
)

// The trace-hash tag tables. Every core event, action, control-plane type and
// fact value gets a frozen numeric tag, written out by hand: never %T, never
// reflection, so renaming a Go type cannot move a TraceHash. Tags are
// append-only; changing one re-baselines every golden trace. Ranges: events
// 1..99, actions 101..199, CP ops 201..299, CP responses 301..399, field
// values 401..499. A type without a row panics (fail closed); the tag test
// scans core/ for types that lack one.

func (h *stateHasher) ref(r core.NodeInstance) {
	h.Str(string(r.Node))
	h.Bytes(r.Inc[:])
}

func (h *stateHasher) segmentRef(s core.SegmentRef) {
	h.Str(s.Key)
	h.I64(s.Off)
	h.I64(s.Len)
}

func (h *stateHasher) batchRange(b core.BatchRange) {
	h.Str(b.TP.Topic)
	h.U64(uint64(uint32(b.TP.Partition)))
	h.I64(int64(b.Base))
	h.I64(b.Count)
	h.segmentRef(b.Loc)
}

func (h *stateHasher) event(b core.EventData) {
	switch e := b.(type) {
	case core.NodeStarted:
		h.U64(1)
		h.ref(e.Self)
	case core.StopRequest:
		h.U64(2)
		h.I64(int64(e.Deadline))
	case core.ConnOpened:
		h.U64(3)
		h.U64(uint64(e.Conn))
		h.Str(e.Peer)
	case core.BytesReceived:
		h.U64(4)
		h.U64(uint64(e.Conn))
		h.Bytes(e.Data)
	case core.ConnClosed:
		h.U64(5)
		h.U64(uint64(e.Conn))
		h.Bool(e.Abort)
	case core.SendDone:
		h.U64(6)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Conn))
		h.U64(uint64(e.Err))
	case core.ConnectDone:
		h.U64(7)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Conn))
		h.U64(uint64(e.Err))
	case core.TimerFired:
		h.U64(8)
		h.U64(uint64(e.ID))
		h.U64(uint64(e.Gen))
	default:
		h.eventDone(b)
	}
}

func (h *stateHasher) eventDone(b core.EventData) {
	switch e := b.(type) {
	case core.ObjectPutDone:
		h.U64(9)
		h.U64(uint64(e.Req))
		h.Str(e.Key)
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.ObjectGetDone:
		h.U64(10)
		h.U64(uint64(e.Req))
		h.Bytes(e.Data)
		h.U64(uint64(e.Err))
	case core.ObjectDeleteDone:
		h.U64(11)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.ObjectListDone:
		h.U64(12)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(len(e.Keys)))
		for _, k := range e.Keys {
			h.Str(k)
		}
	case core.ControlPlaneDone:
		h.U64(13)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
		h.controlPlaneResponse(e.Resp)
	case core.ControlPlaneDeliver:
		h.U64(18)
		h.ref(e.From)
		h.U64(uint64(e.Req))
		h.controlPlaneOperation(e.Op)
	default:
		h.eventLogDone(b)
	}
}

func (h *stateHasher) eventLogDone(b core.EventData) {
	switch e := b.(type) {
	case core.LogAppendDone:
		h.U64(14)
		h.U64(uint64(e.Req))
		h.I64(int64(e.Slot))
		h.Bool(e.Taken)
		h.Bytes(e.Existing)
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.LogReadDone:
		h.U64(15)
		h.U64(uint64(e.Req))
		h.U64(uint64(len(e.Entries)))
		for _, ent := range e.Entries {
			h.I64(int64(ent.Slot))
			h.Bytes(ent.Data)
		}
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.LogGetEndDone:
		h.U64(16)
		h.U64(uint64(e.Req))
		h.I64(int64(e.Tail))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.LogDeleteOldDone:
		h.U64(17)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.LiveBrokers:
		h.U64(19)
		h.U64(uint64(len(e.Brokers)))
		for _, bk := range e.Brokers {
			h.I64(int64(bk.NodeID))
			h.Str(bk.Host)
			h.I64(int64(bk.Port))
		}
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged event %T", b))
	}
}

func (h *stateHasher) action(a core.Action) {
	switch x := a.(type) {
	case core.SetTimer:
		h.U64(101)
		h.U64(uint64(x.ID))
		h.U64(uint64(x.Gen))
		h.I64(int64(x.After))
	case core.CancelTimer:
		h.U64(102)
		h.U64(uint64(x.ID))
	case core.Send:
		h.U64(103)
		h.U64(uint64(x.Req))
		h.U64(uint64(x.Conn))
		h.Bytes(x.Data)
	case core.CloseConn:
		h.U64(104)
		h.U64(uint64(x.Conn))
		h.Bool(x.Abort)
	case core.PauseReads:
		h.U64(105)
		h.U64(uint64(x.Conn))
	case core.ResumeReads:
		h.U64(106)
		h.U64(uint64(x.Conn))
	case core.Connect:
		h.U64(107)
		h.U64(uint64(x.Req))
		h.Str(x.Addr)
	default:
		h.actionIO(a)
	}
}

func (h *stateHasher) actionIO(a core.Action) {
	switch x := a.(type) {
	case core.ObjectPut:
		h.U64(108)
		h.U64(uint64(x.Req))
		h.Str(x.Key)
		h.Bytes(x.Data)
	case core.ObjectGet:
		h.U64(109)
		h.U64(uint64(x.Req))
		h.segmentRef(x.Ref)
	case core.ObjectDelete:
		h.U64(110)
		h.U64(uint64(x.Req))
		h.Str(x.Key)
	case core.ObjectList:
		h.U64(111)
		h.U64(uint64(x.Req))
		h.Str(x.Prefix)
	case core.ControlPlaneCall:
		h.U64(112) // Timeout is not folded: the simulator never sets it
		h.U64(uint64(x.Req))
		h.controlPlaneOperation(x.Op)
	case core.ControlPlaneReply:
		h.U64(120)
		h.ref(x.To)
		h.U64(uint64(x.Done.Req))
		h.U64(uint64(x.Done.Err))
		h.U64(uint64(x.Done.Outcome))
		h.controlPlaneResponse(x.Done.Resp)
	case core.RecordFact:
		h.U64(113) // the fact itself is folded when it reaches the timeline
	case core.StopForRestart:
		h.U64(114)
		h.Str(x.Cause)
	case core.ShutdownDone:
		h.U64(115)
	default:
		h.actionLog(a)
	}
}

func (h *stateHasher) actionLog(a core.Action) {
	switch x := a.(type) {
	case core.LogAppend:
		h.U64(116)
		h.U64(uint64(x.Req))
		h.I64(int64(x.Slot))
		h.Bytes(x.Data)
	case core.LogRead:
		h.U64(117)
		h.U64(uint64(x.Req))
		h.I64(int64(x.From))
		h.U64(uint64(uint32(x.Limit)))
	case core.LogGetEnd:
		h.U64(118)
		h.U64(uint64(x.Req))
	case core.LogDeleteOld:
		h.U64(119)
		h.U64(uint64(x.Req))
		h.I64(int64(x.Through))
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged action %T", a))
	}
}

// controlPlaneOperation folds an op; nil is tag 200.
func (h *stateHasher) controlPlaneOperation(op core.ControlPlaneOperation) {
	switch x := op.(type) {
	case nil:
		h.U64(200)
	case core.CommitSegment:
		h.U64(202)
		h.Str(x.SegmentKey)
		h.U64(uint64(len(x.Batches)))
		for _, b := range x.Batches {
			h.Str(b.TP.Topic)
			h.U64(uint64(uint32(b.TP.Partition)))
			h.I64(b.Records)
			h.segmentRef(b.Loc)
		}
	case core.FindSegments:
		h.U64(203)
		h.Str(x.TP.Topic)
		h.U64(uint64(uint32(x.TP.Partition)))
		h.I64(int64(x.From))
		h.U64(uint64(uint32(x.MaxBytes)))
	case core.DescribeTopics:
		h.U64(204)
		h.U64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.Str(t)
		}
	case core.CreateTopic:
		tag := uint64(205) // an exclusive create has its own tag; plain creates keep theirs
		if x.FailIfExists {
			tag = 218
		}
		h.U64(tag)
		h.Str(x.Name)
		h.U64(uint64(uint32(x.Partitions)))
	default:
		if !h.controlPlaneGroupOperation(op) {
			panic(fmt.Sprintf("sim: trace hash: untagged CP op %T", op))
		}
	}
}

// controlPlaneResponse folds a response; nil (an error completion) is tag 300.
func (h *stateHasher) controlPlaneResponse(r core.ControlPlaneResponse) {
	switch x := r.(type) {
	case nil:
		h.U64(300)
	case core.CommitSegmentResp:
		h.U64(302)
		h.U64(uint64(len(x.Assigned)))
		for _, b := range x.Assigned {
			h.batchRange(b)
		}
	case core.FindSegmentsResp:
		h.U64(303)
		h.U64(uint64(len(x.Ranges)))
		for _, b := range x.Ranges {
			h.batchRange(b)
		}
		h.I64(int64(x.End))
	case core.DescribeTopicsResp:
		h.U64(304)
		h.U64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.Str(t.Name)
			h.U64(uint64(uint32(t.Partitions)))
		}
	default:
		if !h.controlPlaneGroupResponse(r) {
			panic(fmt.Sprintf("sim: trace hash: untagged CP response %T", r))
		}
	}
}

func (h *stateHasher) fieldValue(v core.FieldValue) {
	switch x := v.(type) {
	case core.I64:
		h.U64(401)
		h.I64(int64(x))
	case core.U64:
		h.U64(402)
		h.U64(uint64(x))
	case core.Str:
		h.U64(403)
		h.Str(string(x))
	case core.Bool:
		h.U64(404)
		h.Bool(bool(x))
	case core.Bytes:
		h.U64(405)
		h.Bytes(x)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged field value %T", v))
	}
}

func (h *stateHasher) fact(f core.Fact) {
	h.U64(501)
	h.Str(f.Name)
	h.U64(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.Str(fl.Key)
		h.fieldValue(fl.Value)
	}
}

// partitionOffset folds one committed offset (a group write or read).
func (h *stateHasher) partitionOffset(o core.PartitionOffset) {
	h.Str(o.TP.Topic)
	h.U64(uint64(uint32(o.TP.Partition)))
	h.I64(o.Offset)
	h.I64(int64(o.LeaderEpoch))
	h.Str(o.Metadata)
}

// groupMember folds the fields of a member that a join or a describe both carry.
func (h *stateHasher) groupMember(m core.GroupMember) {
	h.Str(m.MemberID)
	h.Str(m.ClientID)
	h.Str(m.ClientHost)
	h.Str(m.GroupInstanceID)
	h.I64(int64(m.SessionTimeoutMs))
	h.I64(int64(m.RebalanceTimeoutMs))
	h.Str(string(m.Metadata))
}

// controlPlaneGroupOperation folds a group operation. It reports false for other operations.
func (h *stateHasher) controlPlaneGroupOperation(op core.ControlPlaneOperation) bool {
	switch x := op.(type) {
	case core.GroupInstall:
		h.U64(206)
		h.Str(x.Group)
		h.Str(x.ProtocolType)
		h.Str(x.Protocol)
		h.I64(int64(x.Generation))
		h.Str(x.Leader)
		h.I64(x.TimeMs)
		h.U64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m)
		}
	case core.GroupAssign:
		h.U64(207)
		h.Str(x.Group)
		h.I64(int64(x.Generation))
		h.Str(x.Leader)
		h.I64(x.TimeMs)
		h.U64(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.Str(a.Member)
			h.Str(string(a.Bytes))
		}
	case core.GroupRemove:
		h.U64(208)
		h.Str(x.Group)
		h.I64(x.TimeMs)
		h.Str(x.Reason)
		h.U64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.Str(m)
		}
	case core.OffsetCommit:
		h.U64(209)
		h.Str(x.Group)
		h.Str(x.Member)
		h.I64(int64(x.Generation))
		h.I64(x.TimeMs)
		h.U64(uint64(len(x.Offsets)))
		for _, o := range x.Offsets {
			h.partitionOffset(o)
		}
	default:
		return h.controlPlaneGroupRead(op) || h.controlPlaneGroupCall(op)
	}
	return true
}

// controlPlaneGroupCall folds a broker group call to the control plane (decision Q3).
// It reports false for other operations.
func (h *stateHasher) controlPlaneGroupCall(op core.ControlPlaneOperation) bool {
	switch x := op.(type) {
	case core.GroupJoin:
		h.U64(213)
		h.Str(x.Group)
		h.Str(x.MemberID)
		h.Str(x.GroupInstanceID)
		h.I64(int64(x.SessionTimeoutMs))
		h.I64(int64(x.RebalanceTimeoutMs))
		h.Str(x.ProtocolType)
		h.U64(uint64(len(x.Protocols)))
		for _, p := range x.Protocols {
			h.Str(p.Name)
			h.Str(string(p.Metadata))
		}
	case core.GroupSync:
		h.U64(214)
		h.Str(x.Group)
		h.Str(x.MemberID)
		h.I64(int64(x.Generation))
		h.U64(uint64(len(x.Assignments)))
		for _, a := range x.Assignments {
			h.Str(a.Member)
			h.Str(string(a.Bytes))
		}
	case core.GroupHeartbeat:
		h.U64(215)
		h.Str(x.Group)
		h.Str(x.MemberID)
		h.I64(int64(x.Generation))
	case core.GroupLeave:
		h.U64(216)
		h.Str(x.Group)
		h.U64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.Str(m)
		}
	case core.DescribeGroups:
		h.U64(217)
		h.U64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.Str(g)
		}
	default:
		return false
	}
	return true
}

// controlPlaneGroupRead folds a group read. It reports false for other operations.
func (h *stateHasher) controlPlaneGroupRead(op core.ControlPlaneOperation) bool {
	switch x := op.(type) {
	case core.OffsetFetch:
		h.U64(210)
		h.Str(x.Group)
		h.U64(uint64(len(x.Partitions)))
		for _, tp := range x.Partitions {
			h.Str(tp.Topic)
			h.U64(uint64(uint32(tp.Partition)))
		}
	case core.DescribeGroup:
		h.U64(211)
		h.Str(x.Group)
	case core.ListGroups:
		h.U64(212)
	default:
		return false
	}
	return true
}

// controlPlaneGroupResponse folds a group response. It reports false for other responses.
func (h *stateHasher) controlPlaneGroupResponse(r core.ControlPlaneResponse) bool {
	switch x := r.(type) {
	case core.OffsetCommitResp:
		h.U64(305)
		h.U64(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.U64(uint64(c))
		}
	case core.OffsetFetchResp:
		h.U64(306)
		h.U64(uint64(len(x.Offsets)))
		for _, o := range x.Offsets {
			h.partitionOffset(o)
		}
	case core.DescribeGroupResp:
		h.U64(307)
		h.U64(map[bool]uint64{false: 0, true: 1}[x.Exists])
		h.I64(int64(x.Generation))
		h.Str(x.ProtocolType)
		h.Str(x.Protocol)
		h.Str(x.Leader)
		h.I64(int64(x.AssignedGeneration))
		h.Str(x.State)
		h.U64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.groupMember(m.GroupMember)
			h.I64(m.JoinedTimeMs)
			h.Str(string(m.Assignment))
		}
	case core.ListGroupsResp:
		h.U64(308)
		h.U64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.Str(g.Name)
			h.Str(g.ProtocolType)
			h.Str(g.State)
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
		h.U64(309)
		h.I64(int64(x.Generation))
		h.Str(x.Protocol)
		h.Str(x.Leader)
		h.Str(x.MemberID)
		h.U64(uint64(len(x.Members)))
		for _, m := range x.Members {
			h.Str(m.MemberID)
			h.Str(m.GroupInstanceID)
			h.Str(string(m.Metadata))
		}
	case core.GroupSyncResp:
		h.U64(310)
		h.Str(string(x.Assignment))
	case core.GroupLeaveResp:
		h.U64(311)
		h.U64(uint64(len(x.Errs)))
		for _, c := range x.Errs {
			h.U64(uint64(c))
		}
	case core.DescribeGroupsResp:
		h.U64(312)
		h.U64(uint64(len(x.Groups)))
		for _, g := range x.Groups {
			h.Str(g.Name)
			h.U64(map[bool]uint64{false: 0, true: 1}[g.Exists])
			h.I64(int64(g.Generation))
			h.Str(g.ProtocolType)
			h.Str(g.Protocol)
			h.Str(g.Leader)
			h.Str(g.State)
			h.U64(uint64(len(g.Members)))
			for _, m := range g.Members {
				h.groupMember(m.GroupMember)
				h.I64(m.JoinedTimeMs)
				h.Str(string(m.Assignment))
			}
		}
	default:
		return false
	}
	return true
}
