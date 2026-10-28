package sim

import (
	"fmt"

	"manifester/core"
)

// The trace-hash tag tables. Every core event, action, control-plane type and
// fact value gets a frozen numeric tag, written out by hand: never %T, never
// reflection, so renaming a Go type cannot move a TraceHash. Tags are
// append-only; changing one re-baselines every golden trace. Ranges: events
// 1..99, actions 101..199, CP ops 201..299, CP responses 301..399, field
// values 401..499. A type without a row panics (fail closed); the tag test
// scans core/ for types that lack one.

func (h *hasher) ref(r core.NodeRef) {
	h.Str(string(r.Node))
	h.Bytes(r.Inc[:])
}

func (h *hasher) segRef(s core.SegmentRef) {
	h.Str(s.Key)
	h.I64(s.Off)
	h.I64(s.Len)
}

func (h *hasher) batchRange(b core.BatchRange) {
	h.Str(b.TP.Topic)
	h.U64(uint64(uint32(b.TP.Partition)))
	h.I64(int64(b.Base))
	h.I64(b.Count)
	h.segRef(b.Loc)
}

func (h *hasher) event(b core.EventBody) {
	switch e := b.(type) {
	case core.Started:
		h.U64(1)
		h.ref(e.Self)
	case core.Stop:
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
	case core.DialDone:
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

func (h *hasher) eventDone(b core.EventBody) {
	switch e := b.(type) {
	case core.ObjPutDone:
		h.U64(9)
		h.U64(uint64(e.Req))
		h.Str(e.Key)
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.ObjGetDone:
		h.U64(10)
		h.U64(uint64(e.Req))
		h.Bytes(e.Data)
		h.U64(uint64(e.Err))
	case core.ObjDeleteDone:
		h.U64(11)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.ObjListDone:
		h.U64(12)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(len(e.Keys)))
		for _, k := range e.Keys {
			h.Str(k)
		}
	case core.CPDone:
		h.U64(13)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
		h.cpResp(e.Resp)
	case core.CPOffer:
		h.U64(18)
		h.ref(e.From)
		h.U64(uint64(e.Req))
		h.cpOp(e.Op)
	default:
		h.eventLogDone(b)
	}
}

func (h *hasher) eventLogDone(b core.EventBody) {
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
	case core.LogCheckTailDone:
		h.U64(16)
		h.U64(uint64(e.Req))
		h.I64(int64(e.Tail))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	case core.LogTrimDone:
		h.U64(17)
		h.U64(uint64(e.Req))
		h.U64(uint64(e.Err))
		h.U64(uint64(e.Outcome))
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged event %T", b))
	}
}

func (h *hasher) action(a core.Action) {
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
	case core.Dial:
		h.U64(107)
		h.U64(uint64(x.Req))
		h.Str(x.Addr)
	default:
		h.actionIO(a)
	}
}

func (h *hasher) actionIO(a core.Action) {
	switch x := a.(type) {
	case core.ObjPut:
		h.U64(108)
		h.U64(uint64(x.Req))
		h.Str(x.Key)
		h.Bytes(x.Data)
	case core.ObjGet:
		h.U64(109)
		h.U64(uint64(x.Req))
		h.segRef(x.Ref)
	case core.ObjDelete:
		h.U64(110)
		h.U64(uint64(x.Req))
		h.Str(x.Key)
	case core.ObjList:
		h.U64(111)
		h.U64(uint64(x.Req))
		h.Str(x.Prefix)
	case core.CPCall:
		h.U64(112)
		h.U64(uint64(x.Req))
		h.cpOp(x.Op)
	case core.CPReply:
		h.U64(120)
		h.ref(x.To)
		h.U64(uint64(x.Done.Req))
		h.U64(uint64(x.Done.Err))
		h.U64(uint64(x.Done.Outcome))
		h.cpResp(x.Done.Resp)
	case core.Emit:
		h.U64(113) // the fact itself is folded when it reaches the timeline
	case core.Trip:
		h.U64(114)
		h.Str(x.Cause)
	case core.Exit:
		h.U64(115)
	default:
		h.actionLog(a)
	}
}

func (h *hasher) actionLog(a core.Action) {
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
	case core.LogCheckTail:
		h.U64(118)
		h.U64(uint64(x.Req))
	case core.LogTrim:
		h.U64(119)
		h.U64(uint64(x.Req))
		h.I64(int64(x.Through))
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged action %T", a))
	}
}

// cpOp folds an op; nil is tag 200.
func (h *hasher) cpOp(op core.CPOp) {
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
			h.segRef(b.Loc)
		}
	case core.Lookup:
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
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged CP op %T", op))
	}
}

// cpResp folds a response; nil (an error completion) is tag 300.
func (h *hasher) cpResp(r core.CPResp) {
	switch x := r.(type) {
	case nil:
		h.U64(300)
	case core.CommitSegmentResp:
		h.U64(302)
		h.U64(uint64(len(x.Assigned)))
		for _, b := range x.Assigned {
			h.batchRange(b)
		}
	case core.LookupResp:
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
		panic(fmt.Sprintf("sim: trace hash: untagged CP response %T", r))
	}
}

func (h *hasher) fieldValue(v core.FieldValue) {
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

func (h *hasher) fact(f core.Fact) {
	h.U64(501)
	h.Str(f.Name)
	h.U64(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.Str(fl.Key)
		h.fieldValue(fl.Value)
	}
}
