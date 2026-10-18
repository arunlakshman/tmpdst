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
	h.str(string(r.Node))
	h.bytes(r.Inc[:])
}

func (h *hasher) segRef(s core.SegmentRef) {
	h.str(s.Key)
	h.i64(s.Off)
	h.i64(s.Len)
}

func (h *hasher) batchRange(b core.BatchRange) {
	h.str(b.TP.Topic)
	h.u64(uint64(uint32(b.TP.Partition)))
	h.i64(int64(b.Base))
	h.i64(b.Count)
	h.segRef(b.Loc)
}

func (h *hasher) event(b core.EventBody) {
	switch e := b.(type) {
	case core.Started:
		h.u64(1)
		h.ref(e.Self)
	case core.Stop:
		h.u64(2)
		h.i64(int64(e.Deadline))
	case core.ConnOpened:
		h.u64(3)
		h.u64(uint64(e.Conn))
		h.str(e.Peer)
	case core.BytesReceived:
		h.u64(4)
		h.u64(uint64(e.Conn))
		h.bytes(e.Data)
	case core.ConnClosed:
		h.u64(5)
		h.u64(uint64(e.Conn))
		h.bool(e.Abort)
	case core.SendDone:
		h.u64(6)
		h.u64(uint64(e.Req))
		h.u64(uint64(e.Conn))
		h.u64(uint64(e.Err))
	case core.DialDone:
		h.u64(7)
		h.u64(uint64(e.Req))
		h.u64(uint64(e.Conn))
		h.u64(uint64(e.Err))
	case core.TimerFired:
		h.u64(8)
		h.u64(uint64(e.ID))
		h.u64(uint64(e.Gen))
	case core.ObjPutDone:
		h.u64(9)
		h.u64(uint64(e.Req))
		h.str(e.Key)
		h.u64(uint64(e.Err))
		h.u64(uint64(e.Outcome))
	case core.ObjGetDone:
		h.u64(10)
		h.u64(uint64(e.Req))
		h.bytes(e.Data)
		h.u64(uint64(e.Err))
	case core.ObjDeleteDone:
		h.u64(11)
		h.u64(uint64(e.Req))
		h.u64(uint64(e.Err))
		h.u64(uint64(e.Outcome))
	case core.ObjListDone:
		h.u64(12)
		h.u64(uint64(e.Req))
		h.u64(uint64(e.Err))
		h.u64(uint64(len(e.Keys)))
		for _, k := range e.Keys {
			h.str(k)
		}
	case core.CPDone:
		h.u64(13)
		h.u64(uint64(e.Req))
		h.u64(uint64(e.Err))
		h.u64(uint64(e.Outcome))
		h.cpResp(e.Resp)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged event %T", b))
	}
}

func (h *hasher) action(a core.Action) {
	switch x := a.(type) {
	case core.SetTimer:
		h.u64(101)
		h.u64(uint64(x.ID))
		h.u64(uint64(x.Gen))
		h.i64(int64(x.After))
	case core.CancelTimer:
		h.u64(102)
		h.u64(uint64(x.ID))
	case core.Send:
		h.u64(103)
		h.u64(uint64(x.Req))
		h.u64(uint64(x.Conn))
		h.bytes(x.Data)
	case core.CloseConn:
		h.u64(104)
		h.u64(uint64(x.Conn))
		h.bool(x.Abort)
	case core.PauseReads:
		h.u64(105)
		h.u64(uint64(x.Conn))
	case core.ResumeReads:
		h.u64(106)
		h.u64(uint64(x.Conn))
	case core.Dial:
		h.u64(107)
		h.u64(uint64(x.Req))
		h.str(x.Addr)
	case core.ObjPut:
		h.u64(108)
		h.u64(uint64(x.Req))
		h.str(x.Key)
		h.bytes(x.Data)
	case core.ObjGet:
		h.u64(109)
		h.u64(uint64(x.Req))
		h.segRef(x.Ref)
	case core.ObjDelete:
		h.u64(110)
		h.u64(uint64(x.Req))
		h.str(x.Key)
	case core.ObjList:
		h.u64(111)
		h.u64(uint64(x.Req))
		h.str(x.Prefix)
	case core.CPCall:
		h.u64(112)
		h.u64(uint64(x.Req))
		h.cpOp(x.Op)
	case core.Emit:
		h.u64(113) // the fact itself is folded when it reaches the timeline
	case core.Trip:
		h.u64(114)
		h.str(x.Cause)
	case core.Exit:
		h.u64(115)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged action %T", a))
	}
}

// cpOp folds an op; nil is tag 200.
func (h *hasher) cpOp(op core.CPOp) {
	switch x := op.(type) {
	case nil:
		h.u64(200)
	case core.CommitSegment:
		h.u64(202)
		h.str(x.SegmentKey)
		h.u64(uint64(len(x.Batches)))
		for _, b := range x.Batches {
			h.str(b.TP.Topic)
			h.u64(uint64(uint32(b.TP.Partition)))
			h.i64(b.Records)
			h.segRef(b.Loc)
		}
	case core.Lookup:
		h.u64(203)
		h.str(x.TP.Topic)
		h.u64(uint64(uint32(x.TP.Partition)))
		h.i64(int64(x.From))
		h.u64(uint64(uint32(x.MaxBytes)))
	case core.DescribeTopics:
		h.u64(204)
		h.u64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.str(t)
		}
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged CP op %T", op))
	}
}

// cpResp folds a response; nil (an error completion) is tag 300.
func (h *hasher) cpResp(r core.CPResp) {
	switch x := r.(type) {
	case nil:
		h.u64(300)
	case core.CommitSegmentResp:
		h.u64(302)
		h.u64(uint64(len(x.Assigned)))
		for _, b := range x.Assigned {
			h.batchRange(b)
		}
	case core.LookupResp:
		h.u64(303)
		h.u64(uint64(len(x.Ranges)))
		for _, b := range x.Ranges {
			h.batchRange(b)
		}
		h.i64(int64(x.End))
	case core.DescribeTopicsResp:
		h.u64(304)
		h.u64(uint64(len(x.Topics)))
		for _, t := range x.Topics {
			h.str(t.Name)
			h.u64(uint64(uint32(t.Partitions)))
		}
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged CP response %T", r))
	}
}

func (h *hasher) fieldValue(v core.FieldValue) {
	switch x := v.(type) {
	case core.I64:
		h.u64(401)
		h.i64(int64(x))
	case core.U64:
		h.u64(402)
		h.u64(uint64(x))
	case core.Str:
		h.u64(403)
		h.str(string(x))
	case core.Bool:
		h.u64(404)
		h.bool(bool(x))
	case core.Bytes:
		h.u64(405)
		h.bytes(x)
	default:
		panic(fmt.Sprintf("sim: trace hash: untagged field value %T", v))
	}
}

func (h *hasher) fact(f core.Fact) {
	h.u64(501)
	h.str(f.Name)
	h.u64(uint64(len(f.Fields)))
	for _, fl := range f.Fields {
		h.str(fl.Key)
		h.fieldValue(fl.Value)
	}
}
