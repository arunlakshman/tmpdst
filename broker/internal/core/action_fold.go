//nolint:dupl // Fold methods are flat lists of fields. They look alike on purpose.
package core

import "github.com/manifesterio/mds/dst/trace"

// Tag and Fold of every Action case. The tag numbers are frozen (see fold.go).

// Tag returns the frozen trace-hash number of this action.
func (SetTimer) Tag() uint64 { return 101 }

// Fold writes the action into the trace hash.
func (a SetTimer) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.ID))
	h.Uint64(uint64(a.Gen))
	h.Int64(int64(a.After))
}

// Tag returns the frozen trace-hash number of this action.
func (CancelTimer) Tag() uint64 { return 102 }

// Fold writes the action into the trace hash.
func (a CancelTimer) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.ID))
}

// Tag returns the frozen trace-hash number of this action.
func (Send) Tag() uint64 { return 103 }

// Fold writes the action into the trace hash.
func (a Send) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.Uint64(uint64(a.Conn))
	h.Bytes(a.Data)
}

// Tag returns the frozen trace-hash number of this action.
func (CloseConn) Tag() uint64 { return 104 }

// Fold writes the action into the trace hash.
func (a CloseConn) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Conn))
	h.Bool(a.Abort)
}

// Tag returns the frozen trace-hash number of this action.
func (PauseReads) Tag() uint64 { return 105 }

// Fold writes the action into the trace hash.
func (a PauseReads) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Conn))
}

// Tag returns the frozen trace-hash number of this action.
func (ResumeReads) Tag() uint64 { return 106 }

// Fold writes the action into the trace hash.
func (a ResumeReads) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Conn))
}

// Tag returns the frozen trace-hash number of this action.
func (Connect) Tag() uint64 { return 107 }

// Fold writes the action into the trace hash.
func (a Connect) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.String(a.Addr)
}

// Tag returns the frozen trace-hash number of this action.
func (ObjectPut) Tag() uint64 { return 108 }

// Fold writes the action into the trace hash.
func (a ObjectPut) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.String(a.Key)
	h.Bytes(a.Data)
}

// Tag returns the frozen trace-hash number of this action.
func (ObjectGet) Tag() uint64 { return 109 }

// Fold writes the action into the trace hash.
func (a ObjectGet) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	foldSegmentRef(h, a.Ref)
}

// Tag returns the frozen trace-hash number of this action.
func (ObjectDelete) Tag() uint64 { return 110 }

// Fold writes the action into the trace hash.
func (a ObjectDelete) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.String(a.Key)
}

// Tag returns the frozen trace-hash number of this action.
func (ObjectList) Tag() uint64 { return 111 }

// Fold writes the action into the trace hash.
func (a ObjectList) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.String(a.Prefix)
}

// Tag returns the frozen trace-hash number of this action.
func (ControlPlaneCall) Tag() uint64 { return 112 }

// Fold writes the action into the trace hash. Timeout is not folded: the simulator
// never sets it.
func (a ControlPlaneCall) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	foldOperation(h, a.Op)
}

// Tag returns the frozen trace-hash number of this action.
func (ControlPlaneReply) Tag() uint64 { return 120 }

// Fold writes the action into the trace hash. The completion is folded without
// its own tag.
func (a ControlPlaneReply) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	foldInstance(h, a.To)
	a.Done.foldBody(h)
}

// Tag returns the frozen trace-hash number of this action.
func (RecordFact) Tag() uint64 { return 113 }

// Fold writes the action into the trace hash. The fact itself is folded when it
// reaches the timeline.
func (a RecordFact) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
}

// Tag returns the frozen trace-hash number of this action.
func (StopForRestart) Tag() uint64 { return 114 }

// Fold writes the action into the trace hash.
func (a StopForRestart) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.String(a.Cause)
}

// Tag returns the frozen trace-hash number of this action.
func (ShutdownDone) Tag() uint64 { return 115 }

// Fold writes the action into the trace hash.
func (a ShutdownDone) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
}

// Tag returns the frozen trace-hash number of this action.
func (LogAppend) Tag() uint64 { return 116 }

// Fold writes the action into the trace hash.
func (a LogAppend) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.Int64(int64(a.Slot))
	h.Bytes(a.Data)
}

// Tag returns the frozen trace-hash number of this action.
func (LogRead) Tag() uint64 { return 117 }

// Fold writes the action into the trace hash.
func (a LogRead) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.Int64(int64(a.From))
	h.Uint64(uint64(uint32(a.Limit)))
}

// Tag returns the frozen trace-hash number of this action.
func (LogGetEnd) Tag() uint64 { return 118 }

// Fold writes the action into the trace hash.
func (a LogGetEnd) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
}

// Tag returns the frozen trace-hash number of this action.
func (LogDeleteOld) Tag() uint64 { return 119 }

// Fold writes the action into the trace hash.
func (a LogDeleteOld) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.Int64(int64(a.Through))
}

// Tag returns the frozen trace-hash number of this action.
func (LogClaimEpoch) Tag() uint64 { return 121 }

// Fold writes the action into the trace hash.
func (a LogClaimEpoch) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	h.Uint64(uint64(a.Req))
	h.Uint64(a.Expect)
	h.String(a.Owner)
}
