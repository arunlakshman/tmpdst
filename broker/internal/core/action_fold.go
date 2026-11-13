//nolint:dupl // Fold methods are flat lists of fields. They look alike on purpose.
package core

import (
	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

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

// Tag returns the frozen trace-hash number of this action: 108 for a Put, 109 for a Get,
// 110 for a Delete and 111 for a List.
func (a ObjectCall) Tag() uint64 {
	switch a.Call.(type) {
	case objectstore.Put:
		return 108
	case objectstore.Get:
		return 109
	case objectstore.Delete:
		return 110
	case objectstore.List:
		return 111
	}
	panic("core.ObjectCall: unknown object store call")
}

// Fold writes the action into the trace hash. Each case writes the bytes that the
// separate action of that case wrote before the wrapper existed.
func (a ObjectCall) Fold(h *trace.Hash) {
	h.Uint64(a.Tag())
	switch c := a.Call.(type) {
	case objectstore.Put:
		h.Uint64(uint64(c.Req))
		h.String(c.Key)
		h.Bytes(c.Data)
	case objectstore.Get:
		h.Uint64(uint64(c.Req))
		foldSegmentRef(h, c.Ref)
	case objectstore.Delete:
		h.Uint64(uint64(c.Req))
		h.String(c.Key)
	case objectstore.List:
		h.Uint64(uint64(c.Req))
		h.String(c.Prefix)
	}
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
