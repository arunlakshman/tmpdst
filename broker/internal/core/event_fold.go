//nolint:dupl // Fold methods are flat lists of fields. They look alike on purpose.
package core

import (
	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// Tag and Fold of every EventData case. The tag numbers are frozen (see fold.go).
// Fold writes the tag, then the fields in the order that is written here. Both
// were hand-written tables in the simulator (the old tags.go, deleted in T1.12d) before.

// Tag returns the frozen trace-hash number of this event.
func (NodeStarted) Tag() uint64 { return 1 }

// Fold writes the event into the trace hash.
func (e NodeStarted) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	foldInstance(h, e.Self)
}

// Tag returns the frozen trace-hash number of this event.
func (StopRequest) Tag() uint64 { return 2 }

// Fold writes the event into the trace hash.
func (e StopRequest) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Int64(int64(e.Deadline))
}

// Tag returns the frozen trace-hash number of this event.
func (ConnOpened) Tag() uint64 { return 3 }

// Fold writes the event into the trace hash.
func (e ConnOpened) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Conn))
	h.String(e.Peer)
}

// Tag returns the frozen trace-hash number of this event.
func (BytesReceived) Tag() uint64 { return 4 }

// Fold writes the event into the trace hash.
func (e BytesReceived) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Conn))
	h.Bytes(e.Data)
}

// Tag returns the frozen trace-hash number of this event.
func (ConnClosed) Tag() uint64 { return 5 }

// Fold writes the event into the trace hash.
func (e ConnClosed) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Conn))
	h.Bool(e.Abort)
}

// Tag returns the frozen trace-hash number of this event.
func (SendDone) Tag() uint64 { return 6 }

// Fold writes the event into the trace hash.
func (e SendDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.Uint64(uint64(e.Conn))
	h.Uint64(uint64(e.Err))
}

// Tag returns the frozen trace-hash number of this event.
func (ConnectDone) Tag() uint64 { return 7 }

// Fold writes the event into the trace hash.
func (e ConnectDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.Uint64(uint64(e.Conn))
	h.Uint64(uint64(e.Err))
}

// Tag returns the frozen trace-hash number of this event.
func (TimerFired) Tag() uint64 { return 8 }

// Fold writes the event into the trace hash.
func (e TimerFired) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.ID))
	h.Uint64(uint64(e.Gen))
}

// Tag returns the frozen trace-hash number of this event: 9 for a PutDone, 10 for a
// GetDone, 11 for a DeleteDone and 12 for a ListDone.
func (e ObjectDone) Tag() uint64 {
	switch e.Result.(type) {
	case objectstore.PutDone:
		return 9
	case objectstore.GetDone:
		return 10
	case objectstore.DeleteDone:
		return 11
	case objectstore.ListDone:
		return 12
	}
	panic("core.ObjectDone: unknown object store result")
}

// Fold writes the event into the trace hash. Each case writes the bytes that the
// separate event of that case wrote before the wrapper existed.
func (e ObjectDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	switch r := e.Result.(type) {
	case objectstore.PutDone:
		h.Uint64(uint64(r.Req))
		h.String(r.Key)
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(r.Outcome))
	case objectstore.GetDone:
		h.Uint64(uint64(r.Req))
		h.Bytes(r.Data)
		h.Uint64(uint64(r.Err))
	case objectstore.DeleteDone:
		h.Uint64(uint64(r.Req))
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(r.Outcome))
	case objectstore.ListDone:
		h.Uint64(uint64(r.Req))
		h.Uint64(uint64(r.Err))
		h.Uint64(uint64(len(r.Keys)))
		for _, k := range r.Keys {
			h.String(k)
		}
	}
}

// Tag returns the frozen trace-hash number of this event.
func (ControlPlaneDone) Tag() uint64 { return 13 }

// Fold writes the event into the trace hash.
func (e ControlPlaneDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	e.foldBody(h)
}

// foldBody writes the fields of the completion. ControlPlaneReply uses it too.
func (e ControlPlaneDone) foldBody(h *trace.Hash) {
	h.Uint64(uint64(e.Req))
	h.Uint64(uint64(e.Err))
	h.Uint64(uint64(e.Outcome))
	foldResponse(h, e.Resp)
}

// Tag returns the frozen trace-hash number of this event.
func (ControlPlaneDeliver) Tag() uint64 { return 18 }

// Fold writes the event into the trace hash.
func (e ControlPlaneDeliver) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	foldInstance(h, e.From)
	h.Uint64(uint64(e.Req))
	foldOperation(h, e.Op)
}

// Tag returns the frozen trace-hash number of this event.
func (LiveBrokers) Tag() uint64 { return 19 }

// Fold writes the event into the trace hash.
func (e LiveBrokers) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(len(e.Brokers)))
	for _, bk := range e.Brokers {
		h.Int64(int64(bk.NodeID))
		h.String(bk.Host)
		h.Int64(int64(bk.Port))
	}
}
