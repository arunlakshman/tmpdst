//nolint:dupl // Fold methods are flat lists of fields. They look alike on purpose.
package core

import "github.com/manifesterio/mds/dst/trace"

// Tag and Fold of every EventData case. The tag numbers are frozen (see fold.go).
// Fold writes the tag, then the fields in the order that is written here. Both
// were hand-written tables in the simulator (verify/simulator/tags.go) before.

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

// Tag returns the frozen trace-hash number of this event.
func (ObjectPutDone) Tag() uint64 { return 9 }

// Fold writes the event into the trace hash.
func (e ObjectPutDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.String(e.Key)
	h.Uint64(uint64(e.Err))
	h.Uint64(uint64(e.Outcome))
}

// Tag returns the frozen trace-hash number of this event.
func (ObjectGetDone) Tag() uint64 { return 10 }

// Fold writes the event into the trace hash.
func (e ObjectGetDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.Bytes(e.Data)
	h.Uint64(uint64(e.Err))
}

// Tag returns the frozen trace-hash number of this event.
func (ObjectDeleteDone) Tag() uint64 { return 11 }

// Fold writes the event into the trace hash.
func (e ObjectDeleteDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.Uint64(uint64(e.Err))
	h.Uint64(uint64(e.Outcome))
}

// Tag returns the frozen trace-hash number of this event.
func (ObjectListDone) Tag() uint64 { return 12 }

// Fold writes the event into the trace hash.
func (e ObjectListDone) Fold(h *trace.Hash) {
	h.Uint64(e.Tag())
	h.Uint64(uint64(e.Req))
	h.Uint64(uint64(e.Err))
	h.Uint64(uint64(len(e.Keys)))
	for _, k := range e.Keys {
		h.String(k)
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
