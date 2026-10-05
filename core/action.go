package core

// Action is output of Step: a request for the outside world to do something.
// Every I/O action carries a ReqID; its result returns as a completion event.
type Action interface{ isAction() }

// ---- timers ----

// SetTimer asks for TimerFired{ID, Gen} after the given duration of node-local time.
type SetTimer struct {
	ID    TimerID
	Gen   TimerGen
	After Duration
}

// CancelTimer is an optimization only; correctness relies on Gen.
type CancelTimer struct{ ID TimerID }

// ---- network (outbound) ----

// Send writes bytes to a connection, in order with earlier Sends on it.
type Send struct {
	Req  ReqID
	Conn ConnID
	Data []byte
}

// CloseConn closes gracefully (FIN after queued bytes) or aborts (RST).
type CloseConn struct {
	Conn  ConnID
	Abort bool
}

// PauseReads stops reading from a connection (admission control; see
// docs/flow-control.md §2). Bytes already received are still processed. In
// production the reader goroutine parks; in simulation the bytes stay in the
// sender's flow-control window.
type PauseReads struct{ Conn ConnID }

// ResumeReads undoes PauseReads.
type ResumeReads struct{ Conn ConnID }

// Dial opens an outbound connection (used by sim workloads; brokers reach the
// control plane through CPCall, not raw dials).
type Dial struct {
	Req  ReqID
	Addr string
}

// ---- object store ----

type ObjPut struct {
	Req  ReqID
	Key  string // unique per upload, so a MaybeApplied PUT is safe to retry
	Data []byte
}

type ObjGet struct {
	Req ReqID
	Ref SegmentRef
}

type ObjDelete struct {
	Req ReqID
	Key string
}

type ObjList struct {
	Req    ReqID
	Prefix string
}

// ---- control plane (HTTP) ----

type CPCall struct {
	Req ReqID
	Op  CPOp
}

// ---- observability ----

// Emit appends a fact to the timeline (sim: invariants read it; prod: tracing).
type Emit struct{ Fact Fact }

func (SetTimer) isAction()    {}
func (CancelTimer) isAction() {}
func (Send) isAction()        {}
func (CloseConn) isAction()   {}
func (PauseReads) isAction()  {}
func (ResumeReads) isAction() {}
func (Dial) isAction()        {}
func (ObjPut) isAction()      {}
func (ObjGet) isAction()      {}
func (ObjDelete) isAction()   {}
func (ObjList) isAction()     {}
func (CPCall) isAction()      {}
func (Emit) isAction()        {}
