package core

// Action is output of Step: a request for the outside world to do something.
// Every I/O action carries a ReqID; its result returns as a completion event.
//
// Actions in one slice start in slice order; I/O among them may complete in
// any order.
type Action interface{ isAction() }

// IOAction is an Action the outside world executes and answers with exactly one
// Completion carrying the same ReqID. A live incarnation always gets that
// completion: a lost response surfaces as ErrTimeout+MaybeApplied, never as
// silence. Completions addressed to a dead incarnation are dropped.
type IOAction interface {
	Action
	ReqID() ReqID
}

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

// ---- lifecycle ----

// Trip asks the runtime to kill this incarnation: the machine's memory can no
// longer be trusted. It is the last action of its Step; the runtime starts the
// actions before it, then stops. Sim: same path as a crash, counted separately
// as a trip. Prod: the loop returns ErrTripped and the process exits for its
// supervisor to restart.
type Trip struct{ Cause string }

// Exit is the last action of a graceful shutdown (see Stop). Like Trip it ends
// the incarnation, but it is not a fault: the sim counts it as a graceful stop.
type Exit struct{}

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
func (Trip) isAction()        {}
func (Exit) isAction()        {}

func (a Send) ReqID() ReqID      { return a.Req }
func (a Dial) ReqID() ReqID      { return a.Req }
func (a ObjPut) ReqID() ReqID    { return a.Req }
func (a ObjGet) ReqID() ReqID    { return a.Req }
func (a ObjDelete) ReqID() ReqID { return a.Req }
func (a ObjList) ReqID() ReqID   { return a.Req }
func (a CPCall) ReqID() ReqID    { return a.Req }

var (
	_ IOAction = Send{}
	_ IOAction = Dial{}
	_ IOAction = ObjPut{}
	_ IOAction = ObjGet{}
	_ IOAction = ObjDelete{}
	_ IOAction = ObjList{}
	_ IOAction = CPCall{}
)
