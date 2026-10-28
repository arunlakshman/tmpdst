package core

// Action is output of Step: a request for the outside world to do something.
// Every I/O action carries a RequestID; its result returns as a completion event.
//
// Actions in one slice start in slice order; I/O among them may complete in
// any order.
type Action interface{ isAction() }

// IOCall is an Action the outside world executes and answers with exactly one
// IOResult carrying the same RequestID. A live incarnation always gets that
// completion: a lost response surfaces as ErrTimeout+MaybeApplied, never as
// silence. IOResults addressed to a dead incarnation are dropped.
type IOCall interface {
	Action
	RequestID() RequestID
}

// ---- timers ----

// SetTimer asks for TimerFired{ID, Gen} after the given duration of node-local time.
type SetTimer struct {
	ID    TimerID
	Gen   TimerVersion
	After Duration
}

// CancelTimer is an optimization only; correctness relies on Gen.
type CancelTimer struct{ ID TimerID }

// ---- network (outbound) ----

// Send writes bytes to a connection, in order with earlier Sends on it.
type Send struct {
	Req  RequestID
	Conn ConnID
	Data []byte
}

// CloseConn closes gracefully (FIN after queued bytes) or aborts (RST).
type CloseConn struct {
	Conn  ConnID
	Abort bool
}

// PauseReads stops reading from a connection (admission control; see
// docs/flow-control.md sec. 2). Bytes already received are still processed. In
// production the reader goroutine parks; in simulation the bytes stay in the
// sender's flow-control window.
type PauseReads struct{ Conn ConnID }

// ResumeReads undoes PauseReads.
type ResumeReads struct{ Conn ConnID }

// Connect opens an outbound connection (used by sim workloads; brokers reach the
// control plane through ControlPlaneCall, not raw dials).
type Connect struct {
	Req  RequestID
	Addr string
}

// ---- object store ----

// ObjectPut uploads Data under Key.
type ObjectPut struct {
	Req  RequestID
	Key  string // unique per upload, so a MaybeApplied PUT is safe to retry
	Data []byte
}

// ObjectGet reads the byte range Ref.
type ObjectGet struct {
	Req RequestID
	Ref SegmentRef
}

// ObjectDelete removes the object at Key.
type ObjectDelete struct {
	Req RequestID
	Key string
}

// ObjectList lists the keys under Prefix.
type ObjectList struct {
	Req    RequestID
	Prefix string
}

// ---- control plane (gRPC wire contract: controlplane/api/v1) ----

// ControlPlaneCall asks the executor to perform one control-plane RPC. The wire contract
// is controlplane/api/v1 (decision 57). Op is the in-process vocabulary brokers
// and sim use today; controlplane/apiconv maps Op to generated request
// messages. Typed *Request fields stay off this struct until the gRPC cutover:
// importing the generated package would pull grpc into core's dep closure and
// fail the determinism audit.
type ControlPlaneCall struct {
	Req RequestID
	Op  ControlPlaneOperation
}

// ---- shared command log (Dynamo loglet; engine lands in a later PR) ----

// ControlPlaneReply asks the sim CP bridge to deliver Done to To (the broker that
// issued the matching ControlPlaneCall). Production uses gRPC waiters instead.
type ControlPlaneReply struct {
	To   NodeInstance
	Done ControlPlaneDone
}

// LogAppend is an if-absent put of Data at Slot.
type LogAppend struct {
	Req  RequestID
	Slot LogPosition
	Data []byte
}

// LogRead reads up to Limit entries at or after From, in slot order.
type LogRead struct {
	Req   RequestID
	From  LogPosition
	Limit int32
}

// LogGetEnd reports the highest contiguous written slot plus one (the tail).
type LogGetEnd struct{ Req RequestID }

// LogDeleteOld tombstones every slot at or below Through. Trimmed slots must not be
// rewritten.
type LogDeleteOld struct {
	Req     RequestID
	Through LogPosition
}

// ---- lifecycle ----

// StopForRestart asks the runtime to kill this incarnation: the machine's memory can no
// longer be trusted. It is the last action of its Step; the runtime starts the
// actions before it, then stops. Sim: same path as a crash, counted separately
// as a trip. Prod: the loop returns ErrStoppedForRestart and the process exits for its
// supervisor to restart.
type StopForRestart struct{ Cause string }

// ShutdownDone is the last action of a graceful shutdown (see StopRequest). Like StopForRestart it ends
// the incarnation, but it is not a fault: the sim counts it as a graceful stop.
type ShutdownDone struct{}

// ---- observability ----

// RecordFact appends a fact to the timeline (sim: invariants read it; prod: tracing).
type RecordFact struct{ Fact Fact }

func (SetTimer) isAction()          {}
func (CancelTimer) isAction()       {}
func (Send) isAction()              {}
func (CloseConn) isAction()         {}
func (PauseReads) isAction()        {}
func (ResumeReads) isAction()       {}
func (Connect) isAction()           {}
func (ObjectPut) isAction()         {}
func (ObjectGet) isAction()         {}
func (ObjectDelete) isAction()      {}
func (ObjectList) isAction()        {}
func (ControlPlaneCall) isAction()  {}
func (ControlPlaneReply) isAction() {}
func (LogAppend) isAction()         {}
func (LogRead) isAction()           {}
func (LogGetEnd) isAction()         {}
func (LogDeleteOld) isAction()      {}
func (RecordFact) isAction()        {}
func (StopForRestart) isAction()    {}
func (ShutdownDone) isAction()      {}

// RequestID returns the request this action carries.
func (a Send) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a Connect) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectPut) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectGet) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectDelete) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectList) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ControlPlaneCall) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a LogAppend) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a LogRead) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a LogGetEnd) RequestID() RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a LogDeleteOld) RequestID() RequestID { return a.Req }

//nolint:dupl // compile-time IOCall assertions; parallel list lives on IOResults
var (
	_ IOCall = Send{}
	_ IOCall = Connect{}
	_ IOCall = ObjectPut{}
	_ IOCall = ObjectGet{}
	_ IOCall = ObjectDelete{}
	_ IOCall = ObjectList{}
	_ IOCall = ControlPlaneCall{}
	_ IOCall = LogAppend{}
	_ IOCall = LogRead{}
	_ IOCall = LogGetEnd{}
	_ IOCall = LogDeleteOld{}
)
