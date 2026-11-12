package core

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
)

// Action is output of Step: a request for the outside world to do something.
// Every I/O action carries a RequestID; its result returns as a completion event.
//
// Actions in one slice start in slice order; I/O among them may complete in
// any order.
//
// Every case has Tag and Fold (action_fold.go): they write the case into the trace hash.
type Action interface {
	isAction()
	// Tag is the frozen number of the case in the trace hash.
	Tag() uint64
	machine.Message
}

// IOCall is an Action the outside world executes and answers with exactly one
// IOResult carrying the same RequestID. A live incarnation always gets that
// completion: a lost response surfaces as ErrTimeout+MaybeApplied, never as
// silence. IOResults addressed to a dead incarnation are dropped.
type IOCall interface {
	Action
	RequestID() machine.RequestID
}

// ---- timers ----

// SetTimer asks for TimerFired{ID, Gen} after the given duration of node-local time.
type SetTimer struct {
	ID    machine.TimerID
	Gen   machine.TimerVersion
	After machine.Duration
}

// CancelTimer is an optimization only; correctness relies on Gen.
type CancelTimer struct{ ID machine.TimerID }

// ---- network (outbound) ----

// Send writes bytes to a connection, in order with earlier Sends on it.
type Send struct {
	Req  machine.RequestID
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

// Connect opens an outbound connection (used by simulator workloads; brokers reach the
// control plane through ControlPlaneCall, not raw dials).
type Connect struct {
	Req  machine.RequestID
	Addr string
}

// ---- object store ----

// ObjectPut uploads Data under Key.
type ObjectPut struct {
	Req  machine.RequestID
	Key  string // unique per upload, so a MaybeApplied PUT is safe to retry
	Data []byte
}

// ObjectGet reads the byte range Ref.
type ObjectGet struct {
	Req machine.RequestID
	Ref SegmentRef
}

// ObjectDelete removes the object at Key.
type ObjectDelete struct {
	Req machine.RequestID
	Key string
}

// ObjectList lists the keys under Prefix.
type ObjectList struct {
	Req    machine.RequestID
	Prefix string
}

// ---- control plane (gRPC wire contract: controlplane/api/v1) ----

// ControlPlaneCall asks the executor to perform one control-plane RPC. The wire contract
// is controlplane/api/v1 (decision 57). Op is the in-process vocabulary brokers
// and simulator use today; controlplane/wireboundary maps Op to generated request
// messages. Typed *Request fields stay off this struct until the gRPC cutover:
// importing the generated package would pull grpc into core's dep closure and
// fail the determinism audit.
type ControlPlaneCall struct {
	Req machine.RequestID
	Op  contract.Operation
	// Timeout is the call's deadline. Zero takes the executor's default. Group
	// joins and syncs wait for a rebalance, so they set a long one.
	Timeout machine.Duration
}

// ControlPlaneReply asks the simulator CP bridge to deliver Done to To (the broker that
// issued the matching ControlPlaneCall). Production uses gRPC waiters instead.
type ControlPlaneReply struct {
	To   machine.NodeInstance
	Done ControlPlaneDone
}

// ---- lifecycle ----

// StopForRestart asks the runtime to kill this incarnation: the machine's memory can no
// longer be trusted. It is the last action of its Step; the runtime starts the
// actions before it, then stops. Sim: same path as a crash, counted separately
// as a trip. Prod: the loop returns ErrStoppedForRestart and the process exits for its
// supervisor to restart.
type StopForRestart struct{ Cause string }

// ShutdownDone is the last action of a graceful shutdown (see StopRequest). Like StopForRestart it ends
// the incarnation, but it is not a fault: the simulator counts it as a graceful stop.
type ShutdownDone struct{}

// ---- observability ----

// RecordFact appends a fact to the timeline (simulator: invariants read it; prod: tracing).
type RecordFact struct{ Fact machine.Fact }

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
func (RecordFact) isAction()        {}
func (StopForRestart) isAction()    {}
func (ShutdownDone) isAction()      {}

// RequestID returns the request this action carries.
func (a Send) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a Connect) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectPut) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectGet) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectDelete) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ObjectList) RequestID() machine.RequestID { return a.Req }

// RequestID returns the request this action carries.
func (a ControlPlaneCall) RequestID() machine.RequestID { return a.Req }

var (
	_ IOCall = Send{}
	_ IOCall = Connect{}
	_ IOCall = ObjectPut{}
	_ IOCall = ObjectGet{}
	_ IOCall = ObjectDelete{}
	_ IOCall = ObjectList{}
	_ IOCall = ControlPlaneCall{}
)
