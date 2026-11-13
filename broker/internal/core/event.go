package core

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/objectstore"
)

// Event is the input to Step. The runtime (prod) or simulator stamps At and
// Wall when the event is dequeued, so At never decreases in Step order.
type Event struct {
	At   machine.NodeTime // node-local monotonic now
	Wall machine.RealTime // UTC now
	Body EventData
}

// EventData is a closed set of plain-data structs.
//
// Every case has Tag and Fold (event_fold.go): they write the case into the trace hash.
type EventData interface {
	isEvent()
	// Tag is the frozen number of the case in the trace hash.
	Tag() uint64
	machine.Message
}

// IOResult is the EventData that answers an IOCall, matched by RequestID. A
// completion with an unknown, already-finished or wrong-kind RequestID is ignored:
// no state change (StateHash is unchanged).
type IOResult interface {
	EventData
	RequestID() machine.RequestID
}

// ---- lifecycle ----

// NodeStarted is the first event of every boot, and always the first Step of every
// incarnation. The node rebuilds what it needs from the control plane and
// object store; nothing in memory survived.
type NodeStarted struct{ Self machine.NodeInstance }

// StopRequest asks the node to shut down gracefully by Deadline (node-local time):
// stop admitting, finish or hand off in-flight work, then emit ShutdownDone. If the
// deadline passes first the runtime kills the node. InboxKind: Control.
type StopRequest struct{ Deadline machine.NodeTime }

// ---- network (inbound) ----

// ConnOpened reports that a connection was accepted.
type ConnOpened struct {
	Conn ConnID
	Peer string // remote address, informational only
}

// BytesReceived is a chunk of a TCP byte stream. Chunks may be arbitrarily
// short: framing is the core's job (decision A).
type BytesReceived struct {
	Conn ConnID
	Data []byte
}

// ConnClosed reports EOF (graceful) or reset (abort) from the peer or network.
type ConnClosed struct {
	Conn  ConnID
	Abort bool
}

// SendDone completes a Send. Err != OK means the bytes may be partially sent.
type SendDone struct {
	Req  machine.RequestID
	Conn ConnID
	Err  ErrCode
}

// ConnectDone completes a Connect. Conn is valid only when Err == OK.
type ConnectDone struct {
	Req  machine.RequestID
	Conn ConnID
	Err  ErrCode
}

// ---- timers ----

// TimerFired is ignored by the receiver if Gen is not the current generation
// for ID. Timers are matched by generation, not cancelled.
type TimerFired struct {
	ID  machine.TimerID
	Gen machine.TimerVersion
}

// ---- object store completions ----

// ObjectDone reports that an object store call finished. Result is one of
// objectstore.PutDone, GetDone, DeleteDone or ListDone. The case is a wrapper
// because a sealed sum cannot get a case from another package. The trace hash gets
// the same bytes as the four separate events had before (tags 9 to 12).
type ObjectDone struct{ Result objectstore.Result }

// ---- control plane completions ----

// ControlPlaneDone reports that a control-plane call finished. Resp is the in-process
// response (nil unless Err == OK). Generated API response messages map through
// controlplane/wireboundary; typed *Response fields wait for the gRPC cutover so
// core does not import grpc (decision 57).
type ControlPlaneDone struct {
	Req     machine.RequestID
	Resp    contract.Response
	Err     ErrCode
	Outcome Outcome
}

// ControlPlaneDeliver delivers one broker ControlPlaneCall to a control-plane replica process. The
// simulator bridge engine issues this; the machine answers with ControlPlaneReply actions.
type ControlPlaneDeliver struct {
	From machine.NodeInstance // broker that issued the ControlPlaneCall
	Req  machine.RequestID
	Op   contract.Operation
}

// ---- broker live set ----

// LiveBrokers gives the broker the live broker set from the control plane. The
// production shell sends it after each heartbeat, and sends an empty list when
// the set is stale (the control plane is down). The broker keeps the last list
// and answers Metadata with it. Metadata always lists the broker itself, so an
// empty list means "self only". The simulator never sends it. InboxKind: Control.
type LiveBrokers struct{ Brokers []BrokerAddr }

// ResponseAs returns d.Resp asserted to T, or the zero value.
func ResponseAs[T any](d ControlPlaneDone) T {
	r, _ := d.Resp.(T)
	return r
}

func (NodeStarted) isEvent()         {}
func (StopRequest) isEvent()         {}
func (ConnectDone) isEvent()         {}
func (ConnOpened) isEvent()          {}
func (BytesReceived) isEvent()       {}
func (ConnClosed) isEvent()          {}
func (SendDone) isEvent()            {}
func (TimerFired) isEvent()          {}
func (ObjectDone) isEvent()          {}
func (ControlPlaneDone) isEvent()    {}
func (ControlPlaneDeliver) isEvent() {}
func (LiveBrokers) isEvent()         {}

// RequestID returns the request this completion answers.
func (e SendDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ConnectDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectDone) RequestID() machine.RequestID { return e.Result.RequestID() }

// RequestID returns the request this completion answers.
func (e ControlPlaneDone) RequestID() machine.RequestID { return e.Req }

var (
	_ IOResult = SendDone{}
	_ IOResult = ConnectDone{}
	_ IOResult = ObjectDone{}
	_ IOResult = ControlPlaneDone{}
)
