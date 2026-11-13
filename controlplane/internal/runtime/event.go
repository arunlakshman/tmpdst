package runtime

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
)

// EventData is the input of the replica machine: a closed set of plain-data structs.
// It is a sealed sum. Every case has a Tag and a Fold method (event_fold.go), so a new
// case without them does not compile.
//
//sumtype:decl
type EventData interface {
	machine.Message
	// Tag is the frozen trace-hash number of the case.
	Tag() uint64
	isEvent()
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
// stop admitting, finish or hand off in-flight work, then return sim.Exit. If the
// deadline passes first the runtime kills the node. InboxKind: Control.
type StopRequest struct{ Deadline machine.NodeTime }

// ---- timers ----

// TimerFired is ignored by the receiver if Gen is not the current generation
// for ID. Timers are matched by generation, not cancelled.
type TimerFired struct {
	ID  machine.TimerID
	Gen machine.TimerVersion
}

// ---- object store completions ----

// ObjectPutDone reports that an object PUT finished.
type ObjectPutDone struct {
	Req     machine.RequestID
	Key     string
	Err     ErrCode
	Outcome Outcome
}

// ObjectGetDone reports that an object GET finished.
type ObjectGetDone struct {
	Req  machine.RequestID
	Data []byte
	Err  ErrCode
}

// ObjectDeleteDone reports that an object DELETE finished.
type ObjectDeleteDone struct {
	Req     machine.RequestID
	Err     ErrCode
	Outcome Outcome
}

// ObjectListDone reports that an object LIST finished.
type ObjectListDone struct {
	Req  machine.RequestID
	Keys []string // sorted
	Err  ErrCode
}

// ---- control plane completions ----

// ControlPlaneDone reports that a control-plane call finished. Resp is the in-process
// response (nil unless Err == OK). The gRPC server turns it into the generated
// API response messages (controlplane/server); typed *Response fields wait for the gRPC cutover so
// runtime does not import grpc (decision 57).
type ControlPlaneDone struct {
	Req     machine.RequestID
	Resp    ControlPlaneResponse
	Err     ErrCode
	Outcome Outcome
}

// LocalRead asks a node to answer a read from its own state, now. Unlike a read
// in ControlPlaneDeliver it does not wait for a fresh check of the log tail, so
// the answer can be a little behind the log. The gRPC server uses it to keep
// reads fast. The answer is a ControlPlaneReply to From.
type LocalRead struct {
	From machine.NodeInstance
	Req  machine.RequestID
	Op   ControlPlaneOperation
}

// ControlPlaneDeliver delivers one broker ControlPlaneCall to a control-plane replica process. The
// simulator bridge engine issues this; the machine answers with ControlPlaneReply actions.
type ControlPlaneDeliver struct {
	From machine.NodeInstance // broker that issued the ControlPlaneCall
	Req  machine.RequestID
	Op   ControlPlaneOperation
}

// ---- shared command log completions ----

// LogEntry is one slot's payload as returned by LogRead.
type LogEntry struct {
	Slot LogPosition
	Data []byte
}

// LogAppendDone reports that a LogAppend finished. Taken means the slot was
// already held; Existing is the winner's bytes when Taken is true.
type LogAppendDone struct {
	Req      machine.RequestID
	Slot     LogPosition
	Taken    bool
	Existing []byte
	Err      ErrCode
	Outcome  Outcome
}

// LogReadDone reports that a LogRead finished. Entries are in slot order.
type LogReadDone struct {
	Req     machine.RequestID
	Entries []LogEntry
	Err     ErrCode
	Outcome Outcome
}

// LogGetEndDone reports the log tail (next slot to write).
type LogGetEndDone struct {
	Req     machine.RequestID
	Tail    LogPosition
	Err     ErrCode
	Outcome Outcome
}

// LogDeleteOldDone reports that a LogDeleteOld finished.
type LogDeleteOldDone struct {
	Req     machine.RequestID
	Err     ErrCode
	Outcome Outcome
}

// LogClaimEpochDone reports that a LogClaimEpoch finished. Epoch is the new
// epoch when Won is true, and the epoch stored now when Won is false. Owner is
// the owner stored with that epoch, so a node whose claim timed out can see
// that the claim had landed (same Owner, Epoch == Expect+1).
type LogClaimEpochDone struct {
	Req     machine.RequestID
	Epoch   uint64
	Owner   string
	Won     bool
	Err     ErrCode
	Outcome Outcome
}

// ResponseAs returns d.Resp asserted to T, or the zero value.
func ResponseAs[T any](d ControlPlaneDone) T {
	r, _ := d.Resp.(T)
	return r
}

// Answer returns the answer of the call without the request id: the plain
// contract.Done that the reference model (package spec) also gives.
func (e ControlPlaneDone) Answer() contract.Done {
	return contract.Done{Resp: e.Resp, Err: e.Err, Outcome: e.Outcome}
}

func (NodeStarted) isEvent()         {}
func (StopRequest) isEvent()         {}
func (TimerFired) isEvent()          {}
func (ObjectPutDone) isEvent()       {}
func (ObjectGetDone) isEvent()       {}
func (ObjectDeleteDone) isEvent()    {}
func (ObjectListDone) isEvent()      {}
func (ControlPlaneDone) isEvent()    {}
func (ControlPlaneDeliver) isEvent() {}
func (LocalRead) isEvent()           {}
func (LogAppendDone) isEvent()       {}
func (LogReadDone) isEvent()         {}
func (LogGetEndDone) isEvent()       {}
func (LogDeleteOldDone) isEvent()    {}
func (LogClaimEpochDone) isEvent()   {}

// RequestID returns the request this completion answers.
func (e ObjectPutDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectGetDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectDeleteDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectListDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ControlPlaneDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogAppendDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogReadDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogGetEndDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogDeleteOldDone) RequestID() machine.RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogClaimEpochDone) RequestID() machine.RequestID { return e.Req }

//nolint:dupl // compile-time IOResult assertions; parallel list lives on IOCalls
var (
	_ IOResult = ObjectPutDone{}
	_ IOResult = ObjectGetDone{}
	_ IOResult = ObjectDeleteDone{}
	_ IOResult = ObjectListDone{}
	_ IOResult = ControlPlaneDone{}
	_ IOResult = LogAppendDone{}
	_ IOResult = LogReadDone{}
	_ IOResult = LogGetEndDone{}
	_ IOResult = LogDeleteOldDone{}
	_ IOResult = LogClaimEpochDone{}
)
