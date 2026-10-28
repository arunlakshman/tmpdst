package core

// Event is the input to Step. The runtime (prod) or simulator stamps At and
// Wall when the event is dequeued, so At never decreases in Step order.
type Event struct {
	At   Time     // node-local monotonic now
	Wall WallTime // UTC now
	Body EventData
}

// EventData is a closed set of plain-data structs.
type EventData interface{ isEvent() }

// IOResult is the EventData that answers an IOCall, matched by ReqID. A
// completion with an unknown, already-finished or wrong-kind ReqID is ignored:
// no state change (StateHash is unchanged).
type IOResult interface {
	EventData
	ReqID() ReqID
}

// ---- lifecycle ----

// NodeStarted is the first event of every boot, and always the first Step of every
// incarnation. The node rebuilds what it needs from the control plane and
// object store; nothing in memory survived.
type NodeStarted struct{ Self NodeInstance }

// StopRequest asks the node to shut down gracefully by Deadline (node-local time):
// stop admitting, finish or hand off in-flight work, then emit ShutdownDone. If the
// deadline passes first the runtime kills the node. InboxKind: Control.
type StopRequest struct{ Deadline Time }

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
	Req  ReqID
	Conn ConnID
	Err  ErrCode
}

// ConnectDone completes a Connect. Conn is valid only when Err == OK.
type ConnectDone struct {
	Req  ReqID
	Conn ConnID
	Err  ErrCode
}

// ---- timers ----

// TimerFired is ignored by the receiver if Gen is not the current generation
// for ID. Timers are matched by generation, not cancelled.
type TimerFired struct {
	ID  TimerID
	Gen TimerGen
}

// ---- object store completions ----

// ObjectPutDone reports that an object PUT finished.
type ObjectPutDone struct {
	Req     ReqID
	Key     string
	Err     ErrCode
	Outcome Outcome
}

// ObjectGetDone reports that an object GET finished.
type ObjectGetDone struct {
	Req  ReqID
	Data []byte
	Err  ErrCode
}

// ObjectDeleteDone reports that an object DELETE finished.
type ObjectDeleteDone struct {
	Req     ReqID
	Err     ErrCode
	Outcome Outcome
}

// ObjectListDone reports that an object LIST finished.
type ObjectListDone struct {
	Req  ReqID
	Keys []string // sorted
	Err  ErrCode
}

// ---- control plane completions ----

// ControlPlaneDone reports that a control-plane call finished. Resp is the in-process
// response (nil unless Err == OK). Generated API response messages map through
// controlplane/apiconv; typed *Response fields wait for the gRPC cutover so
// core does not import grpc (decision 57).
type ControlPlaneDone struct {
	Req     ReqID
	Resp    ControlPlaneResponse
	Err     ErrCode
	Outcome Outcome
}

// ControlPlaneDeliver delivers one broker ControlPlaneCall to a control-plane replica process. The
// sim bridge engine issues this; the machine answers with ControlPlaneReply actions.
type ControlPlaneDeliver struct {
	From NodeInstance // broker that issued the ControlPlaneCall
	Req  ReqID
	Op   ControlPlaneOperation
}

// ---- shared command log completions ----

// LogEntry is one slot's payload as returned by LogRead.
type LogEntry struct {
	Slot LogSlot
	Data []byte
}

// LogAppendDone reports that a LogAppend finished. Taken means the slot was
// already held; Existing is the winner's bytes when Taken is true.
type LogAppendDone struct {
	Req      ReqID
	Slot     LogSlot
	Taken    bool
	Existing []byte
	Err      ErrCode
	Outcome  Outcome
}

// LogReadDone reports that a LogRead finished. Entries are in slot order.
type LogReadDone struct {
	Req     ReqID
	Entries []LogEntry
	Err     ErrCode
	Outcome Outcome
}

// LogGetEndDone reports the log tail (next slot to write).
type LogGetEndDone struct {
	Req     ReqID
	Tail    LogSlot
	Err     ErrCode
	Outcome Outcome
}

// LogDeleteOldDone reports that a LogDeleteOld finished.
type LogDeleteOldDone struct {
	Req     ReqID
	Err     ErrCode
	Outcome Outcome
}

// RespAs returns d.Resp asserted to T, or the zero value.
func RespAs[T any](d ControlPlaneDone) T {
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
func (ObjectPutDone) isEvent()       {}
func (ObjectGetDone) isEvent()       {}
func (ObjectDeleteDone) isEvent()    {}
func (ObjectListDone) isEvent()      {}
func (ControlPlaneDone) isEvent()    {}
func (ControlPlaneDeliver) isEvent() {}
func (LogAppendDone) isEvent()       {}
func (LogReadDone) isEvent()         {}
func (LogGetEndDone) isEvent()       {}
func (LogDeleteOldDone) isEvent()    {}

// ReqID returns the request this completion answers.
func (e SendDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ConnectDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjectPutDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjectGetDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjectDeleteDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjectListDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ControlPlaneDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogAppendDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogReadDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogGetEndDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogDeleteOldDone) ReqID() ReqID { return e.Req }

//nolint:dupl // compile-time IOResult assertions; parallel list lives on IOCalls
var (
	_ IOResult = SendDone{}
	_ IOResult = ConnectDone{}
	_ IOResult = ObjectPutDone{}
	_ IOResult = ObjectGetDone{}
	_ IOResult = ObjectDeleteDone{}
	_ IOResult = ObjectListDone{}
	_ IOResult = ControlPlaneDone{}
	_ IOResult = LogAppendDone{}
	_ IOResult = LogReadDone{}
	_ IOResult = LogGetEndDone{}
	_ IOResult = LogDeleteOldDone{}
)
