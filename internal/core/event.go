package core

// Event is the input to Step. The runtime (prod) or simulator stamps At and
// Wall when the event is dequeued, so At never decreases in Step order.
type Event struct {
	At   NodeTime // node-local monotonic now
	Wall RealTime // UTC now
	Body EventData
}

// EventData is a closed set of plain-data structs.
type EventData interface{ isEvent() }

// IOResult is the EventData that answers an IOCall, matched by RequestID. A
// completion with an unknown, already-finished or wrong-kind RequestID is ignored:
// no state change (StateHash is unchanged).
type IOResult interface {
	EventData
	RequestID() RequestID
}

// ---- lifecycle ----

// NodeStarted is the first event of every boot, and always the first Step of every
// incarnation. The node rebuilds what it needs from the control plane and
// object store; nothing in memory survived.
type NodeStarted struct{ Self NodeInstance }

// StopRequest asks the node to shut down gracefully by Deadline (node-local time):
// stop admitting, finish or hand off in-flight work, then emit ShutdownDone. If the
// deadline passes first the runtime kills the node. InboxKind: Control.
type StopRequest struct{ Deadline NodeTime }

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
	Req  RequestID
	Conn ConnID
	Err  ErrCode
}

// ConnectDone completes a Connect. Conn is valid only when Err == OK.
type ConnectDone struct {
	Req  RequestID
	Conn ConnID
	Err  ErrCode
}

// ---- timers ----

// TimerFired is ignored by the receiver if Gen is not the current generation
// for ID. Timers are matched by generation, not cancelled.
type TimerFired struct {
	ID  TimerID
	Gen TimerVersion
}

// ---- object store completions ----

// ObjectPutDone reports that an object PUT finished.
type ObjectPutDone struct {
	Req     RequestID
	Key     string
	Err     ErrCode
	Outcome Outcome
}

// ObjectGetDone reports that an object GET finished.
type ObjectGetDone struct {
	Req  RequestID
	Data []byte
	Err  ErrCode
}

// ObjectDeleteDone reports that an object DELETE finished.
type ObjectDeleteDone struct {
	Req     RequestID
	Err     ErrCode
	Outcome Outcome
}

// ObjectListDone reports that an object LIST finished.
type ObjectListDone struct {
	Req  RequestID
	Keys []string // sorted
	Err  ErrCode
}

// ---- control plane completions ----

// ControlPlaneDone reports that a control-plane call finished. Resp is the in-process
// response (nil unless Err == OK). Generated API response messages map through
// controlplane/apiconvert; typed *Response fields wait for the gRPC cutover so
// core does not import grpc (decision 57).
type ControlPlaneDone struct {
	Req     RequestID
	Resp    ControlPlaneResponse
	Err     ErrCode
	Outcome Outcome
}

// ControlPlaneDeliver delivers one broker ControlPlaneCall to a control-plane replica process. The
// simulator bridge engine issues this; the machine answers with ControlPlaneReply actions.
type ControlPlaneDeliver struct {
	From NodeInstance // broker that issued the ControlPlaneCall
	Req  RequestID
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
	Req      RequestID
	Slot     LogPosition
	Taken    bool
	Existing []byte
	Err      ErrCode
	Outcome  Outcome
}

// LogReadDone reports that a LogRead finished. Entries are in slot order.
type LogReadDone struct {
	Req     RequestID
	Entries []LogEntry
	Err     ErrCode
	Outcome Outcome
}

// LogGetEndDone reports the log tail (next slot to write).
type LogGetEndDone struct {
	Req     RequestID
	Tail    LogPosition
	Err     ErrCode
	Outcome Outcome
}

// LogDeleteOldDone reports that a LogDeleteOld finished.
type LogDeleteOldDone struct {
	Req     RequestID
	Err     ErrCode
	Outcome Outcome
}

// ---- broker live set ----

// BrokerAddr is one broker's advertised address, as Metadata lists it.
type BrokerAddr struct {
	NodeID int32
	Host   string
	Port   int32
}

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
func (LiveBrokers) isEvent()         {}

// RequestID returns the request this completion answers.
func (e SendDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ConnectDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectPutDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectGetDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectDeleteDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ObjectListDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e ControlPlaneDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogAppendDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogReadDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogGetEndDone) RequestID() RequestID { return e.Req }

// RequestID returns the request this completion answers.
func (e LogDeleteOldDone) RequestID() RequestID { return e.Req }

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
