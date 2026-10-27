package core

// Event is the input to Step. The runtime (prod) or simulator stamps At and
// Wall when the event is dequeued, so At never decreases in Step order.
type Event struct {
	At   Time     // node-local monotonic now
	Wall WallTime // UTC now
	Body EventBody
}

// EventBody is a closed set of plain-data structs.
type EventBody interface{ isEvent() }

// Completion is the EventBody that answers an IOAction, matched by ReqID. A
// completion with an unknown, already-finished or wrong-kind ReqID is ignored:
// no state change (StateHash is unchanged).
type Completion interface {
	EventBody
	ReqID() ReqID
}

// ---- lifecycle ----

// Started is the first event of every boot, and always the first Step of every
// incarnation. The node rebuilds what it needs from the control plane and
// object store; nothing in memory survived.
type Started struct{ Self NodeRef }

// Stop asks the node to shut down gracefully by Deadline (node-local time):
// stop admitting, finish or hand off in-flight work, then emit Exit. If the
// deadline passes first the runtime kills the node. Class: Control.
type Stop struct{ Deadline Time }

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

// DialDone completes a Dial. Conn is valid only when Err == OK.
type DialDone struct {
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

// ObjPutDone reports that an object PUT finished.
type ObjPutDone struct {
	Req     ReqID
	Key     string
	Err     ErrCode
	Outcome Outcome
}

// ObjGetDone reports that an object GET finished.
type ObjGetDone struct {
	Req  ReqID
	Data []byte
	Err  ErrCode
}

// ObjDeleteDone reports that an object DELETE finished.
type ObjDeleteDone struct {
	Req     ReqID
	Err     ErrCode
	Outcome Outcome
}

// ObjListDone reports that an object LIST finished.
type ObjListDone struct {
	Req  ReqID
	Keys []string // sorted
	Err  ErrCode
}

// ---- control plane completions ----

// CPDone reports that a control-plane call finished. Resp is the in-process
// response (nil unless Err == OK). Generated API response messages map through
// controlplane/apiconv; typed *Response fields wait for the gRPC cutover so
// core does not import grpc (decision 57).
type CPDone struct {
	Req     ReqID
	Resp    CPResp
	Err     ErrCode
	Outcome Outcome
}

// CPOffer delivers one broker CPCall to a control-plane replica process. The
// sim bridge engine issues this; the machine answers with CPReply actions.
type CPOffer struct {
	From NodeRef // broker that issued the CPCall
	Req  ReqID
	Op   CPOp
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

// LogCheckTailDone reports the log tail (next slot to write).
type LogCheckTailDone struct {
	Req     ReqID
	Tail    LogSlot
	Err     ErrCode
	Outcome Outcome
}

// LogTrimDone reports that a LogTrim finished.
type LogTrimDone struct {
	Req     ReqID
	Err     ErrCode
	Outcome Outcome
}

// RespAs returns d.Resp asserted to T, or the zero value.
func RespAs[T any](d CPDone) T {
	r, _ := d.Resp.(T)
	return r
}

func (Started) isEvent()          {}
func (Stop) isEvent()             {}
func (DialDone) isEvent()         {}
func (ConnOpened) isEvent()       {}
func (BytesReceived) isEvent()    {}
func (ConnClosed) isEvent()       {}
func (SendDone) isEvent()         {}
func (TimerFired) isEvent()       {}
func (ObjPutDone) isEvent()       {}
func (ObjGetDone) isEvent()       {}
func (ObjDeleteDone) isEvent()    {}
func (ObjListDone) isEvent()      {}
func (CPDone) isEvent()           {}
func (CPOffer) isEvent()          {}
func (LogAppendDone) isEvent()    {}
func (LogReadDone) isEvent()      {}
func (LogCheckTailDone) isEvent() {}
func (LogTrimDone) isEvent()      {}

// ReqID returns the request this completion answers.
func (e SendDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e DialDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjPutDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjGetDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjDeleteDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e ObjListDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e CPDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogAppendDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogReadDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogCheckTailDone) ReqID() ReqID { return e.Req }

// ReqID returns the request this completion answers.
func (e LogTrimDone) ReqID() ReqID { return e.Req }

//nolint:dupl // compile-time Completion assertions; parallel list lives on IOActions
var (
	_ Completion = SendDone{}
	_ Completion = DialDone{}
	_ Completion = ObjPutDone{}
	_ Completion = ObjGetDone{}
	_ Completion = ObjDeleteDone{}
	_ Completion = ObjListDone{}
	_ Completion = CPDone{}
	_ Completion = LogAppendDone{}
	_ Completion = LogReadDone{}
	_ Completion = LogCheckTailDone{}
	_ Completion = LogTrimDone{}
)
