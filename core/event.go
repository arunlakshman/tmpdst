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

// ---- lifecycle ----

// Started is the first event of every boot. The node rebuilds what it needs
// from the control plane and object store; nothing in memory survived.
type Started struct{ Self NodeRef }

// ---- network (inbound) ----

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

// ---- timers ----

// TimerFired is ignored by the receiver if Gen is not the current generation
// for ID. Timers are matched by generation, not cancelled.
type TimerFired struct {
	ID  TimerID
	Gen TimerGen
}

// ---- object store completions ----

type ObjPutDone struct {
	Req     ReqID
	Key     string
	Err     ErrCode
	Outcome Outcome
}

type ObjGetDone struct {
	Req  ReqID
	Data []byte
	Err  ErrCode
}

type ObjDeleteDone struct {
	Req     ReqID
	Err     ErrCode
	Outcome Outcome
}

type ObjListDone struct {
	Req  ReqID
	Keys []string // sorted
	Err  ErrCode
}

// ---- control plane completions ----

type CPDone struct {
	Req     ReqID
	Resp    CPResp // nil unless Err == OK
	Err     ErrCode
	Outcome Outcome
}

func (Started) isEvent()       {}
func (ConnOpened) isEvent()    {}
func (BytesReceived) isEvent() {}
func (ConnClosed) isEvent()    {}
func (SendDone) isEvent()      {}
func (TimerFired) isEvent()    {}
func (ObjPutDone) isEvent()    {}
func (ObjGetDone) isEvent()    {}
func (ObjDeleteDone) isEvent() {}
func (ObjListDone) isEvent()   {}
func (CPDone) isEvent()        {}
