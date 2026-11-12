package runtime

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

// ---- control plane (gRPC wire contract: controlplane/api/controlplane/v1) ----

// ControlPlaneCall asks the executor to perform one control-plane RPC. The wire contract
// is controlplane/api/controlplane/v1 (decision 57). Op is the in-process vocabulary brokers
// and simulator use today; a client turns Op into generated request messages. Typed *Request fields stay off this struct until the gRPC cutover:
// importing the generated package would pull grpc into runtime's dep closure and
// fail the determinism audit.
type ControlPlaneCall struct {
	Req RequestID
	Op  ControlPlaneOperation
	// Timeout is the call's deadline. Zero takes the executor's default. Group
	// joins and syncs wait for a rebalance, so they set a long one.
	Timeout Duration
}

// ---- shared command log (Dynamo loglet; engine lands in a later PR) ----

// ControlPlaneReply asks the simulator CP bridge to deliver Done to To (the broker that
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

// LogClaimEpoch asks the log to make a node the one writer. The log keeps one
// writer epoch. The claim succeeds only if the stored epoch still equals
// Expect (zero when none was stored yet). Then the stored epoch becomes
// Expect+1 and Owner is recorded. A claim that loses reports the epoch that is
// stored now. Nothing is written to a log slot.
type LogClaimEpoch struct {
	Req    RequestID
	Expect uint64
	Owner  string
}

// LogDeleteOld removes every slot at or below Through. The log first raises its
// floor to Through, and only then deletes the slots. A LogAppend that puts into
// a slot at or below the floor is not a commit: the log answers it as a
// conflict with no existing entry.
type LogDeleteOld struct {
	Req     RequestID
	Through LogPosition
}

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
func (LogClaimEpoch) isAction()     {}

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

// RequestID returns the request this action carries.
func (a LogClaimEpoch) RequestID() RequestID { return a.Req }

//nolint:dupl // compile-time IOCall assertions; parallel list lives on IOResults
var (
	_ IOCall = ObjectPut{}
	_ IOCall = ObjectGet{}
	_ IOCall = ObjectDelete{}
	_ IOCall = ObjectList{}
	_ IOCall = ControlPlaneCall{}
	_ IOCall = LogAppend{}
	_ IOCall = LogRead{}
	_ IOCall = LogGetEnd{}
	_ IOCall = LogDeleteOld{}
	_ IOCall = LogClaimEpoch{}
)
