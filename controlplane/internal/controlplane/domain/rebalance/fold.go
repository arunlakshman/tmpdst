package rebalance

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/trace"
)

// Trace-hash tags and folds of the machine.
//
// Every case of Input and Output has two methods: Tag, and Fold(h *trace.Hash). Fold writes
// the tag first, then the fields in a fixed order. The numbers are written by hand, never
// by %T and never by reflection, so renaming a Go type can not move a golden hash. Tags are
// append-only. The control plane operations and responses inside a value fold themselves
// (their tags are 200 to 399, in contract).
//
// Tag ranges:
//
//	inputs   1001 to 1099
//	outputs  1101 to 1199
//	answers  1200 (no answer) to 1299
//
// The interfaces include Fold, so a new case without it does not compile.

func (Join) isInput()          {}
func (Sync) isInput()          {}
func (Heartbeat) isInput()     {}
func (Leave) isInput()         {}
func (OffsetCommit) isInput()  {}
func (Describe) isInput()      {}
func (List) isInput()          {}
func (CallAbandoned) isInput() {}
func (RoleChanged) isInput()   {}
func (ReplicaReply) isInput()  {}
func (TimerFired) isInput()    {}
func (drainLater) isInput()    {}

func (SubmitCommand) isOutput() {}
func (SubmitRead) isOutput()    {}
func (Reply) isOutput()         {}
func (SetTimer) isOutput()      {}
func (CancelTimer) isOutput()   {}
func (SendSelf) isOutput()      {}

// Tag returns the trace-hash number of this input.
func (Join) Tag() uint64 { return 1001 }

// Fold writes the input into the trace hash.
func (in Join) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	h.String(in.Group)
	h.String(in.MemberID)
	h.String(in.GroupInstanceID)
	h.String(in.ClientID)
	h.String(in.ClientHost)
	h.Int64(int64(in.SessionTimeoutMs))
	h.Int64(int64(in.RebalanceTimeoutMs))
	h.String(in.ProtocolType)
	h.Uint64(uint64(len(in.Protocols)))
	for _, p := range in.Protocols {
		h.String(p.Name)
		h.Bytes(p.Metadata)
	}
}

// Tag returns the trace-hash number of this input.
func (Sync) Tag() uint64 { return 1002 }

// Fold writes the input into the trace hash.
func (in Sync) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	h.String(in.Group)
	h.String(in.MemberID)
	h.Int64(int64(in.Generation))
	h.Uint64(uint64(len(in.Assignments)))
	for _, a := range in.Assignments {
		h.String(a.Member)
		h.Bytes(a.Bytes)
	}
}

// Tag returns the trace-hash number of this input.
func (Heartbeat) Tag() uint64 { return 1003 }

// Fold writes the input into the trace hash.
func (in Heartbeat) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	h.String(in.Group)
	h.String(in.MemberID)
	h.Int64(int64(in.Generation))
}

// Tag returns the trace-hash number of this input.
func (Leave) Tag() uint64 { return 1004 }

// Fold writes the input into the trace hash.
func (in Leave) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	h.String(in.Group)
	foldStrings(h, in.Members)
}

// Tag returns the trace-hash number of this input.
func (OffsetCommit) Tag() uint64 { return 1005 }

// Fold writes the input into the trace hash.
func (in OffsetCommit) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	in.Op.Fold(h)
}

// Tag returns the trace-hash number of this input.
func (Describe) Tag() uint64 { return 1006 }

// Fold writes the input into the trace hash.
func (in Describe) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
	foldStrings(h, in.Groups)
}

// Tag returns the trace-hash number of this input.
func (List) Tag() uint64 { return 1007 }

// Fold writes the input into the trace hash.
func (in List) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
}

// Tag returns the trace-hash number of this input.
func (CallAbandoned) Tag() uint64 { return 1008 }

// Fold writes the input into the trace hash.
func (in CallAbandoned) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Call))
}

// Tag returns the trace-hash number of this input.
func (RoleChanged) Tag() uint64 { return 1009 }

// Fold writes the input into the trace hash.
func (in RoleChanged) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Bool(in.Active)
}

// Tag returns the trace-hash number of this input.
func (ReplicaReply) Tag() uint64 { return 1010 }

// Fold writes the input into the trace hash. The completion is folded without its own tag.
func (in ReplicaReply) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.Req))
	in.Done.FoldBody(h)
}

// Tag returns the trace-hash number of this input.
func (TimerFired) Tag() uint64 { return 1011 }

// Fold writes the input into the trace hash.
func (in TimerFired) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.Uint64(uint64(in.ID))
	h.Uint64(uint64(in.Gen))
}

// Tag returns the trace-hash number of this input.
func (drainLater) Tag() uint64 { return 1012 }

// Fold writes the input into the trace hash.
func (in drainLater) Fold(h *trace.Hash) {
	h.Uint64(in.Tag())
	h.String(in.group)
}

// Tag returns the trace-hash number of this output.
func (SubmitCommand) Tag() uint64 { return 1101 }

// Fold writes the output into the trace hash.
func (o SubmitCommand) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(o.Req))
	o.Op.Fold(h)
}

// Tag returns the trace-hash number of this output.
func (SubmitRead) Tag() uint64 { return 1102 }

// Fold writes the output into the trace hash.
func (o SubmitRead) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(o.Req))
	o.Op.Fold(h)
	h.Bool(o.Fresh)
}

// Tag returns the trace-hash number of this output.
func (Reply) Tag() uint64 { return 1103 }

// Fold writes the output into the trace hash.
func (o Reply) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(o.Call))
	h.Uint64(uint64(o.Err))
	foldAnswer(h, o.Body)
}

// Tag returns the trace-hash number of this output.
func (SetTimer) Tag() uint64 { return 1104 }

// Fold writes the output into the trace hash.
func (o SetTimer) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(o.ID))
	h.Uint64(uint64(o.Gen))
	h.Int64(int64(o.After))
}

// Tag returns the trace-hash number of this output.
func (CancelTimer) Tag() uint64 { return 1105 }

// Fold writes the output into the trace hash.
func (o CancelTimer) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	h.Uint64(uint64(o.ID))
}

// Tag returns the trace-hash number of this output.
func (SendSelf) Tag() uint64 { return 1106 }

// Fold writes the output into the trace hash.
func (o SendSelf) Fold(h *trace.Hash) {
	h.Uint64(o.Tag())
	o.Msg.Fold(h)
}

// foldStrings folds a list of strings, with its length first.
func foldStrings(h *trace.Hash, ss []string) {
	h.Uint64(uint64(len(ss)))
	for _, s := range ss {
		h.String(s)
	}
}

// foldAnswer folds the body of a Reply. No body (nil) is tag 1200.
func foldAnswer(h *trace.Hash, a Answer) {
	switch a := a.(type) {
	case nil:
		h.Uint64(1200)
	case JoinAnswer:
		h.Uint64(1201)
		h.String(a.MemberID)
		h.Int64(int64(a.Generation))
		h.String(a.Protocol)
		h.String(a.Leader)
		h.Uint64(uint64(len(a.Members)))
		for _, mem := range a.Members {
			h.String(mem.ID)
			h.String(mem.InstanceID)
			h.Bytes(mem.Metadata)
		}
	case SyncAnswer:
		h.Uint64(1202)
		h.Bytes(a.Assignment)
	case LeaveAnswer:
		h.Uint64(1203)
		h.Uint64(uint64(len(a.Errs)))
		for _, e := range a.Errs {
			h.Uint64(uint64(e))
		}
	case CommitAnswer:
		h.Uint64(1204)
		a.Fold(h) // the Fold of the embedded contract.OffsetCommitResp
	case DescribeAnswer:
		h.Uint64(1205)
		h.Uint64(uint64(len(a.Groups)))
		for _, g := range a.Groups {
			h.String(g.Name)
			g.Fold(h) // the Fold of the embedded contract.DescribeGroupResp
		}
	case ListAnswer:
		h.Uint64(1206)
		contract.ListGroupsResp{Groups: a.Groups}.Fold(h)
	}
}
