package runtime

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Trace-hash tags and folds of the replica machine.
//
// Every case of EventData and Action has two methods: Tag, and Fold(h *trace.Hash).
// Fold writes the tag first, then the fields in a fixed order. The tag numbers are
// written by hand, never by %T and never by reflection, so renaming a Go type can not
// move a golden hash. Tags are append-only.
//
// Tag ranges (the same as in the broker core, so one number means one thing in both):
//
//	events                    1 to 99
//	actions                   101 to 199
//	control plane operations  201 to 299 (200: no operation), written in contract
//	control plane responses   301 to 399 (300: no response), written in contract
//
// The tags of the shared command log values (LogAppend and its relatives, events 14 to 17
// and 20, actions 116 to 119 and 121) were retired in the broker. The replica still uses
// these values, so it keeps the same numbers. LocalRead (21) is only in the control plane.
//
// The interfaces include Fold, so a new case without it does not compile.

// foldInstance folds the node name and the boot number.
func foldInstance(h *trace.Hash, r machine.NodeInstance) {
	h.String(string(r.Node))
	h.Bytes(r.Inc[:])
}

// foldSegmentRef folds a byte range of an object.
func foldSegmentRef(h *trace.Hash, s SegmentRef) {
	h.String(s.Key)
	h.Int64(s.Off)
	h.Int64(s.Len)
}

// foldOperation folds an operation. No operation (nil) is tag 200.
func foldOperation(h *trace.Hash, op contract.Operation) {
	if op == nil {
		h.Uint64(200)
		return
	}
	op.Fold(h)
}

// foldResponse folds a response. No response (nil, an error completion) is tag 300.
func foldResponse(h *trace.Hash, r contract.Response) {
	if r == nil {
		h.Uint64(300)
		return
	}
	r.Fold(h)
}
