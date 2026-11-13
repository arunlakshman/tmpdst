package core

import (
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

// Trace-hash tags and folds.
//
// Every case of EventData, Action, contract.Operation and contract.Response
// has two methods: Tag, and Fold(h *trace.Hash). Fold writes the tag first, then the
// fields in a fixed order. The tag numbers and the field order are frozen: they are
// written by hand, never by %T and never by reflection, so renaming a Go type cannot
// move a golden hash. Changing a tag or a field order moves every golden hash.
// Tags are append-only.
//
// Tag ranges:
//
//	events                    1 to 99
//	actions                   101 to 199
//	control plane operations  201 to 299 (200: no operation)
//	control plane responses   301 to 399 (300: no response)
//	field values              401 to 499 (in dst/machine)
//	facts                     501        (in dst/machine)
//
// Retired tags: 14 to 17 and 20 (events) and 116 to 119 and 121 (actions) were the
// shared command log values (LogAppend and its relatives). They were deleted. The
// numbers are not used again.
//
// The control plane operations (202 to 218) and responses (302 to 312) write themselves:
// their Tag and Fold are in controlplane/contract (fold.go). This package writes only the
// tags 200 and 300, for a missing value.
//
// The interfaces include Fold, so a new case without it does not compile. Before,
// the simulator had a table of 600 lines (the old tags.go, deleted in T1.12d) that gave the
// same numbers, and it panicked for a case without a row.

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
