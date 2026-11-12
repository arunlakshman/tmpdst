package simulator

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/machine"
)

// The trace-hash folds of the kernel. Every core event, action, control plane
// operation and response folds itself: it has a Fold method that writes a frozen tag
// number and its fields (core/fold.go says how). The kernel only calls them.
//
// This file was a table of 600 lines with one switch per kind of value. The table is
// now a test, tags_oracle_test.go. It checks that each Fold method still writes the
// bytes the table wrote, also for the cases that no golden scenario produces.

func (h *stateHasher) ref(r machine.NodeInstance) {
	h.String(string(r.Node))
	h.Bytes(r.Inc[:])
}

func (h *stateHasher) event(b core.EventData) { b.Fold(&h.Hash) }

func (h *stateHasher) action(a core.Action) { a.Fold(&h.Hash) }

func (h *stateHasher) fact(f machine.Fact) { f.Fold(&h.Hash) }
