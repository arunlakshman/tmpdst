// Package module is the one interface that every control-plane context (topics,
// groups) implements to take part in the replicated state. The metadata package
// holds a list of modules and sends every operation to the one that owns it.
// Adding a context means writing a Module, not editing a switch in four places.
package module

import (
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/core"
)

// Mode says how a module treats an operation.
type Mode int

const (
	// NotMine means the module does not handle the operation.
	NotMine Mode = iota
	// Logged means a command. It is written to the log first, and then applied with
	// Write at its slot, on every replica, in log order.
	Logged
	// ReadOnly means a read. It changes nothing, so it is not logged. It is answered
	// with Read from the state of one replica.
	ReadOnly
)

// Kind is the number of a log entry kind. It is the first byte of every entry in
// the log, so a number is part of the log format and must never change or be
// reused. Zero is the no-op, which the kernel owns. Each module owns its own
// kinds.
type Kind uint8

// Module is one context's part of the replicated state, kept in the shared
// ordered key-value store. A module owns its own key prefixes, its own log entry
// kinds, and the bytes of its own entries.
type Module interface {
	// Mode reports how the module treats operation.
	Mode(operation core.ControlPlaneOperation) Mode
	// Write applies a Logged operation at log slot slot. The answer depends only
	// on the state before the command and on the command, so every replica
	// gets the same answer.
	Write(transaction kv.Transaction, slot int64, operation core.ControlPlaneOperation) core.ControlPlaneDone
	// Read answers a ReadOnly operation.
	Read(transaction kv.Transaction, operation core.ControlPlaneOperation) core.ControlPlaneDone

	// Kinds lists the log entry kinds the module owns.
	Kinds() []Kind
	// KindOf is the kind of a Logged operation. It is false for any other.
	KindOf(operation core.ControlPlaneOperation) (Kind, bool)
	// Encode writes the payload of a Logged operation, the bytes after the kind
	// and the id. The kernel writes those two.
	Encode(w *kv.Writer, kind Kind, operation core.ControlPlaneOperation)
	// Decode reads the payload of an entry of kind. A damaged payload fails the
	// reader (r.OK() is false) and the kernel reports a corrupt entry.
	Decode(r *kv.Reader, kind Kind) core.ControlPlaneOperation
}
