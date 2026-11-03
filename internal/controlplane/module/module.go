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

// Module is one context's part of the replicated state, kept in the shared
// ordered key-value store. A module owns its own key prefixes.
type Module interface {
	// Mode reports how the module treats operation.
	Mode(operation core.ControlPlaneOperation) Mode
	// Write applies a Logged operation at log slot slot. The answer depends only
	// on the state before the command and on the command, so every replica
	// gets the same answer.
	Write(transaction kv.Transaction, slot int64, operation core.ControlPlaneOperation) core.ControlPlaneDone
	// Read answers a ReadOnly operation.
	Read(transaction kv.Transaction, operation core.ControlPlaneOperation) core.ControlPlaneDone
}
