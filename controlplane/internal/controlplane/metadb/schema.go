package metadb

import (
	"fmt"
	"reflect"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// Mode says how the store treats an operation.
type Mode int

const (
	// NotMine means no schema handles the operation.
	NotMine Mode = iota
	// Logged means a command. It is written to the log first, and then applied
	// at its slot, on every replica, in log order.
	Logged
	// ReadOnly means a read. It changes nothing, so it is not logged. It is
	// answered from the state of one replica.
	ReadOnly
)

// A Schema is the part of the replicated state that one context (topic,
// group) owns: its commands and its reads, one row per operation. The context
// owns its keys and its rules. metadb puts the rows of every schema in one
// table and finds the row of an operation by the operation's type, so adding
// an operation is one row.
type Schema struct {
	Commands []Command
	Reads    []Read
}

// Command is the row of one logged operation: the function that applies it at
// its log slot. The answer depends only on the state before the command and on
// the command, so every replica gets the same answer.
type Command struct {
	op    reflect.Type
	apply func(engine.Txn, int64, runtime.ControlPlaneOperation) runtime.ControlPlaneDone
}

// Read is the row of one read: the function that answers it.
type Read struct {
	op   reflect.Type
	read func(engine.Txn, runtime.ControlPlaneOperation) runtime.ControlPlaneDone
}

// CommandOf is the row of the command T.
func CommandOf[T runtime.ControlPlaneOperation](apply func(transaction engine.Txn, slot int64, operation T) runtime.ControlPlaneDone) Command {
	return Command{op: reflect.TypeFor[T](), apply: func(transaction engine.Txn, slot int64, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
		return apply(transaction, slot, operation.(T))
	}}
}

// ReadOf is the row of the read T.
func ReadOf[T runtime.ControlPlaneOperation](read func(transaction engine.Txn, operation T) runtime.ControlPlaneDone) Read {
	return Read{op: reflect.TypeFor[T](), read: func(transaction engine.Txn, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
		return read(transaction, operation.(T))
	}}
}

// table is the rows of every schema, by operation type.
type table struct {
	commands map[reflect.Type]Command
	reads    map[reflect.Type]Read
}

// newTable puts the rows of schemas in one table. Two rows for one operation
// type are a bug in the wiring, and newTable panics.
func newTable(schemas ...Schema) table {
	t := table{commands: map[reflect.Type]Command{}, reads: map[reflect.Type]Read{}}
	for _, s := range schemas {
		for _, c := range s.Commands {
			t.mustBeNew(c.op)
			t.commands[c.op] = c
		}
		for _, r := range s.Reads {
			t.mustBeNew(r.op)
			t.reads[r.op] = r
		}
	}
	return t
}

func (t table) mustBeNew(op reflect.Type) {
	if t.mode(op) != NotMine {
		panic(fmt.Sprintf("metadata: two schema rows for %v", op))
	}
}

func (t table) mode(op reflect.Type) Mode {
	if _, ok := t.commands[op]; ok {
		return Logged
	}
	if _, ok := t.reads[op]; ok {
		return ReadOnly
	}
	return NotMine
}
