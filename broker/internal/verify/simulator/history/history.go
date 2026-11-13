// Package history is the record of the client operations of one run. A workload
// writes it (Invoke when it sends an operation, Return when it knows the
// result). A rule reads it and judges it. The workload never judges.
//
// The record is generic over the operation type and the result type of the
// scenario, so a rule reads a typed value and not an empty interface. A scenario
// uses a sealed sum for each of them.
package history

import (
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// Outcome is what is known about one operation.
type Outcome uint8

const (
	// Pending means there is no result yet.
	Pending Outcome = iota
	// ResultKnown means the result is known. The effect lies between Invoke and Return.
	ResultKnown
	// ResultUnknown means the client saw "maybe applied". The operation may take
	// effect at any time after Invoke, also after the client went on (Jepsen
	// :info). It never closes. It stays a candidate in every later window.
	ResultUnknown
	// GivenUp means the client gave up and will not retry this operation.
	GivenUp
)

// Operation is one client operation. ID names the request. Value names the
// identity of a write and is shared by its retries (two requests of an
// at-least-once producer have one Value).
type Operation[Op, Res any] struct {
	ID             machine.ID
	Value          machine.ID
	Client         machine.NodeInstance
	Op             Op
	InvSeq, RetSeq uint64 // order of events; virtual time can be equal
	Invoke, Return kernel.Time
	Outcome        Outcome
	Result         Res
}

// Reader is the read side of a history. A rule takes a Reader, so a test can
// give it a history that was built by hand.
type Reader[Op, Res any] interface {
	Ops() []Operation[Op, Res]
}

// History is the append-only record of the operations of a run, in the order
// they were invoked. Each event gets a sequence number and the virtual time.
type History[Op, Res any] struct {
	ops []Operation[Op, Res]
	idx map[machine.ID]int
	seq uint64
	now func() kernel.Time
	bad func(msg string)
}

var _ Reader[int, int] = (*History[int, int])(nil)

// New returns an empty history. now gives the virtual time. bad is called with a
// message when the workload breaks the rules of the record (the same id twice).
func New[Op, Res any](now func() kernel.Time, bad func(msg string)) *History[Op, Res] {
	return &History[Op, Res]{idx: map[machine.ID]int{}, now: now, bad: bad}
}

// Invoke records the start of an operation.
func (h *History[Op, Res]) Invoke(client machine.NodeInstance, id, value machine.ID, op Op) {
	if _, dup := h.idx[id]; dup {
		h.bad("history: duplicate invoke of an op id")
		return
	}
	h.seq++
	h.idx[id] = len(h.ops)
	h.ops = append(h.ops, Operation[Op, Res]{ID: id, Value: value, Client: client, Op: op, InvSeq: h.seq, Invoke: h.now(), Outcome: Pending})
}

// Return completes the operation id. It reports false for an unknown or already
// completed id, and that is a protocol failure.
func (h *History[Op, Res]) Return(id machine.ID, result Res, o Outcome) bool {
	i, ok := h.idx[id]
	if !ok || h.ops[i].Outcome != Pending {
		h.bad("history: return of an unknown or already completed op id")
		return false
	}
	h.seq++
	operation := &h.ops[i]
	operation.RetSeq, operation.Return, operation.Result, operation.Outcome = h.seq, h.now(), result, o
	return true
}

// Ops returns every operation so far, in invoke order. Do not change it.
func (h *History[Op, Res]) Ops() []Operation[Op, Res] { return h.ops }
