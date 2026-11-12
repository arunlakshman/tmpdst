package kernel

import "github.com/manifesterio/mds/dst/machine"

// This file is the only place in dst (and in every product) where a typed
// value is kept in a way that hides its type ("type erasure"). The World keeps
// nodes, services and queue entries of many different types in one list, so it
// needs a view of each one that does not name the type parameters.
//
// No value of type any exists in this package. erase_test.go checks that, and
// it checks that no other file declares an unexported interface type or a
// field of type func(): those are the two ways to hide a generic node, service
// or entry.

// nodeSlot is one node, whatever its input and output types. Each one is a
// *nodeHost[In, Out].
type nodeSlot interface {
	// base is the part of the node that does not depend on its types.
	base() *nodeBase
	// boot starts a new incarnation, if the node is down and not broken.
	boot()
	// pump gives the node one Step if it is free and has an input waiting.
	pump()
	// forget drops the machine, the inbox and the timers of the incarnation
	// that just ended.
	forget()
	// stateHash is the StateHash of the machine.
	stateHash() uint64
	// inboxLens is the number of inputs waiting in each inbox.
	inboxLens() (control, data int)
	// stopRequest puts the graceful stop request in the Control inbox. It
	// reports false, and does nothing, when the node has no StopMsg.
	stopRequest(deadline machine.NodeTime) bool
	// phaseStep gives a driver its OnPhase input as one Step.
	phaseStep(p Phase)
}

// serviceSlot is one service, whatever its request and input types. Each one
// is a *serviceHost[Req, In].
type serviceSlot interface {
	base() *serviceBase
	stateHash() uint64
	onCrash(who machine.NodeInstance)
	enterRecovery()
}

// entry is one item of the event queue. The closure run was made by a generic
// function that knew the types. body is kept only to fold it into the trace
// hash and to print it in a log line; it may be nil.
type entry struct {
	at   Time
	tie  uint64 // drawn when the entry is added
	seq  uint64 // the order of adding
	lane Lane
	kind entryKind

	// The target, for the trace hash and for dropping the entries of an
	// incarnation that ended. node is -1 when the target is not a node. inc
	// is zero when the entry goes to whatever incarnation is up.
	node    int
	inc     machine.BootNumber
	service string

	body machine.Message
	run  func()
	dead bool // cancelled or dropped; thrown away when it reaches the top
}
