package core

// Action is an output of Step: a request for the runtime to do something.
// Every filesystem action carries an IOID; its result returns as the matching
// ...Done event. Actions in one returned slice are started in order, but the
// filesystem actions among them may complete in any order.
type Action interface{ isAction() }

// ---- client replies ----

// Reply answers the Request with the same Client id. Body is the object
// payload for a 200 GET and nil otherwise.
type Reply struct {
	Client ClientReq
	Status Status
	Body   []byte
}

// ---- process control ----

// Halt asks the runtime to kill this incarnation and restart it (fail-stop),
// after executing the actions that precede it in the same slice. Use it when
// in-memory state can no longer be trusted, e.g. after an fsync error. In
// simulation it is an ordinary crash; in production the process exits.
type Halt struct{ Reason string }

// ---- observability ----

// Emit records a Fact. The simulator's invariants read facts; production logs
// them. Emit has no effect on behavior.
type Emit struct{ Fact Fact }

func (Reply) isAction() {}
func (Halt) isAction()  {}
func (Emit) isAction()  {}
