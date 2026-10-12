package control

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

// Trip is a safety trip: it asks the runtime to kill this incarnation and
// restart it (fail-stop), after executing the actions that precede it in the
// same slice. Use it when in-memory state can no longer be trusted, e.g. after
// an fsync error. Cause says why. In simulation it is an ordinary crash; in
// production the process exits and a supervisor restarts it.
type Trip struct{ Cause string }

// ---- observability ----

// Emit publishes Telemetry. The simulator's invariants read it; production
// logs it. Emit has no effect on behavior.
type Emit struct{ Telemetry Telemetry }

func (Reply) isAction() {}
func (Trip) isAction()  {}
func (Emit) isAction()  {}
