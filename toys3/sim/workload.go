package sim

import "manifester/toys3/core"

// OpMix weights the operations clients issue. Zero weights disable an op.
type OpMix struct {
	Put, Get, Delete, Copy int
}

// WorkloadConfig shapes client traffic.
type WorkloadConfig struct {
	// Clients is the number of concurrent clients; each has at most one
	// request outstanding, so Clients bounds concurrency.
	Clients int
	// Keys is the size of the key set ("k0".."k{n-1}"). Small sets force
	// same-key contention, which is where the interesting bugs live.
	Keys int
	// MaxValueSize bounds PUT payloads (at least ValueHeader).
	MaxValueSize int
	Mix          OpMix
	// Think is the [min, max] delay between a reply and the client's next request.
	Think [2]core.Duration
	// QuiesceEvery, if > 0, periodically stops clients from issuing until all
	// outstanding requests have replied. Quiescent points split the history
	// into independent windows for the linearizability checker, bounding its
	// search. A crash is also a quiescent point.
	QuiesceEvery core.Duration
}

// clientState is one simulated client.
type clientState struct {
	busy bool           // a request is outstanding
	req  core.ClientReq // the outstanding request, if busy
}

// Workload is the set of simulated clients. It is driven by the simulator
// (not a core.StateMachine: it never crashes and needs no runtime).
//
// Every PUT carries a fresh, self-describing value (see MakeValue), so any
// bytes returned by a GET identify the PUT that wrote them, and COPY
// propagates that identity unchanged. This is what lets the oracle check
// reads exactly instead of guessing.
type Workload struct {
	cfg     WorkloadConfig
	rng     *Rand
	nextReq core.ClientReq
	clients []clientState
	keys    []string
}

// NewWorkload returns idle clients over cfg.Keys keys.
func NewWorkload(cfg WorkloadConfig, r *Rand) *Workload {
	panic("toys3/sim: NewWorkload unimplemented (phase 2)")
}

// Issue builds the next request of an idle client and marks it busy.
func (w *Workload) Issue(client int, now core.Time) core.Request {
	panic("toys3/sim: Workload.Issue unimplemented (phase 2)")
}

// Complete marks the client that owns rep.Client idle and returns it. ok is
// false for an unknown or already completed request (a duplicate reply: an
// invariant failure the simulator reports).
func (w *Workload) Complete(rep core.Reply) (client int, ok bool) {
	panic("toys3/sim: Workload.Complete unimplemented (phase 2)")
}

// Crash abandons every outstanding request (their clients become idle and
// wait for the restart) and returns the abandoned ids.
func (w *Workload) Crash() []core.ClientReq {
	panic("toys3/sim: Workload.Crash unimplemented (phase 2)")
}

// Idle reports whether no request is outstanding (a quiescent point).
func (w *Workload) Idle() bool {
	panic("toys3/sim: Workload.Idle unimplemented (phase 2)")
}

// ValueHeader is the size of the self-describing prefix of every value:
// writer ClientReq (8 bytes LE) + total length (4 bytes LE).
const ValueHeader = 12

// MakeValue returns the unique value written by PUT req: ValueHeader followed
// by filler that is a pure function of (req, size). size >= ValueHeader.
func MakeValue(req core.ClientReq, size int) []byte {
	panic("toys3/sim: MakeValue unimplemented (phase 2)")
}

// ValueWriter parses b's header and returns the PUT that wrote it. ok is
// false unless b is byte-for-byte equal to MakeValue(writer, len(b)), so a
// torn or mixed value is always rejected.
func ValueWriter(b []byte) (writer core.ClientReq, ok bool) {
	panic("toys3/sim: ValueWriter unimplemented (phase 2)")
}
