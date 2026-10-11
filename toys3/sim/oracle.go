package sim

import "manifester/toys3/core"

// Outcome is what the client learned about one request.
type Outcome uint8

const (
	Pending   Outcome = iota // no reply yet
	Definite                 // 200, 400 or 404: the op happened exactly as replied
	Failed                   // 500: the op may or may not have taken effect
	Abandoned                // the server crashed (or halted) before replying
)

// HistOp is one client request as the client saw it.
type HistOp struct {
	Req    core.ClientReq
	Client int
	Op     core.Op
	Invoke core.Time // when the Request was delivered
	// Return is the reply time (Definite, Failed) or the crash time (Abandoned).
	Return  core.Time
	Outcome Outcome
	Status  core.Status
	Body    []byte // 200 GET only
}

// History is the complete client-visible history of a run.
type History struct {
	Ops     []HistOp    // in Invoke order
	Crashes []core.Time // crash (and Halt) instants, ascending
}

// Value is the model's view of one key: absent, or the bytes written by PUT
// Writer (COPY preserves Writer, since the bytes are identical).
type Value struct {
	Present bool
	Writer  core.ClientReq
}

// Oracle records the history and decides whether it is linearizable against a
// sequential model: a map from key to Value, starting empty. Nothing about
// the server's internals is consulted.
//
// The rules, which encode invariants (a) and (b) of DESIGN.md:
//
//   - A Definite op takes effect exactly once, at some instant in
//     [Invoke, Return], with the replied result (GET 200 returns the model's
//     value, GET/DELETE/COPY 404 means the key was absent, PUT/COPY/DELETE 200
//     changed the model).
//   - A Failed or Abandoned mutation takes effect at most once, at some
//     instant in [Invoke, Return] (Return = crash time when Abandoned), or
//     never. Failed and Abandoned reads impose nothing.
//   - Crashes erase nothing. So an acked mutation that disappears after a
//     restart, or an unacked one that a read observed and that later
//     vanishes, has no valid linearization and fails the check. (The latter
//     is why the server should Halt after an fsync error.)
//
// Checking is a Wing-Gong-Lowe search over each window between quiescent
// points (no op Pending), memoized on (set of linearized ops, model state).
// Windows are small because Workload.QuiesceEvery and crashes bound them.
type Oracle struct {
	hist History
	open map[core.ClientReq]int // Req → index into hist.Ops while Pending
	// checked is the index of the first op not yet in a checked window.
	checked int
}

// NewOracle returns an oracle with an empty history.
func NewOracle() *Oracle {
	return &Oracle{open: map[core.ClientReq]int{}}
}

// Invoke records a request delivered to the server at time at.
func (o *Oracle) Invoke(at core.Time, client int, req core.Request) {
	panic("toys3/sim: Oracle.Invoke unimplemented (phase 2)")
}

// Return records a reply. ok is false for an unknown or duplicate reply.
func (o *Oracle) Return(at core.Time, rep core.Reply) (ok bool) {
	panic("toys3/sim: Oracle.Return unimplemented (phase 2)")
}

// Crash marks every Pending op Abandoned with Return = at.
func (o *Oracle) Crash(at core.Time) {
	panic("toys3/sim: Oracle.Crash unimplemented (phase 2)")
}

// CheckQuiescent checks the window of ops completed since the last check. The
// simulator calls it at every quiescent point and at the end of the run; the
// model state at the end of a window must be carried as the set of possible
// states into the next one.
func (o *Oracle) CheckQuiescent() []Failure {
	panic("toys3/sim: Oracle.CheckQuiescent unimplemented (phase 2)")
}

// History returns the recorded history (read-only).
func (o *Oracle) History() *History { return &o.hist }
