package sim

import (
	"fmt"
	"strconv"

	"manifester/toys3/control"
)

// The client-visible history of a run, as recorded by the Oracle and read by
// the black-box invariants.

// Outcome is what the client learned about one request.
type Outcome uint8

const (
	Pending   Outcome = iota // no reply yet
	Definite                 // 200, 400 or 404: the op happened exactly as replied
	Failed                   // 500: the op may or may not have taken effect
	Abandoned                // the server crashed (or tripped) before replying
)

// HistOp is one client request as the client saw it.
type HistOp struct {
	Req    control.ClientReq
	Client int
	Op     control.Op
	Invoke control.Time // when the Request was delivered
	// Return is the reply time (Definite, Failed) or the crash time (Abandoned).
	Return  control.Time
	Outcome Outcome
	Status  control.Status
	Body    []byte // 200 GET only
	// InvSeq and RetSeq order Invoke, Return and crash events exactly
	// (several can share one virtual instant); the checker uses these.
	InvSeq, RetSeq uint64
}

// History is the complete client-visible history of a run.
type History struct {
	Ops     []HistOp       // in Invoke order
	Crashes []control.Time // crash (and Trip) instants, ascending
	// Completed lists indices into Ops in reply order (Definite and Failed).
	Completed []int
}

func (o Outcome) String() string {
	switch o {
	case Pending:
		return "pending"
	case Definite:
		return "definite"
	case Failed:
		return "failed"
	case Abandoned:
		return "abandoned"
	}
	return "outcome(?)"
}

// describeOp renders one history op for failure messages and logs.
func describeOp(h *HistOp) string {
	res := h.Outcome.String()
	if h.Outcome == Definite || h.Outcome == Failed {
		res = strconv.Itoa(int(h.Status))
	}
	if w, ok := ValueWriter(h.Body); ok {
		res += fmt.Sprintf(" value-of-%d", w)
	} else if h.Body != nil {
		res += fmt.Sprintf(" garbage(%d bytes)", len(h.Body))
	}
	return fmt.Sprintf("req %d client %d %s [%d..%d @%v..%v] -> %s",
		h.Req, h.Client, DescribeOp(h.Op), h.InvSeq, h.RetSeq, h.Invoke, h.Return, res)
}
