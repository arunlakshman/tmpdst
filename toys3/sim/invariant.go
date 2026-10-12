package sim

import (
	"fmt"
	"sort"
	"strings"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// Sample is one control.Telemetry value stamped by the simulator with its
// sample index (Step), time and incarnation.
type Sample struct {
	Step      int
	At        control.Time
	Inc       Incarnation
	Telemetry control.Telemetry
}

// View is the read-only window invariants get onto a running simulation.
type View interface {
	Step() int
	Now() control.Time
	Alive() bool // server is booted (it may still be recovering)
	Inc() Incarnation
	Settling() bool // in Settling: no new faults, no crashes
	// GroundTruth is the plant's true durable state, for white-box monitors.
	GroundTruth() GroundTruth
	Root() string // the server's data directory (control.Config.Dir)
	Oracle() *Oracle
	History() *History
	// Telemetry emitted so far, in order. Invariants remember how far they read.
	Telemetry() []Sample
}

// GroundTruth is the anti-corruption interface between verification and the
// plant model: the only things a monitor may read of the plant are its true
// durable state and a generation counter. It uses only builtin types, so the
// plant model never depends on verification and monitors never reach into
// the plant model's internals. *PlantModel implements it.
type GroundTruth interface {
	// DurableFile returns path's content in the durable view.
	DurableFile(path string) ([]byte, bool)
	// EachDurable calls fn for every durable file, sorted by path.
	EachDurable(fn func(path string, data []byte))
	// DurableGen changes whenever the durable view may have changed.
	DurableGen() uint64
}

var _ GroundTruth = (*PlantModel)(nil)

// Invariant is checked after every simulator step and once at the end of the
// run. Checking every step (not just at the end) reports a bug at the step
// that caused it, which is what makes the event log readable.
type Invariant interface {
	Name() string
	AfterStep(v View) []Failure
	AtEnd(v View) []Failure
}

// DefaultInvariants returns fresh instances of every invariant below.
func DefaultInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &AckedIsDurable{}, &ObjNeverTorn{}, &Linearizable{}, &Liveness{}}
}

// BlackBoxInvariants returns only the invariants that judge what clients
// see (no peeking at the plant model's durable state). Useful to learn
// which bugs are visible end to end, and which only to a ground-truth check.
func BlackBoxInvariants() []Invariant {
	return []Invariant{&NoCorruptReads{}, &Linearizable{}, &Liveness{}}
}

// NoCorruptReads is invariant (c): every 200 GET body is exactly a value some
// PUT wrote (ValueWriter accepts it). A torn or corrupt object served with 200
// fails here immediately, even before the linearizability check.
type NoCorruptReads struct{ next int }

// AckedIsDurable checks each control.Committed sample against the plant's
// durable view (GroundTruth) on the step it is emitted: Committed is the
// controller's claim, the durable view is the true state. The key's durable
// file must exist and decode to a payload with the committed checksum (or be
// absent for a delete). This is (b) checked eagerly: a missing fsync fails
// here without needing a crash.
type AckedIsDurable struct{ next int }

// ObjNeverTorn checks, after every step, that every file under obj/ in the
// durable view (GroundTruth) is a complete object (header, length and CRC
// valid). The protocol renames only fsynced files into obj/, so even the most
// adversarial crash leaves obj/ clean; the object CRC is a safety net, not a
// mechanism.
type ObjNeverTorn struct{ gen uint64 }

// Linearizable is invariants (a) and (b): it runs Oracle.CheckQuiescent at
// every quiescent point and at the end.
type Linearizable struct{}

// Liveness is invariant (d): at the end of Settling (server alive, no
// faults) no op is Pending, and every boot reached control.Recovered.
// Additionally, a request issued during Settling never gets a 500: with no
// faults injected a correct server has no reason to fail (this is what makes
// a lost-but-acked object visible to clients: it reads back as 500 forever).
type Liveness struct {
	next         int
	settlingFrom int // index of the first op invoked in Settling (once settlingSeen)
	settlingSeen bool
}

func (*NoCorruptReads) Name() string { return "no-corrupt-reads" }
func (n *NoCorruptReads) AfterStep(v View) []Failure {
	h := v.History()
	var fails []Failure
	for ; n.next < len(h.Completed); n.next++ {
		op := &h.Ops[h.Completed[n.next]]
		if _, isGet := op.Op.(control.Get); !isGet || op.Status != control.StatusOK {
			continue
		}
		w, ok := ValueWriter(op.Body)
		if !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned bytes no PUT wrote: %s", describeOp(op))})
			continue
		}
		i := sort.Search(len(h.Ops), func(i int) bool { return h.Ops[i].Req >= w })
		if i == len(h.Ops) || h.Ops[i].Req != w {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned the value of unknown request %d: %s", w, describeOp(op))})
			continue
		}
		if _, isPut := h.Ops[i].Op.(control.Put); !isPut {
			fails = append(fails, Failure{Msg: fmt.Sprintf("200 GET returned a value attributed to non-PUT %s", describeOp(&h.Ops[i]))})
		}
	}
	return fails
}
func (*NoCorruptReads) AtEnd(v View) []Failure { return nil }

func (*AckedIsDurable) Name() string { return "acked-is-durable" }
func (a *AckedIsDurable) AfterStep(v View) []Failure {
	samples := v.Telemetry()
	var fails []Failure
	for ; a.next < len(samples); a.next++ {
		c, ok := samples[a.next].Telemetry.(control.Committed)
		if !ok {
			continue
		}
		path := format.ObjPath(v.Root(), c.Key)
		data, present := v.GroundTruth().DurableFile(path)
		switch {
		case c.Deleted && present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: DELETE %q acked but %s is still durable (%s)", c.Client, c.Key, path, describePayload(data))})
		case c.Deleted:
		case !present:
			fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but %s has no durable entry", c.Client, c.Key, path)})
		default:
			payload, valid := format.DecodeObject(data)
			if !valid {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s is torn (%d bytes)", c.Client, c.Key, path, len(data))})
			} else if format.Checksum(payload) != c.Sum {
				fails = append(fails, Failure{Msg: fmt.Sprintf("c%d: write of %q acked but durable %s holds %s", c.Client, c.Key, path, describePayload(data))})
			}
		}
	}
	return fails
}
func (*AckedIsDurable) AtEnd(v View) []Failure { return nil }

func (*ObjNeverTorn) Name() string { return "obj-never-torn" }
func (o *ObjNeverTorn) AfterStep(v View) []Failure {
	g := v.GroundTruth().DurableGen()
	if g == o.gen {
		return nil // nothing durable changed since the last check
	}
	o.gen = g
	prefix := format.ObjDir(v.Root()) + "/"
	var fails []Failure
	v.GroundTruth().EachDurable(func(path string, data []byte) {
		if !strings.HasPrefix(path, prefix) {
			return
		}
		if _, ok := format.DecodeObject(data); !ok {
			fails = append(fails, Failure{Msg: fmt.Sprintf("durable %s is torn or corrupt (%d bytes)", path, len(data))})
		}
	})
	return fails
}
func (*ObjNeverTorn) AtEnd(v View) []Failure { return nil }

func (*Linearizable) Name() string { return "linearizable" }
func (*Linearizable) AfterStep(v View) []Failure {
	o := v.Oracle()
	if !o.Quiescent() || o.Unchecked() == 0 {
		return nil
	}
	return o.CheckQuiescent()
}
func (*Linearizable) AtEnd(v View) []Failure { return v.Oracle().CheckFinal() }

func (*Liveness) Name() string { return "liveness" }
func (l *Liveness) AfterStep(v View) []Failure {
	h := v.History()
	if v.Settling() && !l.settlingSeen {
		l.settlingSeen, l.settlingFrom = true, len(h.Ops)
	}
	var fails []Failure
	for ; l.next < len(h.Completed); l.next++ {
		i := h.Completed[l.next]
		op := &h.Ops[i]
		if l.settlingSeen && i >= l.settlingFrom && op.Status == control.StatusServerError {
			fails = append(fails, Failure{Msg: "500 with no fault injected (Settling): " + describeOp(op)})
		}
	}
	return fails
}
func (*Liveness) AtEnd(v View) []Failure {
	var fails []Failure
	if !v.Alive() {
		fails = append(fails, Failure{Msg: "server not alive at the end of the run"})
	}
	recovered := false
	for _, f := range v.Telemetry() {
		if _, ok := f.Telemetry.(control.Recovered); ok && f.Inc == v.Inc() {
			recovered = true
		}
	}
	if !recovered {
		fails = append(fails, Failure{Msg: fmt.Sprintf("incarnation %d never emitted Recovered", v.Inc())})
	}
	h := v.History()
	for i := range h.Ops {
		if h.Ops[i].Outcome == Pending {
			fails = append(fails, Failure{Msg: "no reply by the end of Settling: " + describeOp(&h.Ops[i])})
		}
	}
	return fails
}
