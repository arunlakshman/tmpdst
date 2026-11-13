package fakenetwork

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/machine"
)

// TestIncrementalStateHashMatchesFull drives random dials, sends, pauses,
// closes, crashes and partitions, and compares the cached StateHash with a
// from-scratch fold after every operation and every internal step. A mutation
// that forgets to invalidate its pair fails here.
func TestIncrementalStateHashMatchesFull(t *testing.T) {
	f := Faults{Latency: lat, PartialIOMax: 4, PairLatencyMax: 20000, PartitionProb: 0.1,
		PartitionDur: [2]machine.Duration{10000, 400000}, ClogProb: 0.1, ClogDur: [2]machine.Duration{1000, 100000},
		RandomCloseProb: 0.03, BlackHoleProb: 0.03}
	for seed := uint64(1); seed <= 40; seed++ {
		h := newHarness(t, seed, f)
		names := []string{"a", "b", "c"}
		type cn struct {
			node string
			conn core.ConnID
		}
		var conns []cn
		check := func(what string) {
			t.Helper()
			if got, want := h.eng.StateHash(), h.eng.fullStateHash(); got != want {
				t.Fatalf("seed %d after %s: cached %016x, full %016x", seed, what, got, want)
			}
		}
		for i := range 300 {
			a := names[h.rnd.Intn(3)]
			what := ""
			switch h.rnd.Intn(8) {
			case 0:
				what = "dial"
				r := h.nextRequestID()
				h.Issue(a, core.Connect{Req: r, Addr: names[h.rnd.Intn(3)]})
			case 1, 2, 3:
				what = "send"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.Issue(c.node, core.Send{Req: h.nextRequestID(), Conn: c.conn, Data: fmt.Append(nil, "payload-", i)})
				}
			case 4:
				what = "pause"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.Issue(c.node, core.PauseReads{Conn: c.conn})
				}
			case 5:
				what = "resume"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.Issue(c.node, core.ResumeReads{Conn: c.conn})
				}
			case 6:
				what = "close"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.Issue(c.node, core.CloseConn{Conn: c.conn, Abort: h.rnd.Intn(2) == 0})
				}
			case 7:
				what = "crash"
				if h.rnd.Intn(4) == 0 {
					h.crash(a)
					h.boot(a)
				}
			}
			check(what)
			for k := h.rnd.Intn(4); k > 0; k-- {
				if !h.Step() {
					break
				}
				check("step")
			}
			// Remember every conn the service knows, to aim later actions.
			conns = conns[:0]
			for _, p := range h.eng.pairs {
				for s := range 2 {
					conns = append(conns, cn{string(p.end[s].ref.Addr().ID()), p.end[s].id})
				}
			}
			if i == 150 {
				h.Recover()
				check("recovery")
			}
		}
		h.run()
		check("drain")
	}
}
