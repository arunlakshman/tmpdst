package netsim

import (
	"fmt"
	"testing"

	"manifester/core"
	"manifester/sim"
)

// TestIncrementalStateHashMatchesFull drives random dials, sends, pauses,
// closes, crashes and partitions, and compares the cached StateHash with a
// from-scratch fold after every operation and every internal step. A mutation
// that forgets to invalidate its pair fails here.
func TestIncrementalStateHashMatchesFull(t *testing.T) {
	f := sim.NetworkFaults{Latency: lat, PartialIOMax: 4, PairLatencyMax: 20000, PartitionProb: 0.1,
		PartitionDur: [2]core.Duration{10000, 400000}, ClogProb: 0.1, ClogDur: [2]core.Duration{1000, 100000},
		RandomCloseProb: 0.03, BlackHoleProb: 0.03}
	for seed := uint64(1); seed <= 40; seed++ {
		h := newHarness(t, seed, f)
		nodes := map[string]core.NodeRef{}
		for _, n := range []string{"a", "b", "c"} {
			nodes[n] = h.boot(n)
		}
		names := []string{"a", "b", "c"}
		type cn struct {
			ref  core.NodeRef
			conn core.ConnID
		}
		var conns []cn
		check := func(what string) {
			t.Helper()
			if got, want := h.eng.StateHash(), h.eng.fullStateHash(); got != want {
				t.Fatalf("seed %d after %s: cached %016x, full %016x", seed, what, got, want)
			}
		}
		for i := 0; i < 300; i++ {
			a := nodes[names[h.rnd.Intn(3)]]
			what := ""
			switch h.rnd.Intn(8) {
			case 0:
				what = "dial"
				r := h.req()
				h.eng.Execute(h, a, core.Dial{Req: r, Addr: names[h.rnd.Intn(3)]})
			case 1, 2, 3:
				what = "send"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.eng.Execute(h, c.ref, core.Send{Req: h.req(), Conn: c.conn, Data: []byte(fmt.Sprint("payload-", i))})
				}
			case 4:
				what = "pause"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.eng.Execute(h, c.ref, core.PauseReads{Conn: c.conn})
				}
			case 5:
				what = "resume"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.eng.Execute(h, c.ref, core.ResumeReads{Conn: c.conn})
				}
			case 6:
				what = "close"
				if len(conns) > 0 {
					c := conns[h.rnd.Intn(len(conns))]
					h.eng.Execute(h, c.ref, core.CloseConn{Conn: c.conn, Abort: h.rnd.Intn(2) == 0})
				}
			case 7:
				what = "crash"
				if h.rnd.Intn(4) == 0 {
					h.crash(a)
					nodes[string(a.Node)] = h.boot(string(a.Node))
				}
			}
			check(what)
			for k := h.rnd.Intn(4); k > 0 && len(h.q) > 0; k-- {
				h.step()
				check("step")
			}
			// Remember every conn the engine knows, to aim later actions.
			conns = conns[:0]
			for _, p := range h.eng.pairs {
				for s := 0; s < 2; s++ {
					conns = append(conns, cn{p.end[s].ref, p.end[s].id})
				}
			}
			if i == 150 {
				h.eng.EnterRecovery()
				check("recovery")
			}
		}
		h.run()
		check("drain")
	}
}
