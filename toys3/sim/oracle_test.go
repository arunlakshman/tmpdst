package sim

import (
	"testing"

	"manifester/toys3/control"
)

// hist builds histories by hand: inv starts an op, ret replies to it.
type hist struct {
	o   *Oracle
	now control.Time
}

func newHist() *hist { return &hist{o: NewOracle()} }

func (h *hist) inv(req control.ClientReq, op control.Op) {
	h.now++
	h.o.Invoke(h.now, int(req), control.Request{Client: req, Op: op})
}

func (h *hist) ret(req control.ClientReq, st control.Status, body []byte) {
	h.now++
	if !h.o.Return(h.now, control.Reply{Client: req, Status: st, Body: body}) {
		panic("unknown reply")
	}
}

func (h *hist) crash() { h.now++; h.o.Crash(h.now) }

func put(key string, req control.ClientReq) control.Put {
	return control.Put{Key: key, Data: MakeValue(req, 20)}
}
func val(req control.ClientReq) []byte { return MakeValue(req, 20) }

func (h *hist) check(t *testing.T, wantOK bool) {
	t.Helper()
	f := h.o.CheckQuiescent()
	if f2 := h.o.CheckFinal(); f == nil {
		f = f2
	}
	if wantOK && f != nil {
		t.Fatalf("valid history rejected: %s", f[0].Msg)
	}
	if !wantOK && f == nil {
		t.Fatal("invalid history accepted")
	}
	if !wantOK {
		t.Logf("rejected as expected: %.300s", f[0].Msg)
	}
}

func TestOracleAcceptsLinearizableConcurrency(t *testing.T) {
	h := newHist()
	h.inv(1, put("a", 1))
	h.inv(2, control.Get{Key: "a"}) // overlaps the PUT: may see absent...
	h.inv(3, control.Get{Key: "a"}) // ...or the new value
	h.ret(2, control.StatusOK, val(1))
	h.ret(3, control.StatusNotFound, nil)
	h.ret(1, control.StatusOK, nil)
	h.inv(4, control.Copy{Src: "a", Dst: "b"})
	h.ret(4, control.StatusOK, nil)
	h.inv(5, control.Get{Key: "b"})
	h.ret(5, control.StatusOK, val(1))
	h.inv(6, control.Delete{Key: "a"})
	h.ret(6, control.StatusOK, nil)
	h.inv(7, control.Delete{Key: "a"})
	h.ret(7, control.StatusNotFound, nil)
	h.check(t, true)
}

func TestOracleRejectsReadNotYetPossible(t *testing.T) {
	// GET 2 saw the value; the strictly later GET 3 saw it absent, with
	// nothing in between deleting it.
	h := newHist()
	h.inv(1, put("a", 1))
	h.ret(1, control.StatusOK, nil)
	h.inv(2, control.Get{Key: "a"})
	h.ret(2, control.StatusOK, val(1))
	h.inv(3, control.Get{Key: "a"})
	h.ret(3, control.StatusNotFound, nil)
	h.check(t, false)
}

func TestOracleRejectsStaleRead(t *testing.T) {
	h := newHist()
	h.inv(1, put("a", 1))
	h.ret(1, control.StatusOK, nil)
	h.inv(2, put("a", 2))
	h.ret(2, control.StatusOK, nil)
	h.inv(3, control.Get{Key: "a"})
	h.ret(3, control.StatusOK, val(1))
	h.check(t, false)
}

func TestOracleRejectsAckedThenLost(t *testing.T) {
	h := newHist()
	h.inv(1, put("a", 1))
	h.ret(1, control.StatusOK, nil)
	h.crash()
	if f := h.o.CheckQuiescent(); f != nil {
		t.Fatal(f[0].Msg)
	}
	h.inv(2, control.Get{Key: "a"}) // after the restart, in the next window
	h.ret(2, control.StatusNotFound, nil)
	h.check(t, false)
}

func TestOracleFailedMutationMayOrMayNotApply(t *testing.T) {
	for _, applied := range []bool{true, false} {
		h := newHist()
		h.inv(1, put("a", 1))
		h.ret(1, control.StatusOK, nil)
		h.inv(2, put("a", 2))
		h.ret(2, control.StatusServerError, nil)
		h.crash()
		h.inv(3, control.Get{Key: "a"})
		if applied {
			h.ret(3, control.StatusOK, val(2))
		} else {
			h.ret(3, control.StatusOK, val(1))
		}
		h.check(t, true)
	}
	// An abandoned DELETE: the key may be gone or not.
	for _, gone := range []bool{true, false} {
		h := newHist()
		h.inv(1, put("a", 1))
		h.ret(1, control.StatusOK, nil)
		h.inv(2, control.Delete{Key: "a"})
		h.crash()
		h.inv(3, control.Get{Key: "a"})
		if gone {
			h.ret(3, control.StatusNotFound, nil)
		} else {
			h.ret(3, control.StatusOK, val(1))
		}
		h.check(t, true)
	}
}

func TestOracleRejectsObservedThenVanished(t *testing.T) {
	// A 500 PUT that a read observed must not disappear later: crashes
	// erase nothing (this is why the server trips after an fsync error).
	h := newHist()
	h.inv(1, put("a", 1))
	h.ret(1, control.StatusOK, nil)
	h.inv(2, put("a", 2))
	h.ret(2, control.StatusServerError, nil)
	h.inv(3, control.Get{Key: "a"})
	h.ret(3, control.StatusOK, val(2))
	h.crash()
	h.inv(4, control.Get{Key: "a"})
	h.ret(4, control.StatusOK, val(1))
	h.check(t, false)
}

func TestOracleRejectsGarbageAndMustRespectRealTime(t *testing.T) {
	h := newHist()
	h.inv(1, control.Get{Key: "a"})
	h.ret(1, control.StatusOK, []byte("not a value at all"))
	h.check(t, false)

	// A PUT invoked after a GET returned cannot explain that GET.
	h = newHist()
	h.inv(1, control.Get{Key: "a"})
	h.ret(1, control.StatusOK, val(2))
	h.inv(2, put("a", 2))
	h.ret(2, control.StatusOK, nil)
	h.check(t, false)
}

func TestOracleCarriesUncertaintyAcrossWindows(t *testing.T) {
	// Two abandoned PUTs on different keys leave 4 possible states; reads in
	// later windows pin them down one key at a time, and a contradiction
	// is still found.
	h := newHist()
	h.inv(1, put("a", 1))
	h.inv(2, put("b", 2))
	h.crash()
	h.check(t, true)
	h.inv(3, control.Get{Key: "a"})
	h.ret(3, control.StatusOK, val(1))
	h.check(t, true)
	h.inv(4, control.Get{Key: "b"})
	h.ret(4, control.StatusNotFound, nil)
	h.check(t, true)
	h.inv(5, control.Get{Key: "a"})
	h.ret(5, control.StatusNotFound, nil)
	h.check(t, false)
}

func TestOracleWidensInsteadOfGivingUp(t *testing.T) {
	// 14 keys, each with 2 possible values after a crash, then a COPY chain
	// tying them into one component: 2^14 start states is over the cap, so
	// the oracle widens (forgets) instead of failing.
	h := newHist()
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n"}
	req := control.ClientReq(0)
	for _, k := range keys {
		req++
		h.inv(req, put(k, req))
	}
	h.crash()
	h.check(t, true)
	for i := 1; i < len(keys); i++ {
		req++
		h.inv(req, control.Copy{Src: keys[i-1], Dst: keys[i]})
	}
	for i := 1; i < len(keys); i++ {
		h.ret(control.ClientReq(len(keys)+i), control.StatusOK, nil)
	}
	h.check(t, true)
	if h.o.Stats().Widenings == 0 {
		t.Fatal("expected a widening")
	}
	// Precision on a fresh write is regained: a stale read still fails.
	req++
	h.inv(req, put("a", req))
	h.ret(req, control.StatusOK, nil)
	req++
	h.inv(req, put("a", req))
	h.ret(req, control.StatusOK, nil)
	req++
	h.inv(req, control.Get{Key: "a"})
	h.ret(req, control.StatusOK, val(req-2))
	h.check(t, false)
}
