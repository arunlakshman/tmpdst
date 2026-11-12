package check_test

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/check"
)

type fakeLog struct {
	entries [][]byte
	hole    bool
	changes []bool
}

func (f fakeLog) End() (check.Slot, bool)   { return check.Slot(len(f.entries)), f.hole }
func (f fakeLog) Entry(s check.Slot) []byte { return f.entries[s] }
func (f fakeLog) QuietAfter(from check.Slot) bool {
	for s := int(from); s < len(f.changes); s++ {
		if f.changes[s] {
			return false
		}
	}
	return true
}

func newChecker() (check.Checker, *[]string) {
	var fails []string
	return check.Checker{Log: assert.New(func(p, d string) { fails = append(fails, p+": "+d) }), Prefix: "t: "}, &fails
}

func TestReplayAppliesInOrderAndJudgesEveryPrefix(t *testing.T) {
	c, fails := newChecker()
	l := fakeLog{entries: [][]byte{{1, 2}, {3}}}
	var sum int
	var prefixes []check.Slot
	ok := check.Replay(c, l, &sum,
		func(e []byte) ([]int, error) {
			out := make([]int, len(e))
			for i, b := range e {
				out[i] = int(b)
			}
			return out, nil
		},
		func(s *int, op int) { *s += op },
		func(p check.Slot) { prefixes = append(prefixes, p) })
	if !ok || sum != 6 || len(*fails) != 0 {
		t.Fatalf("ok=%v sum=%d fails=%v", ok, sum, *fails)
	}
	if len(prefixes) != 3 || prefixes[0] != 0 || prefixes[2] != 2 {
		t.Fatalf("prefixes=%v", prefixes)
	}
}

func TestReplayStopsOnDecodeError(t *testing.T) {
	c, fails := newChecker()
	l := fakeLog{entries: [][]byte{{1}, {2}}}
	n := 0
	ok := check.Replay(c, l, struct{}{},
		func([]byte) ([]int, error) { return nil, errors.New("bad") },
		func(struct{}, int) { n++ }, func(check.Slot) {})
	if ok || n != 0 || len(*fails) != 1 {
		t.Fatalf("ok=%v n=%d fails=%v", ok, n, *fails)
	}
}

func TestNoHole(t *testing.T) {
	c, fails := newChecker()
	c.NoHole(fakeLog{entries: [][]byte{{1}}})
	if len(*fails) != 0 {
		t.Fatal(*fails)
	}
	c.NoHole(fakeLog{entries: [][]byte{{1}}, hole: true})
	if len(*fails) != 1 {
		t.Fatal(*fails)
	}
}

func TestHolders(t *testing.T) {
	var h check.Holders[string]
	if _, ok := h.Note(1, "a"); !ok {
		t.Fatal("first holder")
	}
	if _, ok := h.Note(1, "a"); !ok {
		t.Fatal("same holder")
	}
	if first, ok := h.Note(1, "b"); ok || first != "a" {
		t.Fatalf("first=%q ok=%v", first, ok)
	}
}

func TestPrefixes(t *testing.T) {
	var p check.Prefixes[int]
	p.Note(5, 0xaa, 0)
	if _, _, ok := p.Note(5, 0xaa, 1); !ok {
		t.Fatal("same hash")
	}
	if h, who, ok := p.Note(5, 0xbb, 2); ok || h != 0xaa || who != 0 {
		t.Fatalf("h=%x who=%d ok=%v", h, who, ok)
	}
}

func TestMonotonic(t *testing.T) {
	var f check.Monotonic[string, int32]
	f.Raise("a", 3)
	f.Raise("a", 2)
	if f.Floor("a") != 3 || f.AtLeast("a", 2) || !f.AtLeast("a", 3) || !f.AtLeast("b", 0) {
		t.Fatal("floor must only rise")
	}
}

func TestConverged(t *testing.T) {
	l := fakeLog{entries: make([][]byte, 3), changes: []bool{true, false, false}}
	up := check.Node{Up: true, Ready: true, Applied: 1}
	if !check.Converged(l, []check.Node{up, up}) {
		t.Fatal("quiet tail: converged")
	}
	if check.Converged(l, []check.Node{up, {Up: true, Ready: true, Applied: 0}}) {
		t.Fatal("a node misses slot 0")
	}
	if check.Converged(l, []check.Node{up, {Up: false, Ready: true, Applied: 3}}) {
		t.Fatal("a node is down")
	}
}

func TestAllEqual(t *testing.T) {
	if !check.AllEqual[uint64](nil) || !check.AllEqual([]uint64{4, 4}) || check.AllEqual([]uint64{4, 5}) {
		t.Fatal("AllEqual")
	}
}
