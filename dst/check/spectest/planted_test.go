package spectest

import (
	"fmt"
	"strings"
	"testing"
)

// captureTB keeps the Fatalf message and does not stop the test.
type captureTB struct {
	testing.TB
	failed bool
	msg    string
}

func (c *captureTB) Fatalf(format string, args ...any) {
	c.failed = true
	c.msg = fmt.Sprintf(format, args...)
}

func TestRequireCaughtFindsEachBug(t *testing.T) {
	seen := map[Bug]uint64{}
	RequireCaught(t, []Bug{"a", "b"}, 10, func(bug Bug, seed uint64) bool {
		if (bug == "a" && seed == 3) || (bug == "b" && seed == 7) {
			seen[bug] = seed
			return true
		}
		return false
	})
	if seen["a"] != 3 || seen["b"] != 7 {
		t.Fatalf("seen = %v", seen)
	}
}

func TestRequireCaughtFailsWhenABugIsMissed(t *testing.T) {
	tb := &captureTB{TB: t}
	RequireCaught(tb, []Bug{"ghost"}, 5, func(Bug, uint64) bool { return false })
	if !tb.failed || !strings.Contains(tb.msg, "ghost") || !strings.Contains(tb.msg, "1 to 5") {
		t.Fatalf("failed=%v msg=%q", tb.failed, tb.msg)
	}
}

func TestRequireCaughtRejectsAZeroBudget(t *testing.T) {
	tb := &captureTB{TB: t}
	RequireCaught(tb, []Bug{"x"}, 0, func(Bug, uint64) bool { return true })
	if !tb.failed {
		t.Fatal("a zero budget must fail")
	}
}

func TestRequireCleanRunsEverySeed(t *testing.T) {
	var n int
	RequireClean(t, 8, func(uint64) bool { n++; return false })
	if n != 8 {
		t.Fatalf("ran %d seeds", n)
	}
}

func TestRequireCleanFailsOnAFalseAlarm(t *testing.T) {
	tb := &captureTB{TB: t}
	RequireClean(tb, 10, func(seed uint64) bool { return seed == 4 })
	if !tb.failed || !strings.Contains(tb.msg, "seed 4") {
		t.Fatalf("failed=%v msg=%q", tb.failed, tb.msg)
	}
	tb = &captureTB{TB: t}
	RequireClean(tb, 0, func(uint64) bool { return false })
	if !tb.failed {
		t.Fatal("a zero budget must fail")
	}
}
