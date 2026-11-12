package mutant_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/mutant"
)

// captureTB records Fatalf without failing the parent test.
type captureTB struct {
	testing.TB
	failed bool
	msg    string
}

func (c *captureTB) Fatalf(format string, args ...any) {
	c.failed = true
	c.msg = fmt.Sprintf(format, args...)
}

func TestRequireFindsEachBug(t *testing.T) {
	bugs := []mutant.Bug{"a", "b"}
	seen := map[mutant.Bug]uint64{}
	mutant.Require(t, bugs, 10, func(bug mutant.Bug, seed uint64) bool {
		if bug == "a" && seed == 3 {
			seen[bug] = seed
			return true
		}
		if bug == "b" && seed == 7 {
			seen[bug] = seed
			return true
		}
		return false
	})
	if seen["a"] != 3 || seen["b"] != 7 {
		t.Fatalf("seen=%v", seen)
	}
}

func TestRequireFailsWhenBugMissed(t *testing.T) {
	tb := &captureTB{TB: t}
	mutant.Require(tb, []mutant.Bug{"ghost"}, 5, func(mutant.Bug, uint64) bool {
		return false
	})
	if !tb.failed {
		t.Fatal("expected Require to fail")
	}
	if !strings.Contains(tb.msg, "ghost") {
		t.Fatalf("msg=%q", tb.msg)
	}
}

func TestRequireCleanPasses(t *testing.T) {
	var n int
	mutant.RequireClean(t, 8, func(uint64) bool {
		n++
		return false
	})
	if n != 8 {
		t.Fatalf("ran %d seeds", n)
	}
}

func TestRequireCleanFailsOnFalseAlarm(t *testing.T) {
	tb := &captureTB{TB: t}
	mutant.RequireClean(tb, 10, func(seed uint64) bool {
		return seed == 4
	})
	if !tb.failed {
		t.Fatal("expected RequireClean to fail")
	}
	if !strings.Contains(tb.msg, "4") {
		t.Fatalf("msg=%q", tb.msg)
	}
}

func TestRequireRejectsZeroBudget(t *testing.T) {
	tb := &captureTB{TB: t}
	mutant.Require(tb, []mutant.Bug{"x"}, 0, func(mutant.Bug, uint64) bool { return true })
	if !tb.failed {
		t.Fatal("expected zero budget to fail")
	}
}
