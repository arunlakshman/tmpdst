package assert_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/assert"
)

func TestAlwaysCoverageAndFail(t *testing.T) {
	var fails []string
	l := assert.New(func(property, detail string) {
		fails = append(fails, property+":"+detail)
	})
	l.Always(true, "ok", assert.Pair{Key: "k", Value: 1})
	l.Always(false, "bad", assert.Pair{Key: "k", Value: "x"})
	cov := l.Coverage()
	if cov["ok"].Checks != 1 || cov["ok"].Passes != 1 || cov["ok"].Kind != assert.KindAlways {
		t.Fatalf("ok coverage %#v", cov["ok"])
	}
	if cov["bad"].Checks != 1 || cov["bad"].Passes != 0 {
		t.Fatalf("bad coverage %#v", cov["bad"])
	}
	if len(fails) != 1 || fails[0] != "bad:k=x" {
		t.Fatalf("fails = %v", fails)
	}
}

func TestSometimesReachableUnreachable(t *testing.T) {
	var fails []string
	l := assert.New(func(property, detail string) {
		fails = append(fails, property+"|"+detail)
	})
	l.Sometimes(true, "rare")
	l.Sometimes(false, "rare")
	l.Reachable("path")
	l.AlwaysOrUnreachable(true, "alias")
	l.Unreachable("never", assert.Pair{Key: "why", Value: 1})

	cov := l.Coverage()
	if cov["rare"].Kind != assert.KindSometimes || cov["rare"].Checks != 2 || cov["rare"].Passes != 1 {
		t.Fatalf("rare %#v", cov["rare"])
	}
	if cov["path"].Kind != assert.KindReachable || cov["path"].Checks != 1 || cov["path"].Passes != 1 {
		t.Fatalf("path %#v", cov["path"])
	}
	if cov["alias"].Kind != assert.KindAlwaysOrUnreachable || cov["alias"].Passes != 1 {
		t.Fatalf("alias %#v", cov["alias"])
	}
	if cov["never"].Kind != assert.KindUnreachable || cov["never"].Checks != 1 || cov["never"].Passes != 0 {
		t.Fatalf("never %#v", cov["never"])
	}
	if len(fails) != 1 || fails[0] != "never|reached an Unreachable point why=1" {
		t.Fatalf("fails = %v", fails)
	}
}

func TestAlwaysOrUnreachableFails(t *testing.T) {
	var fails []string
	l := assert.New(func(property, detail string) { fails = append(fails, property+"|"+detail) })
	l.AlwaysOrUnreachable(false, "x", assert.Pair{Key: "k", Value: 2})
	if len(fails) != 1 || fails[0] != "x|k=2" {
		t.Fatalf("fails = %v", fails)
	}
}

// FirstStep is the step of the first failed safety check, or of the first
// passed coverage check.
func TestFirstStep(t *testing.T) {
	step := uint64(0)
	l := assert.New(nil)
	l.SetStepCounter(func() uint64 { return step })
	step = 3
	l.Always(true, "safe")
	l.Sometimes(false, "rare")
	step = 5
	l.Always(false, "safe")
	l.Sometimes(true, "rare")
	step = 9
	l.Always(false, "safe")
	l.Sometimes(true, "rare")
	l.Reachable("here")
	cov := l.Coverage()
	if cov["safe"].FirstStep != 5 || cov["rare"].FirstStep != 5 || cov["here"].FirstStep != 9 {
		t.Fatalf("first steps: %+v", cov)
	}
}

func TestNilOnFail(t *testing.T) {
	l := assert.New(nil)
	l.Always(false, "x")
	l.Unreachable("y")
}
