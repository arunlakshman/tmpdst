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
	if cov["alias"].Kind != assert.KindAlways {
		t.Fatalf("alias kind %s", cov["alias"].Kind)
	}
	if len(fails) != 1 || fails[0] != "never|reached an Unreachable point why=1" {
		t.Fatalf("fails = %v", fails)
	}
}

func TestNilOnFail(t *testing.T) {
	l := assert.New(nil)
	l.Always(false, "x")
	l.Unreachable("y")
}
