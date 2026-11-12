package spectest

import (
	"strings"
	"testing"
)

func newSetSpec() Spec[setOp, setRes] { return &setSpec{} }

func TestSelfCheckPassesWhenSpecAndNaiveModelAgree(t *testing.T) {
	SelfCheck(t, newSetSpec, func() Spec[setOp, setRes] { return &naiveSet{} }, genSetOp(), equalRes)
}

func TestSelfCheckCatchesADisagreement(t *testing.T) {
	l := selfCheck(newSetSpec, func() Spec[setOp, setRes] { return &naiveSet{bug: true} }, genSetOp(), equalRes)
	var f *Failure[setOp]
	for seed := uint64(1); f == nil && seed <= 20; seed++ {
		f = l.RunSeed(seed)
	}
	if f == nil {
		t.Fatal("the broken naive model was not caught in 20 seeds")
	}
	// Add, Remove, then an op that sees the key: no hash is compared, so the
	// shortest list has 3 ops.
	if len(f.Steps) > 3 || !strings.Contains(f.Message, "not the spec answer") {
		t.Fatalf("failure = %+v", f)
	}
}
