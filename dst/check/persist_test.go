package check

import (
	"errors"
	"testing"
)

func TestCrashHelpers(t *testing.T) {
	answered := []Write[string, int]{{Key: "a", Value: 1}, {Key: "b", Value: 2}}
	all := func(string, int) bool { return true }
	noB := func(k string, _ int) bool { return k != "b" }
	if err := Persistence(answered, all); err != nil {
		t.Fatal(err)
	}
	if err := Persistence(answered, noB); err == nil {
		t.Fatal("a missing write must fail")
	}

	eq := func(want, got int) error {
		if want != got {
			return errors.New("different")
		}
		return nil
	}
	three := func() (int, error) { return 3, nil }
	four := func() (int, error) { return 4, nil }
	broken := func() (int, error) { return 0, errors.New("down") }
	if err := ForwardProgress(three, three, eq); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][2]func() (int, error){{three, four}, {broken, three}, {three, broken}} {
		if err := ForwardProgress(c[0], c[1], eq); err == nil {
			t.Fatal("a mismatch or an error must fail")
		}
	}

	if err := DirtyReboot(answered, all, three, three, eq); err != nil {
		t.Fatal(err)
	}
	if err := DirtyReboot(answered, noB, three, three, eq); err == nil {
		t.Fatal("a lost write must fail")
	}
	if err := DirtyReboot(answered, all, three, four, eq); err == nil {
		t.Fatal("no forward progress must fail")
	}
}
