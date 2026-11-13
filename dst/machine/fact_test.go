package machine_test

import (
	"testing"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/dst/trace"
)

func sampleFact() machine.Fact {
	return machine.Fact{Name: "f", Fields: []machine.Field{
		{Key: "a", Value: machine.Int64Value(-1)},
		{Key: "b", Value: machine.Uint64Value(2)},
		{Key: "c", Value: machine.StringValue("x")},
		{Key: "d", Value: machine.Bool(true)},
		{Key: "e", Value: machine.Bytes{1, 2}},
	}}
}

// The broker kernel folded this fact to 0x433e150fa2bbbd6c (tags.go, fact and
// fieldValue). Fact.Fold must write the same bytes, so that moving the tags
// into Fold methods moves no golden hash.
func TestFactFoldMatchesTheOldTagTable(t *testing.T) {
	h := trace.New()
	sampleFact().Fold(&h)
	if got := h.Sum(); got != 0x433e150fa2bbbd6c {
		t.Fatalf("fact hash %#x, want 0x433e150fa2bbbd6c", got)
	}
}

// Every case of FieldValue has its own tag, so equal raw values of different
// cases fold differently.
func TestFieldValueCasesFoldDifferently(t *testing.T) {
	cases := []machine.FieldValue{
		machine.Int64Value(1), machine.Uint64Value(1), machine.StringValue("\x01"),
		machine.Bool(true), machine.Bytes{1},
	}
	seen := map[uint64]int{}
	for i, v := range cases {
		h := trace.New()
		v.Fold(&h)
		if j, dup := seen[h.Sum()]; dup {
			t.Fatalf("cases %d and %d fold to the same hash", j, i)
		}
		seen[h.Sum()] = i
	}
}

func TestFactField(t *testing.T) {
	f := sampleFact()
	if v, ok := f.Field("b").(machine.Uint64Value); !ok || v != 2 {
		t.Fatalf("field b = %v", f.Field("b"))
	}
	if f.Field("zz") != nil {
		t.Fatal("a missing field must be nil")
	}
}

func TestEndNames(t *testing.T) {
	if machine.Crash.String() != "crash" || machine.Trip.String() != "trip" || machine.Exit.String() != "exit" {
		t.Fatal("end names changed")
	}
}
