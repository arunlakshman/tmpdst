package applier_test

import (
	"bytes"
	"reflect"
	"testing"

	"manifester/controlplane/applier"
	"manifester/core"
)

func TestAssignedStates(t *testing.T) {
	a := applier.New(applier.NewMemKV())
	op := commit("k1", pb("a", 0, 3), pb("a", 1, 2))
	first := mustCommit(t, a, 5, op)
	want := first.Resp.(core.CommitSegmentResp).Assigned

	rs, st, err := a.Assigned(5, op)
	if err != nil || st != applier.AssignHere || !reflect.DeepEqual(rs, want) {
		t.Fatalf("here: %v %v %+v", err, st, rs)
	}
	if _, st, _ = a.Assigned(9, op); st != applier.AssignElsewhere { // a replay at a later slot
		t.Fatalf("other slot: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("k1", pb("a", 0, 4))); st != applier.AssignElsewhere { // a conflict
		t.Fatalf("conflict: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("nope", pb("a", 0, 1))); st != applier.AssignUnknown {
		t.Fatalf("unknown: %v", st)
	}
	if _, err = a.Trim(6); err != nil {
		t.Fatal(err)
	}
	if _, st, _ = a.Assigned(5, op); st != applier.AssignUnknown { // forgotten by the window
		t.Fatalf("trimmed: %v", st)
	}
}

func TestSnapshotAtTagsTheViewsSlot(t *testing.T) {
	a := applier.New(applier.NewMemKV())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 1)))
	var buf bytes.Buffer
	called := 0
	if err := a.SnapshotAt(&buf, func() int64 { called++; return 77 }); err != nil || called != 1 {
		t.Fatalf("%v %d", err, called)
	}
	b := applier.New(applier.NewMemKV())
	if slot, err := b.Restore(&buf); err != nil || slot != 77 {
		t.Fatalf("slot %d err %v", slot, err)
	}
	if a.StateHash() != b.StateHash() {
		t.Fatal("state differs")
	}
}
