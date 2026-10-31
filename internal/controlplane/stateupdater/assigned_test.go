package stateupdater_test

import (
	"bytes"
	"reflect"
	"testing"

	"mds/internal/controlplane/stateupdater"
	"mds/internal/core"
)

func TestAssignedStates(t *testing.T) {
	a := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
	operation := commit("k1", pb("a", 0, 3), pb("a", 1, 2))
	first := mustCommit(t, a, 5, operation)
	want := first.Resp.(core.CommitSegmentResp).Assigned

	rs, st, err := a.Assigned(5, operation)
	if err != nil || st != stateupdater.OffsetOwnedHere || !reflect.DeepEqual(rs, want) {
		t.Fatalf("here: %v %v %+v", err, st, rs)
	}
	if _, st, _ = a.Assigned(9, operation); st != stateupdater.OffsetOwnedElsewhere { // a replay at a later slot
		t.Fatalf("other slot: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("k1", pb("a", 0, 4))); st != stateupdater.OffsetOwnedElsewhere { // a conflict
		t.Fatalf("conflict: %v", st)
	}
	if _, st, _ = a.Assigned(5, commit("nope", pb("a", 0, 1))); st != stateupdater.OffsetOwnerUnknown {
		t.Fatalf("unknown: %v", st)
	}
	if _, err = a.Trim(6); err != nil {
		t.Fatal(err)
	}
	if _, st, _ = a.Assigned(5, operation); st != stateupdater.OffsetOwnerUnknown { // forgotten by the window
		t.Fatalf("trimmed: %v", st)
	}
}

func TestSnapshotAtTagsTheViewsSlot(t *testing.T) {
	a := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
	mustCommit(t, a, 0, commit("k", pb("a", 0, 1)))
	var buf bytes.Buffer
	called := 0
	if err := a.SnapshotAt(&buf, func() int64 { called++; return 77 }); err != nil || called != 1 {
		t.Fatalf("%v %d", err, called)
	}
	b := stateupdater.New(stateupdater.NewMemoryKeyValueStore())
	if slot, err := b.Restore(&buf); err != nil || slot != 77 {
		t.Fatalf("slot %d err %v", slot, err)
	}
	if a.StateHash() != b.StateHash() {
		t.Fatal("state differs")
	}
}
