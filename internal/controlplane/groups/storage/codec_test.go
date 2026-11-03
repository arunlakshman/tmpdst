package storage_test

import (
	"reflect"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/groupfix"
	"github.com/manifesterio/mds/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/module"
	"github.com/manifesterio/mds/internal/core"
)

func TestCodecRoundTrip(t *testing.T) {
	m := storage.NewModule(nil)
	for _, operation := range []core.ControlPlaneOperation{
		groupfix.Install("g", 2, "m1", "m1", "m2"),
		groupfix.Assign("g", 2, "m1", "m1", "a1", "m2", "a2"),
		groupfix.Remove("g", "m1", "m2"),
		groupfix.CommitOp("g", "m1", 2, groupfix.Pair("t", 0, 5), groupfix.Pair("t", 1, 9)),
	} {
		kind, ok := m.KindOf(operation)
		if !ok {
			t.Fatalf("%T has no kind", operation)
		}
		w := kv.NewWriter()
		m.Encode(w, kind, operation)
		r := kv.NewReader(w.Bytes())
		got := m.Decode(r, kind)
		if !r.Done() || !reflect.DeepEqual(got, operation) {
			t.Fatalf("%T: got %+v (done %v), want %+v", operation, got, r.Done(), operation)
		}
		// every cut of a valid entry must fail the reader, and never panic
		for n := range len(w.Bytes()) {
			cut := kv.NewReader(w.Bytes()[:n])
			m.Decode(cut, kind)
			if cut.Done() {
				t.Fatalf("%T cut to %d of %d bytes was accepted", operation, n, len(w.Bytes()))
			}
		}
	}
}

func TestKindsModeAndForeignOperations(t *testing.T) {
	m := storage.NewModule(nil)
	if len(m.Kinds()) != 4 {
		t.Fatalf("kinds %v", m.Kinds())
	}
	if _, ok := m.KindOf(core.CreateTopic{}); ok {
		t.Fatal("CreateTopic is not a group operation")
	}
	for _, c := range []struct {
		operation core.ControlPlaneOperation
		want      module.Mode
	}{
		{core.GroupInstall{}, module.Logged},
		{core.OffsetCommit{}, module.Logged},
		{core.OffsetFetch{}, module.ReadOnly},
		{core.DescribeGroup{}, module.ReadOnly},
		{core.ListGroups{}, module.ReadOnly},
		{core.CreateTopic{}, module.NotMine},
		{core.DescribeTopics{}, module.NotMine},
	} {
		if got := m.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
	}
	r := kv.NewReader(nil)
	if got := m.Decode(r, 99); got != nil || r.OK() {
		t.Fatal("an unknown kind must fail the reader")
	}
	w := kv.NewWriter()
	m.Encode(w, storage.KindGroupInstall, core.CreateTopic{})
	if len(w.Bytes()) != 0 {
		t.Fatal("encoded an operation that is not a group operation")
	}
	if d := m.Read(nil, core.CreateTopic{}); d.Err != core.ErrInvalid {
		t.Fatalf("Read of a foreign operation: %+v", d)
	}
}
