package veritas

import (
	"slices"
	"testing"

	"manifester/toys3/control"
)

func TestFS(t *testing.T) {
	fs := NewFS("/d/obj", "/d/tmp")
	steps := []struct {
		a    control.Actuation
		want control.Feedback
	}{
		{control.FSRead{IO: 1, Path: "/d/obj/a"}, control.FSReadDone{IO: 1, Err: control.ErrNotFound}},
		{control.FSWrite{IO: 2, Path: "/d/tmp/1", Data: []byte("one")}, control.FSWriteDone{IO: 2}},
		{control.FSFsync{IO: 3, Path: "/d/tmp/1"}, control.FSFsyncDone{IO: 3}},
		{control.FSFsync{IO: 4, Path: "/d/tmp/2"}, control.FSFsyncDone{IO: 4, Err: control.ErrNotFound}},
		{control.FSRename{IO: 5, From: "/d/tmp/1", To: "/d/obj/a"}, control.FSRenameDone{IO: 5}},
		{control.FSRename{IO: 6, From: "/d/tmp/1", To: "/d/obj/a"}, control.FSRenameDone{IO: 6, Err: control.ErrNotFound}},
		{control.FSRename{IO: 7, From: "/d/obj/a", To: "/d/obj/a"}, control.FSRenameDone{IO: 7}},
		{control.FSFsyncDir{IO: 8, Dir: "/d/obj"}, control.FSFsyncDirDone{IO: 8}},
		{control.FSFsyncDir{IO: 9, Dir: "/d/nope"}, control.FSFsyncDirDone{IO: 9, Err: control.ErrNotFound}},
		{control.FSWrite{IO: 10, Path: "/d/obj/b", Data: []byte("longer")}, control.FSWriteDone{IO: 10}},
		{control.FSWrite{IO: 11, Path: "/d/obj/b", Data: []byte("x")}, control.FSWriteDone{IO: 11}}, // truncates
		{control.FSRename{IO: 12, From: "/d/obj/b", To: "/d/obj/a"}, control.FSRenameDone{IO: 12}},  // replaces
		{control.FSRead{IO: 13, Path: "/d/obj/a"}, control.FSReadDone{IO: 13, Data: []byte("x")}},
		{control.FSWrite{IO: 14, Path: "/d/nope/x"}, control.FSWriteDone{IO: 14, Err: control.ErrNotFound}},
		{control.FSWrite{IO: 15, Path: "/d/obj/c"}, control.FSWriteDone{IO: 15}},
		{control.FSList{IO: 16, Dir: "/d/obj"}, control.FSListDone{IO: 16, Names: []string{"a", "c"}}},
		{control.FSList{IO: 17, Dir: "/d/tmp"}, control.FSListDone{IO: 17}},
		{control.FSList{IO: 18, Dir: "/d/nope"}, control.FSListDone{IO: 18, Err: control.ErrNotFound}},
		{control.FSUnlink{IO: 19, Path: "/d/obj/c"}, control.FSUnlinkDone{IO: 19}},
		{control.FSUnlink{IO: 20, Path: "/d/obj/c"}, control.FSUnlinkDone{IO: 20, Err: control.ErrNotFound}},
	}
	for _, s := range steps {
		got := fs.Exec(s.a)
		if !equal(got, s.want) {
			t.Fatalf("%#v:\n got  %#v\n want %#v", s.a, got, s.want)
		}
	}
	if got := fs.Paths(); !slices.Equal(got, []string{"/d/obj/a"}) {
		t.Fatalf("Paths = %q", got)
	}
	if d, ok := fs.ReadFile("/d/obj/c"); ok {
		t.Fatalf("unlinked file still readable: %q", d)
	}
}

func equal(a, b control.Feedback) bool {
	ra, aok := a.(control.FSReadDone)
	rb, bok := b.(control.FSReadDone)
	if aok && bok {
		return ra.IO == rb.IO && ra.Err == rb.Err && string(ra.Data) == string(rb.Data)
	}
	la, aok := a.(control.FSListDone)
	lb, bok := b.(control.FSListDone)
	if aok && bok {
		return la.IO == lb.IO && la.Err == lb.Err && slices.Equal(la.Names, lb.Names)
	}
	return a == b
}

func TestFSNeverAliases(t *testing.T) {
	fs := NewFS("/d")
	in := []byte("abc")
	fs.Exec(control.FSWrite{IO: 1, Path: "/d/k", Data: in})
	in[0] = 'X'
	r := fs.Exec(control.FSRead{IO: 2, Path: "/d/k"}).(control.FSReadDone)
	if string(r.Data) != "abc" {
		t.Fatalf("write did not copy: %q", r.Data)
	}
	r.Data[1] = 'Y'
	if d, _ := fs.ReadFile("/d/k"); string(d) != "abc" {
		t.Fatalf("read did not copy: %q", d)
	}
	d, _ := fs.ReadFile("/d/k")
	d[2] = 'Z'
	if d2, _ := fs.ReadFile("/d/k"); string(d2) != "abc" {
		t.Fatalf("ReadFile did not copy: %q", d2)
	}
}
