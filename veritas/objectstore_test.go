package veritas

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"manifester/core"
	"manifester/veritas/veritastest"
)

func put(s *ObjectStore, key, data string) core.ObjPutDone {
	return s.Exec(core.ObjPut{Req: 1, Key: key, Data: []byte(data)}).(core.ObjPutDone)
}

func get(s *ObjectStore, key string, off, n int64) core.ObjGetDone {
	return s.Exec(core.ObjGet{Req: 2, Ref: core.SegmentRef{Key: key, Off: off, Len: n}}).(core.ObjGetDone)
}

func TestObjectStoreRangeGet(t *testing.T) {
	s := NewObjectStore()
	if d := put(s, "k", "abcdef"); d.Err != core.OK || d.Outcome != core.Applied || d.Req != 1 {
		t.Fatalf("put: %+v", d)
	}
	for _, c := range []struct {
		off, n int64
		want   string
		err    core.ErrCode
	}{
		{0, 0, "abcdef", core.OK},
		{0, -1, "abcdef", core.OK},
		{2, 0, "cdef", core.OK},
		{2, 3, "cde", core.OK},
		{3, 3, "def", core.OK},
		{6, 0, "", core.OK},
		{3, 4, "", core.ErrInvalid},
		{7, 0, "", core.ErrInvalid},
		{-1, 2, "", core.ErrInvalid},
	} {
		d := get(s, "k", c.off, c.n)
		if d.Err != c.err || string(d.Data) != c.want {
			t.Errorf("get off=%d len=%d: %q %v, want %q %v", c.off, c.n, d.Data, d.Err, c.want, c.err)
		}
	}
	if d := get(s, "nope", 0, 0); d.Err != core.ErrNotFound {
		t.Fatalf("missing: %+v", d)
	}
}

func TestObjectStorePutCopiesAndOverwrites(t *testing.T) {
	s := NewObjectStore()
	buf := []byte("xy")
	s.Exec(core.ObjPut{Key: "k", Data: buf})
	buf[0] = 'z'
	d := get(s, "k", 0, 0)
	d.Data[1] = 'q'
	if got := string(get(s, "k", 0, 0).Data); got != "xy" {
		t.Fatalf("aliasing: %q", got)
	}
	put(s, "k", "new")
	if got := string(get(s, "k", 0, 0).Data); got != "new" {
		t.Fatalf("overwrite: %q", got)
	}
	if d := put(s, "", "x"); d.Err != core.ErrInvalid || d.Outcome != core.NotApplied {
		t.Fatalf("empty key: %+v", d)
	}
}

func TestObjectStoreDeleteAndHash(t *testing.T) {
	s := NewObjectStore()
	empty := s.StateHash()
	put(s, "k", "v")
	if s.StateHash() == empty {
		t.Fatal("hash unchanged by put")
	}
	if d := s.Exec(core.ObjDelete{Req: 3, Key: "k"}).(core.ObjDeleteDone); d.Err != core.OK || d.Outcome != core.Applied {
		t.Fatalf("delete: %+v", d)
	}
	if s.StateHash() != empty {
		t.Fatal("hash not restored by delete")
	}
	if d := s.Exec(core.ObjDelete{Req: 4, Key: "k"}).(core.ObjDeleteDone); d.Err != core.ErrNotFound || d.Outcome != core.NotApplied {
		t.Fatalf("delete missing: %+v", d)
	}
	if s.StateHash() != empty {
		t.Fatal("failed delete changed hash")
	}
	// Same contents by different histories hash alike.
	a, b := NewObjectStore(), NewObjectStore()
	put(a, "x", "1")
	put(a, "y", "2")
	put(b, "y", "2")
	put(b, "x", "1")
	if a.StateHash() != b.StateHash() {
		t.Fatal("hash depends on insertion order")
	}
}

func TestObjectStoreListOrdering(t *testing.T) {
	s := NewObjectStore()
	for _, k := range []string{"b", "a/2", "a/10", "a", "c/1", "a/1"} {
		put(s, k, "")
	}
	list := func(p string) []string {
		return s.Exec(core.ObjList{Prefix: p}).(core.ObjListDone).Keys
	}
	if got, want := list(""), []string{"a", "a/1", "a/10", "a/2", "b", "c/1"}; !slices.Equal(got, want) {
		t.Fatalf("all: %v", got)
	}
	if got, want := list("a/"), []string{"a/1", "a/10", "a/2"}; !slices.Equal(got, want) {
		t.Fatalf("a/: %v", got)
	}
	if got := list("zz"); got != nil {
		t.Fatalf("none: %v", got)
	}
}

// ---- lockstep against a sorted-slice model ----

// sliceStore is a different implementation of the same rules: a slice kept
// sorted by key, searched with sort.Search.
type sliceStore struct {
	keys []string
	vals [][]byte
}

func (m *sliceStore) find(k string) (int, bool) {
	i := sort.SearchStrings(m.keys, k)
	return i, i < len(m.keys) && m.keys[i] == k
}

func (m *sliceStore) Exec(a core.IOAction) core.Completion {
	switch a := a.(type) {
	case core.ObjPut:
		if a.Key == "" {
			return core.ObjPutDone{Req: a.Req, Key: a.Key, Err: core.ErrInvalid}
		}
		i, ok := m.find(a.Key)
		v := append([]byte{}, a.Data...)
		if ok {
			m.vals[i] = v
		} else {
			m.keys = slices.Insert(m.keys, i, a.Key)
			m.vals = slices.Insert(m.vals, i, v)
		}
		return core.ObjPutDone{Req: a.Req, Key: a.Key, Outcome: core.Applied}
	case core.ObjGet:
		i, ok := m.find(a.Ref.Key)
		if !ok {
			return core.ObjGetDone{Req: a.Req, Err: core.ErrNotFound}
		}
		v, off := m.vals[i], a.Ref.Off
		n := int64(len(v)) - off
		if a.Ref.Len > 0 {
			n = a.Ref.Len
		}
		if off < 0 || n < 0 || off+n > int64(len(v)) {
			return core.ObjGetDone{Req: a.Req, Err: core.ErrInvalid}
		}
		return core.ObjGetDone{Req: a.Req, Data: append([]byte{}, v[off:off+n]...)}
	case core.ObjDelete:
		i, ok := m.find(a.Key)
		if !ok {
			return core.ObjDeleteDone{Req: a.Req, Err: core.ErrNotFound}
		}
		m.keys = slices.Delete(m.keys, i, i+1)
		m.vals = slices.Delete(m.vals, i, i+1)
		return core.ObjDeleteDone{Req: a.Req, Outcome: core.Applied}
	case core.ObjList:
		var out []string
		for i := sort.SearchStrings(m.keys, a.Prefix); i < len(m.keys) && strings.HasPrefix(m.keys[i], a.Prefix); i++ {
			out = append(out, m.keys[i])
		}
		return core.ObjListDone{Req: a.Req, Keys: out}
	}
	panic("sliceStore: unknown action")
}

type objExec interface {
	Exec(core.IOAction) core.Completion
	Keys() []string
}

type objPair struct{ impl, model objExec }

func (m *sliceStore) Keys() []string { return slices.Clone(m.keys) }

func objCheck(impl func() objExec) veritastest.Check[*objPair, core.IOAction] {
	keys := []string{"", "a", "a/b", "a/c", "b"}
	prefixes := []string{"", "a", "a/", "b", "z"}
	return veritastest.Check[*objPair, core.IOAction]{
		Name: "core.IOAction",
		New:  func() *objPair { return &objPair{impl(), &sliceStore{}} },
		Gen: func(r *veritastest.Rand, i int) core.IOAction {
			req, key := core.ReqID(i+1), keys[r.Intn(len(keys))]
			switch r.Intn(7) {
			case 0, 1, 2:
				data := make([]byte, r.Intn(5))
				for j := range data {
					data[j] = byte('a' + r.Intn(26))
				}
				return core.ObjPut{Req: req, Key: key, Data: data}
			case 3, 4:
				return core.ObjGet{Req: req, Ref: core.SegmentRef{Key: key, Off: int64(r.Intn(7) - 1), Len: int64(r.Intn(7) - 1)}}
			case 5:
				return core.ObjDelete{Req: req, Key: key}
			}
			return core.ObjList{Req: req, Prefix: prefixes[r.Intn(len(prefixes))]}
		},
		Step: func(p *objPair, op core.IOAction) error {
			got, want := fmt.Sprintf("%#v", p.impl.Exec(op)), fmt.Sprintf("%#v", p.model.Exec(op))
			if got != want {
				return fmt.Errorf("impl %s, model %s", got, want)
			}
			return nil
		},
		Invariant: func(p *objPair) error {
			if a, b := p.impl.Keys(), p.model.Keys(); !slices.Equal(a, b) {
				return fmt.Errorf("keys %v, model %v", a, b)
			}
			return nil
		},
		Shrink: func(op core.IOAction) []core.IOAction {
			switch op := op.(type) {
			case core.ObjPut:
				if len(op.Data) > 0 {
					op.Data = nil
					return []core.IOAction{op}
				}
			case core.ObjGet:
				var out []core.IOAction
				if op.Ref.Off != 0 {
					r := op
					r.Ref.Off = 0
					out = append(out, r)
				}
				if op.Ref.Len != 0 {
					r := op
					r.Ref.Len = 0
					out = append(out, r)
				}
				return out
			}
			return nil
		},
		Format: func(op core.IOAction) string { return fmt.Sprintf("%#v", op) },
		Ops:    60,
	}
}

func TestObjectStoreVsSortedSlice(t *testing.T) {
	c := objCheck(func() objExec { return NewObjectStore() })
	veritastest.Run(t, c, veritastest.Options{Seeds: 200, Seed: -1})
}

// brokenStore ignores Off when Len <= 0.
type brokenStore struct{ *ObjectStore }

func (b brokenStore) Exec(a core.IOAction) core.Completion {
	if g, ok := a.(core.ObjGet); ok && g.Ref.Len <= 0 {
		g.Ref.Off = 0
		return b.ObjectStore.Exec(g)
	}
	return b.ObjectStore.Exec(a)
}

func TestObjectStoreCheckShrinksPlantedBug(t *testing.T) {
	c := objCheck(func() objExec { return brokenStore{NewObjectStore()} })
	var f *veritastest.Failure[core.IOAction]
	for seed := uint64(1); f == nil && seed <= 50; seed++ {
		f = c.RunSeed(seed)
	}
	if f == nil {
		t.Fatal("planted bug not found")
	}
	if len(f.Ops) != 2 {
		t.Fatalf("want put+get, got:\n%s", c.Report(f, ""))
	}
	put, ok1 := f.Ops[0].(core.ObjPut)
	get, ok2 := f.Ops[1].(core.ObjGet)
	// Any put then a to-the-end get at Off > 0 shows it; shrinking empties the
	// data and zeroes Len.
	if !ok1 || !ok2 || len(put.Data) != 0 || get.Ref.Off <= 0 || get.Ref.Len != 0 || put.Key != get.Ref.Key {
		t.Fatalf("not minimal:\n%s", c.Report(f, ""))
	}
	t.Logf("example report:\n%s", c.Report(f, "go test ./veritas -run TestObjectStoreCheckShrinksPlantedBug"))
}
