package objectstore

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/manifesterio/mds/dst/machine"
	"github.com/manifesterio/mds/shared/status"
)

func put(s *Spec, key, data string) PutDone {
	return s.Exec(Put{Req: 1, Key: key, Data: []byte(data)}).(PutDone)
}

func get(s *Spec, key string, off, n int64) GetDone {
	return s.Exec(Get{Req: 2, Ref: SegmentRef{Key: key, Off: off, Len: n}}).(GetDone)
}

func TestSpecRangeGet(t *testing.T) {
	s := NewSpec()
	if d := put(s, "k", "abcdef"); d.Err != status.OK || d.Outcome != status.Applied || d.Req != 1 {
		t.Fatalf("put: %+v", d)
	}
	for _, c := range []struct {
		off, n int64
		want   string
		err    status.ErrCode
	}{
		{0, 0, "abcdef", status.OK},
		{0, -1, "abcdef", status.OK},
		{2, 0, "cdef", status.OK},
		{2, 3, "cde", status.OK},
		{3, 3, "def", status.OK},
		{6, 0, "", status.OK},
		{3, 4, "", status.ErrInvalid},
		{7, 0, "", status.ErrInvalid},
		{-1, 2, "", status.ErrInvalid},
	} {
		d := get(s, "k", c.off, c.n)
		if d.Err != c.err || string(d.Data) != c.want {
			t.Errorf("get off=%d len=%d: %q %v, want %q %v", c.off, c.n, d.Data, d.Err, c.want, c.err)
		}
	}
	if d := get(s, "nope", 0, 0); d.Err != status.ErrNotFound {
		t.Fatalf("missing: %+v", d)
	}
}

func TestSpecPutCopiesAndOverwrites(t *testing.T) {
	s := NewSpec()
	buf := []byte("xy")
	s.Exec(Put{Key: "k", Data: buf})
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
	if d := put(s, "", "x"); d.Err != status.ErrInvalid || d.Outcome != status.NotApplied {
		t.Fatalf("empty key: %+v", d)
	}
}

func TestSpecDeleteAndHash(t *testing.T) {
	s := NewSpec()
	empty := s.StateHash()
	put(s, "k", "v")
	if s.StateHash() == empty {
		t.Fatal("hash unchanged by put")
	}
	if d := s.Exec(Delete{Req: 3, Key: "k"}).(DeleteDone); d.Err != status.OK || d.Outcome != status.Applied {
		t.Fatalf("delete: %+v", d)
	}
	if s.StateHash() != empty {
		t.Fatal("hash not restored by delete")
	}
	if d := s.Exec(Delete{Req: 4, Key: "k"}).(DeleteDone); d.Err != status.ErrNotFound || d.Outcome != status.NotApplied {
		t.Fatalf("delete missing: %+v", d)
	}
	if s.StateHash() != empty {
		t.Fatal("failed delete changed hash")
	}
	// Same contents by different histories hash alike.
	a, b := NewSpec(), NewSpec()
	put(a, "x", "1")
	put(a, "y", "2")
	put(b, "y", "2")
	put(b, "x", "1")
	if a.StateHash() != b.StateHash() {
		t.Fatal("hash depends on insertion order")
	}
}

func TestSpecListOrdering(t *testing.T) {
	s := NewSpec()
	for _, k := range []string{"b", "a/2", "a/10", "a", "c/1", "a/1"} {
		put(s, k, "")
	}
	list := func(p string) []string {
		return s.Exec(List{Prefix: p}).(ListDone).Keys
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

// TestIncrementalStateHashMatchesFull drives random call sequences and checks
// the prefix-cached StateHash against a from-scratch fold, hashing at random
// points so several changes can pile up between two hashes.
func TestIncrementalStateHashMatchesFull(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		r := rand.New(rand.NewPCG(seed, 0)) //nolint:gosec // test data from a fixed seed, not security
		s := NewSpec()
		for step := range 300 {
			switch r.IntN(5) {
			case 0, 1, 2:
				put(s, fmt.Sprintf("seg/%d", r.IntN(40)), strconv.Itoa(r.IntN(1000)))
			default:
				s.Exec(Delete{Req: 3, Key: fmt.Sprintf("seg/%d", r.IntN(40))})
			}
			if r.IntN(3) == 0 {
				if got, want := s.StateHash(), s.fullStateHash(); got != want {
					t.Fatalf("seed %d step %d: object store %x != full %x", seed, step, got, want)
				}
			}
		}
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

func (m *sliceStore) Exec(call Call) Result {
	switch c := call.(type) {
	case Put:
		if c.Key == "" {
			return PutDone{Req: c.Req, Key: c.Key, Err: status.ErrInvalid}
		}
		i, ok := m.find(c.Key)
		v := append([]byte{}, c.Data...)
		if ok {
			m.vals[i] = v
		} else {
			m.keys = slices.Insert(m.keys, i, c.Key)
			m.vals = slices.Insert(m.vals, i, v)
		}
		return PutDone{Req: c.Req, Key: c.Key, Outcome: status.Applied}
	case Get:
		i, ok := m.find(c.Ref.Key)
		if !ok {
			return GetDone{Req: c.Req, Err: status.ErrNotFound}
		}
		v, off := m.vals[i], c.Ref.Off
		n := int64(len(v)) - off
		if c.Ref.Len > 0 {
			n = c.Ref.Len
		}
		if off < 0 || n < 0 || off+n > int64(len(v)) {
			return GetDone{Req: c.Req, Err: status.ErrInvalid}
		}
		return GetDone{Req: c.Req, Data: append([]byte{}, v[off:off+n]...)}
	case Delete:
		i, ok := m.find(c.Key)
		if !ok {
			return DeleteDone{Req: c.Req, Err: status.ErrNotFound}
		}
		m.keys = slices.Delete(m.keys, i, i+1)
		m.vals = slices.Delete(m.vals, i, i+1)
		return DeleteDone{Req: c.Req, Outcome: status.Applied}
	case List:
		var out []string
		for i := sort.SearchStrings(m.keys, c.Prefix); i < len(m.keys) && strings.HasPrefix(m.keys[i], c.Prefix); i++ {
			out = append(out, m.keys[i])
		}
		return ListDone{Req: c.Req, Keys: out}
	}
	panic("sliceStore: unknown call")
}

func (m *sliceStore) Keys() []string { return slices.Clone(m.keys) }

// execKeys is what the lockstep check needs from a store.
type execKeys interface {
	Exec(Call) Result
	Keys() []string
}

// randomCall makes the i-th call of a sequence. The few keys and prefixes make
// the calls meet each other often.
func randomCall(r *rand.Rand, i int) Call {
	keys := []string{"", "a", "a/b", "a/c", "b"}
	prefixes := []string{"", "a", "a/", "b", "z"}
	request, key := machine.RequestID(i+1), keys[r.IntN(len(keys))]
	switch r.IntN(7) {
	case 0, 1, 2:
		data := make([]byte, r.IntN(5))
		for j := range data {
			data[j] = byte('a' + r.IntN(26))
		}
		return Put{Req: request, Key: key, Data: data}
	case 3, 4:
		return Get{Req: request, Ref: SegmentRef{Key: key, Off: int64(r.IntN(7) - 1), Len: int64(r.IntN(7) - 1)}}
	case 5:
		return Delete{Req: request, Key: key}
	}
	return List{Req: request, Prefix: prefixes[r.IntN(len(prefixes))]}
}

// lockstep runs 60 random calls on impl and on the sorted-slice model. It returns
// "" when they agree, else a sentence that names the first difference.
func lockstep(seed uint64, impl execKeys) string {
	r := rand.New(rand.NewPCG(seed, 1)) //nolint:gosec // test data from a fixed seed, not security
	model := &sliceStore{}
	for i := range 60 {
		call := randomCall(r, i)
		got, want := fmt.Sprintf("%#v", impl.Exec(call)), fmt.Sprintf("%#v", model.Exec(call))
		if got != want {
			return fmt.Sprintf("seed %d, call %d %#v: impl %s, model %s", seed, i, call, got, want)
		}
		if a, b := impl.Keys(), model.Keys(); !slices.Equal(a, b) {
			return fmt.Sprintf("seed %d, call %d %#v: keys %v, model %v", seed, i, call, a, b)
		}
	}
	return ""
}

func TestSpecVsSortedSlice(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		if msg := lockstep(seed, NewSpec()); msg != "" {
			t.Fatal(msg)
		}
	}
}

// brokenSpec ignores Off when Len <= 0.
type brokenSpec struct{ *Spec }

func (b brokenSpec) Exec(call Call) Result {
	if g, ok := call.(Get); ok && g.Ref.Len <= 0 {
		g.Ref.Off = 0
		return b.Spec.Exec(g)
	}
	return b.Spec.Exec(call)
}

// TestSortedSliceCheckFindsInjectedBug proves that the lockstep check can fail.
func TestSortedSliceCheckFindsInjectedBug(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		if lockstep(seed, brokenSpec{NewSpec()}) != "" {
			return
		}
	}
	t.Fatal("injected bug not found")
}
