package core

import (
	"testing"

	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/objectstore"
)

// The object store wrappers must write the same bytes into the trace hash as the four
// separate actions and the four separate events did before. Each case below writes the
// old layout by hand, so a change of Tag or Fold shows up as a different hash.

func hashOf(write func(h *trace.Hash)) uint64 {
	h := trace.New()
	write(&h)
	return uint64(h)
}

func TestObjectCallFoldKeepsTheOldBytes(t *testing.T) {
	ref := SegmentRef{Key: "seg/1", Off: 10, Len: -1}
	cases := []struct {
		name string
		call objectstore.Call
		tag  uint64
		old  func(h *trace.Hash)
	}{
		{"put", objectstore.Put{Req: 1, Key: "k", Data: []byte("v")}, 108, func(h *trace.Hash) {
			h.Uint64(108)
			h.Uint64(1)
			h.String("k")
			h.Bytes([]byte("v"))
		}},
		{"get", objectstore.Get{Req: 2, Ref: ref}, 109, func(h *trace.Hash) {
			h.Uint64(109)
			h.Uint64(2)
			foldSegmentRef(h, ref)
		}},
		{"delete", objectstore.Delete{Req: 3, Key: "k"}, 110, func(h *trace.Hash) {
			h.Uint64(110)
			h.Uint64(3)
			h.String("k")
		}},
		{"list", objectstore.List{Req: 4, Prefix: "p/"}, 111, func(h *trace.Hash) {
			h.Uint64(111)
			h.Uint64(4)
			h.String("p/")
		}},
	}
	for _, c := range cases {
		a := ObjectCall{Call: c.call}
		if a.Tag() != c.tag {
			t.Errorf("%s: tag %d, want %d", c.name, a.Tag(), c.tag)
		}
		if a.RequestID() != c.call.RequestID() {
			t.Errorf("%s: request id", c.name)
		}
		if got, want := hashOf(a.Fold), hashOf(c.old); got != want {
			t.Errorf("%s: the hash of the wrapper differs from the old layout", c.name)
		}
	}
}

func TestObjectDoneFoldKeepsTheOldBytes(t *testing.T) {
	cases := []struct {
		name   string
		result objectstore.Result
		tag    uint64
		old    func(h *trace.Hash)
	}{
		{"put", objectstore.PutDone{Req: 9, Key: "k", Err: ErrTimeout, Outcome: MaybeApplied}, 9, func(h *trace.Hash) {
			h.Uint64(9)
			h.Uint64(9)
			h.String("k")
			h.Uint64(uint64(ErrTimeout))
			h.Uint64(uint64(MaybeApplied))
		}},
		{"get", objectstore.GetDone{Req: 9, Data: []byte("d"), Err: ErrNotFound}, 10, func(h *trace.Hash) {
			h.Uint64(10)
			h.Uint64(9)
			h.Bytes([]byte("d"))
			h.Uint64(uint64(ErrNotFound))
		}},
		{"delete", objectstore.DeleteDone{Req: 9, Err: ErrIO, Outcome: Applied}, 11, func(h *trace.Hash) {
			h.Uint64(11)
			h.Uint64(9)
			h.Uint64(uint64(ErrIO))
			h.Uint64(uint64(Applied))
		}},
		{"list", objectstore.ListDone{Req: 9, Keys: []string{"a", "b"}, Err: ErrUnavailable}, 12, func(h *trace.Hash) {
			h.Uint64(12)
			h.Uint64(9)
			h.Uint64(uint64(ErrUnavailable))
			h.Uint64(2)
			h.String("a")
			h.String("b")
		}},
	}
	for _, c := range cases {
		e := ObjectDone{Result: c.result}
		if e.Tag() != c.tag {
			t.Errorf("%s: tag %d, want %d", c.name, e.Tag(), c.tag)
		}
		if e.RequestID() != c.result.RequestID() {
			t.Errorf("%s: request id", c.name)
		}
		if got, want := hashOf(e.Fold), hashOf(c.old); got != want {
			t.Errorf("%s: the hash of the wrapper differs from the old layout", c.name)
		}
	}
}
