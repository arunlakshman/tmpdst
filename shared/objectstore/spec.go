package objectstore

import (
	"slices"
	"strings"

	"github.com/manifesterio/mds/dst/trace"
	"github.com/manifesterio/mds/shared/status"
)

// Spec is the spec of the object store: a map from key to bytes.
// Sequential and fault-free, so every result is Err OK (or a truthful
// ErrNotFound/ErrInvalid) and Outcome is Applied or NotApplied, never
// MaybeApplied. Rules:
//
//	Put     empty Key: ErrInvalid, no change. Else stores a copy, replacing
//	        any object: OK, Applied.
//	Get     absent Key: ErrNotFound. Reads [Off, Off+Len), Len <= 0 meaning
//	        to the end. Off < 0, Off > size or Off+Len > size: ErrInvalid
//	        (a range outside the object is a malformed request, not a
//	        missing one). Off == size with Len <= 0 gives empty data.
//	Delete  absent Key: ErrNotFound, NotApplied. Else removes it: OK, Applied.
//	List    keys with Prefix, sorted; nil if none. Never fails.
//
// The spec shares the types of this package with the code it checks, and never
// the logic. It does not import any adapter.
type Spec struct {
	objs map[string][]byte
	keys []string // every key, sorted
	// pre[i] is the StateHash state after folding keys[:i]; len(pre)-1 is how
	// many are valid. A change at index i truncates it to i+1 entries.
	pre []trace.Hash
}

// NewSpec returns an empty store.
func NewSpec() *Spec { return &Spec{objs: map[string][]byte{}} }

// touch records that the entry at index i of keys changed (or was inserted or
// removed there), so folded prefix states beyond it are stale.
func (s *Spec) touch(i int) {
	if len(s.pre) > i+1 {
		s.pre = s.pre[:i+1]
	}
}

// Exec runs one call and returns its result.
func (s *Spec) Exec(call Call) Result {
	switch c := call.(type) {
	case Put:
		return s.execPut(c)
	case Get:
		return s.execGet(c)
	case Delete:
		return s.execDelete(c)
	case List:
		return s.execList(c)
	}
	//nolint:forbidigo // impossible: Call is a sealed sum and the switch lists every case
	panic("objectstore.Spec: not an object store call")
}

func (s *Spec) execPut(c Put) Result {
	if c.Key == "" {
		return PutDone{Req: c.Req, Key: c.Key, Err: status.ErrInvalid}
	}
	i, found := slices.BinarySearch(s.keys, c.Key)
	if !found {
		s.keys = slices.Insert(s.keys, i, c.Key)
	}
	s.touch(i)
	s.objs[c.Key] = clone(c.Data)
	return PutDone{Req: c.Req, Key: c.Key, Outcome: status.Applied}
}

func (s *Spec) execGet(c Get) Result {
	data, ok := s.objs[c.Ref.Key]
	if !ok {
		return GetDone{Req: c.Req, Err: status.ErrNotFound}
	}
	got, ok := cutRange(data, c.Ref)
	if !ok {
		return GetDone{Req: c.Req, Err: status.ErrInvalid}
	}
	return GetDone{Req: c.Req, Data: clone(got)}
}

func cutRange(data []byte, ref SegmentRef) ([]byte, bool) {
	end := int64(len(data))
	if ref.Len > 0 {
		end = ref.Off + ref.Len
	}
	if ref.Off < 0 || ref.Off > int64(len(data)) || end > int64(len(data)) {
		return nil, false
	}
	return data[ref.Off:end], true
}

func (s *Spec) execDelete(c Delete) Result {
	if _, ok := s.objs[c.Key]; !ok {
		return DeleteDone{Req: c.Req, Err: status.ErrNotFound}
	}
	i, _ := slices.BinarySearch(s.keys, c.Key)
	s.keys = slices.Delete(s.keys, i, i+1)
	s.touch(i)
	delete(s.objs, c.Key)
	return DeleteDone{Req: c.Req, Outcome: status.Applied}
}

func (s *Spec) execList(c List) Result {
	var keys []string
	for _, k := range s.keys {
		if strings.HasPrefix(k, c.Prefix) {
			keys = append(keys, k)
		}
	}
	return ListDone{Req: c.Req, Keys: keys}
}

// Keys returns every key, sorted.
func (s *Spec) Keys() []string { return slices.Clone(s.keys) }

// StateHash folds the sorted (key, data) pairs. It resumes from the last
// folded prefix a change left intact, so a put near the end of the key order
// costs little.
func (s *Spec) StateHash() uint64 {
	s.pre = nextPrefix(s.pre, len(s.keys), func(i int, h trace.Hash) trace.Hash {
		h.String(s.keys[i])
		h.Bytes(s.objs[s.keys[i]])
		return h
	})
	return uint64(s.pre[len(s.keys)])
}

// fullStateHash is StateHash without the prefix cache (for tests).
func (s *Spec) fullStateHash() uint64 {
	h := trace.New()
	for _, k := range sortedKeys(s.objs) {
		h.String(k)
		h.Bytes(s.objs[k])
	}
	return uint64(h)
}

// clone copies b; the result is never nil, so empty data reads back the same
// whether it was stored as nil or empty.
func clone(b []byte) []byte { return append([]byte{}, b...) }

// sortedKeys returns the keys of m in order, the only way the spec walks a map.
func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m { //determinismcheck:ok keys are sorted before use
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// nextPrefix folds keys[len(pre)-1, n) onto a prefix cache. pre[0] is the
// empty hash. step receives the key index and the hash of the keys before it.
func nextPrefix(pre []trace.Hash, n int, step func(i int, h trace.Hash) trace.Hash) []trace.Hash {
	if len(pre) == 0 {
		pre = append(pre, trace.New())
	}
	for i := len(pre) - 1; i < n; i++ {
		pre = append(pre, step(i, pre[i]))
	}
	return pre
}
