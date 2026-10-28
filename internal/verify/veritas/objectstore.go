package veritas

import (
	"slices"
	"strings"

	"manifester/internal/core"
)

// ObjectStore is the spec of the object store: a map from key to bytes.
// Sequential and fault-free, so every completion is Err OK (or a truthful
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
// Exec panics on an action that is not one of these four.
type ObjectStore struct {
	objs map[string][]byte
	keys []string // every key, sorted
	// pre[i] is the StateHash state after folding keys[:i]; len(pre)-1 is how
	// many are valid. A change at index i truncates it to i+1 entries.
	pre []hasher
}

// NewObjectStore returns an empty store.
func NewObjectStore() *ObjectStore { return &ObjectStore{objs: map[string][]byte{}} }

// touch records that the entry at index i of keys changed (or was inserted or
// removed there), so folded prefix states beyond it are stale.
func (s *ObjectStore) touch(i int) {
	if len(s.pre) > i+1 {
		s.pre = s.pre[:i+1]
	}
}

// Exec runs one action and returns its completion.
func (s *ObjectStore) Exec(a core.IOCall) core.IOResult {
	switch a := a.(type) {
	case core.ObjectPut:
		return s.execPut(a)
	case core.ObjectGet:
		return s.execGet(a)
	case core.ObjectDelete:
		return s.execDelete(a)
	case core.ObjectList:
		return s.execList(a)
	}
	panic("veritas.ObjectStore: not an object store action")
}

func (s *ObjectStore) execPut(a core.ObjectPut) core.IOResult {
	if a.Key == "" {
		return core.ObjectPutDone{Req: a.Req, Key: a.Key, Err: core.ErrInvalid}
	}
	i, found := slices.BinarySearch(s.keys, a.Key)
	if !found {
		s.keys = slices.Insert(s.keys, i, a.Key)
	}
	s.touch(i)
	s.objs[a.Key] = clone(a.Data)
	return core.ObjectPutDone{Req: a.Req, Key: a.Key, Outcome: core.Applied}
}

func (s *ObjectStore) execGet(a core.ObjectGet) core.IOResult {
	data, ok := s.objs[a.Ref.Key]
	if !ok {
		return core.ObjectGetDone{Req: a.Req, Err: core.ErrNotFound}
	}
	got, ok := sliceRange(data, a.Ref)
	if !ok {
		return core.ObjectGetDone{Req: a.Req, Err: core.ErrInvalid}
	}
	return core.ObjectGetDone{Req: a.Req, Data: clone(got)}
}

func sliceRange(data []byte, ref core.SegmentRef) ([]byte, bool) {
	end := int64(len(data))
	if ref.Len > 0 {
		end = ref.Off + ref.Len
	}
	if ref.Off < 0 || ref.Off > int64(len(data)) || end > int64(len(data)) {
		return nil, false
	}
	return data[ref.Off:end], true
}

func (s *ObjectStore) execDelete(a core.ObjectDelete) core.IOResult {
	if _, ok := s.objs[a.Key]; !ok {
		return core.ObjectDeleteDone{Req: a.Req, Err: core.ErrNotFound}
	}
	i, _ := slices.BinarySearch(s.keys, a.Key)
	s.keys = slices.Delete(s.keys, i, i+1)
	s.touch(i)
	delete(s.objs, a.Key)
	return core.ObjectDeleteDone{Req: a.Req, Outcome: core.Applied}
}

func (s *ObjectStore) execList(a core.ObjectList) core.IOResult {
	var keys []string
	for _, k := range s.keys {
		if strings.HasPrefix(k, a.Prefix) {
			keys = append(keys, k)
		}
	}
	return core.ObjectListDone{Req: a.Req, Keys: keys}
}

// Keys returns every key, sorted.
func (s *ObjectStore) Keys() []string { return slices.Clone(s.keys) }

// StateHash folds the sorted (key, data) pairs. It resumes from the last
// folded prefix a change left intact, so a put near the end of the key order
// costs little.
func (s *ObjectStore) StateHash() uint64 {
	s.pre = extendPrefix(s.pre, len(s.keys), func(i int, h hasher) hasher {
		h.Str(s.keys[i])
		h.Bytes(s.objs[s.keys[i]])
		return h
	})
	return uint64(s.pre[len(s.keys)])
}

// fullStateHash is StateHash without the prefix cache (for tests).
func (s *ObjectStore) fullStateHash() uint64 {
	h := newHasher()
	for _, k := range sortedKeys(s.objs) {
		h.Str(k)
		h.Bytes(s.objs[k])
	}
	return uint64(h)
}
