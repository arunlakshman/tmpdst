package veritas

import (
	"slices"

	"manifester/toys3/control"
	"manifester/toys3/format"
)

// Store is the veritas of the object store (control.Controller seen from a
// client): a map from key to payload. It is sequential and never fails, so it
// never replies 500. The rules (DESIGN.md, control/event.go):
//
//	any op      400 and no change if a key is invalid (format.ValidKey)
//	Put         400 and no change if Data is over MaxObjectSize; else stores
//	            a copy of Data under Key, replacing any object, and 200
//	Get         200 with the payload; 404 if Key is absent
//	Delete      removes Key and 200; 404 if Key is absent
//	Copy        stores Src's payload under Dst and 200, Src == Dst included
//	            (it rewrites the object); 404 and no change if Src is absent
//
// Copy needs no size check: Src got in through Put. Store never aliases a
// caller's slice.
type Store struct {
	// MaxObjectSize bounds Put payloads, as control.Config.MaxObjectSize
	// (0: no limit).
	MaxObjectSize int
	objs          map[string][]byte
}

// NewStore returns an empty store.
func NewStore(maxObjectSize int) *Store {
	return &Store{MaxObjectSize: maxObjectSize, objs: map[string][]byte{}}
}

// Apply runs one request and returns its reply's status and body (the payload
// for a 200 Get, else nil).
func (s *Store) Apply(op control.Op) (control.Status, []byte) {
	switch op := op.(type) {
	case control.Put:
		if !format.ValidKey(op.Key) || (s.MaxObjectSize > 0 && len(op.Data) > s.MaxObjectSize) {
			return control.StatusBadRequest, nil
		}
		s.objs[op.Key] = clone(op.Data)
		return control.StatusOK, nil
	case control.Get:
		if !format.ValidKey(op.Key) {
			return control.StatusBadRequest, nil
		}
		data, ok := s.objs[op.Key]
		if !ok {
			return control.StatusNotFound, nil
		}
		return control.StatusOK, clone(data)
	case control.Delete:
		if !format.ValidKey(op.Key) {
			return control.StatusBadRequest, nil
		}
		if _, ok := s.objs[op.Key]; !ok {
			return control.StatusNotFound, nil
		}
		delete(s.objs, op.Key)
		return control.StatusOK, nil
	case control.Copy:
		if !format.ValidKey(op.Src) || !format.ValidKey(op.Dst) {
			return control.StatusBadRequest, nil
		}
		data, ok := s.objs[op.Src]
		if !ok {
			return control.StatusNotFound, nil
		}
		s.objs[op.Dst] = data // payloads are never mutated in place
		return control.StatusOK, nil
	}
	return control.StatusBadRequest, nil // an Op the store does not know
}

// Keys returns the stored keys, sorted.
func (s *Store) Keys() []string {
	keys := make([]string, 0, len(s.objs))
	for k := range s.objs { //detcheck:ok keys are sorted before use
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// Value returns a copy of key's payload.
func (s *Store) Value(key string) ([]byte, bool) {
	data, ok := s.objs[key]
	if !ok {
		return nil, false
	}
	return clone(data), true
}
