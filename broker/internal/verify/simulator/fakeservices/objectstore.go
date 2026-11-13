package fakeservices

import (
	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/dst/check/faulty"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/shared/objectstore"
)

// ObjectStoreFaults are the faults of the simulated object store: the faults of
// faulty.Faults, and one more: a read can come back with one bit flipped.
type ObjectStoreFaults struct {
	faulty.Faults
	// GetCorruptProb is the chance that a Get answer has one bit flipped. The
	// stored object stays whole.
	GetCorruptProb float64
}

// ObjectStore is the simulated object store: objectstore.Spec as the state,
// with the faults of faulty around it. ErrNotFound and ErrInvalid come only
// from the spec.
//
// It is a kernel.Service. A broker node hands it a core.ObjectCall, and the
// service answers with core.ObjectDone.
type ObjectStore struct {
	*faulty.Service[core.ObjectCall, core.EventData, objectstore.Call, objectstore.Result]
	spec *objectstore.Spec
}

var _ kernel.Service[core.ObjectCall, core.EventData] = (*ObjectStore)(nil)

// objectSpec gives objectstore.Spec the Apply method of faulty.Spec.
type objectSpec struct{ *objectstore.Spec }

// Apply runs one call on the spec.
func (s objectSpec) Apply(call objectstore.Call) objectstore.Result { return s.Exec(call) }

// NewObjectStore returns an empty object store with the given faults. The world
// gives it its lanes.
func NewObjectStore(w *kernel.World, f ObjectStoreFaults) *ObjectStore {
	spec := objectstore.NewSpec()
	shape := faulty.Shape[core.ObjectCall, core.EventData, objectstore.Call, objectstore.Result]{
		Name:      "fakeobjectstore",
		Prefix:    "obj",
		Asserts:   objectAsserts,
		Op:        func(c core.ObjectCall) objectstore.Call { return c.Call },
		RequestID: core.ObjectCall.RequestID,
		Mutating:  objectMutating,
		Changed:   objectChanged,
		Reply:     func(_ core.ObjectCall, r objectstore.Result) core.EventData { return core.ObjectDone{Result: r} },
		Fail:      objectFail,
		Corrupt: &faulty.Corruption[objectstore.Call, objectstore.Result]{
			Prob: f.GetCorruptProb,
			Targets: func(c objectstore.Call) bool {
				_, isGet := c.(objectstore.Get)
				return isGet
			},
			Flip:    flipGetBit,
			Kind:    "obj_get_corrupt",
			Reached: objectGetCorrupted,
		},
	}
	return &ObjectStore{Service: faulty.New(w, faulty.Spec[objectstore.Call, objectstore.Result](objectSpec{spec}), f.Faults, shape), spec: spec}
}

// objectMutating reports whether a call may change the stored objects.
func objectMutating(c objectstore.Call) bool {
	switch c.(type) {
	case objectstore.Put, objectstore.Delete:
		return true
	case objectstore.Get, objectstore.List:
		return false
	}
	return false
}

// objectChanged reports whether an answer says that the stored objects changed.
func objectChanged(r objectstore.Result) bool {
	switch r := r.(type) {
	case objectstore.PutDone:
		return r.Outcome == core.Applied
	case objectstore.DeleteDone:
		return r.Outcome == core.Applied
	case objectstore.GetDone, objectstore.ListDone:
		return false
	}
	return false
}

// objectFail builds the answer of a failed call. A read has no outcome.
func objectFail(req core.ObjectCall, f faulty.Failure) core.EventData {
	err, outcome := core.ErrUnavailable, core.NotApplied
	if f == faulty.TimedOut {
		err, outcome = core.ErrTimeout, core.MaybeApplied
	}
	switch c := req.Call.(type) {
	case objectstore.Put:
		return core.ObjectDone{Result: objectstore.PutDone{Req: c.Req, Key: c.Key, Err: err, Outcome: outcome}}
	case objectstore.Get:
		return core.ObjectDone{Result: objectstore.GetDone{Req: c.Req, Err: err}}
	case objectstore.Delete:
		return core.ObjectDone{Result: objectstore.DeleteDone{Req: c.Req, Err: err, Outcome: outcome}}
	case objectstore.List:
		return core.ObjectDone{Result: objectstore.ListDone{Req: c.Req, Err: err}}
	}
	panic("fakeservices: not an object store call")
}

// flipGetBit flips one bit of the data of a Get answer. The spec gives a copy of
// the data, so the stored object stays whole. An answer with an error or with
// no data has nothing to flip.
func flipGetBit(r objectstore.Result, bits uint64) (objectstore.Result, uint64, bool) {
	g, ok := r.(objectstore.GetDone)
	if !ok || g.Err != core.OK || len(g.Data) == 0 {
		return r, 0, false
	}
	bit := bits % uint64(len(g.Data)*8)
	g.Data[bit/8] ^= 1 << (bit % 8)
	return g, bit, true
}

// Object returns a copy of the stored object.
func (s *ObjectStore) Object(key string) ([]byte, bool) {
	g, ok := s.spec.Exec(objectstore.Get{Ref: core.SegmentRef{Key: key}}).(objectstore.GetDone)
	if !ok || g.Err != core.OK {
		return nil, false
	}
	return g.Data, true
}

// EachObject visits every object in sorted key order.
func (s *ObjectStore) EachObject(fn func(key string, data []byte)) {
	for _, k := range s.spec.Keys() {
		if d, ok := s.Object(k); ok {
			fn(k, d)
		}
	}
}
