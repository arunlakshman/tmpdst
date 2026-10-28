// Package vsys adapts the veritas specs to the production seams in sys, so a
// production Executor can run against them with no network. Unlike veritas it
// is not deterministic (context, sync): calls are serialized by a mutex.
package vsys

import (
	"context"
	"sync"

	"manifester/internal/core"
	"manifester/internal/sys"
	"manifester/internal/verify/veritas"
)

// Error is a spec failure as a Go error; Code is what a Classifier maps it to.
type Error struct{ Code core.ErrCode }

func (e *Error) Error() string { return "veritas: " + e.Code.String() }

func fail(code core.ErrCode) error {
	if code == core.OK {
		return nil
	}
	return &Error{code}
}

// locked runs fn while holding mu, after ctx is checked. fn's code becomes the error.
func locked[T any](ctx context.Context, mu *sync.Mutex, fn func() (T, core.ErrCode)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	mu.Lock()
	defer mu.Unlock()
	v, code := fn()
	return v, fail(code)
}

// ObjectStore is a sys.ObjectStore over veritas.ObjectStore.
type ObjectStore struct {
	mu   sync.Mutex
	spec *veritas.ObjectStore
}

// NewObjectStore returns an empty store.
func NewObjectStore() *ObjectStore { return &ObjectStore{spec: veritas.NewObjectStore()} }

// Put stores data under key.
func (s *ObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(core.ObjPut{Key: key, Data: data}).(core.ObjPutDone).Err)
}

// Get returns the bytes referenced by ref.
func (s *ObjectStore) Get(ctx context.Context, ref core.SegmentRef) ([]byte, error) {
	return locked(ctx, &s.mu, func() ([]byte, core.ErrCode) {
		return s.objectBytes(core.ObjGet{Ref: ref})
	})
}

func (s *ObjectStore) objectBytes(a core.ObjGet) ([]byte, core.ErrCode) {
	d := s.spec.Exec(a).(core.ObjGetDone)
	return d.Data, d.Err
}

// Delete removes the object under key.
func (s *ObjectStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(core.ObjDelete{Key: key}).(core.ObjDeleteDone).Err)
}

// List returns the keys that start with prefix.
func (s *ObjectStore) List(ctx context.Context, prefix string) ([]string, error) {
	return locked(ctx, &s.mu, func() ([]string, core.ErrCode) {
		return s.objectKeys(core.ObjList{Prefix: prefix})
	})
}

func (s *ObjectStore) objectKeys(a core.ObjList) ([]string, core.ErrCode) {
	d := s.spec.Exec(a).(core.ObjListDone)
	return d.Keys, d.Err
}

// ControlPlane is a sys.ControlPlane over veritas.ControlPlane.
type ControlPlane struct {
	mu   sync.Mutex
	spec *veritas.ControlPlane
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane { return &ControlPlane{spec: veritas.NewControlPlane()} }

// CreateTopic is veritas.ControlPlane.CreateTopic, serialized.
func (c *ControlPlane) CreateTopic(name string, partitions int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fail(c.spec.CreateTopic(name, partitions))
}

// Call executes op against the control-plane spec.
func (c *ControlPlane) Call(ctx context.Context, op core.CPOp) (core.CPResp, error) {
	return locked(ctx, &c.mu, func() (core.CPResp, core.ErrCode) {
		return c.callResp(core.CPCall{Op: op})
	})
}

func (c *ControlPlane) callResp(a core.CPCall) (core.CPResp, core.ErrCode) {
	d := c.spec.Exec(a).(core.CPDone)
	return d.Resp, d.Err
}

var (
	_ sys.ObjectStore  = (*ObjectStore)(nil)
	_ sys.ControlPlane = (*ControlPlane)(nil)
)
