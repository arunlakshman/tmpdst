// Package vsys adapts the veritas specs to the production seams in sys, so a
// production Executor can run against them with no network. Unlike veritas it
// is not deterministic (context, sync): calls are serialized by a mutex.
package vsys

import (
	"context"
	"sync"

	"manifester/core"
	"manifester/sys"
	"manifester/veritas"
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

// ObjectStore is a sys.ObjectStore over veritas.ObjectStore.
type ObjectStore struct {
	mu   sync.Mutex
	spec *veritas.ObjectStore
}

// NewObjectStore returns an empty store.
func NewObjectStore() *ObjectStore { return &ObjectStore{spec: veritas.NewObjectStore()} }

func (s *ObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(core.ObjPut{Key: key, Data: data}).(core.ObjPutDone).Err)
}

func (s *ObjectStore) Get(ctx context.Context, ref core.SegmentRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.spec.Exec(core.ObjGet{Ref: ref}).(core.ObjGetDone)
	return d.Data, fail(d.Err)
}

func (s *ObjectStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(core.ObjDelete{Key: key}).(core.ObjDeleteDone).Err)
}

func (s *ObjectStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.spec.Exec(core.ObjList{Prefix: prefix}).(core.ObjListDone)
	return d.Keys, fail(d.Err)
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

func (c *ControlPlane) Call(ctx context.Context, op core.CPOp) (core.CPResp, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	d := c.spec.Exec(core.CPCall{Op: op}).(core.CPDone)
	return d.Resp, fail(d.Err)
}

var (
	_ sys.ObjectStore  = (*ObjectStore)(nil)
	_ sys.ControlPlane = (*ControlPlane)(nil)
)
