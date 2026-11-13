// Package specsystem adapts the specs (controlplane/spec and shared/objectstore) to the production seams in sys, so a
// production Executor can run against them with no network. Unlike speccheck it
// is not deterministic (context, sync): calls are serialized by a mutex.
package specsystem

import (
	"context"
	"sync"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/system"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/spec"
	"github.com/manifesterio/mds/shared/objectstore"
)

// Error is a spec failure as a Go error; Code is what a Classifier maps it to.
type Error struct{ Code core.ErrCode }

func (e *Error) Error() string { return "speccheck: " + e.Code.String() }

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

// ObjectStore is a system.ObjectStore over objectstore.Spec.
type ObjectStore struct {
	mu   sync.Mutex
	spec *objectstore.Spec
}

// NewObjectStore returns an empty store.
func NewObjectStore() *ObjectStore { return &ObjectStore{spec: objectstore.NewSpec()} }

// Put stores data under key.
func (s *ObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(objectstore.Put{Key: key, Data: data}).(objectstore.PutDone).Err)
}

// Get returns the bytes referenced by ref.
func (s *ObjectStore) Get(ctx context.Context, ref objectstore.SegmentRef) ([]byte, error) {
	return locked(ctx, &s.mu, func() ([]byte, core.ErrCode) {
		return s.objectBytes(objectstore.Get{Ref: ref})
	})
}

func (s *ObjectStore) objectBytes(a objectstore.Get) ([]byte, core.ErrCode) {
	d := s.spec.Exec(a).(objectstore.GetDone)
	return d.Data, d.Err
}

// Delete removes the object under key.
func (s *ObjectStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return fail(s.spec.Exec(objectstore.Delete{Key: key}).(objectstore.DeleteDone).Err)
}

// List returns the keys that start with prefix.
func (s *ObjectStore) List(ctx context.Context, prefix string) ([]string, error) {
	return locked(ctx, &s.mu, func() ([]string, core.ErrCode) {
		return s.objectKeys(objectstore.List{Prefix: prefix})
	})
}

func (s *ObjectStore) objectKeys(a objectstore.List) ([]string, core.ErrCode) {
	d := s.spec.Exec(a).(objectstore.ListDone)
	return d.Keys, d.Err
}

// ControlPlane is a system.ControlPlane over the control plane spec
// (controlplane/spec). It takes contract operations and gives contract
// responses, so no conversion is needed.
type ControlPlane struct {
	mu   sync.Mutex
	spec *spec.ControlPlane
}

// NewControlPlane returns an empty control plane.
func NewControlPlane() *ControlPlane { return &ControlPlane{spec: spec.New()} }

// CreateTopic makes a topic (or grows it), serialized.
func (c *ControlPlane) CreateTopic(name string, partitions int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fail(c.spec.Apply(contract.CreateTopic{Name: name, Partitions: partitions}).Err)
}

// Call executes op against the control-plane spec.
func (c *ControlPlane) Call(ctx context.Context, operation contract.Operation) (contract.Response, error) {
	return locked(ctx, &c.mu, func() (contract.Response, core.ErrCode) {
		done := c.spec.Apply(operation)
		return done.Resp, done.Err
	})
}

var (
	_ system.ObjectStore  = (*ObjectStore)(nil)
	_ system.ControlPlane = (*ControlPlane)(nil)
)
