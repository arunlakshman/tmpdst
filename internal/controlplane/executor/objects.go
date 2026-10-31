package executor

import (
	"context"
	"sync"

	"mds/internal/core"
)

// ObjectStore is the checkpoint object store (S3 in prod). Defined here so
// controlplane packages do not import mds/sys.
type ObjectStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, ref core.SegmentRef) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// MemoryObjects is an in-memory ObjectStore.
type MemoryObjects struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemoryObjects returns an empty object store.
func NewMemoryObjects() *MemoryObjects {
	return &MemoryObjects{data: map[string][]byte{}}
}

// Put implements ObjectStore.
func (o *MemoryObjects) Put(_ context.Context, key string, data []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.data[key] = append([]byte(nil), data...)
	return nil
}

// Get implements ObjectStore.
func (o *MemoryObjects) Get(_ context.Context, ref core.SegmentRef) ([]byte, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	b, ok := o.data[ref.Key]
	if !ok {
		return nil, ErrNotFound
	}
	end := int(ref.Off + ref.Len)
	if ref.Off < 0 || ref.Len < 0 || end > len(b) {
		return nil, ErrNotFound
	}
	if ref.Len == 0 {
		return append([]byte(nil), b[ref.Off:]...), nil
	}
	return append([]byte(nil), b[ref.Off:end]...), nil
}

// Delete implements ObjectStore.
func (o *MemoryObjects) Delete(_ context.Context, key string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.data, key)
	return nil
}

// List implements ObjectStore.
func (o *MemoryObjects) List(_ context.Context, prefix string) ([]string, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []string
	for k := range o.data {
		if len(prefix) == 0 || (len(k) >= len(prefix) && k[:len(prefix)] == prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}
