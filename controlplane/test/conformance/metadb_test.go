package conformance

import (
	"bytes"
	"context"
	"path/filepath"
	"strconv"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/pebble"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

// slots runs a metadb.DB as one node with no log: a command gets the next
// local slot, and a read is answered from the store as it is.
type slots struct {
	store *metadb.DB
	next  int64
}

func (s *slots) exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	if err := ctx.Err(); err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	switch s.store.Mode(op) {
	case metadb.Logged:
		s.next++
		return s.store.Write(s.next-1, op)
	case metadb.ReadOnly:
		return s.store.Read(op)
	}
	return runtime.ControlPlaneDone{Err: runtime.ErrInvalid}, nil
}

// TestMetadataMemory is the metadb on the memory engine. Its reboot
// writes a checkpoint, and a new store on a new engine starts from it.
func TestMetadataMemory(t *testing.T) {
	Run(t, func(rt *rapid.T) Target {
		s := &slots{store: metadb.New(memory.New())}
		return Target{Exec: s.exec, Reboot: func() {
			var b bytes.Buffer
			if err := s.store.Snapshot(&b, s.next); err != nil {
				rt.Fatalf("checkpoint: %v", err)
			}
			s.store = metadb.New(memory.New())
			next, err := s.store.Restore(&b)
			if err != nil {
				rt.Fatalf("restore: %v", err)
			}
			s.next = next
		}}
	})
}

// TestMetadataPebble is the metadb on the Pebble engine. Its reboot
// closes the engine and opens it again on the same files.
func TestMetadataPebble(t *testing.T) {
	fewerChecks(t, 20, 20) // each open and close of Pebble takes tens of ms
	dir, n := t.TempDir(), 0
	Run(t, func(rt *rapid.T) Target {
		n++
		path := filepath.Join(dir, strconv.Itoa(n))
		open := func() *metadb.DB {
			db, err := pebble.Open(path, pebble.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
			if err != nil {
				rt.Fatalf("open: %v", err)
			}
			return metadb.New(db)
		}
		s := &slots{store: open()}
		rt.Cleanup(func() { _ = s.store.Close() })
		return Target{Exec: s.exec, Reboot: func() {
			if err := s.store.Close(); err != nil {
				rt.Fatalf("close: %v", err)
			}
			s.store = open()
		}}
	})
}
