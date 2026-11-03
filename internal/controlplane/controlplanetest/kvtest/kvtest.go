// Package kvtest gives tests the key-value stores to run on: the in-memory one
// and Pebble in a temporary folder.
package kvtest

import (
	"path/filepath"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/kv/pebblestore"
)

// NewPebble opens a Pebble store in a temporary folder that is removed at the end of the test.
func NewPebble(t testing.TB) kv.Store {
	t.Helper()
	keyValue, err := pebblestore.Open(filepath.Join(t.TempDir(), "state"), pebblestore.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = keyValue.Close() })
	return keyValue
}

// Backends are the stores that a semantic test runs on, by name.
var Backends = map[string]func(testing.TB) kv.Store{
	"mem":    func(testing.TB) kv.Store { return kv.NewMemory() },
	"pebble": NewPebble,
}
