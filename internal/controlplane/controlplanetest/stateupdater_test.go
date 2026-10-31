package controlplanetest

import (
	"path/filepath"
	"strconv"
	"testing"

	"manifester/internal/controlplane/stateupdater"
	"manifester/internal/controlplane/stateupdater/pebblestore"
)

// Conformance C3 for the production applier (decision 49): the shared control
// plane suite runs on controlplane/stateupdater as a controlplane.Store. The applier also has its own
// test with the StateUpdater type directly, in package stateupdater.

func TestConformanceStateUpdaterMem(t *testing.T) {
	runProfiles(t, func() impl {
		return storeImpl{&stateupdater.Store{A: stateupdater.New(stateupdater.NewMemoryKeyValueStore())}}
	}, 20)
}

// Pebble is slower, so it runs fewer seeds. MANIFESTER_SEEDS still overrides.
func TestConformanceStateUpdaterPebble(t *testing.T) {
	dir := t.TempDir()
	n := 0
	runProfiles(t, func() impl {
		n++
		kv, err := pebblestore.Open(filepath.Join(dir, strconv.Itoa(n)), pebblestore.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = kv.Close() })
		return storeImpl{&stateupdater.Store{A: stateupdater.New(kv)}}
	}, 3)
}
