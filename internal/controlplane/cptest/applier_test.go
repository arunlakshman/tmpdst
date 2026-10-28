package cptest

import (
	"path/filepath"
	"strconv"
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/controlplane/applier/pebblekv"
)

// Conformance C3 for the production applier (decision 49): the shared control
// plane suite runs on cp/applier as a cp.Store. The applier also has its own
// test with the StateUpdater type directly, in package applier.

func TestConformanceApplierMem(t *testing.T) {
	runProfiles(t, func() impl {
		return storeImpl{&applier.Store{A: applier.New(applier.NewMemoryKeyValueStore())}}
	}, 20)
}

// Pebble is slower, so it runs fewer seeds. MANIFESTER_SEEDS still overrides.
func TestConformanceApplierPebble(t *testing.T) {
	dir := t.TempDir()
	n := 0
	runProfiles(t, func() impl {
		n++
		kv, err := pebblekv.Open(filepath.Join(dir, strconv.Itoa(n)), pebblekv.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = kv.Close() })
		return storeImpl{&applier.Store{A: applier.New(kv)}}
	}, 3)
}
