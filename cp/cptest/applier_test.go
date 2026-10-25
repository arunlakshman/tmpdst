package cptest

import (
	"path/filepath"
	"strconv"
	"testing"

	"manifester/cp/applier"
	"manifester/cp/applier/pebblekv"
)

// Conformance C3 for the production applier (decision 49): the same random
// operations against cp/applier as a cp.Store and a fresh veritas.ControlPlane.

func TestConformanceApplierMem(t *testing.T) {
	run(t, func() impl {
		return storeImpl{&applier.Store{A: applier.New(applier.NewMemKV())}}
	}, 200, 200)
}

func TestConformanceApplierPebble(t *testing.T) {
	dir := t.TempDir()
	n := 0
	run(t, func() impl {
		n++
		kv, err := pebblekv.Open(filepath.Join(dir, strconv.Itoa(n)), pebblekv.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = kv.Close() })
		return storeImpl{&applier.Store{A: applier.New(kv)}}
	}, 100, 20)
}
