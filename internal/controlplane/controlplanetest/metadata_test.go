package controlplanetest

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/controlplanetest/updaterstore"
	"github.com/manifesterio/mds/internal/controlplane/kv"
	"github.com/manifesterio/mds/internal/controlplane/kv/pebblestore"
	"github.com/manifesterio/mds/internal/controlplane/metadata"
)

// Conformance C3 for the production applier (decision 49): the shared control
// plane suite runs on controlplane/metadata through updaterstore. The applier also has its own
// test with the Store type directly, in package metadata.

func TestConformanceMetadataMem(t *testing.T) {
	runProfiles(t, func() impl {
		return &updaterstore.Store{A: metadata.New(kv.NewMemory())}
	}, 20)
}

// Pebble is slower, so it runs fewer seeds. MDS_SEEDS still overrides.
func TestConformanceMetadataPebble(t *testing.T) {
	dir := t.TempDir()
	n := 0
	runProfiles(t, func() impl {
		n++
		keyValue, err := pebblestore.Open(filepath.Join(dir, strconv.Itoa(n)), pebblestore.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = keyValue.Close() })
		return &updaterstore.Store{A: metadata.New(keyValue)}
	}, 3)
}
