package metadb_test

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb"
)

// TestEntryBytesPinned pins the bytes of one small entry. The log format may
// change (nothing is deployed), but it must not change by accident, for example
// with a new protobuf version. It is the one pin of the log format: the trace
// hashes of the simulator do not read entry bytes.
func TestEntryBytesPinned(t *testing.T) {
	e, _ := metadb.EntryOf(contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true})
	e.ID = "x2"
	const want = "0a0278321a0c0a066f726465727310031801"
	if got := hex.EncodeToString(metadb.Encode(e)); got != want {
		t.Fatalf("the log format changed:\n got %s\nwant %s", got, want)
	}
	for _, e := range oneOfEachKind() {
		back, err := metadb.Decode(metadb.Encode(e))
		if err != nil || !reflect.DeepEqual(back, e) {
			t.Fatalf("round trip: %+v %v, want %+v", back, err, e)
		}
	}
}
