package broker

import (
	"bytes"
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/wire/kafkawire"
	"github.com/manifesterio/mds/broker/internal/wire/segmentformat"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/shared/objectstore"
)

// countGets counts the object GETs among acts.
func countGets(acts []core.Action) int {
	n := 0
	for _, a := range acts {
		if oc, ok := a.(core.ObjectCall); ok {
			if _, isGet := oc.Call.(objectstore.Get); isGet {
				n++
			}
		}
	}
	return n
}

// served is batch b as a Fetch returns it at base offset base.
func served(b []byte, base int64) []byte {
	return kafkawire.ChangeBatchOffset(b, base, 0)
}

// The seal puts each partition's batches together. Batches of one partition
// keep their arrival order.
func TestSealSortsPartitionsContiguous(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	b1, b2, b3 := plain(t, "a", "b"), plain(t, "c"), plain(t, "d", "e", "f")
	h.send(c, verProduce, prodReq(-1, e("t", 0, b1), e("t", 1, b2), e("t", 0, b3)))
	h.flush()
	var objects [][]byte
	for _, a := range h.all {
		if oc, ok := a.(core.ObjectCall); ok {
			if put, isPut := oc.Call.(objectstore.Put); isPut {
				objects = append(objects, put.Data)
			}
		}
	}
	if len(objects) != 1 {
		t.Fatalf("%d objects for one flush", len(objects))
	}
	seg, err := segmentformat.Decode(objects[0])
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		topicPartition contract.TopicPartition
		data           []byte
	}{
		{contract.NewTopicPartition("t", 0), b1},
		{contract.NewTopicPartition("t", 0), b3},
		{contract.NewTopicPartition("t", 1), b2},
	}
	if len(seg.Batches) != len(want) {
		t.Fatalf("%d batches in the object, want %d", len(seg.Batches), len(want))
	}
	for i, w := range want {
		g := seg.Batches[i]
		if g.TopicPartition != w.topicPartition || !bytes.Equal(g.Data, w.data) {
			t.Fatalf("object batch %d: %+v", i, g.TopicPartition)
		}
	}
	// Offsets still follow the arrival order of each partition.
	ps := produced(h.next(c))
	if ps[0].BaseOffset != 0 || ps[1].BaseOffset != 0 || ps[2].BaseOffset != 2 {
		t.Fatalf("base offsets %d %d %d", ps[0].BaseOffset, ps[1].BaseOffset, ps[2].BaseOffset)
	}
	h.noViolations()
}

// Adjacent batches of one partition are read with one GET, and the bytes
// are the same as one GET per batch.
func TestFetchCoalescesAdjacentBatches(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	b1, b2, b3 := plain(t, "a", "b"), plain(t, "c"), plain(t, "d", "e", "f")
	h.send(c, verProduce, prodReq(-1, e("t", 0, b1), e("t", 1, b2), e("t", 0, b3)))
	h.flush()
	produced(h.next(c))
	before := len(h.all)
	p := fetched(h.call(c, verFetch, fetchReq(contract.NewTopicPartition("t", 0), 0, 1<<20)))
	gets := countGets(h.all[before:])
	if gets != 1 {
		t.Fatalf("%d GETs for 2 adjacent batches, want 1", gets)
	}
	want := append(served(b1, 0), served(b3, 2)...)
	if !bytes.Equal(p.RecordBatches, want) {
		t.Fatalf("fetch bytes differ from the batches: %d bytes, want %d", len(p.RecordBatches), len(want))
	}
	h.noViolations()
}

// A merged read with one bad batch is never served. The whole read is
// retried, and then the good bytes are served.
func TestCorruptMergedReadRetried(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	b1, b3 := plain(t, "a", "b"), plain(t, "d", "e", "f")
	h.send(c, verProduce, prodReq(-1, e("t", 0, b1), e("t", 0, b3)))
	h.flush()
	produced(h.next(c))
	before := len(h.all)
	merged := int64(len(b1) + len(b3))
	corrupt := true
	h.objHook = func(a core.IOCall, run func() core.IOResult) core.IOResult {
		d := run()
		if g, ok := getDone(d); ok && corrupt {
			corrupt = false
			g.Data = bytes.Clone(g.Data)
			g.Data[len(g.Data)-1] ^= 0x55 // the last byte is in the second batch
			return core.ObjectDone{Result: g}
		}
		return d
	}
	h.send(c, verFetch, fetchReq(contract.NewTopicPartition("t", 0), 0, 1<<20))
	if len(c.out) != 0 {
		t.Fatal("corrupt merged read was served")
	}
	h.fireAll()
	p := fetched(h.next(c))
	if n := len(factsNamed(h.all, "corruption_detected")); n != 1 {
		t.Fatalf("%d corruption facts, want 1", n)
	}
	if gets := countGets(h.all[before:]); gets != 2 {
		t.Fatalf("%d GETs: want the merged read twice (corrupt, then retry)", gets)
	}
	for _, a := range h.all[before:] {
		if oc, ok := a.(core.ObjectCall); ok {
			if g, isGet := oc.Call.(objectstore.Get); isGet && g.Ref.Len != merged {
				t.Fatalf("GET of %d bytes, want the merged %d", g.Ref.Len, merged)
			}
		}
	}
	want := append(served(b1, 0), served(b3, 2)...)
	if !bytes.Equal(p.RecordBatches, want) {
		t.Fatal("retried read served wrong bytes")
	}
	h.noViolations()
}
