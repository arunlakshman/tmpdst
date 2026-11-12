package topic

import (
	"slices"
	"testing"

	"github.com/manifesterio/mds/controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds/controlplane/internal/controlplane/metadb/engine/memory"
	"github.com/manifesterio/mds/controlplane/internal/runtime"
)

func TestCorruptStoredRecordsPanic(t *testing.T) {
	store := memory.New()
	topicPartition := contract.NewTopicPartition("t", 0)
	_ = store.Update(func(transaction engine.Txn) {
		transaction.Set(batchRunKey(topicPartition, 0), []byte{9, 9})
		transaction.Set(endKey(topicPartition), []byte{5})
		transaction.Set(duplicateCheckKey("k"), []byte{0x80})
	})
	for name, f := range map[string]func(){
		"lookup": func() {
			_ = store.View(func(transaction engine.Txn) {
				FindSegments(transaction, contract.FindSegments{TopicPartition: topicPartition})
			})
		},
		"replay": func() {
			_ = store.Update(func(transaction engine.Txn) { CommitSegment(transaction, 1, contract.CommitSegment{SegmentKey: "k"}) })
		},
		"known": func() {
			_ = store.View(func(transaction engine.Txn) { Known(transaction, contract.CommitSegment{SegmentKey: "k"}) })
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic on a corrupt record", name)
				}
			}()
			f()
		}()
	}
}

// TestApplyWritesTheStateRecords pins the bytes that Apply writes for one
// commit that spans two partitions of a new topic: the state bytes must not
// change when the rules move.
func TestApplyWritesTheStateRecords(t *testing.T) {
	store := memory.New()
	tp0, tp1 := contract.NewTopicPartition("t", 0), contract.NewTopicPartition("t", 1)
	op := contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: tp1, Records: 2, Loc: segRef("s", 0, 10)},
		{TopicPartition: tp0, Records: 3, Loc: segRef("s", 10, 20)},
		{TopicPartition: tp1, Records: 4, Loc: segRef("s", 30, 40)},
	}}
	_ = store.Update(func(transaction engine.Txn) { CommitSegment(transaction, 7, op) })
	want := map[string][]byte{
		string(topicKey("t")):          writeVarint(2),
		string(endKey(tp0)):            writeVarint(3),
		string(endKey(tp1)):            writeVarint(6),
		string(batchRunKey(tp0, 0)):    {runFormat, 1, 1, 's', 1, 0, 3, 20, 40},
		string(batchRunKey(tp1, 0)):    {runFormat, 1, 1, 's', 2, 0, 2, 0, 20, 0, 4, 60, 80},
		string(duplicateCheckKey("k")): append(append([]byte{7}, checksumOf(op)...), 0, 0, 2),
		string(slotKey(7, "k")):        {1},
	}
	got := map[string][]byte{}
	_ = store.View(func(transaction engine.Txn) {
		transaction.Ascend([]byte{0}, nil, func(k, v []byte) bool {
			got[string(k)] = append([]byte(nil), v...)
			return true
		})
	})
	if len(got) != len(want) {
		t.Fatalf("%d keys, want %d: %q", len(got), len(want), got)
	}
	for k, v := range want {
		if string(got[k]) != string(v) {
			t.Errorf("key %q: %v, want %v", k, got[k], v)
		}
	}
}

// TestFindSegmentsStopsAtMaxBytes: the read stops at the range that reaches
// MaxBytes, not after it, and starts at the range that holds From.
func TestFindSegmentsStopsAtMaxBytes(t *testing.T) {
	store := memory.New()
	tp := contract.NewTopicPartition("t", 0)
	op := contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: tp, Records: 2, Loc: segRef("s", 0, 10)},
		{TopicPartition: tp, Records: 2, Loc: segRef("s", 10, 10)},
		{TopicPartition: tp, Records: 2, Loc: segRef("s", 20, 10)},
	}}
	_ = store.Update(func(transaction engine.Txn) { CommitSegment(transaction, 0, op) })
	for _, c := range []struct {
		from     contract.Offset
		maxBytes int32
		bases    []contract.Offset
	}{
		{0, 0, []contract.Offset{0, 2, 4}},
		{0, 20, []contract.Offset{0, 2}},
		{0, 21, []contract.Offset{0, 2, 4}},
		{3, 10, []contract.Offset{2}},
		{6, 0, nil},
	} {
		var d runtime.ControlPlaneDone
		_ = store.View(func(transaction engine.Txn) {
			d = FindSegments(transaction, contract.FindSegments{TopicPartition: tp, From: c.from, MaxBytes: c.maxBytes})
		})
		resp := d.Resp.(contract.FindSegmentsResp)
		var bases []contract.Offset
		for _, r := range resp.Ranges {
			bases = append(bases, r.Base)
		}
		if resp.End != 6 || !slices.Equal(bases, c.bases) {
			t.Errorf("from %d max %d: end %d bases %v, want %v", c.from, c.maxBytes, resp.End, bases, c.bases)
		}
	}
}

func segRef(key string, off, n int64) runtime.SegmentRef {
	return runtime.SegmentRef{Key: key, Off: off, Len: n}
}

func checksumOf(op contract.CommitSegment) []byte {
	dig := checksum(op.Batches)
	return dig[:]
}
