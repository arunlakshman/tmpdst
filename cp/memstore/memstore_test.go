package memstore

import (
	"context"
	"slices"
	"sync"
	"testing"

	"manifester/core"
)

var tpA = core.TopicPartition{Topic: "t", Partition: 0}

func batch(tp core.TopicPartition, base core.Offset, n int64) core.PendingBatch {
	return core.PendingBatch{TP: tp, Base: base, Records: n, Loc: core.SegmentRef{Key: "o", Len: n * 10}}
}

func TestCommitLookupEnd(t *testing.T) {
	ctx := context.Background()
	s := New()
	if code, err := s.CreateTopic(ctx, "t", 2); code != core.OK || err != nil {
		t.Fatal(code, err)
	}
	r, code, err := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 0, 3), batch(tpA, 3, 2)}})
	if code != core.OK || err != nil || len(r.Assigned) != 2 || r.Assigned[1].Base != 3 {
		t.Fatalf("%+v %v %v", r, code, err)
	}
	// replay: same answer; different request: conflict; taken base: conflict
	r2, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 0, 3), batch(tpA, 3, 2)}})
	if code != core.OK || !slices.Equal(r2.Assigned, r.Assigned) {
		t.Fatalf("replay %+v %v", r2, code)
	}
	if _, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 5, 1)}}); code != core.ErrConflict {
		t.Fatalf("other request under key: %v", code)
	}
	if _, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k2", Batches: []core.PendingBatch{batch(tpA, 2, 1)}}); code != core.ErrConflict {
		t.Fatalf("taken base: %v", code)
	}
	l, code, _ := s.Lookup(ctx, core.Lookup{TP: tpA, From: 3})
	if code != core.OK || l.End != 5 || len(l.Ranges) != 1 || l.Ranges[0].Base != 3 {
		t.Fatalf("lookup %+v %v", l, code)
	}
	e, err := s.EndOffsets(ctx, core.EndOffsets{TPs: []core.TopicPartition{tpA, {Topic: "t", Partition: 1}}})
	if err != nil || !slices.Equal(e.Ends, []core.Offset{5, 0}) {
		t.Fatalf("ends %+v %v", e, err)
	}
	d, err := s.DescribeTopics(ctx, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 2 {
		t.Fatalf("describe %+v %v", d, err)
	}
}

func TestConcurrentWritersRetryOnConflict(t *testing.T) {
	ctx := context.Background()
	s := New()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for try := 0; try < 100; try++ {
				e, err := s.EndOffsets(ctx, core.EndOffsets{TPs: []core.TopicPartition{tpA}})
				if err != nil {
					t.Error(err)
					return
				}
				_, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: string(rune('a' + i)), Batches: []core.PendingBatch{batch(tpA, e.Ends[0], 2)}})
				if code == core.OK {
					return
				}
				if code != core.ErrConflict {
					t.Error(code)
					return
				}
			}
			t.Error("starved")
		}()
	}
	wg.Wait()
	if l, _, _ := s.Lookup(ctx, core.Lookup{TP: tpA}); l.End != 16 || len(l.Ranges) != 8 {
		t.Fatalf("%+v", l)
	}
}

func TestCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := New().CommitSegment(ctx, core.CommitSegment{}); err == nil {
		t.Fatal("want ctx error")
	}
}
