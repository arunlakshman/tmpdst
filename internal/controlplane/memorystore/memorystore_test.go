package memorystore

import (
	"context"
	"slices"
	"sync"
	"testing"

	"mds/internal/core"
)

var tpA = core.NewTopicPartition("t", 0)

func batch(topicPartition core.TopicPartition, n int64) core.PendingBatch {
	return core.PendingBatch{TopicPartition: topicPartition, Records: n, Loc: core.SegmentRef{Key: "o", Len: n * 10}}
}

func TestCommitLookupEnd(t *testing.T) {
	ctx := t.Context()
	s := New()
	if code, err := s.CreateTopic(ctx, core.CreateTopic{Name: "t", Partitions: 2}); code != core.OK || err != nil {
		t.Fatal(code, err)
	}
	r, code, err := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 3), batch(tpA, 2)}})
	if code != core.OK || err != nil || len(r.Assigned) != 2 || r.Assigned[0].Base != 0 || r.Assigned[1].Base != 3 {
		t.Fatalf("%+v %v %v", r, code, err)
	}
	// replay: same answer, even after the end moved; different request: conflict
	if _, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k2", Batches: []core.PendingBatch{batch(tpA, 1)}}); code != core.OK {
		t.Fatal(code)
	}
	r2, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 3), batch(tpA, 2)}})
	if code != core.OK || !slices.Equal(r2.Assigned, r.Assigned) {
		t.Fatalf("replay %+v %v", r2, code)
	}
	if _, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: "k1", Batches: []core.PendingBatch{batch(tpA, 1)}}); code != core.ErrConflict {
		t.Fatalf("other request under key: %v", code)
	}
	l, code, _ := s.Lookup(ctx, core.FindSegments{TopicPartition: tpA, From: 3})
	if code != core.OK || l.End != 6 || len(l.Ranges) != 2 || l.Ranges[0].Base != 3 || l.Ranges[1].Base != 5 {
		t.Fatalf("lookup %+v %v", l, code)
	}
	d, err := s.DescribeTopics(ctx, core.DescribeTopics{})
	if err != nil || len(d.Topics) != 1 || d.Topics[0].Partitions != 2 {
		t.Fatalf("describe %+v %v", d, err)
	}
}

// Concurrent committers on one partition get contiguous, disjoint offsets.
func TestConcurrentCommittersContiguous(t *testing.T) {
	ctx := t.Context()
	s := New()
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			if _, code, _ := s.CommitSegment(ctx, core.CommitSegment{SegmentKey: string(rune('a' + i)), Batches: []core.PendingBatch{batch(tpA, 2), batch(tpA, 1)}}); code != core.OK {
				t.Error(code)
			}
		})
	}
	wg.Wait()
	l, _, _ := s.Lookup(ctx, core.FindSegments{TopicPartition: tpA})
	next := core.Offset(0)
	for _, r := range l.Ranges {
		if r.Base != next {
			t.Fatalf("gap or overlap at %d, want %d", r.Base, next)
		}
		next += core.Offset(r.Count)
	}
	if l.End != 48 || len(l.Ranges) != 32 {
		t.Fatalf("%+v", l)
	}
}

func TestCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := New().CommitSegment(ctx, core.CommitSegment{}); err == nil {
		t.Fatal("want ctx error")
	}
}
