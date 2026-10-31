package specsystem

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"manifester/internal/core"
)

func code(err error) core.ErrCode {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	if err == nil {
		return core.OK
	}
	return core.ErrIO
}

func TestObjectStore(t *testing.T) {
	ctx := t.Context()
	s := NewObjectStore()
	if err := s.Put(ctx, "a/1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_ = s.Put(ctx, "b", nil)
	got, err := s.Get(ctx, core.SegmentRef{Key: "a/1", Off: 1, Len: 3})
	if err != nil || string(got) != "ell" {
		t.Fatalf("get: %q %v", got, err)
	}
	if _, err := s.Get(ctx, core.SegmentRef{Key: "zz"}); code(err) != core.ErrNotFound {
		t.Fatalf("missing: %v", err)
	}
	if keys, _ := s.List(ctx, "a"); !slices.Equal(keys, []string{"a/1"}) {
		t.Fatalf("list: %v", keys)
	}
	if err := s.Delete(ctx, "b"); err != nil || code(s.Delete(ctx, "b")) != core.ErrNotFound {
		t.Fatal("delete")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Put(cancelled, "c", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestControlPlaneConcurrent(t *testing.T) {
	ctx := t.Context()
	c := NewControlPlane()
	tp := core.NewTopicPartition("t", 0)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			_, err := c.Call(ctx, core.CommitSegment{
				SegmentKey: string(rune('a' + i)),
				Batches:    []core.PendingBatch{{TP: tp, Records: 2}},
			})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	resp, _ := c.Call(ctx, core.FindSegments{TP: tp})
	if end := resp.(core.FindSegmentsResp).End; end != 16 {
		t.Fatalf("end %d", end)
	}
	if got := len(resp.(core.FindSegmentsResp).Ranges); got != 8 {
		t.Fatalf("%d ranges", got)
	}
	next := core.Offset(0)
	for _, r := range resp.(core.FindSegmentsResp).Ranges {
		if r.Base != next {
			t.Fatalf("gap or overlap at %d, want %d", r.Base, next)
		}
		next += core.Offset(r.Count)
	}
	_, err := c.Call(ctx, core.CommitSegment{SegmentKey: "a", Batches: []core.PendingBatch{{TP: tp, Records: 1}}})
	if code(err) != core.ErrConflict {
		t.Fatalf("different request under a used key: %v", err)
	}
	if err := c.CreateTopic("", 1); code(err) != core.ErrInvalid {
		t.Fatalf("create: %v", err)
	}
}
