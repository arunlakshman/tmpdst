package vsys

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"

	"manifester/core"
)

func code(err error) core.ErrCode {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	if err == nil {
		return core.OK
	}
	return core.ErrIO
}

func TestObjectStore(t *testing.T) {
	ctx := context.Background()
	s := NewObjectStore()
	if err := s.Put(ctx, "a/1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	s.Put(ctx, "b", nil)
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
	ctx := context.Background()
	c := NewControlPlane()
	tp := core.TopicPartition{Topic: "t"}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Read the end, propose it as the base, retry on a lost compare-and-set.
			for try := 0; try < 100; try++ {
				resp, err := c.Call(ctx, core.EndOffsets{TPs: []core.TopicPartition{tp}})
				if err != nil {
					t.Error(err)
					return
				}
				base := resp.(core.EndOffsetsResp).Ends[0]
				_, err = c.Call(ctx, core.CommitSegment{
					SegmentKey: string(rune('a' + i)),
					Batches:    []core.PendingBatch{{TP: tp, Base: base, Records: 2}},
				})
				if code(err) == core.ErrConflict {
					continue
				}
				if err != nil {
					t.Error(err)
				}
				return
			}
			t.Error("never won the compare-and-set")
		}()
	}
	wg.Wait()
	resp, _ := c.Call(ctx, core.Lookup{TP: tp})
	if end := resp.(core.LookupResp).End; end != 16 {
		t.Fatalf("end %d", end)
	}
	_, err := c.Call(ctx, core.CommitSegment{SegmentKey: "x", Batches: []core.PendingBatch{{TP: tp, Base: 0, Records: 1}}})
	if code(err) != core.ErrConflict {
		t.Fatalf("taken base: %v", err)
	}
	if err := c.CreateTopic("", 1); code(err) != core.ErrInvalid {
		t.Fatalf("create: %v", err)
	}
}
