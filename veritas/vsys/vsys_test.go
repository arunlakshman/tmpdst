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
	resp, err := c.Call(ctx, core.Register{Self: core.NodeRef{Node: "n"}})
	if err != nil {
		t.Fatal(err)
	}
	e := resp.(core.RegisterResp).Epoch
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := c.Call(ctx, core.CommitSegment{
				SegmentKey: string(rune('a' + i)), Owner: core.NodeRef{Node: "n"}, Epoch: e,
				Batches: []core.PendingBatch{{TP: core.TopicPartition{Topic: "t"}, Records: 2, Producer: core.NoProducer}},
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	resp, _ = c.Call(ctx, core.Lookup{TP: core.TopicPartition{Topic: "t"}})
	if hwm := resp.(core.LookupResp).HighWatermark; hwm != 16 {
		t.Fatalf("hwm %d", hwm)
	}
	_, err = c.Call(ctx, core.CommitSegment{SegmentKey: "x", Owner: core.NodeRef{Node: "n"}, Epoch: e + 1,
		Batches: []core.PendingBatch{{TP: core.TopicPartition{Topic: "t"}, Records: 1, Producer: core.NoProducer}}})
	if code(err) != core.ErrFenced {
		t.Fatalf("fenced: %v", err)
	}
	if err := c.CreateTopic("", 1); code(err) != core.ErrInvalid {
		t.Fatalf("create: %v", err)
	}
}
