package it

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type tp struct {
	topic string
	part  int32
}

func newClient(t *testing.T, seeds []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(seeds...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(), // the broker rejects idempotent batches (decision 41)
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.MetadataMinAge(100 * time.Millisecond), // default 5s makes broker failover slow
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// sent is what a producer was told about the records it acked.
type sent struct {
	mu    sync.Mutex
	acked map[tp][]*kgo.Record // in ack-callback order
	fails []error
}

func newSent() *sent { return &sent{acked: map[tp][]*kgo.Record{}} }

func (s *sent) promise(r *kgo.Record, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.fails = append(s.fails, err)
		return
	}
	k := tp{r.Topic, r.Partition}
	s.acked[k] = append(s.acked[k], r)
}

func value(topic string, p int32, i int) []byte {
	return []byte(fmt.Sprintf("%s/%d/%06d", topic, p, i))
}

// produce sends n records to every partition of every topic (partition
// explicit, values increasing per partition) and waits for the acks.
func produce(t *testing.T, cl *kgo.Client, topics map[string]int32, n int, s *sent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		for topic, parts := range topics {
			for p := int32(0); p < parts; p++ {
				cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: value(topic, p, i)}, s.promise)
			}
		}
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

// consume reads every partition in want from offset 0 until it has want[tp]
// records (or fails at the deadline). It never joins a group.
func consume(t *testing.T, seeds []string, want map[tp]int) map[tp][]*kgo.Record {
	t.Helper()
	parts := map[string]map[int32]kgo.Offset{}
	total := 0
	for k, n := range want {
		if parts[k.topic] == nil {
			parts[k.topic] = map[int32]kgo.Offset{}
		}
		parts[k.topic][k.part] = kgo.NewOffset().AtStart()
		total += n
	}
	cl := newClient(t, seeds, kgo.ConsumePartitions(parts))
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	got := map[tp][]*kgo.Record{}
	have := 0
	for have < total {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consume: %v after %d/%d records", err, have, total)
		}
		fs.EachError(func(topic string, p int32, err error) {
			t.Errorf("fetch %s/%d: %v", topic, p, err)
		})
		fs.EachRecord(func(r *kgo.Record) {
			k := tp{r.Topic, r.Partition}
			got[k] = append(got[k], r)
			have++
		})
		if t.Failed() {
			t.FailNow()
		}
	}
	// A moment more, to catch records beyond the expected ones (duplicates).
	extra, ecancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer ecancel()
	cl.PollFetches(extra).EachRecord(func(r *kgo.Record) {
		t.Errorf("unexpected extra record %s/%d@%d %q", r.Topic, r.Partition, r.Offset, r.Value)
	})
	return got
}

// verify checks that each partition holds exactly the acked records: offsets
// 0..n-1 with no gap, no duplicate, the values the producer was acked for at
// the offsets it was told, and (inOrder) in the order they were sent.
func verify(t *testing.T, acked *sent, got map[tp][]*kgo.Record, inOrder bool) {
	t.Helper()
	acked.mu.Lock()
	defer acked.mu.Unlock()
	for k, recs := range got {
		seen := map[string]int64{}
		for i, r := range recs {
			if r.Offset != int64(i) {
				t.Fatalf("%v: record %d has offset %d (gap or reorder)", k, i, r.Offset)
			}
			if _, dup := seen[string(r.Value)]; dup {
				t.Fatalf("%v: %q delivered twice", k, r.Value)
			}
			seen[string(r.Value)] = r.Offset
		}
		for _, a := range acked.acked[k] {
			off, ok := seen[string(a.Value)]
			if !ok {
				t.Fatalf("%v: acked record %q (offset %d) is lost", k, a.Value, a.Offset)
			}
			if off != a.Offset {
				t.Fatalf("%v: %q acked at offset %d, stored at %d", k, a.Value, a.Offset, off)
			}
		}
		if inOrder {
			for i, r := range recs {
				if want := value(k.topic, k.part, i); string(r.Value) != string(want) {
					t.Fatalf("%v: record %d is %q, want %q", k, i, r.Value, want)
				}
			}
		}
	}
}

func wantCounts(topics map[string]int32, n int) map[tp]int {
	m := map[tp]int{}
	for topic, parts := range topics {
		for p := int32(0); p < parts; p++ {
			m[tp{topic, p}] = n
		}
	}
	return m
}
