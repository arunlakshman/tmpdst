package it

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/broker"
	"manifester/internal/controlplane/memstore"
	"manifester/internal/core"
)

func memStack(t *testing.T, brokers int) *stack {
	t.Helper()
	s := newStack(t, memstore.New(), nil, nil)
	s.addBrokers(brokers)
	return s
}

func waitAcked(t *testing.T, s *sent, n int) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		s.mu.Lock()
		total := 0
		for _, r := range s.acked {
			total += len(r)
		}
		s.mu.Unlock()
		if total >= n {
			return
		}
	}
	t.Fatalf("fewer than %d acks in 30s", n)
}

var topics = map[string]int32{"orders": 3, "events": 2}

func create(s *stack) {
	for name, parts := range topics {
		s.createTopic(name, parts)
	}
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	s := memStack(t, 2)
	create(s)
	runRoundTrip(t, s)
}

// runRoundTrip is the flow shared with the full-stack test.
func runRoundTrip(t *testing.T, s *stack) {
	cl := newClient(t, s.seeds())
	defer closeClient(cl)
	acked := newSent()
	const n = 10 // functional coverage, not load
	produce(t, cl, topics, n, acked)
	if len(acked.fails) > 0 {
		t.Fatalf("produce failures: %v", acked.fails)
	}
	got := consumeFrom(t, cl, wantCounts(topics, n))
	for k, recs := range got {
		if len(recs) != n {
			t.Fatalf("%v: %d records, want %d", k, len(recs), n)
		}
	}
	verify(t, acked, got, true)

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)
	ends, err := adm.ListEndOffsets(ctx, "orders", "events")
	if err != nil {
		t.Fatal(err)
	}
	starts, err := adm.ListStartOffsets(ctx, "orders", "events")
	if err != nil {
		t.Fatal(err)
	}
	for k := range wantCounts(topics, n) {
		if e, _ := ends.Lookup(k.topic, k.part); e.Offset != n || e.Err != nil {
			t.Errorf("%v end offset %+v, want %d", k, e, n)
		}
		if st, _ := starts.Lookup(k.topic, k.part); st.Offset != 0 || st.Err != nil {
			t.Errorf("%v start offset %+v, want 0", k, st)
		}
	}
}

func TestMetadataAndApiVersions(t *testing.T) {
	s := memStack(t, 2)
	create(s)
	cl := newClient(t, s.seeds()[:1])
	defer closeClient(cl)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	adm := kadm.NewClient(cl)

	md, err := adm.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for name, parts := range topics {
		td, ok := md.Topics[name]
		if !ok || td.Err != nil || int32(len(td.Partitions)) != parts {
			t.Errorf("metadata for %s: %+v", name, td)
		}
	}
	if len(md.Brokers) == 0 {
		t.Fatal("metadata lists no brokers")
	}
	// The advertised address is the one the client dialled.
	if got := md.Brokers[0]; got.Host+":"+strconv.Itoa(int(got.Port)) != s.nodes[0].Addr().String() {
		t.Errorf("advertised %s:%d, listening on %s", got.Host, got.Port, s.nodes[0].Addr())
	}
	md2, err := adm.Metadata(ctx, "nope")
	if err != nil {
		t.Fatal(err)
	}
	if td := md2.Topics["nope"]; td.Err == nil {
		t.Errorf("unknown topic in metadata: %+v", td)
	}

	vs, err := adm.ApiVersions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range vs {
		if v.Err != nil {
			t.Fatal(v.Err)
		}
		for _, key := range []int16{0, 1, 2, 3, 18} { // produce fetch listoffsets metadata apiversions
			if _, ok := v.KeyMaxVersion(key); !ok {
				t.Errorf("broker %d does not offer API key %d", v.NodeID, key)
			}
		}
		if _, ok := v.KeyMaxVersion(22); ok { // InitProducerId: no idempotent producers (decision 41)
			t.Errorf("broker %d offers InitProducerId", v.NodeID)
		}
	}
}

func TestProduceUnknownTopic(t *testing.T) {
	s := memStack(t, 1)
	create(s)
	cl := newClient(t, s.seeds(), kgo.UnknownTopicRetries(0), kgo.RecordDeliveryTimeout(10*time.Second))
	defer closeClient(cl)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	start := time.Now()
	res := cl.ProduceSync(ctx, &kgo.Record{Topic: "ghost", Partition: 0, Value: []byte("x")})
	err := res.FirstErr()
	if err == nil {
		t.Fatal("produce to unknown topic succeeded")
	}
	if !strings.Contains(err.Error(), kerr.UnknownTopicOrPartition.Message) {
		t.Errorf("error %v, want UNKNOWN_TOPIC_OR_PARTITION", err)
	}
	if d := time.Since(start); d > 8*time.Second {
		t.Errorf("failed after %v: not a clean failure", d)
	}
	// Known topics keep working on the same client.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("ok")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestListOffsetsRequest(t *testing.T) {
	s := memStack(t, 1)
	create(s)
	cl := newClient(t, s.seeds())
	defer closeClient(cl)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	for i := range 5 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 1, Value: value("orders", 1, i)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = "orders"
	for _, ts := range []int64{-1, -2} {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = 1
		rp.Timestamp = ts
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var offs []int64
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			t.Fatalf("list offsets: %v", kerr.ErrorForCode(p.ErrorCode))
		}
		offs = append(offs, p.Offset)
	}
	if len(offs) != 2 || offs[0] != 5 || offs[1] != 0 {
		t.Errorf("latest/earliest = %v, want [5 0]", offs)
	}
}

// A broker killed in the middle of a produce stream: the client retries via
// another broker and no acked record is lost or stored twice.
func TestKillBrokerMidProduce(t *testing.T) {
	s := memStack(t, 3)
	create(s)
	runKill(t, s)
}

// runKill needs three brokers; it is shared with the full-stack test.
func runKill(t *testing.T, s *stack) {
	cl := newClient(t, s.seeds(), kgo.RetryTimeout(60*time.Second), kgo.RecordDeliveryTimeout(60*time.Second))
	defer closeClient(cl)
	acked := newSent()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	const n = 40 // enough to survive two kills; not a load test
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range n {
			for topic, parts := range topics {
				for p := range parts {
					cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: p, Value: value(topic, p, i)}, acked.promise)
				}
			}
			if i%5 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	// The client talks to the first seed. Kill it once acks are flowing, then
	// the broker it moved to, so retries must land on a different broker twice.
	waitAcked(t, acked, 20)
	s.nodes[0].Kill()
	waitAcked(t, acked, 80)
	s.nodes[1].Kill()
	<-done
	if err := cl.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(acked.fails) > 0 {
		t.Fatalf("produce failures despite a live broker: %v", acked.fails)
	}
	closeClient(cl)
	got := consume(t, s.seeds()[2:], wantCounts(topics, n))
	verify(t, acked, got, false)
	for k, recs := range got {
		if len(recs) != n {
			t.Fatalf("%v: %d records, want %d (all were acked)", k, len(recs), n)
		}
	}
}

// Stop while a produce sits in the open segment: it is flushed, answered,
// and the broker exits cleanly.
func TestGracefulStopFlushesProduce(t *testing.T) {
	s := newStack(t, memstore.New(), nil, nil)
	s.brokers.BatchWindow = core.Duration(3 * time.Second)
	s.addBrokers(1)
	create(s)
	cl := newClient(t, s.seeds())
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	// Warm the connection so the timing below is the
	// batch window and nothing else.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("warm")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	type result struct {
		err error
		at  time.Duration
	}
	start := time.Now()
	out := make(chan result, 1)
	go func() {
		err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("inflight")}).FirstErr()
		out <- result{err, time.Since(start)}
	}()
	time.Sleep(300 * time.Millisecond) // the batch is now waiting out the window
	s.nodes[0].Stop()
	r := <-out
	if r.err != nil {
		t.Fatalf("in-flight produce failed across a graceful stop: %v", r.err)
	}
	if r.at > 2500*time.Millisecond {
		t.Errorf("answered after %v: waited for the batch window instead of flushing", r.at)
	}
	if err := s.nodes[0].Wait(); err != nil {
		t.Fatalf("broker exit: %v", err)
	}
	closeClient(cl)
	// The data is durable: a fresh broker serves it.
	s.addBroker()
	got := consume(t, s.seeds()[1:], map[tp]int{{"orders", 0}: 2})
	if v := string(got[tp{"orders", 0}][1].Value); v != "inflight" {
		t.Errorf("second record %q", v)
	}
}

// The broker's own defaults (16 MiB, 200 ms): a lone produce waits out the
// fixed window, and is answered right after it.
func TestDefaultFlushRule(t *testing.T) {
	s := newStack(t, memstore.New(), nil, nil)
	s.brokers = broker.Config{} // every flush knob at its default
	s.addBrokers(1)
	create(s)
	cl := newClient(t, s.seeds())
	defer closeClient(cl)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	warm := &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("warm")}
	if err := cl.ProduceSync(ctx, warm).FirstErr(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 150*time.Millisecond || d > 5*time.Second {
		t.Errorf("produce answered after %v, want about the 200ms window", d)
	}
	got := consumeFrom(t, cl, map[tp]int{{"orders", 0}: 2})
	if v := string(got[tp{"orders", 0}][1].Value); v != "x" {
		t.Errorf("second record %q", v)
	}
}
