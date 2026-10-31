package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
)

// longFetch is a Fetch of offset 0 on topic t, for the partitions given.
func longFetch(minBytes, maxWaitMillis int32, parts ...int32) *kmsg.FetchRequest {
	r := kmsg.NewPtrFetchRequest()
	r.MaxBytes, r.MinBytes, r.MaxWaitMillis = 1<<20, minBytes, maxWaitMillis
	t := kmsg.FetchRequestTopic{Topic: "t"}
	for _, p := range parts {
		t.Partitions = append(t.Partitions, kmsg.FetchRequestTopicPartition{Partition: p,
			PartitionMaxBytes: 1 << 20, CurrentLeaderEpoch: -1})
	}
	r.Topics = []kmsg.FetchRequestTopic{t}
	return r
}

// lookups counts the FindSegments calls the broker made for tp.
func lookups(h *harness, tp core.TopicPartition) int {
	n := 0
	for _, op := range h.cpCalls {
		if f, ok := op.(core.FindSegments); ok && f.TopicPartition == tp {
			n++
		}
	}
	return n
}

// fireUntilAnswer fires the timers in order until c has a response.
func fireUntilAnswer(t *testing.T, h *harness, c *client) {
	t.Helper()
	for n := 0; len(c.out) == 0; n++ {
		if n > 50 {
			t.Fatal("fetch never answered")
		}
		h.fireNext()
	}
}

func TestFetchAnswersAtOnce(t *testing.T) {
	for _, tc := range []struct {
		name     string
		data     bool // a batch is committed before the Fetch
		minBytes int32
		maxWait  int32
	}{
		{"max wait 0, no data", false, 1, 0},
		{"min bytes 0, no data", false, 0, 500},
		{"min bytes met by data", true, 1, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := startedHarness(t, DefaultConfig())
			c := h.open(1)
			if tc.data {
				h.produceOne(c, e("t", 0, plain(t, "a")))
			}
			h.send(c, verFetch, longFetch(tc.minBytes, tc.maxWait, 0))
			if len(c.out) != 1 || len(h.b.ops) != 0 {
				t.Fatalf("%d responses, %d parts parked: want an answer at once", len(c.out), len(h.b.ops))
			}
			if got := len(fetched(h.next(c)).RecordBatches) > 0; got != tc.data {
				t.Fatalf("data %v, want %v", got, tc.data)
			}
			h.noViolations()
		})
	}
}

func TestFetchDataOnSecondPoll(t *testing.T) {
	tp := core.NewTopicPartition("t", 0)
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	w := h.open(2)
	h.send(c, verFetch, longFetch(1, 500, 0))
	if len(c.out) != 0 || len(h.timers) != 1 || lookups(h, tp) != 1 {
		t.Fatalf("parked: %d responses, %d timers, %d lookups", len(c.out), len(h.timers), lookups(h, tp))
	}
	h.fireNext() // first poll: still no data
	if len(c.out) != 0 || lookups(h, tp) != 2 {
		t.Fatalf("after first poll: %d responses, %d lookups", len(c.out), lookups(h, tp))
	}
	h.produceOne(w, e("t", 0, plain(t, "a", "b")))
	h.fireNext() // second poll finds the data
	if len(c.out) != 1 || lookups(h, tp) != 3 {
		t.Fatalf("after second poll: %d responses, %d lookups", len(c.out), lookups(h, tp))
	}
	if p := fetched(h.next(c)); len(p.RecordBatches) == 0 || p.HighWatermark != 2 {
		t.Fatalf("second poll: %+v", p)
	}
	h.noViolations()
}

func TestFetchMaxWaitExpiresEmpty(t *testing.T) {
	tp := core.NewTopicPartition("t", 0)
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verFetch, longFetch(1, 300, 0))
	start := h.now
	fireUntilAnswer(t, h, c)
	// The answer comes at MaxWait, not a poll interval later: the last poll
	// waits only until the deadline.
	if elapsed := h.now - start; elapsed < core.NodeTime(300*core.Millisecond) || elapsed > core.NodeTime(310*core.Millisecond) {
		t.Fatalf("answered after %d ns, want about 300 ms", elapsed)
	}
	if p := fetched(h.next(c)); len(p.RecordBatches) != 0 || p.ErrorCode != 0 {
		t.Fatalf("expired fetch: %+v", p)
	}
	if n := lookups(h, tp); n != 2 { // the first Lookup and the poll at 200 ms
		t.Fatalf("%d lookups, want 2", n)
	}
	h.noViolations()
}

func TestFetchTwoPartitionsOneShort(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, e("t", 0, plain(t, "a")))
	h.send(c, verFetch, longFetch(1, 500, 0, 1))
	if len(c.out) != 1 || len(h.b.ops) != 0 {
		t.Fatalf("%d responses, %d parts parked: partition 1 must not hold the answer", len(c.out), len(h.b.ops))
	}
	r := h.next(c).(*kmsg.FetchResponse)
	if p0, p1 := r.Topics[0].Partitions[0], r.Topics[0].Partitions[1]; len(p0.RecordBatches) == 0 || len(p1.RecordBatches) != 0 {
		t.Fatalf("p0 %d bytes, p1 %d bytes", len(p0.RecordBatches), len(p1.RecordBatches))
	}
	h.noViolations()
}

// A part with data holds it while the Fetch is short of MinBytes. Only the
// partition with no data is polled again.
func TestFetchPollsOnlyShortPartition(t *testing.T) {
	tp0, tp1 := core.NewTopicPartition("t", 0), core.NewTopicPartition("t", 1)
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	w := h.open(2)
	h.produceOne(w, e("t", 0, plain(t, "a")))
	h.send(c, verFetch, longFetch(1000, 500, 0, 1))
	if len(c.out) != 0 || lookups(h, tp0) != 1 || lookups(h, tp1) != 1 {
		t.Fatalf("parked: %d responses, lookups %d and %d", len(c.out), lookups(h, tp0), lookups(h, tp1))
	}
	h.produceOne(w, e("t", 1, plain(t, "b")))
	start := h.now
	fireUntilAnswer(t, h, c)
	if elapsed := h.now - start; elapsed < core.NodeTime(300*core.Millisecond) {
		t.Fatalf("answered before MaxWait with %d ns to go, MinBytes not met", elapsed)
	}
	if lookups(h, tp0) != 1 {
		t.Fatalf("partition 0 polled %d times, want 1", lookups(h, tp0)-1)
	}
	if lookups(h, tp1) < 2 {
		t.Fatalf("partition 1 never polled")
	}
	r := h.next(c).(*kmsg.FetchResponse)
	for i, p := range r.Topics[0].Partitions {
		if len(p.RecordBatches) == 0 {
			t.Fatalf("partition %d has no data in the answer", i)
		}
	}
	h.noViolations()
}

func TestFetchParkedEndsOnClose(t *testing.T) {
	tp := core.NewTopicPartition("t", 0)
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verFetch, longFetch(1, 500, 0))
	h.do(core.ConnClosed{Conn: 1, Abort: true})
	if len(h.b.ops) != 0 {
		t.Fatalf("%d parts still parked after the close", len(h.b.ops))
	}
	before := lookups(h, tp)
	if len(h.timers) > 0 { // a stale timer must do nothing
		h.fireNext()
	}
	if lookups(h, tp) != before || len(c.out) != 0 {
		t.Fatal("a closed fetch polled or answered")
	}
	h.noViolations()
}

func TestFetchParkedAnswersOnStop(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verFetch, longFetch(1, 500, 0))
	h.do(core.StopRequest{Deadline: h.now + core.NodeTime(core.Second)})
	if !h.exited {
		t.Fatal("a parked fetch held up the stop")
	}
	if p := fetched(h.next(c)); len(p.RecordBatches) != 0 || p.ErrorCode != 0 {
		t.Fatalf("stopped fetch: %+v", p)
	}
	h.noViolations()
}

// A request pipelined behind a parked Fetch is still answered after it.
func TestFetchParkKeepsConnectionOrder(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	var chunk []byte
	chunk = append(chunk, h.frame(c, verFetch, longFetch(1, 500, 0))...)
	chunk = append(chunk, h.frame(c, 3, kmsg.NewPtrApiVersionsRequest())...)
	h.do(core.BytesReceived{Conn: 1, Data: chunk})
	if len(c.out) != 0 {
		t.Fatal("ApiVersions overtook the parked Fetch")
	}
	fireUntilAnswer(t, h, c)
	if _, ok := h.next(c).(*kmsg.FetchResponse); !ok {
		t.Fatal("first response is not the Fetch")
	}
	if len(c.out) != 1 {
		t.Fatalf("%d responses after the Fetch, want the ApiVersions reply", len(c.out))
	}
	h.noViolations()
}
