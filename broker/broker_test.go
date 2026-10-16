package broker

import (
	"encoding/binary"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
)

func TestApiVersions(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	r := h.call(c, 3, func() kmsg.Request {
		q := kmsg.NewPtrApiVersionsRequest()
		q.ClientSoftwareName, q.ClientSoftwareVersion = "x", "1"
		return q
	}()).(*kmsg.ApiVersionsResponse)
	if r.ErrorCode != 0 || len(r.ApiKeys) != len(kwire.Supported()) {
		t.Fatalf("got %+v", r)
	}
	// Unsupported version: raw frame, answered with UNSUPPORTED_VERSION at v0.
	hd := kwire.RequestHeader{APIKey: kwire.KeyApiVersions, APIVersion: 99, CorrelationID: 77, ClientID: "x"}
	c.want = append(c.want, hd)
	frame := binary.BigEndian.AppendUint16(nil, 18)
	frame = binary.BigEndian.AppendUint16(frame, 99)
	frame = binary.BigEndian.AppendUint32(frame, 77)
	frame = binary.BigEndian.AppendUint16(frame, 1)
	frame = append(frame, 'x')
	h.do(core.BytesReceived{Conn: 1, Data: binary.BigEndian.AppendUint32(nil, uint32(len(frame)))})
	h.do(core.BytesReceived{Conn: 1, Data: frame}) // split across chunks
	u := h.next(c).(*kmsg.ApiVersionsResponse)
	if u.ErrorCode != kwire.ErrCodeUnsupportedVersion {
		t.Fatalf("error %d", u.ErrorCode)
	}
	if h.closed[1] {
		t.Fatal("connection closed")
	}
}

func TestMetadata(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Host, cfg.Port, cfg.BrokerID = "h1", 1234, 5
	h := startedHarness(t, cfg)
	h.cp.CreateTopic("u", 1)
	c := h.open(1)
	all := kmsg.NewPtrMetadataRequest()
	r := h.call(c, verMetadata, all).(*kmsg.MetadataResponse)
	if len(r.Brokers) != 1 || r.Brokers[0].NodeID != 5 || r.Brokers[0].Host != "h1" || r.Brokers[0].Port != 1234 || r.ControllerID != 5 {
		t.Fatalf("brokers %+v", r.Brokers)
	}
	if len(r.Topics) != 2 || *r.Topics[0].Topic != "t" || len(r.Topics[0].Partitions) != 2 || r.Topics[0].Partitions[1].Leader != 5 {
		t.Fatalf("topics %+v", r.Topics)
	}
	q := kmsg.NewPtrMetadataRequest()
	a, b := "t", "nope"
	q.Topics = []kmsg.MetadataRequestTopic{{Topic: &b}, {Topic: &a}}
	r = h.call(c, verMetadata, q).(*kmsg.MetadataResponse)
	if len(r.Topics) != 2 || r.Topics[0].ErrorCode != errUnknownTopicOrPart || r.Topics[1].ErrorCode != 0 {
		t.Fatalf("topics %+v", r.Topics)
	}
	for _, cmd := range h.cp.Exec(core.CPCall{Req: 1, Op: core.DescribeTopics{Topics: []string{"nope"}}}).(core.CPDone).Resp.(core.DescribeTopicsResp).Topics {
		t.Fatalf("topic auto-created: %v", cmd)
	}
	q.Topics = []kmsg.MetadataRequestTopic{}
	r = h.call(c, verMetadata, q).(*kmsg.MetadataResponse)
	if len(r.Topics) != 0 || len(r.Brokers) != 1 {
		t.Fatalf("empty request: %+v", r)
	}
}

func TestInitProducerID(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	a, b := h.initPID(c), h.initPID(c)
	if a < 1000 || b != a+1 {
		t.Fatalf("pids %d %d", a, b)
	}
	q := initReq()
	id := "txn"
	q.TransactionalID = &id
	if r := h.call(c, verInit, q).(*kmsg.InitProducerIDResponse); r.ErrorCode != errInvalidRequest {
		t.Fatalf("txn id accepted: %d", r.ErrorCode)
	}
	// Failures are retried.
	fails := 2
	h.cpHook = func(a core.CPCall, run func() core.Completion) core.Completion {
		if _, ok := a.Op.(core.InitProducer); ok && fails > 0 {
			fails--
			return core.CPDone{Req: a.Req, Err: core.ErrUnavailable}
		}
		return run()
	}
	h.send(c, verInit, initReq())
	h.fireAll()
	if r := h.next(c).(*kmsg.InitProducerIDResponse); r.ErrorCode != 0 || r.ProducerID == 0 {
		t.Fatalf("after retries: %+v", r)
	}
}

func TestProduceFetchRoundTrip(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	b1, b2, b3 := plain(t, "a", "b"), plain(t, "c"), plain(t, "d", "e", "f")
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, b1}, ent{"t", 1, b2}, ent{"t", 0, b3}))
	if len(c.out) != 0 {
		t.Fatal("acked before commit")
	}
	h.flush()
	if len(h.puts) != 1 {
		t.Fatalf("%d segments for one flush", len(h.puts))
	}
	ps := produced(h.next(c))
	if ps[0].BaseOffset != 0 || ps[1].BaseOffset != 0 || ps[2].BaseOffset != 2 || ps[0].ErrorCode+ps[1].ErrorCode+ps[2].ErrorCode != 0 {
		t.Fatalf("results %+v", ps)
	}
	p := fetched(h.call(c, verFetch, fetchReq("t", 0, 0, 1<<20)))
	bs, err := kwire.SplitBatches(p.RecordBatches)
	if err != nil || len(bs) != 2 || p.HighWatermark != 5 || p.LastStableOffset != 5 || p.LogStartOffset != 0 {
		t.Fatalf("fetch: %v %d batches hwm %d", err, len(bs), p.HighWatermark)
	}
	for i, want := range []int64{0, 2} {
		bt, err := kwire.ParseBatch(bs[i])
		if err != nil || bt.BaseOffset != want || bt.PartitionLeaderEpoch != 0 {
			t.Fatalf("batch %d: %v %+v", i, err, bt)
		}
	}
	// From the middle: the containing batch is returned whole.
	p = fetched(h.call(c, verFetch, fetchReq("t", 0, 3, 1<<20)))
	if bs, _ := kwire.SplitBatches(p.RecordBatches); len(bs) != 1 {
		t.Fatalf("mid fetch %d batches", len(bs))
	}
	// Partition limit: at least one batch, no more than fits.
	p = fetched(h.call(c, verFetch, fetchReq("t", 0, 0, 1)))
	if bs, _ := kwire.SplitBatches(p.RecordBatches); len(bs) != 1 {
		t.Fatalf("limited fetch %d batches", len(bs))
	}
	if p = fetched(h.call(c, verFetch, fetchReq("t", 0, 5, 1<<20))); len(p.RecordBatches) != 0 || p.ErrorCode != 0 || p.HighWatermark != 5 {
		t.Fatalf("at HWM: %+v", p)
	}
	if p = fetched(h.call(c, verFetch, fetchReq("t", 0, 6, 1<<20))); p.ErrorCode != errOffsetOutOfRange {
		t.Fatalf("beyond HWM: %d", p.ErrorCode)
	}
	if p = fetched(h.call(c, verFetch, fetchReq("zz", 0, 0, 1<<20))); p.ErrorCode != errUnknownTopicOrPart {
		t.Fatalf("unknown topic: %d", p.ErrorCode)
	}
	h.noViolations()
}

func TestProduceErrors(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	bad := plain(t, "x")
	bad[len(bad)-1] ^= 0xff
	txn := plain(t, "x")
	// A transactional batch needs a valid CRC, so build one with the flag.
	p := params
	p.Attributes, p.ProducerID, p.ProducerEpoch, p.BaseSequence = 0x10, 1000, 0, 0
	txn, _ = kwire.BuildBatch(p, []kmsg.Record{{Value: []byte("x")}})
	two := append(plain(t, "x"), plain(t, "y")...)
	h.send(c, verProduce, prodReq(-1,
		ent{"nope", 0, plain(t, "x")}, ent{"t", 9, plain(t, "x")}, ent{"t", 0, bad},
		ent{"t", 0, txn}, ent{"t", 0, two}, ent{"t", 0, nil}, ent{"t", 0, plain(t, "ok")}))
	h.flush()
	ps := produced(h.next(c))
	want := []int16{errUnknownTopicOrPart, errUnknownTopicOrPart, errCorruptMessage, errInvalidRequest, errCorruptMessage, errCorruptMessage, 0}
	for i, w := range want {
		if ps[i].ErrorCode != w {
			t.Errorf("partition %d: error %d, want %d", i, ps[i].ErrorCode, w)
		}
	}
	if q := h.call(c, verProduce, prodReq(7, ent{"t", 0, plain(t, "x")})); produced(q)[0].ErrorCode != errInvalidRequiredAcks {
		t.Fatal("bad acks accepted")
	}
}

func TestTopicCacheRefresh(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.cp.CreateTopic("late", 1)
	r := h.produceOne(c, ent{"late", 0, plain(t, "x")})
	if r.ErrorCode != 0 || r.BaseOffset != 0 {
		t.Fatalf("%+v", r)
	}
	n := len(h.cpCalls)
	h.produceOne(c, ent{"late", 0, plain(t, "y")})
	for _, op := range h.cpCalls[n:] {
		if _, ok := op.(core.DescribeTopics); ok {
			t.Fatal("cache not used")
		}
	}
}

func TestSizeFlushAndPartitionCap(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSegmentBytes = 100
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}, ent{"t", 0, plain(t, "b")}))
	if len(h.puts) != 1 {
		t.Fatalf("size trigger: %d puts", len(h.puts))
	}
	produced(h.next(c))

	cfg = DefaultConfig()
	cfg.MaxSegmentPartitions = 2
	h = startedHarness(t, cfg)
	h.cp.CreateTopic("m", 5)
	c = h.open(1)
	h.send(c, verProduce, prodReq(-1, ent{"m", 0, plain(t, "a")}, ent{"m", 0, plain(t, "a")}, ent{"m", 1, plain(t, "a")}, ent{"m", 2, plain(t, "a")}))
	if len(h.puts) != 1 {
		t.Fatalf("partition cap: %d puts (want 1 sealed, 1 open)", len(h.puts))
	}
	h.flush()
	if len(h.puts) != 2 {
		t.Fatalf("%d puts", len(h.puts))
	}
	for _, p := range produced(h.next(c)) {
		if p.ErrorCode != 0 {
			t.Fatal(p.ErrorCode)
		}
	}
}

func TestPipelinedResponsesInOrder(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	var chunk []byte
	chunk = append(chunk, h.frame(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))...)
	chunk = append(chunk, h.frame(c, 3, kmsg.NewPtrApiVersionsRequest())...)
	chunk = append(chunk, h.frame(c, verFetch, fetchReq("t", 0, 0, 1<<20))...)
	h.do(core.BytesReceived{Conn: 1, Data: chunk})
	if len(c.out) != 0 {
		t.Fatal("later requests overtook the pending produce")
	}
	h.flush()
	if len(c.out) != 3 {
		t.Fatalf("%d responses", len(c.out))
	}
	if r := produced(h.next(c)); r[0].BaseOffset != 0 {
		t.Fatal("produce")
	}
	h.next(c)
	if p := fetched(h.next(c)); len(p.RecordBatches) != 0 {
		// The fetch was admitted before the commit, so it sees an empty log or the batch;
		// either is valid, but offsets must be consistent.
		if p.HighWatermark != 1 {
			t.Fatalf("hwm %d", p.HighWatermark)
		}
	}
}

func TestIdempotentRetryAfterLostCommit(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	pid := h.initPID(c)
	lose := 1
	h.cpHook = func(a core.CPCall, run func() core.Completion) core.Completion {
		if _, ok := a.Op.(core.CommitSegment); ok && lose > 0 {
			lose--
			run() // applied, response lost
			return core.CPDone{Req: a.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, mkBatch(t, pid, 0, 0, "a", "b")}))
	h.flush()
	if len(c.out) != 0 {
		t.Fatal("acked on a lost commit response")
	}
	h.fireAll() // backoff, retry with the same key
	r := produced(h.next(c))[0]
	if r.ErrorCode != 0 || r.BaseOffset != 0 || h.cp.HighWatermark(core.TopicPartition{Topic: "t"}) != 2 {
		t.Fatalf("%+v hwm %d", r, h.cp.HighWatermark(core.TopicPartition{Topic: "t"}))
	}
	if len(h.puts) != 1 {
		t.Fatal("retry re-uploaded")
	}
	if n := len(factsNamed(h.all, "segment_committed")); n != 1 {
		t.Fatalf("%d segment_committed", n)
	}
	h.noViolations()
}

// A client retry after REQUEST_TIMED_OUT yields the original offset, not a duplicate.
func TestClientRetryAfterTimeoutIsDup(t *testing.T) {
	for _, bug := range []Bug{BugNone, BugNoIdempotence} {
		cfg := DefaultConfig()
		cfg.Bug = bug
		h := startedHarness(t, cfg)
		c := h.open(1)
		pid := h.initPID(c)
		first := true
		h.cpHook = func(a core.CPCall, run func() core.Completion) core.Completion {
			if cs, ok := a.Op.(core.CommitSegment); ok && first {
				run()
				if cs.Batches[0].TP.Topic == "t" {
					return core.CPDone{Req: a.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
				}
			}
			return run()
		}
		batch := mkBatch(t, pid, 0, 0, "a", "b")
		h.send(c, verProduce, prodReq(-1, ent{"t", 0, batch}))
		h.flush()
		h.fireAll() // every retry loses its response
		first = false
		r := produced(h.next(c))[0]
		if r.ErrorCode != errRequestTimedOut {
			t.Fatalf("bug %d: first answer %d", bug, r.ErrorCode)
		}
		r = h.produceOne(c, ent{"t", 0, batch})
		hwm := h.cp.HighWatermark(core.TopicPartition{Topic: "t"})
		switch bug {
		case BugNone:
			if r.ErrorCode != 0 || r.BaseOffset != 0 || hwm != 2 {
				t.Fatalf("dup: %+v hwm %d", r, hwm)
			}
			if f := factsNamed(h.all, "produce_acked"); len(f) != 1 || field(f[0], "dup") != core.Bool(true) {
				t.Fatalf("facts %v", f)
			}
		case BugNoIdempotence:
			if hwm != 4 {
				t.Fatalf("mutant should duplicate: hwm %d", hwm)
			}
		}
	}
}

func TestOutOfOrderAndStaleEpoch(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	pid := h.initPID(c)
	if r := h.produceOne(c, ent{"t", 0, mkBatch(t, pid, 0, 0, "a")}); r.ErrorCode != 0 {
		t.Fatal(r.ErrorCode)
	}
	if r := h.produceOne(c, ent{"t", 0, mkBatch(t, pid, 0, 5, "a")}); r.ErrorCode != errOutOfOrderSeq {
		t.Fatalf("gap: %d", r.ErrorCode)
	}
	if r := h.produceOne(c, ent{"t", 0, mkBatch(t, pid, 1, 0, "a")}); r.ErrorCode != 0 {
		t.Fatalf("epoch bump: %d", r.ErrorCode)
	}
	if r := h.produceOne(c, ent{"t", 0, mkBatch(t, pid, 0, 1, "a")}); r.ErrorCode != errInvalidProducerEpoch {
		t.Fatalf("stale epoch: %d", r.ErrorCode)
	}
	// A producer id the control plane never issued fails alone.
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, mkBatch(t, 5000, 0, 0, "a")}, ent{"t", 1, plain(t, "ok")}))
	h.flush()
	ps := produced(h.next(c))
	if ps[0].ErrorCode != errInvalidPIDMapping || ps[1].ErrorCode != 0 {
		t.Fatalf("isolation: %+v", ps)
	}
	if f := factsNamed(h.all, "produce_acked"); len(f) != 3 {
		t.Fatalf("%d acks", len(f))
	}
}

func TestFactImmediatelyBeforeSend(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	pid := h.initPID(c)
	h.produceOne(c, ent{"t", 0, mkBatch(t, pid, 0, 0, "a", "b")})
	n := 0
	for i, a := range h.all {
		e, ok := a.(core.Emit)
		if !ok || e.Fact.Name != "produce_acked" {
			continue
		}
		n++
		if _, ok := h.all[i+1].(core.Send); !ok {
			t.Fatalf("ack fact not followed by Send: %T", h.all[i+1])
		}
		f := e.Fact
		if field(f, "topic") != core.Str("t") || field(f, "base_offset") != core.I64(0) || field(f, "count") != core.I64(2) ||
			field(f, "producer") != core.I64(pid) || field(f, "base_seq") != core.I64(0) {
			t.Fatalf("fields %v", f.Fields)
		}
	}
	if n != 1 {
		t.Fatalf("%d acks", n)
	}
}

func TestTripOnFencing(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.cp.Exec(core.CPCall{Req: 1, Op: core.Register{Self: h.self}}) // a newer epoch exists
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
	h.flush()
	if !h.tripped || len(c.out) != 0 {
		t.Fatalf("tripped=%v out=%d", h.tripped, len(c.out))
	}
	if _, ok := h.all[len(h.all)-1].(core.Trip); !ok {
		t.Fatal("Trip is not the last action")
	}
	n := len(h.all)
	h.step(core.BytesReceived{Conn: 1, Data: h.frame(c, 3, kmsg.NewPtrApiVersionsRequest())})
	h.step(core.TimerFired{ID: timerFlush, Gen: 1})
	if len(h.all) != n {
		t.Fatal("tripped broker acted")
	}
}

func TestGracefulStop(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
	if len(h.puts) != 0 {
		t.Fatal("flushed early")
	}
	h.do(core.Stop{Deadline: h.now + core.Time(core.Second)})
	if !h.exited || !h.paused[1] {
		t.Fatalf("exited=%v paused=%v", h.exited, h.paused[1])
	}
	if _, ok := h.all[len(h.all)-1].(core.Exit); !ok {
		t.Fatal("Exit is not last")
	}
	if r := produced(h.next(c))[0]; r.ErrorCode != 0 || r.BaseOffset != 0 {
		t.Fatalf("%+v", r)
	}
	if h.cp.HighWatermark(core.TopicPartition{Topic: "t"}) != 1 {
		t.Fatal("not flushed")
	}
	// Idle broker exits at once.
	h2 := startedHarness(t, DefaultConfig())
	h2.do(core.Stop{})
	if !h2.exited {
		t.Fatal("idle stop")
	}
}

func TestListOffsets(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, ent{"t", 0, plain(t, "a", "b", "c")})
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	mk := func(topic string, p int32, ts int64) kmsg.ListOffsetsRequestTopic {
		return kmsg.ListOffsetsRequestTopic{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: p, Timestamp: ts, CurrentLeaderEpoch: -1}}}
	}
	q.Topics = []kmsg.ListOffsetsRequestTopic{mk("t", 0, -2), mk("t", 0, -1), mk("t", 1, -1), mk("t", 0, 12345), mk("zz", 0, -1)}
	r := h.call(c, verList, q).(*kmsg.ListOffsetsResponse)
	get := func(i int) kmsg.ListOffsetsResponseTopicPartition { return r.Topics[i].Partitions[0] }
	if get(0).Offset != 0 || get(0).ErrorCode != 0 || get(1).Offset != 3 || get(2).Offset != 0 ||
		get(3).ErrorCode != errUnsupportedForFormat || get(4).ErrorCode != errUnknownTopicOrPart {
		t.Fatalf("%+v", r.Topics)
	}
}

func TestCorruptGetRetried(t *testing.T) {
	for _, bug := range []Bug{BugNone, BugServeCorrupt} {
		cfg := DefaultConfig()
		cfg.Bug = bug
		h := startedHarness(t, cfg)
		c := h.open(1)
		h.produceOne(c, ent{"t", 0, plain(t, "hello")})
		corrupt := true
		h.objHook = func(a core.IOAction, run func() core.Completion) core.Completion {
			d := run()
			if g, ok := d.(core.ObjGetDone); ok && corrupt {
				corrupt = false
				g.Data = append([]byte(nil), g.Data...)
				g.Data[len(g.Data)-1] ^= 0x55
				return g
			}
			return d
		}
		h.send(c, verFetch, fetchReq("t", 0, 0, 1<<20))
		if bug == BugNone {
			if len(c.out) != 0 {
				t.Fatal("corrupt data served")
			}
			h.fireAll()
		}
		p := fetched(h.next(c))
		_, err := kwire.ParseBatch(p.RecordBatches)
		switch bug {
		case BugNone:
			if err != nil || len(factsNamed(h.all, "corruption_detected")) != 1 {
				t.Fatalf("err %v", err)
			}
		case BugServeCorrupt:
			if err == nil {
				t.Fatal("mutant should serve corrupt bytes")
			}
		}
	}
}

func TestFetchGetsExhausted(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, ent{"t", 0, plain(t, "hello")})
	h.objHook = func(a core.IOAction, run func() core.Completion) core.Completion {
		d := run()
		if g, ok := d.(core.ObjGetDone); ok {
			g.Data = append([]byte(nil), g.Data...)
			g.Data[len(g.Data)-1] ^= 1
			return g
		}
		return d
	}
	h.send(c, verFetch, fetchReq("t", 0, 0, 1<<20))
	h.fireAll()
	if p := fetched(h.next(c)); p.ErrorCode != errKafkaStorage || len(p.RecordBatches) != 0 {
		t.Fatalf("%+v", p.ErrorCode)
	}
}

func TestWrongOffsetMutant(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Bug = BugWrongOffset
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.produceOne(c, ent{"t", 0, plain(t, "a")})
	p := fetched(h.call(c, verFetch, fetchReq("t", 0, 0, 1<<20)))
	if bt, err := kwire.ParseBatch(p.RecordBatches); err != nil || bt.BaseOffset != 1 {
		t.Fatalf("mutant base %v %v", bt.BaseOffset, err)
	}
}

func TestEarlyAckMutant(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Bug = BugEarlyAck
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.call(c, verMetadata, kmsg.NewPtrMetadataRequest()) // warm the topic cache
	h.holdCP = true
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
	h.flush()
	if len(c.out) != 1 || len(h.held) != 1 { // answered while the commit response is outstanding
		t.Fatalf("mutant should ack before the commit: out=%d", len(c.out))
	}
	h.holdCP = false
	h.releaseHeld()
	if len(c.out) != 1 {
		t.Fatal("double answer")
	}
	// The real broker waits.
	h = startedHarness(t, DefaultConfig())
	c = h.open(1)
	h.call(c, verMetadata, kmsg.NewPtrMetadataRequest()) // warm the topic cache
	h.holdCP = true
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
	h.flush()
	if len(c.out) != 0 {
		t.Fatal("real broker acked before commit")
	}
}

func TestBackpressure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxBufferedBytes, cfg.ResumeBufferedBytes = 300, 100
	h := startedHarness(t, cfg)
	c1 := h.open(1)
	h.open(2)
	h.call(c1, verMetadata, kmsg.NewPtrMetadataRequest())
	h.holdCP = true
	h.send(c1, verProduce, prodReq(-1, ent{"t", 0, plain(t, string(make([]byte, 400)))}))
	if !h.paused[1] || !h.paused[2] {
		t.Fatal("reads not paused")
	}
	h.open(3)
	h.do(core.ConnOpened{Conn: 4})
	if !h.paused[3] {
		t.Fatal("new connection not paused")
	}
	h.flush()
	if !h.paused[1] {
		t.Fatal("resumed before the commit")
	}
	h.holdCP = false
	h.releaseHeld()
	if h.paused[1] || h.paused[2] || h.paused[3] {
		t.Fatal("reads not resumed")
	}
	if r := produced(h.next(c1))[0]; r.ErrorCode != 0 {
		t.Fatal("admitted request dropped")
	}
}

func TestParkedUntilRegistered(t *testing.T) {
	h := newHarness(t, DefaultConfig())
	fail := 2
	h.cpHook = func(a core.CPCall, run func() core.Completion) core.Completion {
		if _, ok := a.Op.(core.Register); ok && fail > 0 {
			fail--
			return core.CPDone{Req: a.Req, Err: core.ErrUnavailable}
		}
		return run()
	}
	h.do(core.Started{Self: h.self})
	c := h.open(1)
	chunk := append(h.frame(c, verMetadata, kmsg.NewPtrMetadataRequest()), h.frame(c, 3, kmsg.NewPtrApiVersionsRequest())...)
	h.do(core.BytesReceived{Conn: 1, Data: chunk})
	if len(c.out) != 0 {
		t.Fatal("ApiVersions overtook a parked request")
	}
	h.fireAll()
	if len(c.out) != 2 {
		t.Fatalf("%d responses", len(c.out))
	}
	if _, ok := h.next(c).(*kmsg.MetadataResponse); !ok {
		t.Fatal("order")
	}
	// A stop before registration drops parked requests and exits.
	h = newHarness(t, DefaultConfig())
	h.holdCP = true
	h.do(core.Started{Self: h.self})
	c = h.open(1)
	h.send(c, verMetadata, kmsg.NewPtrMetadataRequest())
	h.do(core.Stop{})
	if !h.exited || !h.closed[1] {
		t.Fatalf("exited=%v closed=%v", h.exited, h.closed[1])
	}
}

func TestBadInputClosesConnection(t *testing.T) {
	cases := map[string][]byte{
		"garbage":     {0, 0, 0, 3, 1, 2, 3},
		"zero length": {0, 0, 0, 0},
		"huge length": {0x7f, 0xff, 0xff, 0xff},
		"unknown key": unknownKey(),
		"acks=0":      nil,
	}
	for name, data := range cases {
		h := startedHarness(t, DefaultConfig())
		c := h.open(1)
		if name == "acks=0" {
			data = h.frame(c, verProduce, prodReq(0, ent{"t", 0, plain(t, "a")}))
		}
		h.do(core.BytesReceived{Conn: 1, Data: data})
		if !h.closed[1] || len(c.out) != 0 {
			t.Errorf("%s: closed=%v out=%d", name, h.closed[1], len(c.out))
		}
		if h.cp.HighWatermark(core.TopicPartition{Topic: "t"}) != 0 || len(h.puts) != 0 {
			t.Errorf("%s: data written", name)
		}
	}
}

func unknownKey() []byte {
	f := binary.BigEndian.AppendUint16(nil, 99)
	f = binary.BigEndian.AppendUint16(f, 0)
	f = binary.BigEndian.AppendUint32(f, 1)
	f = binary.BigEndian.AppendUint16(f, 0xffff)
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(f))), f...)
}

func TestPeerCloseDuringProduce(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
	h.do(core.ConnClosed{Conn: 1, Abort: true})
	h.flush()
	if h.cp.HighWatermark(core.TopicPartition{Topic: "t"}) != 1 || len(c.out) != 0 {
		t.Fatal("write must land, response must not be sent")
	}
	if len(factsNamed(h.all, "produce_acked")) != 0 {
		t.Fatal("ack fact without a Send")
	}
}

func TestStateHash(t *testing.T) {
	run := func() []uint64 {
		h := startedHarness(t, DefaultConfig())
		c := h.open(1)
		var hs []uint64
		hs = append(hs, h.b.StateHash())
		h.send(c, verProduce, prodReq(-1, ent{"t", 0, plain(t, "a")}))
		hs = append(hs, h.b.StateHash())
		h.flush()
		hs = append(hs, h.b.StateHash())
		h.call(c, verFetch, fetchReq("t", 0, 0, 100))
		return append(hs, h.b.StateHash())
	}
	a, b := run(), run()
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("step %d differs", i)
		}
		if i > 0 && a[i] == a[i-1] {
			t.Fatalf("step %d did not change the hash", i)
		}
	}
	// Partial frame content is state.
	h := startedHarness(t, DefaultConfig())
	h.open(1)
	h.do(core.BytesReceived{Conn: 1, Data: []byte{0, 0, 0, 9, 1}})
	x := h.b.StateHash()
	h2 := startedHarness(t, DefaultConfig())
	h2.open(1)
	h2.do(core.BytesReceived{Conn: 1, Data: []byte{0, 0, 0, 9, 2}})
	if x == h2.b.StateHash() {
		t.Fatal("partial frame content not hashed")
	}
}

func TestIgnoresStrayCompletions(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	before := h.b.StateHash()
	h.step(core.CPDone{Req: 999})
	h.step(core.ObjPutDone{Req: 999})
	h.step(core.ObjGetDone{Req: 999})
	h.step(core.SendDone{Req: 1})
	h.step(core.TimerFired{ID: 77, Gen: 3})
	if h.b.StateHash() != before {
		t.Fatal("stray completion changed state")
	}
}

func FuzzBrokerBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 0, 0, 4, 0, 18, 0, 3})
	f.Add(unknownKey())
	good := mustFrames(f)
	for _, g := range good {
		f.Add(g)
		f.Add(g[:len(g)/2])
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		h := startedHarness(t, DefaultConfig())
		h.open(1)
		for len(data) > 0 {
			n := min(len(data), 1+int(data[0])%64)
			h.do(core.BytesReceived{Conn: 1, Data: data[:n]})
			data = data[n:]
		}
		h.fireAll()
		h.b.StateHash()
	})
}

func mustFrames(t testing.TB) [][]byte {
	enc := func(ver int16, req kmsg.Request) []byte {
		hd := kwire.RequestHeader{APIKey: req.Key(), APIVersion: ver, CorrelationID: 1, ClientID: "f"}
		b, err := kwire.Codec{}.EncodeRequest(hd, req)
		if err != nil {
			panic(err)
		}
		return b
	}
	return [][]byte{
		enc(verProduce, prodReq(-1, ent{"t", 0, mkBatch(t, -1, -1, -1, "a")}, ent{"t", 1, mkBatch(t, 1000, 0, 0, "b")})),
		enc(verFetch, fetchReq("t", 0, 0, 100)),
		enc(verMetadata, kmsg.NewPtrMetadataRequest()),
		enc(verInit, initReq()),
		enc(3, kmsg.NewPtrApiVersionsRequest()),
	}
}
