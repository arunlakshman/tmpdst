package broker

import (
	"encoding/binary"
	"reflect"
	"regexp"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/wire/format"
	"manifester/internal/wire/kwire"
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
	for _, k := range r.ApiKeys {
		if k.ApiKey == 22 {
			t.Fatal("InitProducerId advertised")
		}
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
	if len(r.Topics) != 2 || kafkaCode(r.Topics[0].ErrorCode) != errUnknownTopicOrPart || r.Topics[1].ErrorCode != 0 {
		t.Fatalf("topics %+v", r.Topics)
	}
	for _, cmd := range h.cp.Exec(core.ControlPlaneCall{Req: 1, Op: core.DescribeTopics{Topics: []string{"nope"}}}).(core.ControlPlaneDone).Resp.(core.DescribeTopicsResp).Topics {
		t.Fatalf("topic auto-created: %v", cmd)
	}
	q.Topics = []kmsg.MetadataRequestTopic{}
	r = h.call(c, verMetadata, q).(*kmsg.MetadataResponse)
	if len(r.Topics) != 0 || len(r.Brokers) != 1 {
		t.Fatalf("empty request: %+v", r)
	}
}

func TestProduceFetchRoundTrip(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	b1, b2, b3 := plain(t, "a", "b"), plain(t, "c"), plain(t, "d", "e", "f")
	h.send(c, verProduce, prodReq(-1, e("t", 0, b1), e("t", 1, b2), e("t", 0, b3)))
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
	p := fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 0, 1<<20)))
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
	p = fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 3, 1<<20)))
	if bs, _ := kwire.SplitBatches(p.RecordBatches); len(bs) != 1 {
		t.Fatalf("mid fetch %d batches", len(bs))
	}
	// Partition limit: at least one batch, no more than fits.
	p = fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 0, 1)))
	if bs, _ := kwire.SplitBatches(p.RecordBatches); len(bs) != 1 {
		t.Fatalf("limited fetch %d batches", len(bs))
	}
	if p = fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 5, 1<<20))); len(p.RecordBatches) != 0 || p.ErrorCode != 0 || p.HighWatermark != 5 {
		t.Fatalf("at HWM: %+v", p)
	}
	if p = fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 6, 1<<20))); kafkaCode(p.ErrorCode) != errOffsetOutOfRange {
		t.Fatalf("beyond HWM: %d", p.ErrorCode)
	}
	if p = fetched(h.call(c, verFetch, fetchReq(core.TP("zz", 0), 0, 1<<20))); kafkaCode(p.ErrorCode) != errUnknownTopicOrPart {
		t.Fatalf("unknown topic: %d", p.ErrorCode)
	}
	h.noViolations()
}

func TestProduceErrors(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	bad := plain(t, "x")
	bad[len(bad)-1] ^= 0xff
	var txn []byte
	// A transactional batch needs a valid CRC, so build one with the flag.
	p := params
	p.Attributes, p.ProducerID, p.ProducerEpoch, p.BaseSequence = 0x10, 1000, 0, 0
	txn, _ = kwire.BuildBatch(p, []kmsg.Record{{Value: []byte("x")}})
	two := append(plain(t, "x"), plain(t, "y")...)
	h.send(c, verProduce, prodReq(-1,
		e("nope", 0, plain(t, "x")), e("t", 9, plain(t, "x")), e("t", 0, bad),
		e("t", 0, txn), e("t", 0, two), e("t", 0, nil), e("t", 0, plain(t, "ok"))))
	h.flush()
	ps := produced(h.next(c))
	want := []kafkaCode{errUnknownTopicOrPart, errUnknownTopicOrPart, errCorruptMessage, errInvalidRequest, errCorruptMessage, errCorruptMessage, 0}
	for i, w := range want {
		if kafkaCode(ps[i].ErrorCode) != w {
			t.Errorf("partition %d: error %d, want %d", i, ps[i].ErrorCode, w)
		}
	}
	if q := h.call(c, verProduce, prodReq(7, e("t", 0, plain(t, "x")))); kafkaCode(produced(q)[0].ErrorCode) != errInvalidRequiredAcks {
		t.Fatal("bad acks accepted")
	}
}

func TestTopicCacheRefresh(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.cp.CreateTopic("late", 1)
	r := h.produceOne(c, e("late", 0, plain(t, "x")))
	if r.ErrorCode != 0 || r.BaseOffset != 0 {
		t.Fatalf("%+v", r)
	}
	n := len(h.cpCalls)
	h.produceOne(c, e("late", 0, plain(t, "y")))
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
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a")), e("t", 0, plain(t, "b"))))
	if len(h.puts) != 1 {
		t.Fatalf("size trigger: %d puts", len(h.puts))
	}
	produced(h.next(c))

	cfg = DefaultConfig()
	cfg.MaxSegmentPartitions = 2
	h = startedHarness(t, cfg)
	h.cp.CreateTopic("m", 5)
	c = h.open(1)
	h.send(c, verProduce, prodReq(-1, e("m", 0, plain(t, "a")), e("m", 0, plain(t, "a")), e("m", 1, plain(t, "a")), e("m", 2, plain(t, "a"))))
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
	chunk = append(chunk, h.frame(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))...)
	chunk = append(chunk, h.frame(c, 3, kmsg.NewPtrApiVersionsRequest())...)
	chunk = append(chunk, h.frame(c, verFetch, fetchReq(core.TP("t", 0), 0, 1<<20))...)
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

func TestFactImmediatelyBeforeSend(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, e("t", 0, plain(t, "a", "b")))
	n := 0
	for i, a := range h.all {
		e, ok := a.(core.RecordFact)
		if !ok || e.Fact.Name != "produce_acked" {
			continue
		}
		n++
		if _, ok := h.all[i+1].(core.Send); !ok {
			t.Fatalf("ack fact not followed by Send: %T", h.all[i+1])
		}
		f := e.Fact
		if f.Field("topic") != core.Str("t") || f.Field("base_offset") != core.I64(0) || f.Field("count") != core.I64(2) {
			t.Fatalf("fields %v", f.Fields)
		}
	}
	if n != 1 {
		t.Fatalf("%d acks", n)
	}
}

func TestGracefulStop(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	if len(h.puts) != 0 {
		t.Fatal("flushed early")
	}
	h.do(core.StopRequest{Deadline: h.now + core.Time(core.Second)})
	if !h.exited || !h.paused[1] {
		t.Fatalf("exited=%v paused=%v", h.exited, h.paused[1])
	}
	if _, ok := h.all[len(h.all)-1].(core.ShutdownDone); !ok {
		t.Fatal("Exit is not last")
	}
	if r := produced(h.next(c))[0]; r.ErrorCode != 0 || r.BaseOffset != 0 {
		t.Fatalf("%+v", r)
	}
	if h.cp.End(core.TP("t", 0)) != 1 {
		t.Fatal("not flushed")
	}
	// Idle broker exits at once.
	h2 := startedHarness(t, DefaultConfig())
	h2.do(core.StopRequest{})
	if !h2.exited {
		t.Fatal("idle stop")
	}
}

func TestListOffsets(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.produceOne(c, e("t", 0, plain(t, "a", "b", "c")))
	q := kmsg.NewPtrListOffsetsRequest()
	q.ReplicaID = -1
	mk := func(topic string, p int32, ts int64) kmsg.ListOffsetsRequestTopic {
		return kmsg.ListOffsetsRequestTopic{Topic: topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{{Partition: p, Timestamp: ts, CurrentLeaderEpoch: -1}}}
	}
	q.Topics = []kmsg.ListOffsetsRequestTopic{mk("t", 0, -2), mk("t", 0, -1), mk("t", 1, -1), mk("t", 0, 12345), mk("zz", 0, -1)}
	r := h.call(c, verList, q).(*kmsg.ListOffsetsResponse)
	get := func(i int) kmsg.ListOffsetsResponseTopicPartition { return r.Topics[i].Partitions[0] }
	if get(0).Offset != 0 || get(0).ErrorCode != 0 || get(1).Offset != 3 || get(2).Offset != 0 ||
		kafkaCode(get(3).ErrorCode) != errUnsupportedForFormat || kafkaCode(get(4).ErrorCode) != errUnknownTopicOrPart {
		t.Fatalf("%+v", r.Topics)
	}
}

func TestCorruptGetRetried(t *testing.T) {
	for _, bug := range []Bug{BugNone, BugServeCorrupt} {
		cfg := DefaultConfig()
		cfg.Bug = bug
		h := startedHarness(t, cfg)
		c := h.open(1)
		h.produceOne(c, e("t", 0, plain(t, "hello")))
		corrupt := true
		h.objHook = func(a core.IOCall, run func() core.IOResult) core.IOResult {
			d := run()
			if g, ok := d.(core.ObjectGetDone); ok && corrupt {
				corrupt = false
				g.Data = append([]byte(nil), g.Data...)
				g.Data[len(g.Data)-1] ^= 0x55
				return g
			}
			return d
		}
		h.send(c, verFetch, fetchReq(core.TP("t", 0), 0, 1<<20))
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
	h.produceOne(c, e("t", 0, plain(t, "hello")))
	h.objHook = func(a core.IOCall, run func() core.IOResult) core.IOResult {
		d := run()
		if g, ok := d.(core.ObjectGetDone); ok {
			g.Data = append([]byte(nil), g.Data...)
			g.Data[len(g.Data)-1] ^= 1
			return g
		}
		return d
	}
	h.send(c, verFetch, fetchReq(core.TP("t", 0), 0, 1<<20))
	h.fireAll()
	if p := fetched(h.next(c)); kafkaCode(p.ErrorCode) != errKafkaStorage || len(p.RecordBatches) != 0 {
		t.Fatalf("%+v", p.ErrorCode)
	}
}

func TestWrongOffsetMutant(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Bug = BugWrongOffset
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.produceOne(c, e("t", 0, plain(t, "a")))
	p := fetched(h.call(c, verFetch, fetchReq(core.TP("t", 0), 0, 1<<20)))
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
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
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
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
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
	h.send(c1, verProduce, prodReq(-1, e("t", 0, plain(t, string(make([]byte, 400))))))
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
			data = h.frame(c, verProduce, prodReq(0, e("t", 0, plain(t, "a"))))
		}
		h.do(core.BytesReceived{Conn: 1, Data: data})
		if !h.closed[1] || len(c.out) != 0 {
			t.Errorf("%s: closed=%v out=%d", name, h.closed[1], len(c.out))
		}
		if h.cp.End(core.TP("t", 0)) != 0 || len(h.puts) != 0 {
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
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.do(core.ConnClosed{Conn: 1, Abort: true})
	h.flush()
	if h.cp.End(core.TP("t", 0)) != 1 || len(c.out) != 0 {
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
		h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
		hs = append(hs, h.b.StateHash())
		h.flush()
		hs = append(hs, h.b.StateHash())
		h.call(c, verFetch, fetchReq(core.TP("t", 0), 0, 100))
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

func TestIgnoresStrayIOResults(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	before := h.b.StateHash()
	h.step(core.ControlPlaneDone{Req: 999})
	h.step(core.ObjectPutDone{Req: 999})
	h.step(core.ObjectGetDone{Req: 999})
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
		enc(verProduce, prodReq(-1, e("t", 0, mkBatch(t, -1, -1, -1, "a")), e("t", 1, mkBatch(t, 1000, 0, 0, "b")))),
		enc(verFetch, fetchReq(core.TP("t", 0), 0, 100)),
		enc(verMetadata, kmsg.NewPtrMetadataRequest()),
		enc(3, kmsg.NewPtrApiVersionsRequest()),
	}
}

// A batch from an idempotent producer (pid >= 0) is refused alone with
// INVALID_REQUEST; the other partitions of the same produce still work.
func TestIdempotentBatchRejected(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.send(c, verProduce, prodReq(-1,
		e("t", 0, mkBatch(t, 1000, 0, 0, "a")),
		e("t", 1, plain(t, "ok")),
		e("t", 0, mkBatch(t, 0, 0, 0, "a")),
		e("t", 0, plain(t, "ok2"))))
	h.flush()
	ps := produced(h.next(c))
	if kafkaCode(ps[0].ErrorCode) != errInvalidRequest || kafkaCode(ps[2].ErrorCode) != errInvalidRequest ||
		ps[1].ErrorCode != 0 || ps[3].ErrorCode != 0 || ps[3].BaseOffset != 0 {
		t.Fatalf("%+v", ps)
	}
	if h.cp.End(core.TP("t", 0)) != 1 {
		t.Fatal("rejected batch was stored")
	}
	h.noViolations()
}

func commitOps(h *harness) []core.CommitSegment {
	var out []core.CommitSegment
	for _, op := range h.cpCalls {
		if cs, ok := op.(core.CommitSegment); ok {
			out = append(out, cs)
		}
	}
	return out
}

// After a lost commit answer the broker resends the identical request with the
// same key (no new upload) and acks the originally assigned offsets.
func TestCommitRetryAfterMaybeAppliedResendsIdenticalRequest(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	lose := 2
	h.cpHook = func(a core.ControlPlaneCall, run func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CommitSegment); ok && lose > 0 {
			lose--
			run() // applied, response lost
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrTimeout, Outcome: core.MaybeApplied}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a", "b")), e("t", 1, plain(t, "c"))))
	h.flush()
	if len(c.out) != 0 {
		t.Fatal("acked on a lost commit response")
	}
	h.fireAll()
	ps := produced(h.next(c))
	if ps[0].ErrorCode != 0 || ps[0].BaseOffset != 0 || ps[1].ErrorCode != 0 || ps[1].BaseOffset != 0 ||
		h.cp.End(core.TP("t", 0)) != 2 {
		t.Fatalf("%+v", ps)
	}
	cs := commitOps(h)
	if len(cs) != 3 || len(h.puts) != 1 {
		t.Fatalf("%d commits, %d puts", len(cs), len(h.puts))
	}
	for _, x := range cs[1:] {
		if !reflect.DeepEqual(x, cs[0]) {
			t.Fatalf("resend differs:\n%+v\n%+v", x, cs[0])
		}
	}
	if n := len(factsNamed(h.all, "segment_committed")); n != 1 {
		t.Fatalf("%d segment_committed", n)
	}
	h.noViolations()
}

// Two segments in flight commit in seal order and get consecutive offsets.
func TestTwoSegmentsInFlightCommitInOrder(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	h.holdPut = true
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.flush()
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "b", "c"))))
	h.flush()
	if len(h.held) != 2 || len(commitOps(h)) != 0 {
		t.Fatalf("held %d", len(h.held))
	}
	// The second upload finishes first: its commit must still wait.
	h.holdPut = false
	second := h.held[1]
	h.held = h.held[:1]
	h.queue = append(h.queue, second)
	h.run()
	if len(commitOps(h)) != 0 {
		t.Fatal("second segment committed before the first")
	}
	h.releaseHeld()
	cs := commitOps(h)
	if len(cs) != 2 || cs[0].SegmentKey != h.puts[0] || cs[1].SegmentKey != h.puts[1] {
		t.Fatalf("commits %+v puts %v", cs, h.puts)
	}
	p0, p1 := produced(h.next(c))[0], produced(h.next(c))[0]
	if p0.BaseOffset != 0 || p1.BaseOffset != 1 {
		t.Fatalf("bases %d %d", p0.BaseOffset, p1.BaseOffset)
	}
}

var segKeyRE = regexp.MustCompile(`^seg/[0-9a-f]{32}$`)

// Keys are seg/<32 hex> drawn from the Env stream: the same seed gives the
// same keys, and they equal format.SegmentKey of the env's IDs.
func TestSegmentKeysAreFlatAndDeterministic(t *testing.T) {
	run := func() []string {
		h := startedHarness(t, DefaultConfig())
		c := h.open(1)
		for range 3 {
			h.produceOne(c, e("t", 0, plain(t, "a")))
		}
		return h.puts
	}
	a, b := run(), run()
	if len(a) != 3 || !reflect.DeepEqual(a, b) {
		t.Fatalf("keys differ: %v %v", a, b)
	}
	env := &testEnv{state: 7}
	for _, k := range a {
		if !segKeyRE.MatchString(k) || k != format.SegmentKey(env.NewID()) {
			t.Fatalf("key %q", k)
		}
	}
	other := startedHarness(t, DefaultConfig())
	other.env.state = 8
	oc := other.open(1)
	other.produceOne(oc, e("t", 0, plain(t, "a")))
	if other.puts[0] == a[0] {
		t.Fatal("different seeds, same key")
	}
}

// A CP outage far longer than the old six-attempt budget (about 0.6 s) fails
// nothing: the commit is retried with capped, jittered backoff and the produce
// is acked once the CP is back (audit item 8).
func TestCommitSurvivesLongCPOutage(t *testing.T) {
	h := startedHarness(t, DefaultConfig())
	c := h.open(1)
	down := true
	h.cpHook = func(a core.ControlPlaneCall, run func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CommitSegment); ok && down {
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.flush()
	start := h.now
	for h.now-start < core.Time(10*core.Second) {
		if len(h.timers) != 1 {
			t.Fatalf("want one backoff timer, have %d", len(h.timers))
		}
		var id core.TimerID
		for k := range h.timers {
			id = k
		}
		h.now += core.Time(2 * core.Second)
		h.fire(id)
	}
	if len(c.out) != 0 {
		t.Fatal("answered during the outage")
	}
	down = false
	for id := range h.timers {
		h.now += core.Time(2 * core.Second)
		h.fire(id)
		break
	}
	ps := produced(h.next(c))
	if ps[0].ErrorCode != 0 {
		t.Fatalf("%+v", ps)
	}
	if len(commitOps(h)) < 6 {
		t.Fatalf("only %d commits", len(commitOps(h)))
	}
	h.noViolations()
}

// Past CommitDeadline the segment fails with REQUEST_TIMED_OUT and its buffer
// is released.
func TestCommitDeadlineFailsSegmentAndReleasesBuffer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CommitDeadline = 5 * core.Second
	h := startedHarness(t, cfg)
	c := h.open(1)
	h.cpHook = func(a core.ControlPlaneCall, run func() core.IOResult) core.IOResult {
		if _, ok := a.Op.(core.CommitSegment); ok {
			return core.ControlPlaneDone{Req: a.Req, Err: core.ErrUnavailable, Outcome: core.NotApplied}
		}
		return run()
	}
	h.send(c, verProduce, prodReq(-1, e("t", 0, plain(t, "a"))))
	h.flush()
	for i := 0; len(h.timers) > 0; i++ {
		if i > 100 {
			t.Fatal("never gave up")
		}
		var id core.TimerID
		for k := range h.timers {
			id = k
		}
		h.now += core.Time(core.Second)
		h.fire(id)
	}
	ps := produced(h.next(c))
	if kafkaCode(ps[0].ErrorCode) != errRequestTimedOut {
		t.Fatalf("%+v", ps)
	}
	if h.b.bufBytes != 0 || len(h.b.segs) != 0 {
		t.Fatalf("buffer %d, segments %d", h.b.bufBytes, len(h.b.segs))
	}
	h.noViolations()
}
