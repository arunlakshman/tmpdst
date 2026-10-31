package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"mds/internal/core"
	"mds/internal/verify/simulator"
	"mds/internal/verify/simulator/internal/session"
	"mds/internal/wire/kafkawire"
)

// batch is one produce batch for one partition, retried unchanged until
// resolved. Producers are not idempotent (decision 41): a retry after an
// ambiguous answer may commit the batch twice. idem marks the rare batch that
// deliberately claims a producer id: the broker must reject it with
// INVALID_REQUEST, per partition, and never commit it.
type batch struct {
	id        core.ID
	tpIdx     int
	vals      []string
	data      []byte
	attempts  int
	ambiguous bool // some attempt ended without a definite answer
	lastCode  int16
	dig       uint64 // hash of data, so StateHash does not rescan it
	idem      bool   // carries producer id duplicateCheckProducerID: must be rejected
	corrupt   bool   // payload damaged after the CRC was computed: must be rejected
}

// producer is the at-least-once producer state of one client. cur holds the
// unresolved batches of the current request (distinct partitions).
type producer struct {
	serial int // batches built by this client (values stay unique)
	cur    []*batch
	noNew  bool
}

// duplicateCheckProducerID is the producer id of a deliberately idempotent batch.
const duplicateCheckProducerID = 7

// codeInvalidRequest is INVALID_REQUEST, the broker's answer to a batch that
// carries a producer id. codeCorruptMessage is CORRUPT_MESSAGE, its answer to a
// batch whose CRC does not match.
const (
	codeInvalidRequest int16 = 42
	codeCorruptMessage int16 = 2
)

// expect is the code the broker must answer a batch that is never admitted
// (0: the batch is good).
func (b *batch) expect() int16 {
	switch {
	case b.idem:
		return codeInvalidRequest
	case b.corrupt:
		return codeCorruptMessage
	}
	return 0
}

func (p *producer) hash(h *core.Hasher) {
	h.Uint64Value(uint64(p.serial))
	h.Bool(p.noNew)
	for _, b := range p.cur {
		h.Bytes(b.id[:])
		h.Uint64Value(uint64(b.tpIdx))
		h.Uint64Value(uint64(b.attempts))
		h.Bool(b.ambiguous)
		h.Bool(b.idem)
		h.Bool(b.corrupt)
		h.Uint64Value(uint64(b.lastCode))
		h.Uint64Value(b.dig)
	}
}

// Kafka codes the producer reacts to.
const (
	codeTimedOut               int16 = 7
	codeStorage                int16 = 56
	codeUnknown                int16 = -1
	codeCoordinatorUnavailable int16 = 15
	codeUnknownTopic           int16 = 3
	codeLeaderUnavailable      int16 = 5
	codeNotLeader              int16 = 6
)

// unclearResultCode: the broker may still have committed the batch (it gave up
// waiting). clearlyRetryable: the batch was not committed; try again.
func unclearResultCode(c int16) bool {
	return c == codeTimedOut || c == codeStorage || c == codeUnknown || c == codeCoordinatorUnavailable
}
func clearlyRetryable(c int16) bool {
	return c == codeUnknownTopic || c == codeLeaderUnavailable || c == codeNotLeader
}

func (w *Workload) produceNext(c *client) []core.Action {
	p := &c.p
	if len(p.cur) == 0 {
		if p.noNew {
			return nil
		}
		w.newBatches(c)
	}
	for _, b := range p.cur {
		if b.attempts >= w.opts.maxAttempts() {
			return w.giveUp(c, "retries exhausted")
		}
	}
	return w.sendProduce(c)
}

// duplicateCheckEvery: one batch in duplicateCheckEvery claims a producer id and must be rejected.
// corruptEvery: one batch in corruptEvery is damaged in flight (CRC mismatch).
const (
	duplicateCheckEvery = 40
	corruptEvery        = 40
)

// pickCodec draws a codec id: none for a third of the batches, otherwise gzip,
// snappy, lz4 or zstd with equal weight.
func (w *Workload) pickCodec() int16 {
	if c := w.env.Intn(6); c >= 2 {
		return int16(c - 1)
	}
	return kafkawire.CodecNone
}

// newBatches builds the next request: 1 or 2 batches for distinct partitions.
func (w *Workload) newBatches(c *client) {
	p := &c.p
	k := 1 + w.env.Intn(2)
	first := w.env.Intn(len(w.tps))
	for j := range k {
		idx := first
		if j == 1 {
			idx = (first + 1 + w.env.Intn(len(w.tps)-1)) % len(w.tps)
		}
		n := 1 + w.env.Intn(5)
		pad := 0
		switch w.env.Intn(16) {
		case 0:
			pad = 100 + w.env.Intn(300)
			if w.env.Intn(2) == 0 { // a batch of several KiB: larger than a small segment cap
				pad = 2000 + w.env.Intn(2000)
			}
		default:
			pad = w.env.Intn(24)
		}
		p.serial++
		b := &batch{id: w.env.NewID(), tpIdx: idx}
		recs := make([]kmsg.Record, n)
		for i := range recs {
			v := fmt.Sprintf("c%d.b%d.r%d|%s", c.ID, p.serial, i, strings.Repeat("x", pad))
			b.vals = append(b.vals, v)
			recs[i] = kmsg.Record{Value: []byte(v)}
		}
		b.idem = w.env.Intn(duplicateCheckEvery) == 0
		pp := kafkawire.BatchParams{ProducerID: -1, ProducerEpoch: -1, BaseSequence: -1, FirstTimestamp: 1, MaxTimestamp: 1}
		if b.idem {
			pp.ProducerID, pp.ProducerEpoch, pp.BaseSequence = duplicateCheckProducerID, 0, 0
		}
		data, err := buildBatch(pp, recs, w.pickCodec())
		if err != nil {
			w.env.Unreachable("kafka: client batch builds", core.ContextPair{Key: "err", Value: err.Error()})
			continue
		}
		b.corrupt = !b.idem && w.env.Intn(corruptEvery) == 0
		if b.corrupt {
			data[len(data)-1] ^= 0x40
		}
		b.data = data
		dh := core.NewHasher()
		dh.Bytes(data)
		b.dig = uint64(dh)
		if w.history != nil {
			w.history.Invoke(w.self, b.id, b.id, ProduceOp{Client: c.ID, Batch: p.serial, TopicPartition: w.tps[idx], Vals: b.vals, Idem: b.idem})
		}
		p.cur = append(p.cur, b)
	}
}

func (w *Workload) sendProduce(c *client) []core.Action {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = -1, 1000
	for _, b := range c.p.cur {
		b.attempts++
		topicPartition := w.tps[b.tpIdx]
		part := kmsg.ProduceRequestTopicPartition{Partition: topicPartition.Partition, Records: b.data}
		added := false
		for i := range r.Topics {
			if r.Topics[i].Topic == topicPartition.Topic {
				r.Topics[i].Partitions = append(r.Topics[i].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.ProduceRequestTopic{Topic: topicPartition.Topic, Partitions: []kmsg.ProduceRequestTopicPartition{part}})
		}
	}
	return w.send(c, requestProduce, r)
}

func (w *Workload) think(c *client) core.Action {
	return c.Wake(core.Duration(20+w.env.Intn(60)) * core.Millisecond)
}

// onProduce resolves each batch of the request from its partition's answer.
func (w *Workload) onProduce(c *client, r *kmsg.ProduceResponse) []core.Action {
	p := &c.p
	var keep []*batch
	fatal := false
	for _, b := range p.cur {
		topicPartition := w.tps[b.tpIdx]
		code, base, found := codeUnknown, int64(-1), false
		for _, t := range r.Topics {
			for _, pr := range t.Partitions {
				if t.Topic == topicPartition.Topic && pr.Partition == topicPartition.Partition {
					code, base, found = pr.ErrorCode, pr.BaseOffset, true
				}
			}
		}
		b.lastCode = code
		switch {
		case !found:
			b.ambiguous = true
			keep = append(keep, b)
		case b.expect() != 0 && code == b.expect():
			// Rejected at admission, however many earlier attempts were lost.
			w.resolve(b, simulator.OpGivenUp, ProduceRes{Base: -1, Code: code, Why: "rejected", Attempts: b.attempts})
		case b.idem && code == 0:
			w.env.Unreachable("kafka: a batch with a producer id is accepted")
			w.resolve(b, simulator.OpResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case b.corrupt && code == 0:
			w.env.Unreachable("kafka: a batch with a bad CRC is accepted")
			w.resolve(b, simulator.OpResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case code == 0:
			w.resolve(b, simulator.OpResultKnown, ProduceRes{Base: base, Attempts: b.attempts})
		case unclearResultCode(code):
			b.ambiguous = true
			keep = append(keep, b)
		case clearlyRetryable(code):
			keep = append(keep, b)
		default: // corrupt, invalid, ...: not retriable
			fatal = true
			keep = append(keep, b)
		}
	}
	p.cur = keep
	switch {
	case fatal:
		return w.giveUp(c, "fatal produce error")
	case len(keep) > 0:
		return []core.Action{c.Wake(session.RetryWaitTime(keep[0].attempts))}
	}
	c.Fails = 0
	return []core.Action{w.think(c)}
}

func (w *Workload) resolve(b *batch, out simulator.OpOutcome, res ProduceRes) {
	if w.history != nil {
		w.history.Return(b.id, res, out)
	}
}

// giveUp abandons every unresolved batch (Indeterminate if any attempt was
// ambiguous: it may still land at any later time).
func (w *Workload) giveUp(c *client, why string) []core.Action {
	p := &c.p
	for _, b := range p.cur {
		out := simulator.OpGivenUp
		if b.ambiguous {
			out = simulator.OpResultUnknown
		}
		w.resolve(b, out, ProduceRes{Base: -1, Code: b.lastCode, Why: why, Attempts: b.attempts})
	}
	p.cur = nil
	return []core.Action{w.think(c)}
}
