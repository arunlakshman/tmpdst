package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
)

// batch is one produce batch for one partition, retried unchanged (same
// producer id, epoch and base sequence) until resolved.
type batch struct {
	id        core.ID
	tpIdx     int
	vals      []string
	data      []byte
	attempts  int
	ambiguous bool // some attempt ended without a definite answer
	lastCode  int16
	dig       uint64 // hash of data, so StateHash does not rescan it
}

// producer is the idempotent producer state of one client. cur holds the
// unresolved batches of the current request (distinct partitions).
type producer struct {
	pid    int64 // 0: needs InitProducerId
	pepoch int16
	seqs   []int32 // next base sequence per partition index
	serial int     // batches built by this client (values stay unique across re-inits)
	cur    []*batch
	noNew  bool
}

func (p *producer) hash(h *hasher) {
	h.i64(p.pid)
	h.u64(uint64(p.pepoch))
	for _, s := range p.seqs {
		h.u64(uint64(s))
	}
	h.u64(uint64(p.serial))
	h.bool(p.noNew)
	for _, b := range p.cur {
		h.bytes(b.id[:])
		h.u64(uint64(b.tpIdx))
		h.u64(uint64(b.attempts))
		h.bool(b.ambiguous)
		h.u64(uint64(b.lastCode))
		h.u64(b.dig)
	}
}

// Kafka codes the producer reacts to.
const (
	codeTimedOut     int16 = 7
	codeStorage      int16 = 56
	codeUnknown      int16 = -1
	codeCoordNA      int16 = 15
	codeUnknownTopic int16 = 3
	codeLeaderNA     int16 = 5
	codeNotLeader    int16 = 6
)

// ambiguousCode: the broker may still have committed the batch (it gave up
// waiting). definiteRetriable: the batch was not committed; try again.
func ambiguousCode(c int16) bool {
	return c == codeTimedOut || c == codeStorage || c == codeUnknown || c == codeCoordNA
}
func definiteRetriable(c int16) bool {
	return c == codeUnknownTopic || c == codeLeaderNA || c == codeNotLeader
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
		if b.attempts >= maxAttempts {
			return w.giveUp(c, "retries exhausted")
		}
	}
	return w.sendProduce(c)
}

// newBatches builds the next request: 1 or 2 batches for distinct partitions.
func (w *Workload) newBatches(c *client) {
	p := &c.p
	k := 1 + w.env.Intn(2)
	first := w.env.Intn(len(w.tps))
	for j := 0; j < k; j++ {
		idx := first
		if j == 1 {
			idx = (first + 1 + w.env.Intn(len(w.tps)-1)) % len(w.tps)
		}
		n := 1 + w.env.Intn(5)
		pad := 0
		switch w.env.Intn(16) {
		case 0:
			pad = 100 + w.env.Intn(300)
		default:
			pad = w.env.Intn(24)
		}
		p.serial++
		b := &batch{id: w.env.NewID(), tpIdx: idx}
		recs := make([]kmsg.Record, n)
		for i := range recs {
			v := fmt.Sprintf("c%d.b%d.r%d|%s", c.id, p.serial, i, strings.Repeat("x", pad))
			b.vals = append(b.vals, v)
			recs[i] = kmsg.Record{Value: []byte(v)}
		}
		data, err := kwire.BuildBatch(kwire.BatchParams{ProducerID: p.pid, ProducerEpoch: p.pepoch, BaseSequence: p.seqs[idx],
			FirstTimestamp: 1, MaxTimestamp: 1}, recs)
		if err != nil {
			w.env.Unreachable("kafka: client batch builds", core.Detail{Key: "err", Value: err.Error()})
			continue
		}
		b.data = data
		dh := newHasher()
		dh.bytes(data)
		b.dig = uint64(dh)
		if w.hist != nil {
			w.hist.Invoke(w.self, b.id, b.id, ProduceOp{Client: c.id, Batch: p.serial, TP: w.tps[idx], Vals: b.vals,
				Producer: p.pid, PEpoch: p.pepoch, BaseSeq: p.seqs[idx]})
		}
		p.seqs[idx] += int32(n)
		p.cur = append(p.cur, b)
	}
}

func (w *Workload) sendProduce(c *client) []core.Action {
	r := kmsg.NewPtrProduceRequest()
	r.Acks, r.TimeoutMillis = -1, 1000
	for _, b := range c.p.cur {
		b.attempts++
		tp := w.tps[b.tpIdx]
		part := kmsg.ProduceRequestTopicPartition{Partition: tp.Partition, Records: b.data}
		added := false
		for i := range r.Topics {
			if r.Topics[i].Topic == tp.Topic {
				r.Topics[i].Partitions = append(r.Topics[i].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.ProduceRequestTopic{Topic: tp.Topic, Partitions: []kmsg.ProduceRequestTopicPartition{part}})
		}
	}
	return w.send(c, rqProduce, r)
}

func (w *Workload) onInit(c *client, r *kmsg.InitProducerIDResponse) []core.Action {
	if r.ErrorCode != 0 || r.ProducerID <= 0 {
		return w.retryStage(c)
	}
	p := &c.p
	p.pid, p.pepoch = r.ProducerID, r.ProducerEpoch
	for i := range p.seqs {
		p.seqs[i] = 0
	}
	c.stage, c.fails = stRun, 0
	return w.advance(c)
}

func (w *Workload) think(c *client) core.Action {
	return w.later(c, core.Duration(20+w.env.Intn(60))*core.Millisecond)
}

// onProduce resolves each batch of the request from its partition's answer.
func (w *Workload) onProduce(c *client, r *kmsg.ProduceResponse) []core.Action {
	p := &c.p
	var keep []*batch
	fatal := false
	for _, b := range p.cur {
		tp := w.tps[b.tpIdx]
		code, base, found := codeUnknown, int64(-1), false
		for _, t := range r.Topics {
			for _, pr := range t.Partitions {
				if t.Topic == tp.Topic && pr.Partition == tp.Partition {
					code, base, found = pr.ErrorCode, pr.BaseOffset, true
				}
			}
		}
		b.lastCode = code
		switch {
		case !found:
			b.ambiguous = true
			keep = append(keep, b)
		case code == 0:
			w.resolve(b, sim.OpDefinite, ProduceRes{Base: base, Attempts: b.attempts})
		case ambiguousCode(code):
			b.ambiguous = true
			keep = append(keep, b)
		case definiteRetriable(code):
			keep = append(keep, b)
		default: // out of order, stale epoch, corrupt, ...: the producer is unusable
			fatal = true
			keep = append(keep, b)
		}
	}
	p.cur = keep
	switch {
	case fatal:
		return w.giveUp(c, "fatal produce error")
	case len(keep) > 0:
		return []core.Action{w.later(c, retryDelay(keep[0].attempts))}
	}
	c.fails = 0
	return []core.Action{w.think(c)}
}

func (w *Workload) resolve(b *batch, out sim.OpOutcome, res ProduceRes) {
	if w.hist != nil {
		w.hist.Return(b.id, res, out)
	}
}

// giveUp abandons every unresolved batch (Indeterminate if any attempt was
// ambiguous: it may still land at any later time) and re-inits the producer id,
// as Kafka does.
func (w *Workload) giveUp(c *client, why string) []core.Action {
	p := &c.p
	for _, b := range p.cur {
		out := sim.OpAbandoned
		if b.ambiguous {
			out = sim.OpIndeterminate
		}
		w.resolve(b, out, ProduceRes{Base: -1, Code: b.lastCode, Why: why, Attempts: b.attempts})
	}
	p.cur, p.pid = nil, 0
	c.stage = stInit
	return []core.Action{w.think(c)}
}
