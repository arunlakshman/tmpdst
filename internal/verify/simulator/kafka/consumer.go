package kafka

import (
	"github.com/twmb/franz-go/pkg/kmsg"

	"manifester/internal/core"
	"manifester/internal/verify/simulator"
	"manifester/internal/wire/kafkawire"
)

// consumer reads every partition from offset 0 with one multi-partition Fetch
// at a time. pos is the next offset wanted per partition.
type consumer struct {
	pos    []int64
	listed bool
	op     core.ID // outstanding ReadOp, zero when none
	drain  bool    // PhaseSettle began: stop once a fetch finds the end of every partition
	done   bool
}

func (c *consumer) hash(h *core.Hasher) {
	for _, p := range c.pos {
		h.Int64Value(p)
	}
	h.Bool(c.listed)
	h.Bytes(c.op[:])
	h.Bool(c.drain)
	h.Bool(c.done)
}

func (w *Workload) listReq() *kmsg.ListOffsetsRequest {
	r := kmsg.NewPtrListOffsetsRequest()
	r.ReplicaID = -1
	for _, tp := range w.tps {
		part := kmsg.ListOffsetsRequestTopicPartition{Partition: tp.Partition, CurrentLeaderEpoch: -1, Timestamp: -2}
		added := false
		for i := range r.Topics {
			if r.Topics[i].Topic == tp.Topic {
				r.Topics[i].Partitions = append(r.Topics[i].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.ListOffsetsRequestTopic{Topic: tp.Topic, Partitions: []kmsg.ListOffsetsRequestTopicPartition{part}})
		}
	}
	return r
}

// onList starts each partition at its earliest offset.
func (w *Workload) onList(c *client, r *kmsg.ListOffsetsResponse) []core.Action {
	n := 0
	pos := make([]int64, len(w.tps))
	for i, tp := range w.tps {
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				if t.Topic == tp.Topic && p.Partition == tp.Partition && p.ErrorCode == 0 && p.Offset >= 0 {
					pos[i] = p.Offset
					n++
				}
			}
		}
	}
	if n != len(w.tps) {
		return w.retryStage(c)
	}
	c.c.pos, c.c.listed, c.stage, c.Fails = pos, true, stateRunning, 0
	return w.advance(c)
}

func (w *Workload) fetchNext(c *client) []core.Action {
	cs := &c.c
	if cs.done {
		return nil
	}
	r := kmsg.NewPtrFetchRequest()
	r.MaxWaitMillis, r.MinBytes, r.MaxBytes, r.SessionEpoch = 100, 1, 1<<20, -1
	op := ReadOp{Client: c.ID}
	perPart := int32(1024) << w.env.Intn(6)
	for i, tp := range w.tps {
		op.Parts = append(op.Parts, ReadFrom{TopicPartition: tp, Off: cs.pos[i]})
		part := kmsg.FetchRequestTopicPartition{Partition: tp.Partition, CurrentLeaderEpoch: -1, FetchOffset: cs.pos[i],
			LastFetchedEpoch: -1, LogStartOffset: -1, PartitionMaxBytes: perPart}
		added := false
		for j := range r.Topics {
			if r.Topics[j].Topic == tp.Topic {
				r.Topics[j].Partitions = append(r.Topics[j].Partitions, part)
				added = true
			}
		}
		if !added {
			r.Topics = append(r.Topics, kmsg.FetchRequestTopic{Topic: tp.Topic, Partitions: []kmsg.FetchRequestTopicPartition{part}})
		}
	}
	cs.op = w.env.NewID()
	if w.hist != nil {
		w.hist.Invoke(w.self, cs.op, cs.op, op)
	}
	return w.send(c, requestFetch, r)
}

// onFetch parses the answer of each partition (CRC-verified, whole batches,
// records below the fetch offset dropped) and advances the positions.
func (w *Workload) onFetch(c *client, r *kmsg.FetchResponse) []core.Action {
	cs := &c.c
	res := ReadRes{Parts: make([]ReadPart, len(w.tps))}
	got, atEnd := false, true
	for i, tp := range w.tps {
		part := &res.Parts[i]
		part.Code = codeUnknown
		for _, t := range r.Topics {
			for _, p := range t.Partitions {
				if t.Topic == tp.Topic && p.Partition == tp.Partition {
					*part = w.readPart(p, cs.pos[i])
				}
			}
		}
		if n := len(part.Recs); n > 0 {
			cs.pos[i] = part.Recs[n-1].Off + 1
			got = true
		}
		atEnd = atEnd && part.Code == 0 && !part.Corrupt && cs.pos[i] >= part.HWM
	}
	op := cs.op
	cs.op = core.ID{}
	if w.hist != nil {
		w.hist.Return(op, res, simulator.OpResultKnown)
	}
	d := 100 + w.env.Intn(200)
	if got {
		d = 30
	}
	c.Fails = 0
	if cs.drain && atEnd {
		cs.done = true
		return nil
	}
	return []core.Action{c.Wake(core.Duration(d) * core.Millisecond)}
}

func (w *Workload) readPart(p kmsg.FetchResponseTopicPartition, pos int64) ReadPart {
	out := ReadPart{Code: p.ErrorCode, HWM: p.HighWatermark}
	if p.ErrorCode != 0 {
		return out
	}
	bs, err := kafkawire.SplitBatches(p.RecordBatches)
	if err != nil {
		out.Corrupt = true
	}
	for _, raw := range bs {
		bt, err := kafkawire.ParseBatch(raw)
		var recs []kmsg.Record
		if err == nil {
			recs, err = batchRecords(bt)
		}
		if err != nil {
			out.Corrupt = true
			break
		}
		for _, rec := range recs {
			if off := bt.BaseOffset + int64(rec.OffsetDelta); off >= pos {
				out.Recs = append(out.Recs, Rec{Off: off, Val: string(rec.Value)})
			}
		}
	}
	return out
}

// abandonFetch closes the outstanding read: it had no effect.
func (w *Workload) abandonFetch(c *client, why string) {
	if c.c.op != (core.ID{}) && w.hist != nil {
		w.hist.Return(c.c.op, ReadRes{Why: why}, simulator.OpGivenUp)
	}
	c.c.op = core.ID{}
}
