package kafka

import (
	"slices"
	"strconv"

	"manifester/internal/core"
	"manifester/internal/verify/sim"
)

// oracle is the invariant that owns the reconstructed logs and every check
// that reads them (AckedIsCommitted, at-least-once duplicates, NoPhantoms,
// AckedOrder, ReadsMatchLog, AckedDurable, consumers caught up). Delivery is
// at-least-once (decision 41): a retried batch may be committed more than once
// (whole, in order, at most once per send and once per stored object), but an
// acked record is at its acked offset and offsets never gap or overlap. One instance per seed so
// the logs are rebuilt once, incrementally: committed ranges only ever append,
// and Gen() skips steps where the control plane did not change. It reads
// ground truth only (View.CP, View.Objects, facts, history) and never imports
// the spec.
type oracle struct {
	opts Opts
	tps  []core.TopicPartition
	logs []*plog
	idx  map[core.TopicPartition]int

	started       bool
	cpGen, objGen uint64
	ackCur        sim.Cursor
	nops          int
	opIdx         map[[2]int]int            // (client, batch) -> history index
	pend          []int                     // history indexes not yet resolved
	seen          map[string]int            // record value -> number of times committed
	locs          map[core.SegmentRef]int64 // stored batch -> offset it was committed at
	cpos          map[[2]int]int64          // (consumer client, partition index) -> next offset it should see
}

func newOracle(o Opts) *oracle {
	or := &oracle{opts: o, tps: o.tps(), idx: map[core.TopicPartition]int{}, opIdx: map[[2]int]int{},
		seen: map[string]int{}, locs: map[core.SegmentRef]int64{}, cpos: map[[2]int]int64{}}
	for i, tp := range or.tps {
		or.idx[tp] = i
		or.logs = append(or.logs, &plog{tp: tp, idx: i, byBase: map[int64]int{}})
	}
	return or
}

func (*oracle) Name() string { return "KafkaOracle" }

func (o *oracle) Observe(v sim.View) {
	o.scanOps(v)
	o.refresh(v)
	o.acks(v)
	o.reads(v)
}

// scanOps indexes newly invoked ops.
func (o *oracle) scanOps(v sim.View) {
	ops := v.History().Ops()
	for ; o.nops < len(ops); o.nops++ {
		if op, ok := ops[o.nops].Op.(ProduceOp); ok {
			o.opIdx[[2]int{op.Client, op.Batch}] = o.nops
		}
		o.pend = append(o.pend, o.nops)
	}
}

// refresh brings every partition's log up to the current truth.
func (o *oracle) refresh(v sim.View) {
	cg, og := v.CP().Gen(), v.Objects().Gen()
	if o.started && cg == o.cpGen && og == o.objGen {
		return
	}
	o.started, o.cpGen, o.objGen = true, cg, og
	for _, pl := range o.logs {
		if int64(v.CP().HighWatermark(pl.tp)) != int64(len(pl.vals)) || pl.ranges == 0 {
			o.ingest(v, pl)
		}
	}
}

func tpName(tp core.TopicPartition) string { return tp.Topic + "/" + strconv.Itoa(int(tp.Partition)) }

// record checks one committed record: it was produced (to this partition,
// at this position of its batch, never from a definitely rejected batch). A
// value may appear again only as a retry of its batch; AtEnd bounds the count
// by the sends.
func (o *oracle) record(v sim.View, pl *plog, off int64, i int, val string, det []core.ContextPair) {
	as := v.Assert()
	if val == "?" {
		return
	}
	d := append(slices.Clip(det), core.ContextPair{Key: "offset", Value: off}, core.ContextPair{Key: "value", Value: val})
	o.seen[val]++
	c, b, ri, ok := parseVal(val)
	as.Always(ok && ri == i, "kafka: records of a batch keep their order", d...)
	hi, known := o.opIdx[[2]int{c, b}]
	as.Always(ok && known, "kafka: every committed record was produced", d...)
	if !ok || !known {
		return
	}
	op := v.History().Ops()[hi]
	po := op.Op.(ProduceOp)
	as.Always(po.TP == pl.tp && i < len(po.Vals) && po.Vals[i] == val, "kafka: a record is committed where and as it was produced", d...)
	as.Always(op.Outcome != sim.OpAbandoned, "kafka: a definitely rejected batch is never committed", d...)
}

// acks checks each produce_acked fact against the truth at the Step that
// emitted it.
func (o *oracle) acks(v sim.View) {
	for _, tf := range v.Timeline().Since("produce_acked", &o.ackCur) {
		f := tf.Fact
		topic, _ := f.Field("topic").(core.Str)
		part, ok1 := f.Field("partition").(core.I64)
		base, ok2 := f.Field("base_offset").(core.I64)
		count, ok3 := f.Field("count").(core.I64)
		v.Assert().Always(ok1 && ok2 && ok3, "kafka: ack fact is well formed")
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		det := []core.ContextPair{{Key: "tp", Value: string(topic) + "/" + strconv.Itoa(int(part))}, {Key: "base", Value: int64(base)},
			{Key: "count", Value: int64(count)}, {Key: "node", Value: string(tf.Source.Node)}}
		ti, known := o.idx[core.TopicPartitionKey(string(topic), int32(part))]
		bi, found := -1, false
		if known {
			bi, found = o.logs[ti].byBase[int64(base)]
		}
		found = found && o.logs[ti].bats[bi].count == int(count)
		v.Assert().Always(found, "kafka: acked range is committed in the control plane", det...)
	}
}

// reads checks every consumer fetch that has just returned.
func (o *oracle) reads(v sim.View) {
	ops := v.History().Ops()
	keep := o.pend[:0]
	for _, i := range o.pend {
		op := ops[i]
		if op.Outcome == sim.OpPending {
			keep = append(keep, i)
			continue
		}
		if ro, ok := op.Op.(ReadOp); ok && op.Outcome == sim.OpDefinite {
			o.checkRead(v, ro, op.Result.(ReadRes))
		}
	}
	o.pend = keep
}

func (o *oracle) checkRead(v sim.View, ro ReadOp, rr ReadRes) {
	as := v.Assert()
	for i, part := range rr.Parts {
		from := ro.Parts[i]
		ti := o.idx[from.TP]
		pl := o.logs[ti]
		det := []core.ContextPair{{Key: "client", Value: ro.Client}, {Key: "tp", Value: tpName(from.TP)}, {Key: "from", Value: from.Off}}
		as.Always(!part.Corrupt, "kafka: a consumer never receives a corrupt batch", det...)
		as.Always(part.Code != 1, "kafka: a consumer's fetch is never out of range", det...)
		if part.Code != 0 {
			continue
		}
		as.Always(part.HWM <= int64(len(pl.vals)), "kafka: a fetch never reports a high watermark above the committed log",
			append(slices.Clip(det), core.ContextPair{Key: "hwm", Value: part.HWM})...)
		key := [2]int{ro.Client, ti}
		as.Always(from.Off == o.cpos[key], "kafka: a consumer resumes exactly where its last read ended", det...)
		for j, r := range part.Recs {
			d := append(slices.Clip(det), core.ContextPair{Key: "offset", Value: r.Off}, core.ContextPair{Key: "value", Value: r.Val})
			ok := r.Off == from.Off+int64(j)
			as.Always(ok, "kafka: a consumer sees every offset in order, none skipped", d...)
			inLog := r.Off >= 0 && r.Off < int64(len(pl.vals)) && r.Off < part.HWM
			as.Always(inLog, "kafka: a consumer never reads past the high watermark", d...)
			match := inLog && pl.vals[r.Off] == r.Val
			as.Always(match, "kafka: a consumer reads the committed value at each offset", d...)
			if !ok || !inLog || !match {
				break
			}
			o.cpos[key] = r.Off + 1
		}
	}
}

func (o *oracle) AtEnd(v sim.View) {
	o.scanOps(v)
	o.refresh(v)
	as := v.Assert()
	ops := v.History().Ops()
	lastAcked := map[[2]int]int64{} // (client, partition index) -> base of its newest acked batch
	for _, op := range ops {
		po, ok := op.Op.(ProduceOp)
		if !ok {
			continue
		}
		res, _ := op.Result.(ProduceRes)
		ti := o.idx[po.TP]
		if op.Outcome != sim.OpPending && len(po.Vals) > 0 {
			// Every send commits at most once (a MaybeApplied commit is retried
			// under its key), so a batch is in the log at most once per send.
			as.Always(o.seen[po.Vals[0]] <= max(res.Attempts, 1), "kafka: a batch is committed at most once per send",
				core.ContextPair{Key: "tp", Value: tpName(po.TP)}, core.ContextPair{Key: "value", Value: po.Vals[0]},
				core.ContextPair{Key: "times", Value: o.seen[po.Vals[0]]}, core.ContextPair{Key: "sends", Value: res.Attempts})
		}
		switch op.Outcome {
		case sim.OpDefinite:
			for i, val := range po.Vals {
				off := res.Base + int64(i)
				same := res.Base >= 0 && off < int64(len(o.logs[ti].vals)) && o.logs[ti].vals[off] == val
				as.Always(same, "kafka: an acked record is in the log at its acked offset",
					core.ContextPair{Key: "tp", Value: tpName(po.TP)}, core.ContextPair{Key: "offset", Value: off}, core.ContextPair{Key: "value", Value: val})
			}
			// One batch at a time per (client, partition): the next batch is built
			// after this one was acked, so its acked offset is higher.
			k := [2]int{po.Client, ti}
			if prev, had := lastAcked[k]; had {
				as.Always(res.Base >= prev+1, "kafka: a client's acked batches for a partition are at increasing offsets",
					core.ContextPair{Key: "tp", Value: tpName(po.TP)}, core.ContextPair{Key: "base", Value: res.Base}, core.ContextPair{Key: "previous", Value: prev})
			}
			lastAcked[k] = res.Base
		case sim.OpAbandoned:
			for _, val := range po.Vals {
				as.Always(o.seen[val] == 0, "kafka: a definitely rejected batch is never committed", core.ContextPair{Key: "value", Value: val})
			}
		}
	}
	for c := o.opts.Producers; c < o.opts.Producers+o.opts.Consumers; c++ {
		for ti, pl := range o.logs {
			as.Always(o.cpos[[2]int{c, ti}] == int64(len(pl.vals)), "kafka: every consumer caught up with the final log",
				core.ContextPair{Key: "client", Value: c}, core.ContextPair{Key: "tp", Value: tpName(pl.tp)},
				core.ContextPair{Key: "pos", Value: o.cpos[[2]int{c, ti}]}, core.ContextPair{Key: "hwm", Value: int64(len(pl.vals))})
		}
	}
	total := 0
	for _, pl := range o.logs {
		total += len(pl.vals)
	}
	as.Sometimes(total > 0, "kafka: the campaign committed records")
}
