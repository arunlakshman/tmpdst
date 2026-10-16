package kafka

import (
	"strconv"

	"manifester/core"
	"manifester/sim"
)

// oracle is the invariant that owns the reconstructed logs and every check
// that reads them (AckedIsCommitted, NoDuplicates, NoPhantoms, ProducerOrder,
// ReadsMatchLog, AckedDurable, consumers caught up). One instance per seed so
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
	opIdx         map[[2]int]int // (client, batch) -> history index
	pend          []int          // history indexes not yet resolved
	seen          map[string]int // record value -> partition index (NoDuplicates)
	ord           map[ordKey]*ordState
	acked         map[ackKey]int64 // acked batch identity -> base offset
	cpos          map[[2]int]int64 // (consumer client, partition index) -> next offset it should see
}

type ordKey struct {
	tp  int
	pid int64
}

type ordState struct {
	pepoch int16
	next   int32
	serial int
}

type ackKey struct {
	tp     int
	pid    int64
	pepoch int16
	seq    int32
}

func newOracle(o Opts) *oracle {
	or := &oracle{opts: o, tps: o.tps(), idx: map[core.TopicPartition]int{}, opIdx: map[[2]int]int{},
		seen: map[string]int{}, ord: map[ordKey]*ordState{}, acked: map[ackKey]int64{}, cpos: map[[2]int]int64{}}
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
		switch op := ops[o.nops].Op.(type) {
		case ProduceOp:
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
// at this position of its batch, never from a definitely rejected batch) and
// appears once in the whole log.
func (o *oracle) record(v sim.View, pl *plog, off int64, i int, val string, det []core.Detail) {
	as := v.Assert()
	if val == "?" {
		return
	}
	d := append(det[:len(det):len(det)], core.Detail{Key: "offset", Value: off}, core.Detail{Key: "value", Value: val})
	prev, dup := o.seen[val]
	as.Always(!dup, "kafka: no record is committed twice", append(d, core.Detail{Key: "first_tp", Value: tpName(o.tps[prev])})...)
	o.seen[val] = pl.idx
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

// checkOrder: per (partition, producer id) the committed batches carry
// sequence numbers 0, n1, n1+n2, ... in log order, and were built in order.
func (o *oracle) checkOrder(v sim.View, pl *plog, b lbat, det []core.Detail) {
	if b.pid < 0 {
		return
	}
	d := append(det[:len(det):len(det)], core.Detail{Key: "producer", Value: b.pid}, core.Detail{Key: "seq", Value: int64(b.seq)})
	k := ordKey{pl.idx, b.pid}
	st := o.ord[k]
	if st == nil || b.pepoch > st.pepoch {
		v.Assert().Always(b.seq == 0, "kafka: a producer's first committed batch has sequence 0", d...)
		st = &ordState{pepoch: b.pepoch, serial: -1}
		o.ord[k] = st
	} else {
		v.Assert().Always(b.pepoch == st.pepoch, "kafka: a producer's epoch never goes back", d...)
		v.Assert().Always(b.seq == st.next, "kafka: a producer's batches commit in sequence order with no gap", d...)
		v.Assert().Always(b.serial < 0 || b.serial > st.serial, "kafka: a producer's batches commit in the order it sent them", d...)
	}
	st.next = b.seq + int32(b.count)
	if b.serial >= 0 {
		st.serial = b.serial
	}
}

func fieldOf(f core.Fact, key string) core.FieldValue {
	for _, x := range f.Fields {
		if x.Key == key {
			return x.Value
		}
	}
	return nil
}

// acks checks each produce_acked fact against the truth at the Step that
// emitted it.
func (o *oracle) acks(v sim.View) {
	for _, tf := range v.Timeline().Since("produce_acked", &o.ackCur) {
		f := tf.Fact
		topic, _ := fieldOf(f, "topic").(core.Str)
		part, ok1 := fieldOf(f, "partition").(core.I64)
		base, ok2 := fieldOf(f, "base_offset").(core.I64)
		count, ok3 := fieldOf(f, "count").(core.I64)
		pid, ok4 := fieldOf(f, "producer").(core.I64)
		pep, ok5 := fieldOf(f, "pepoch").(core.I64)
		seq, ok6 := fieldOf(f, "base_seq").(core.I64)
		v.Assert().Always(ok1 && ok2 && ok3 && ok4 && ok5 && ok6, "kafka: ack fact is well formed")
		if !(ok1 && ok2 && ok3 && ok4 && ok5 && ok6) {
			continue
		}
		det := []core.Detail{{Key: "tp", Value: string(topic) + "/" + strconv.Itoa(int(part))}, {Key: "base", Value: int64(base)},
			{Key: "count", Value: int64(count)}, {Key: "node", Value: string(tf.Source.Node)}}
		ti, known := o.idx[core.TopicPartition{Topic: string(topic), Partition: int32(part)}]
		bi, found := -1, false
		if known {
			bi, found = o.logs[ti].byBase[int64(base)]
		}
		found = found && o.logs[ti].bats[bi].count == int(count)
		v.Assert().Always(found, "kafka: acked range is committed in the control plane", det...)
		if !found {
			continue
		}
		bat := o.logs[ti].bats[bi]
		v.Assert().Always(bat.pid == int64(pid) && bat.pepoch == int16(pep) && bat.seq == int32(seq),
			"kafka: acked range holds the acked producer batch", det...)
		if pid >= 0 {
			k := ackKey{ti, int64(pid), int16(pep), int32(seq)}
			if prev, again := o.acked[k]; again {
				v.Assert().Always(prev == int64(base), "kafka: an idempotent batch is acked at one offset", det...)
			}
			o.acked[k] = int64(base)
		}
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
		det := []core.Detail{{Key: "client", Value: ro.Client}, {Key: "tp", Value: tpName(from.TP)}, {Key: "from", Value: from.Off}}
		as.Always(!part.Corrupt, "kafka: a consumer never receives a corrupt batch", det...)
		as.Always(part.Code != 1, "kafka: a consumer's fetch is never out of range", det...)
		if part.Code != 0 {
			continue
		}
		as.Always(part.HWM <= int64(len(pl.vals)), "kafka: a fetch never reports a high watermark above the committed log",
			append(det[:len(det):len(det)], core.Detail{Key: "hwm", Value: part.HWM})...)
		key := [2]int{ro.Client, ti}
		as.Always(from.Off == o.cpos[key], "kafka: a consumer resumes exactly where its last read ended", det...)
		for j, r := range part.Recs {
			d := append(det[:len(det):len(det)], core.Detail{Key: "offset", Value: r.Off}, core.Detail{Key: "value", Value: r.Val})
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
	for _, op := range ops {
		po, ok := op.Op.(ProduceOp)
		if !ok {
			continue
		}
		res, _ := op.Result.(ProduceRes)
		ti := o.idx[po.TP]
		switch op.Outcome {
		case sim.OpDefinite:
			for i, val := range po.Vals {
				off := res.Base + int64(i)
				same := res.Base >= 0 && off < int64(len(o.logs[ti].vals)) && o.logs[ti].vals[off] == val
				as.Always(same, "kafka: an acked record is in the log at its acked offset",
					core.Detail{Key: "tp", Value: tpName(po.TP)}, core.Detail{Key: "offset", Value: off}, core.Detail{Key: "value", Value: val})
			}
		case sim.OpAbandoned:
			for _, val := range po.Vals {
				_, in := o.seen[val]
				as.Always(!in, "kafka: a definitely rejected batch is never committed", core.Detail{Key: "value", Value: val})
			}
		}
	}
	for c := o.opts.Producers; c < o.opts.Producers+o.opts.Consumers; c++ {
		for ti, pl := range o.logs {
			as.Always(o.cpos[[2]int{c, ti}] == int64(len(pl.vals)), "kafka: every consumer caught up with the final log",
				core.Detail{Key: "client", Value: c}, core.Detail{Key: "tp", Value: tpName(pl.tp)},
				core.Detail{Key: "pos", Value: o.cpos[[2]int{c, ti}]}, core.Detail{Key: "hwm", Value: int64(len(pl.vals))})
		}
	}
	total := 0
	for _, pl := range o.logs {
		total += len(pl.vals)
	}
	as.Sometimes(total > 0, "kafka: the campaign committed records")
}
