package kafka

import (
	"slices"
	"strconv"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/history"
	"github.com/manifesterio/mds/broker/internal/verify/simulator/truth"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/assert"
	"github.com/manifesterio/mds/dst/kernel"
	"github.com/manifesterio/mds/dst/machine"
)

// expectedLogChecker is the invariant that owns the reconstructed logs and every check
// that reads them (AckedIsCommitted, at-least-once duplicates, NoPhantoms,
// AckedOrder, ReadsMatchLog, AckedDurable, consumers caught up). Delivery is
// at-least-once (decision 41): a retried batch may be committed more than once
// (whole, in order, at most once per send and once per stored object), but an
// acked record is at its acked offset and offsets never gap or overlap. One instance per seed so
// the logs are rebuilt once, incrementally: committed ranges only ever append,
// and Gen() skips steps where the control plane did not change. It reads
// ground truth only (the control plane and the object store as truth, facts,
// history) and never imports the spec.
type expectedLogChecker struct {
	opts Opts
	hist history.Reader[Op, Res]
	objs truth.ObjectStore
	cp   truth.ControlPlane
	tps  []core.TopicPartition
	logs []*partitionLog
	idx  map[core.TopicPartition]int

	started                                  bool
	controlPlaneGeneration, objectGeneration uint64
	ackCur                                   kernel.Cursor
	nops                                     int
	opIdx                                    map[[2]int]int            // (client, batch) -> history index
	pend                                     []int                     // history indexes not yet resolved
	seen                                     map[string]int            // record value -> number of times committed
	locs                                     map[core.SegmentRef]int64 // stored batch -> offset it was committed at
	cpos                                     map[[2]int]int64          // (consumer client, partition index) -> next offset it should see
}

func newOracle(o Opts, hist history.Reader[Op, Res], objs truth.ObjectStore, cp truth.ControlPlane) *expectedLogChecker {
	or := &expectedLogChecker{opts: o, hist: hist, objs: objs, cp: cp, tps: o.topicPartitions(), idx: map[core.TopicPartition]int{}, opIdx: map[[2]int]int{},
		seen: map[string]int{}, locs: map[core.SegmentRef]int64{}, cpos: map[[2]int]int64{}}
	for i, topicPartition := range or.tps {
		or.idx[topicPartition] = i
		or.logs = append(or.logs, &partitionLog{topicPartition: topicPartition, idx: i, byBase: map[int64]int{}})
	}
	return or
}

func (*expectedLogChecker) Name() string { return "KafkaOracle" }

func (o *expectedLogChecker) Observe(v kernel.View) {
	o.scanOps(v)
	o.refresh(v)
	o.acks(v)
	o.reads(v)
}

// scanOps indexes newly invoked ops.
func (o *expectedLogChecker) scanOps(v kernel.View) {
	ops := o.hist.Ops()
	for ; o.nops < len(ops); o.nops++ {
		if operation, ok := ops[o.nops].Op.(ProduceOp); ok {
			o.opIdx[[2]int{operation.Client, operation.Batch}] = o.nops
		}
		o.pend = append(o.pend, o.nops)
	}
}

// refresh brings every partition's log up to the current truth.
func (o *expectedLogChecker) refresh(v kernel.View) {
	cg, og := o.cp.Gen(), o.objs.Gen()
	if o.started && cg == o.controlPlaneGeneration && og == o.objectGeneration {
		return
	}
	o.started, o.controlPlaneGeneration, o.objectGeneration = true, cg, og
	for _, pl := range o.logs {
		if int64(o.cp.HighWatermark(pl.topicPartition)) != int64(len(pl.vals)) || pl.ranges == 0 {
			o.ingest(v, pl)
		}
	}
}

func topicPartitionName(topicPartition core.TopicPartition) string {
	return topicPartition.Topic + "/" + strconv.Itoa(int(topicPartition.Partition))
}

// record checks one committed record: it was produced (to this partition,
// at this position of its batch, never from a definitely rejected batch). A
// value may appear again only as a retry of its batch; AtEnd bounds the count
// by the sends.
func (o *expectedLogChecker) record(v kernel.View, pl *partitionLog, off int64, i int, val string, det []assert.Pair) {
	as := v.Assert()
	if val == "?" {
		return
	}
	d := append(slices.Clip(det), assert.Pair{Key: "offset", Value: off}, assert.Pair{Key: "value", Value: val})
	o.seen[val]++
	c, b, ri, ok := parseVal(val)
	as.Always(ok && ri == i, batchRecordsKeepOrder, d...)
	hi, known := o.opIdx[[2]int{c, b}]
	as.Always(ok && known, everyCommittedRecordProduced, d...)
	if !ok || !known {
		return
	}
	operation := o.hist.Ops()[hi]
	po := operation.Op.(ProduceOp)
	as.Always(po.TopicPartition == pl.topicPartition && i < len(po.Vals) && po.Vals[i] == val, recordCommittedWhereProduced, d...)
	as.Always(operation.Outcome != history.GivenUp, definitelyRejectedBatchNeverCommitted, d...)
}

// acks checks each produce_acked fact against the truth at the Step that
// emitted it.
func (o *expectedLogChecker) acks(v kernel.View) {
	for _, tf := range v.Timeline().Since("produce_acked", &o.ackCur) {
		f := tf.Fact
		topic, _ := f.Field("topic").(machine.StringValue)
		part, ok1 := f.Field("partition").(machine.Int64Value)
		base, ok2 := f.Field("base_offset").(machine.Int64Value)
		count, ok3 := f.Field("count").(machine.Int64Value)
		v.Assert().Always(ok1 && ok2 && ok3, ackFactWellFormed)
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		det := []assert.Pair{{Key: "tp", Value: string(topic) + "/" + strconv.Itoa(int(part))}, {Key: "base", Value: int64(base)},
			{Key: "count", Value: int64(count)}, {Key: "node", Value: string(tf.Source.Node)}}
		ti, known := o.idx[contract.NewTopicPartition(string(topic), int32(part))]
		bi, found := -1, false
		if known {
			bi, found = o.logs[ti].byBase[int64(base)]
		}
		found = found && o.logs[ti].bats[bi].count == int(count)
		v.Assert().Always(found, ackedRangeInControlPlane, det...)
	}
}

// reads checks every consumer fetch that has just returned.
func (o *expectedLogChecker) reads(v kernel.View) {
	ops := o.hist.Ops()
	keep := o.pend[:0]
	for _, i := range o.pend {
		operation := ops[i]
		if operation.Outcome == history.Pending {
			keep = append(keep, i)
			continue
		}
		if ro, ok := operation.Op.(ReadOp); ok && operation.Outcome == history.ResultKnown {
			o.checkRead(v, ro, operation.Result.(ReadRes))
		}
	}
	o.pend = keep
}

func (o *expectedLogChecker) checkRead(v kernel.View, ro ReadOp, rr ReadRes) {
	as := v.Assert()
	for i, part := range rr.Parts {
		from := ro.Parts[i]
		ti := o.idx[from.TopicPartition]
		pl := o.logs[ti]
		det := []assert.Pair{{Key: "client", Value: ro.Client}, {Key: "tp", Value: topicPartitionName(from.TopicPartition)}, {Key: "from", Value: from.Off}}
		as.Always(!part.Corrupt, consumerNeverReceivesCorruptBatch, det...)
		as.Always(part.Code != 1, consumerFetchNeverOutOfRange, det...)
		if part.Code != 0 {
			continue
		}
		as.Always(part.HWM <= int64(len(pl.vals)), fetchHighWatermarkNotAboveLog,
			append(slices.Clip(det), assert.Pair{Key: "hwm", Value: part.HWM})...)
		key := [2]int{ro.Client, ti}
		as.Always(from.Off == o.cpos[key], consumerResumesWhereLastReadEnded, det...)
		for j, r := range part.Recs {
			d := append(slices.Clip(det), assert.Pair{Key: "offset", Value: r.Off}, assert.Pair{Key: "value", Value: r.Val})
			ok := r.Off == from.Off+int64(j)
			as.Always(ok, consumerSeesEveryOffsetInOrder, d...)
			inLog := r.Off >= 0 && r.Off < int64(len(pl.vals)) && r.Off < part.HWM
			as.Always(inLog, consumerNeverReadsPastHighWatermark, d...)
			match := inLog && pl.vals[r.Off] == r.Val
			as.Always(match, consumerReadsCommittedValue, d...)
			if !ok || !inLog || !match {
				break
			}
			o.cpos[key] = r.Off + 1
		}
	}
}

func (o *expectedLogChecker) AtEnd(v kernel.View) {
	o.scanOps(v)
	o.refresh(v)
	as := v.Assert()
	ops := o.hist.Ops()
	lastAcked := map[[2]int]int64{} // (client, partition index) -> base of its newest acked batch
	for _, operation := range ops {
		po, ok := operation.Op.(ProduceOp)
		if !ok {
			continue
		}
		res, _ := operation.Result.(ProduceRes)
		ti := o.idx[po.TopicPartition]
		if operation.Outcome != history.Pending && len(po.Vals) > 0 {
			// Every send commits at most once (a MaybeApplied commit is retried
			// under its key), so a batch is in the log at most once per send.
			as.Always(o.seen[po.Vals[0]] <= max(res.Attempts, 1), batchCommittedAtMostOncePerSend,
				assert.Pair{Key: "tp", Value: topicPartitionName(po.TopicPartition)}, assert.Pair{Key: "value", Value: po.Vals[0]},
				assert.Pair{Key: "times", Value: o.seen[po.Vals[0]]}, assert.Pair{Key: "sends", Value: res.Attempts})
		}
		switch operation.Outcome {
		case history.ResultKnown:
			for i, val := range po.Vals {
				off := res.Base + int64(i)
				same := res.Base >= 0 && off < int64(len(o.logs[ti].vals)) && o.logs[ti].vals[off] == val
				as.Always(same, ackedRecordAtAckedOffset,
					assert.Pair{Key: "tp", Value: topicPartitionName(po.TopicPartition)}, assert.Pair{Key: "offset", Value: off}, assert.Pair{Key: "value", Value: val})
			}
			// One batch at a time per (client, partition): the next batch is built
			// after this one was acked, so its acked offset is higher.
			k := [2]int{po.Client, ti}
			if prev, had := lastAcked[k]; had {
				as.Always(res.Base >= prev+1, clientAckedOffsetsIncrease,
					assert.Pair{Key: "tp", Value: topicPartitionName(po.TopicPartition)}, assert.Pair{Key: "base", Value: res.Base}, assert.Pair{Key: "previous", Value: prev})
			}
			lastAcked[k] = res.Base
		case history.GivenUp:
			for _, val := range po.Vals {
				as.Always(o.seen[val] == 0, definitelyRejectedBatchNeverCommitted, assert.Pair{Key: "value", Value: val})
			}
		}
	}
	for c := o.opts.Producers; c < o.opts.Producers+o.opts.Consumers; c++ {
		for ti, pl := range o.logs {
			as.Always(o.cpos[[2]int{c, ti}] == int64(len(pl.vals)), everyConsumerCaughtUp,
				assert.Pair{Key: "client", Value: c}, assert.Pair{Key: "tp", Value: topicPartitionName(pl.topicPartition)},
				assert.Pair{Key: "pos", Value: o.cpos[[2]int{c, ti}]}, assert.Pair{Key: "hwm", Value: int64(len(pl.vals))})
		}
	}
	total := 0
	for _, pl := range o.logs {
		total += len(pl.vals)
	}
	as.Sometimes(total > 0, campaignCommittedAnyRecords)
}
