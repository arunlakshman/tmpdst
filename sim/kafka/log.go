package kafka

import (
	"slices"
	"strconv"
	"strings"

	"manifester/core"
	"manifester/kwire"
	"manifester/sim"
)

// The oracle's model of one partition's committed log: the control plane's
// committed ranges, with each batch's bytes read from the object store's
// ground truth. It never consults a spec.

// lbat is one committed batch.
type lbat struct {
	base  int64
	count int
}

type plog struct {
	tp     core.TopicPartition
	idx    int
	vals   []string // value at each offset ("?" where unreadable)
	bats   []lbat
	byBase map[int64]int // base offset -> index in bats
	ranges int           // committed ranges consumed so far
}

// recKey names a record value: client, batch serial, index.
func parseVal(v string) (int, int, int, bool) {
	head, _, found := strings.Cut(v, "|")
	if !found || len(head) < 6 || head[0] != 'c' {
		return 0, 0, 0, false
	}
	parts := strings.Split(head[1:], ".")
	if len(parts) != 3 || parts[1][0] != 'b' || parts[2][0] != 'r' {
		return 0, 0, 0, false
	}
	c, e1 := strconv.Atoi(parts[0])
	b, e2 := strconv.Atoi(parts[1][1:])
	i, e3 := strconv.Atoi(parts[2][1:])
	return c, b, i, e1 == nil && e2 == nil && e3 == nil
}

// ingest reads the committed ranges of pl that it has not seen yet and
// appends them to the log. Every structural problem is a violation: the log
// must be contiguous, every segment readable, every batch intact.
func (o *oracle) ingest(v sim.View, pl *plog) {
	rs := v.CP().Committed(pl.tp)
	for ; pl.ranges < len(rs); pl.ranges++ {
		r := rs[pl.ranges]
		det := []core.Detail{{Key: "tp", Value: pl.tp.Topic + "/" + strconv.Itoa(int(pl.tp.Partition))},
			{Key: "base", Value: int64(r.Base)}, {Key: "segment", Value: r.Loc.Key}}
		v.Assert().Always(int64(r.Base) == int64(len(pl.vals)), "kafka: committed offsets are contiguous", det...)
		vals := o.readRange(v, r, det)
		if vals == nil { // unreadable: keep offsets aligned
			vals = make([]string, r.Count)
			for i := range vals {
				vals[i] = "?"
			}
		}
		start := int64(len(pl.vals))
		prev, again := o.locs[r.Loc]
		v.Assert().Always(!again, "kafka: a stored batch is committed at most once",
			append(slices.Clip(det), core.Detail{Key: "first_offset", Value: prev})...)
		o.locs[r.Loc] = start
		pl.byBase[start] = len(pl.bats)
		pl.bats = append(pl.bats, lbat{base: start, count: len(vals)})
		for i, val := range vals {
			o.record(v, pl, start+int64(i), i, val, det)
		}
		o.checkWhole(v, vals, det)
		pl.vals = append(pl.vals, vals...)
	}
	v.Assert().Always(int64(v.CP().HighWatermark(pl.tp)) == int64(len(pl.vals)),
		"kafka: the high watermark is the end of the committed log", core.Detail{Key: "tp", Value: pl.tp.Topic + "/" + strconv.Itoa(int(pl.tp.Partition))})
}

// checkWhole: a committed range is one produced batch, whole: it starts at
// record 0 and holds all of the batch's records (a retry duplicates a batch,
// never part of one).
func (o *oracle) checkWhole(v sim.View, vals []string, det []core.Detail) {
	if len(vals) == 0 || vals[0] == "?" {
		return
	}
	c, b, ri, ok := parseVal(vals[0])
	hi, known := o.opIdx[[2]int{c, b}]
	if !ok || !known {
		return // record() reports it
	}
	po := v.History().Ops()[hi].Op.(ProduceOp)
	v.Assert().Always(ri == 0 && len(vals) == len(po.Vals), "kafka: a committed range is one whole produced batch", det...)
}

// readRange returns the record values of one committed range, or nil after
// reporting why it is unreadable.
func (o *oracle) readRange(v sim.View, r core.BatchRange, det []core.Detail) []string {
	as := v.Assert()
	obj, ok := v.Objects().Object(r.Loc.Key)
	as.Always(ok, "kafka: committed segment exists in the object store", det...)
	if !ok {
		return nil
	}
	end := r.Loc.Off + r.Loc.Len
	in := r.Loc.Off >= 0 && r.Loc.Len > 0 && end <= int64(len(obj))
	as.Always(in, "kafka: committed range lies inside its segment", det...)
	if !in {
		return nil
	}
	bt, err := kwire.ParseBatch(obj[r.Loc.Off:end])
	as.Always(err == nil, "kafka: committed batch is intact", det...)
	if err != nil {
		return nil
	}
	recs, err := batchRecords(bt)
	as.Always(err == nil && int64(len(recs)) == r.Count, "kafka: committed range holds its record count", det...)
	if err != nil || int64(len(recs)) != r.Count {
		return nil
	}
	vals := make([]string, len(recs))
	for i, rec := range recs {
		vals[i] = string(rec.Value)
	}
	return vals
}
