package generator

import (
	"fmt"
	"manifester/internal/core"
	"manifester/internal/verify/veritas/veritastest"
	"strings"
)

// CreateTopic is the direct topic-creation call. It is not a ControlPlaneCall because
// the control plane takes it outside the broker protocol.
type CreateTopic struct {
	Name       string
	Partitions int32
}

// ControlPlaneOp is one control-plane operation. Exactly one field is not nil.
type ControlPlaneOp struct {
	CreateTopic *CreateTopic
	Call        *core.ControlPlaneCall
}

// NewCreateTopic returns an operation that creates a topic.
func NewCreateTopic(name string, partitions int32) ControlPlaneOp {
	return ControlPlaneOp{CreateTopic: &CreateTopic{Name: name, Partitions: partitions}}
}

// NewCall returns an operation that sends call.
func NewCall(call core.ControlPlaneCall) ControlPlaneOp { return ControlPlaneOp{Call: &call} }

// Describe returns a short plain-English text for an operation, for failure
// messages.
func (o ControlPlaneOp) Describe() string {
	switch {
	case o.CreateTopic != nil:
		return fmt.Sprintf("create topic %q with %d partitions", o.CreateTopic.Name, o.CreateTopic.Partitions)
	case o.Call == nil:
		return "empty operation"
	}
	switch op := o.Call.Op.(type) {
	case core.CommitSegment:
		return fmt.Sprintf("request %d: commit segment %q with %d batches", o.Call.Req, op.SegmentKey, len(op.Batches))
	case core.FindSegments:
		return fmt.Sprintf("request %d: look up %v from offset %d with max bytes %d", o.Call.Req, op.TP, op.From, op.MaxBytes)
	case core.DescribeTopics:
		return fmt.Sprintf("request %d: describe topics %q", o.Call.Req, op.Topics)
	}
	return fmt.Sprintf("request %d: %T", o.Call.Req, o.Call.Op)
}

// ControlPlaneOps returns a generator of control-plane operations for profile
// p. Operation i carries RequestID i+1. Commits cover a fresh key, a replay, a
// conflict, the same batches under a fresh key, and five kinds of malformed
// request. Lookups cover From = -1, From inside long runs and MaxBytes cuts.
// DescribeTopics covers known, unknown and all topics.
func ControlPlaneOps(p Profile) func(r *veritastest.Rand, i int) ControlPlaneOp {
	return func(r *veritastest.Rand, i int) ControlPlaneOp {
		req := core.RequestID(i + 1)
		switch pickWeighted(r, p.CreateTopicWeight, p.CommitWeight, p.LookupWeight, p.DescribeWeight) {
		case 0:
			return genCreateTopic(p, r)
		case 1:
			return NewCall(core.ControlPlaneCall{Req: req, Op: genCommit(p, r)})
		case 2:
			return NewCall(core.ControlPlaneCall{Req: req, Op: genLookup(p, r)})
		}
		return NewCall(core.ControlPlaneCall{Req: req, Op: genDescribe(p, r)})
	}
}

func genCreateTopic(p Profile, r *veritastest.Rand) ControlPlaneOp {
	if !percent(r, p.CreateInvalidPercent) {
		return NewCreateTopic(pick(r, p.Topics), int32(1+r.Intn(p.MaxPartitions)))
	}
	if r.Intn(2) == 0 {
		return NewCreateTopic("", int32(1+r.Intn(p.MaxPartitions)))
	}
	return NewCreateTopic(pick(r, p.Topics), int32(-r.Intn(2)))
}

func poolKey(p Profile, r *veritastest.Rand) string {
	return fmt.Sprintf("seg%d", r.Intn(p.SegmentKeys))
}

// genCommit picks one kind of commit. A pool key with variant 0 is a first
// commit the first time and a replay every later time, because Batches gives
// the same batches for the same key.
func genCommit(p Profile, r *veritastest.Rand) core.CommitSegment {
	roll := r.Intn(100)
	switch {
	case roll < p.InvalidPercent:
		return genInvalidCommit(p, r)
	case roll < p.InvalidPercent+p.ConflictPercent:
		key := poolKey(p, r)
		return core.CommitSegment{SegmentKey: key, Batches: Batches(p, key, key, 1+r.Intn(3))}
	case roll < p.InvalidPercent+p.ConflictPercent+p.RetryNewKeyPercent:
		shape := poolKey(p, r)
		key := fmt.Sprintf("retry%d", r.Intn(1000))
		return core.CommitSegment{SegmentKey: key, Batches: Batches(p, shape, key, 0)}
	}
	key := poolKey(p, r)
	return core.CommitSegment{SegmentKey: key, Batches: Batches(p, key, key, 0)}
}

// genInvalidCommit returns a commit that the control plane must reject. It
// uses a key from a separate "bad" pool, so the key is never committed and
// the answer is always ErrInvalid and never ErrConflict.
func genInvalidCommit(p Profile, r *veritastest.Rand) core.CommitSegment {
	key := fmt.Sprintf("bad%d", r.Intn(4))
	batches := Batches(p, key, key, 0)
	at := r.Intn(len(batches))
	switch r.Intn(5) {
	case 0:
		key = ""
	case 1:
		batches = nil
	case 2:
		batches[at].Records = -int64(r.Intn(2))
	case 3:
		batches[at].TP.Partition = -1
	default:
		batches[at].TP.Topic = ""
	}
	return core.CommitSegment{SegmentKey: key, Batches: batches}
}

func genLookup(p Profile, r *veritastest.Rand) core.FindSegments {
	tp := core.TopicPartitionKey(pick(r, p.Topics), int32(r.Intn(p.PartitionCount)))
	from := int64(r.Intn(p.LookupShortSpan))
	if percent(r, p.LookupLongPercent) {
		from = int64(r.Intn(p.LookupLongSpan))
	}
	if percent(r, p.LookupNegativeFromPercent) {
		from = -1
	}
	return core.FindSegments{TP: tp, From: core.Offset(from), MaxBytes: pick(r, p.MaxBytesChoices)}
}

func genDescribe(p Profile, r *veritastest.Rand) core.DescribeTopics {
	var d core.DescribeTopics
	for j := r.Intn(p.MaxDescribeNames + 1); j > 0; j-- {
		name := pick(r, p.Topics)
		if percent(r, p.UnknownTopicPercent) {
			name = "nope"
		}
		d.Topics = append(d.Topics, name)
	}
	return d
}

// Batches returns the batches of a commit. Its result depends only on p, shape,
// segmentKey and variant, so a caller that asks twice gets the same batches
// and the control plane sees a replay.
//
// The shape picks the number, topics, sizes and records of the batches. Two
// calls with the same shape and variant but different segmentKey give the same
// batches except for Loc.Key. This is the "same batches under a fresh key"
// case. A variant above 0 always gives batches that differ from variant 0:
// the first batch has more records, and an even variant also adds one batch.
// This is the "same key, different request" case, which is a conflict.
func Batches(p Profile, shape, segmentKey string, variant int) []core.PendingBatch {
	r := veritastest.NewRand(shapeSeed(shape))
	n := 1 + r.Intn(p.MaxBatches)
	if percent(r, p.ManyBatchesPercent) {
		n = p.ManyBatchesMin + r.Intn(p.ManyBatchesExtra+1)
	}
	out := make([]core.PendingBatch, 0, n+1)
	for range n {
		out = append(out, genBatch(p, r, segmentKey))
	}
	if variant > 0 {
		out[0].Records += int64(variant)
		if variant%2 == 0 {
			out = append(out, out[0])
		}
	}
	return out
}

// shapeSeed turns a shape name into a random seed with the FNV-1a hash. The
// same name always gives the same seed, and the hash is stable on every run.
func shapeSeed(shape string) uint64 {
	h := uint64(14695981039346656037)
	for i := 0; i < len(shape); i++ {
		h = (h ^ uint64(shape[i])) * 1099511628211
	}
	return h
}

func genBatch(p Profile, r *veritastest.Rand, segmentKey string) core.PendingBatch {
	topics := p.Topics[:min(p.BatchTopics, len(p.Topics))]
	return core.PendingBatch{
		TP:      core.TopicPartitionKey(pick(r, topics), int32(r.Intn(p.BatchPartitions))),
		Records: 1 + int64(r.Intn(3)),
		Loc:     core.SegmentRef{Key: segmentKey, Off: int64(r.Intn(4)) * 100, Len: int64(1 + r.Intn(3)*50)},
	}
}

// ShrinkControlPlaneOp returns simpler versions of op, simplest first: fewer
// batches, smaller counts, smaller offsets and shorter keys. It never returns
// op itself, so a caller that keeps shrinking always ends.
func ShrinkControlPlaneOp(op ControlPlaneOp) []ControlPlaneOp {
	switch {
	case op.CreateTopic != nil:
		return shrinkCreateTopic(*op.CreateTopic)
	case op.Call == nil:
		return nil
	}
	call := *op.Call
	var ops []core.ControlPlaneOperation
	switch o := call.Op.(type) {
	case core.CommitSegment:
		ops = shrinkCommit(o)
	case core.FindSegments:
		ops = shrinkLookup(o)
	case core.DescribeTopics:
		ops = shrinkDescribe(o)
	}
	out := make([]ControlPlaneOp, 0, len(ops))
	for _, o := range ops {
		out = append(out, NewCall(core.ControlPlaneCall{Req: call.Req, Op: o}))
	}
	return out
}

func shrinkCreateTopic(c CreateTopic) []ControlPlaneOp {
	var out []ControlPlaneOp
	if c.Partitions > 1 {
		out = append(out, NewCreateTopic(c.Name, 1))
	}
	for _, n := range shorterStrings(c.Name) {
		out = append(out, NewCreateTopic(n, c.Partitions))
	}
	return out
}

func shrinkCommit(c core.CommitSegment) []core.ControlPlaneOperation {
	var out []core.ControlPlaneOperation
	add := func(key string, batches []core.PendingBatch) {
		out = append(out, core.CommitSegment{SegmentKey: key, Batches: batches})
	}
	if len(c.Batches) > 0 {
		add(c.SegmentKey, nil)
	}
	if len(c.Batches) > 1 {
		add(c.SegmentKey, c.Batches[:1])
		add(c.SegmentKey, c.Batches[:len(c.Batches)/2])
		add(c.SegmentKey, c.Batches[:len(c.Batches)-1])
		add(c.SegmentKey, c.Batches[1:])
	}
	if simpler, changed := simplerBatches(c.Batches); changed {
		add(c.SegmentKey, simpler)
	}
	for _, k := range shorterStrings(c.SegmentKey) {
		add(k, c.Batches)
	}
	return out
}

// simplerBatches sets every record count above 1 to 1 and every location
// offset and length to 0. It reports false when nothing changed. It does all
// batches at once, so a commit of 40 batches costs one candidate, not 40.
func simplerBatches(batches []core.PendingBatch) ([]core.PendingBatch, bool) {
	out := make([]core.PendingBatch, len(batches))
	changed := false
	for i, b := range batches {
		out[i] = b
		if b.Records > 1 {
			out[i].Records = 1
		}
		out[i].Loc.Off, out[i].Loc.Len = 0, 0
		if out[i] != b {
			changed = true
		}
	}
	return out, changed
}

func shrinkLookup(l core.FindSegments) []core.ControlPlaneOperation {
	var out []core.ControlPlaneOperation
	if l.MaxBytes != 0 {
		out = append(out, core.FindSegments{TP: l.TP, From: l.From, MaxBytes: 0})
	}
	if l.From > 0 {
		out = append(out, core.FindSegments{TP: l.TP, From: 0, MaxBytes: l.MaxBytes})
	}
	if l.From > 1 {
		out = append(out, core.FindSegments{TP: l.TP, From: l.From / 2, MaxBytes: l.MaxBytes})
	}
	if l.TP.Partition > 0 {
		out = append(out, core.FindSegments{TP: core.TopicPartitionKey(l.TP.Topic, 0), From: l.From, MaxBytes: l.MaxBytes})
	}
	for _, t := range shorterStrings(l.TP.Topic) {
		out = append(out, core.FindSegments{TP: core.TopicPartitionKey(t, l.TP.Partition), From: l.From, MaxBytes: l.MaxBytes})
	}
	return out
}

func shrinkDescribe(d core.DescribeTopics) []core.ControlPlaneOperation {
	var out []core.ControlPlaneOperation
	if len(d.Topics) > 0 {
		out = append(out, core.DescribeTopics{})
	}
	if len(d.Topics) > 1 {
		for i := range d.Topics {
			rest := append(append([]string(nil), d.Topics[:i]...), d.Topics[i+1:]...)
			out = append(out, core.DescribeTopics{Topics: rest})
		}
	}
	return out
}

// FormatControlPlaneOp returns op as a Go expression that you can paste into a
// regression test. The expression needs the packages core and generator.
func FormatControlPlaneOp(op ControlPlaneOp) string {
	switch {
	case op.CreateTopic != nil:
		return fmt.Sprintf("generator.NewCreateTopic(%q, %d)", op.CreateTopic.Name, op.CreateTopic.Partitions)
	case op.Call == nil:
		return "generator.ControlPlaneOp{}"
	}
	return fmt.Sprintf("generator.NewCall(core.ControlPlaneCall{Req: %d, Op: %s})", op.Call.Req, formatCPOp(op.Call.Op))
}

func formatCPOp(op core.ControlPlaneOperation) string {
	switch op := op.(type) {
	case core.CommitSegment:
		return fmt.Sprintf("core.CommitSegment{SegmentKey: %q%s}", op.SegmentKey, formatBatchesField(op.Batches))
	case core.FindSegments:
		return fmt.Sprintf("core.FindSegments{TP: core.TopicPartitionKey(%q, %d), From: %d, MaxBytes: %d}", op.TP.Topic, op.TP.Partition, op.From, op.MaxBytes)
	case core.DescribeTopics:
		if op.Topics == nil {
			return "core.DescribeTopics{}"
		}
		return fmt.Sprintf("core.DescribeTopics{Topics: []string{%s}}", quoteAll(op.Topics))
	}
	return fmt.Sprintf("%#v", op)
}

func formatBatchesField(batches []core.PendingBatch) string {
	if batches == nil {
		return ""
	}
	parts := make([]string, len(batches))
	for i, b := range batches {
		parts[i] = fmt.Sprintf("{TP: core.TopicPartitionKey(%q, %d), Records: %d, Loc: %s}", b.TP.Topic, b.TP.Partition, b.Records, formatRef(b.Loc))
	}
	return ", Batches: []core.PendingBatch{" + strings.Join(parts, ", ") + "}"
}

func quoteAll(items []string) string {
	parts := make([]string, len(items))
	for i, s := range items {
		parts[i] = fmt.Sprintf("%q", s)
	}
	return strings.Join(parts, ", ")
}
