package suites

import (
	"bytes"
	"fmt"

	"manifester/internal/core"
	"manifester/internal/verify/speccheck"
	"manifester/internal/verify/speccheck/checker"
)

// PartitionOp is one operation of the partition suite. Kind is "append",
// "read" or "hwm" (high watermark). Batch is used by append. From and Max are
// used by read.
type PartitionOp struct {
	Kind  string
	Batch [][]byte
	From  int64
	Max   int
}

// PartitionPair is a fresh spec and a fresh subject of the partition suite.
type PartitionPair struct {
	spec    *speccheck.Partition
	subject PartitionSubject
}

func genPartitionOp(r *checker.Rand, _ int) PartitionOp {
	switch r.Intn(5) {
	case 0, 1:
		var batch [][]byte
		for n := r.Intn(4); n > 0; n-- { // a batch can be empty
			value := make([]byte, r.Intn(4))
			for i := range value {
				value[i] = byte('a' + r.Intn(26))
			}
			batch = append(batch, value)
		}
		return PartitionOp{Kind: "append", Batch: batch}
	case 2, 3:
		// From is chosen around the edges: negative, inside, at the end, past it.
		return PartitionOp{Kind: "read", From: int64(r.Intn(14)) - 2, Max: r.Intn(6) - 1}
	}
	return PartitionOp{Kind: "hwm"}
}

func minimizePartitionOp(op PartitionOp) []PartitionOp {
	var out []PartitionOp
	switch op.Kind {
	case "append":
		if len(op.Batch) > 0 {
			out = append(out, PartitionOp{Kind: "append"})
		}
		if len(op.Batch) > 1 {
			out = append(out, PartitionOp{Kind: "append", Batch: op.Batch[:1]})
		}
	case "read":
		if op.Max != 0 {
			out = append(out, PartitionOp{Kind: "read", From: op.From})
		}
		if op.From > 0 {
			out = append(out, PartitionOp{Kind: "read", From: 0, Max: op.Max})
		}
	}
	return out
}

func cloneBatch(batch [][]byte) [][]byte {
	if batch == nil {
		return nil
	}
	out := make([][]byte, len(batch))
	for i, v := range batch {
		out[i] = bytes.Clone(v)
	}
	return out
}

// PartitionComparator returns the comparator of the partition suite. Checks:
// the base offset of Append, whether Read is in range, every record offset and
// value, the high watermark, and the state hash when the subject has one. The
// suite writes over the batch after Append and over the records after Read, so
// a subject that shares memory with the caller fails.
func PartitionComparator(newSubject func() PartitionSubject) checker.SpecChecker[*PartitionPair, PartitionOp] {
	return checker.SpecChecker[*PartitionPair, PartitionOp]{
		Name:          "suites.PartitionOp",
		New:           func() *PartitionPair { return &PartitionPair{spec: &speccheck.Partition{}, subject: newSubject()} },
		Gen:           genPartitionOp,
		Step:          partitionStep,
		Invariant:     partitionInvariant,
		Shrink:        minimizePartitionOp,
		Format:        func(op PartitionOp) string { return fmt.Sprintf("%#v", op) },
		Ops:           suiteOps,
		MaxShrinkRuns: suiteMinimizeRuns,
	}
}

// Partition runs the partition suite on the seeds that opts chooses.
func Partition(t checker.Reporter, newSubject func() PartitionSubject, opts checker.Options) {
	t.Helper()
	checker.Run(t, PartitionComparator(newSubject), opts)
}

func overwriteWithJunk(value []byte) {
	for i := range value {
		value[i] = '!'
	}
}

func partitionStep(p *PartitionPair, op PartitionOp) error {
	switch op.Kind {
	case "append":
		specBatch, subjectBatch := cloneBatch(op.Batch), cloneBatch(op.Batch)
		specBase := p.spec.Append(specBatch)
		subjectBase := p.subject.Append(subjectBatch)
		for _, v := range subjectBatch {
			overwriteWithJunk(v)
		}
		if specBase != subjectBase {
			return fmt.Errorf("append of %d records returned the base offset %d, but the spec returned %d",
				len(op.Batch), subjectBase, specBase)
		}
	case "hwm":
		if s, a := p.spec.HighWatermark(), p.subject.HighWatermark(); s != a {
			return fmt.Errorf("the high watermark is %d, but the spec says %d", a, s)
		}
	default:
		return partitionRead(p, op)
	}
	return nil
}

func partitionRead(p *PartitionPair, op PartitionOp) error {
	from := core.Offset(op.From)
	specRecords, specOK := p.spec.Read(from, op.Max)
	records, ok := p.subject.Read(from, op.Max)
	defer func() {
		for _, r := range records {
			overwriteWithJunk(r.Value)
		}
	}()
	where := fmt.Sprintf("Read(from %d, max %d)", op.From, op.Max)
	if ok != specOK {
		return fmt.Errorf("%s returned ok = %v, but the spec returned ok = %v with high watermark %d",
			where, ok, specOK, p.spec.HighWatermark())
	}
	if len(records) != len(specRecords) {
		return fmt.Errorf("%s returned %d records, but the spec returned %d", where, len(records), len(specRecords))
	}
	for i, r := range records {
		s := specRecords[i]
		if r.Offset != s.Offset || !bytes.Equal(r.Value, s.Value) {
			return fmt.Errorf("%s: record %d has offset %d and value %q, but the spec has offset %d and value %q",
				where, i, r.Offset, r.Value, s.Offset, s.Value)
		}
	}
	return nil
}

func partitionInvariant(p *PartitionPair) error {
	if s, a := p.spec.HighWatermark(), p.subject.HighWatermark(); s != a {
		return fmt.Errorf("the high watermark is %d, but the spec says %d", a, s)
	}
	if hash, ok := p.subject.StateHash(); ok {
		if specHash := p.spec.StateHash(); hash != specHash {
			return fmt.Errorf("the subject state hash is %#x, but the spec state hash is %#x", hash, specHash)
		}
	}
	return nil
}
