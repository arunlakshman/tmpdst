package suites

import (
	"bytes"
	"fmt"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck"
	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker"
)

// BrokerOp is one operation of the broker suite. Kind is "append",
// "read" or "hwm" (high watermark). Batch is used by append. From and Max are
// used by read.
type BrokerOp struct {
	Kind  string
	Batch [][]byte
	From  int64
	Max   int
}

// BrokerPair is a fresh spec and a fresh subject of the broker suite.
type BrokerPair struct {
	spec    *speccheck.Broker
	subject BrokerSubject
}

func genBrokerOp(r *checker.Rand, _ int) BrokerOp {
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
		return BrokerOp{Kind: "append", Batch: batch}
	case 2, 3:
		// From is chosen around the edges: negative, inside, at the end, past it.
		return BrokerOp{Kind: "read", From: int64(r.Intn(14)) - 2, Max: r.Intn(6) - 1}
	}
	return BrokerOp{Kind: "hwm"}
}

func minimizeBrokerOp(operation BrokerOp) []BrokerOp {
	var out []BrokerOp
	switch operation.Kind {
	case "append":
		if len(operation.Batch) > 0 {
			out = append(out, BrokerOp{Kind: "append"})
		}
		if len(operation.Batch) > 1 {
			out = append(out, BrokerOp{Kind: "append", Batch: operation.Batch[:1]})
		}
	case "read":
		if operation.Max != 0 {
			out = append(out, BrokerOp{Kind: "read", From: operation.From})
		}
		if operation.From > 0 {
			out = append(out, BrokerOp{Kind: "read", From: 0, Max: operation.Max})
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

// BrokerComparator returns the comparator of the broker suite. Checks:
// the base offset of Append, whether Read is in range, every record offset and
// value, the high watermark, and the state hash when the subject has one. The
// suite writes over the batch after Append and over the records after Read, so
// a subject that shares memory with the caller fails.
func BrokerComparator(newSubject func() BrokerSubject) checker.SpecChecker[*BrokerPair, BrokerOp] {
	return checker.SpecChecker[*BrokerPair, BrokerOp]{
		Name:          "suites.BrokerOp",
		New:           func() *BrokerPair { return &BrokerPair{spec: &speccheck.Broker{}, subject: newSubject()} },
		Gen:           genBrokerOp,
		Step:          brokerStep,
		Invariant:     brokerInvariant,
		Shrink:        minimizeBrokerOp,
		Format:        func(operation BrokerOp) string { return fmt.Sprintf("%#v", operation) },
		Ops:           suiteOps,
		MaxShrinkRuns: suiteMinimizeRuns,
	}
}

// Broker runs the broker suite on the seeds that opts chooses.
func Broker(t checker.Reporter, newSubject func() BrokerSubject, opts checker.Options) {
	t.Helper()
	checker.Run(t, BrokerComparator(newSubject), opts)
}

func overwriteWithJunk(value []byte) {
	for i := range value {
		value[i] = '!'
	}
}

func brokerStep(p *BrokerPair, operation BrokerOp) error {
	switch operation.Kind {
	case "append":
		specBatch, subjectBatch := cloneBatch(operation.Batch), cloneBatch(operation.Batch)
		specBase := p.spec.Append(specBatch)
		subjectBase := p.subject.Append(subjectBatch)
		for _, v := range subjectBatch {
			overwriteWithJunk(v)
		}
		if specBase != subjectBase {
			return fmt.Errorf("append of %d records returned the base offset %d, but the spec returned %d",
				len(operation.Batch), subjectBase, specBase)
		}
	case "hwm":
		if s, a := p.spec.HighWatermark(), p.subject.HighWatermark(); s != a {
			return fmt.Errorf("the high watermark is %d, but the spec says %d", a, s)
		}
	default:
		return brokerRead(p, operation)
	}
	return nil
}

func brokerRead(p *BrokerPair, operation BrokerOp) error {
	from := core.Offset(operation.From)
	specRecords, specOK := p.spec.Read(from, operation.Max)
	records, ok := p.subject.Read(from, operation.Max)
	defer func() {
		for _, r := range records {
			overwriteWithJunk(r.Value)
		}
	}()
	where := fmt.Sprintf("Read(from %d, max %d)", operation.From, operation.Max)
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

func brokerInvariant(p *BrokerPair) error {
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
