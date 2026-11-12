package spec

import (
	"bytes"
	"fmt"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker"
	"github.com/manifesterio/mds/controlplane/contract"
)

// SuiteOp is one operation of the broker suite. The suite runs the same random
// appends and reads on a subject (the code under test) and on a fresh Broker,
// and compares every answer, the high watermark and the state hash. Spec
// against spec is the test that the suite itself is right (suite_test.go). Kind is "append",
// "read" or "hwm" (high watermark). Batch is used by append. From and Max are
// used by read.
type SuiteOp struct {
	Kind  string
	Batch [][]byte
	From  int64
	Max   int
}

// SuitePair is a fresh spec and a fresh subject of the broker suite.
type SuitePair struct {
	spec    *Broker
	subject Subject
}

const (
	// suiteOps is the number of operations in each generated sequence.
	suiteOps = 120
	// suiteMinimizeRuns is the most sequences that shrinking may run.
	suiteMinimizeRuns = 300
)

// Subject is a log under test: the log of one Kafka partition. The methods have
// the same rules as Broker. StateHash returns ok = false when the subject has
// no spec hash.
type Subject interface {
	Append(batch [][]byte) contract.Offset
	Read(from contract.Offset, max int) ([]StoredRecord, bool)
	HighWatermark() contract.Offset
	StateHash() (hash uint64, ok bool)
}

type specSubject struct{ p *Broker }

// NewSpecSubject returns a fresh Broker as a subject. Use it to run the suite
// spec against spec, and as the correct subject that an injected bug wraps.
func NewSpecSubject() Subject { return &specSubject{p: &Broker{}} }

func (s *specSubject) Append(batch [][]byte) contract.Offset { return s.p.Append(batch) }

func (s *specSubject) Read(from contract.Offset, max int) ([]StoredRecord, bool) {
	return s.p.Read(from, max)
}

func (s *specSubject) HighWatermark() contract.Offset { return s.p.HighWatermark() }

func (s *specSubject) StateHash() (uint64, bool) { return s.p.StateHash(), true }

func genSuiteOp(r *checker.Rand, _ int) SuiteOp {
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
		return SuiteOp{Kind: "append", Batch: batch}
	case 2, 3:
		// From is chosen around the edges: negative, inside, at the end, past it.
		return SuiteOp{Kind: "read", From: int64(r.Intn(14)) - 2, Max: r.Intn(6) - 1}
	}
	return SuiteOp{Kind: "hwm"}
}

func minimizeSuiteOp(operation SuiteOp) []SuiteOp {
	var out []SuiteOp
	switch operation.Kind {
	case "append":
		if len(operation.Batch) > 0 {
			out = append(out, SuiteOp{Kind: "append"})
		}
		if len(operation.Batch) > 1 {
			out = append(out, SuiteOp{Kind: "append", Batch: operation.Batch[:1]})
		}
	case "read":
		if operation.Max != 0 {
			out = append(out, SuiteOp{Kind: "read", From: operation.From})
		}
		if operation.From > 0 {
			out = append(out, SuiteOp{Kind: "read", From: 0, Max: operation.Max})
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

// SuiteComparator returns the comparator of the broker suite. Checks:
// the base offset of Append, whether Read is in range, every record offset and
// value, the high watermark, and the state hash when the subject has one. The
// suite writes over the batch after Append and over the records after Read, so
// a subject that shares memory with the caller fails.
func SuiteComparator(newSubject func() Subject) checker.SpecChecker[*SuitePair, SuiteOp] {
	return checker.SpecChecker[*SuitePair, SuiteOp]{
		Name:          "suites.SuiteOp",
		New:           func() *SuitePair { return &SuitePair{spec: &Broker{}, subject: newSubject()} },
		Gen:           genSuiteOp,
		Step:          suiteStep,
		Invariant:     suiteInvariant,
		Shrink:        minimizeSuiteOp,
		Format:        func(operation SuiteOp) string { return fmt.Sprintf("%#v", operation) },
		Ops:           suiteOps,
		MaxShrinkRuns: suiteMinimizeRuns,
	}
}

// RunSuite runs the broker suite on the seeds that opts chooses.
func RunSuite(t checker.Reporter, newSubject func() Subject, opts checker.Options) {
	t.Helper()
	checker.Run(t, SuiteComparator(newSubject), opts)
}

func overwriteWithJunk(value []byte) {
	for i := range value {
		value[i] = '!'
	}
}

func suiteStep(p *SuitePair, operation SuiteOp) error {
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
		return suiteRead(p, operation)
	}
	return nil
}

func suiteRead(p *SuitePair, operation SuiteOp) error {
	from := contract.Offset(operation.From)
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

func suiteInvariant(p *SuitePair) error {
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
