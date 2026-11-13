package conformance

import (
	"bytes"
	"fmt"

	"pgregory.net/rapid"

	brokerspec "github.com/manifesterio/mds/broker/spec"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/spectest"
)

// BrokerOp is one operation of the broker suite. The suite runs the same
// random appends and reads on a subject (the code under test) and on a fresh
// brokerspec.Broker, and compares every answer, the high watermark and the
// state hash. Spec against spec is the test that the suite itself is right.
// Kind is "append", "read" or "hwm" (high watermark). Batch is used by append.
// From and Max are used by read.
type BrokerOp struct {
	Kind  string
	Batch [][]byte
	From  int64
	Max   int
}

// BrokerResult is the answer to one BrokerOp. Only the fields of its Kind are
// used.
type BrokerResult struct {
	Kind    string
	Base    contract.Offset // append: the offset of the first record
	HWM     contract.Offset // hwm: the high watermark
	OK      bool            // read: false when from is out of range
	Records []brokerspec.StoredRecord
}

// BrokerSubject is a log under test: the log of one Kafka partition. The
// methods have the same rules as brokerspec.Broker. A subject that can give the
// same state hash as the spec also has the method StateHash() uint64.
type BrokerSubject interface {
	Append(batch [][]byte) contract.Offset
	Read(from contract.Offset, max int) ([]brokerspec.StoredRecord, bool)
	HighWatermark() contract.Offset
}

// specBrokerSubject is the broker spec as a subject.
type specBrokerSubject struct{ b *brokerspec.Broker }

// NewSpecBrokerSubject returns a fresh brokerspec.Broker as a subject. Use it
// to run the suite spec against spec, and as the correct subject that a
// planted bug wraps.
func NewSpecBrokerSubject() BrokerSubject { return &specBrokerSubject{b: &brokerspec.Broker{}} }

func (s *specBrokerSubject) Append(batch [][]byte) contract.Offset { return s.b.Append(batch) }

func (s *specBrokerSubject) Read(from contract.Offset, max int) ([]brokerspec.StoredRecord, bool) {
	return s.b.Read(from, max)
}

func (s *specBrokerSubject) HighWatermark() contract.Offset { return s.b.HighWatermark() }

func (s *specBrokerSubject) StateHash() uint64 { return s.b.StateHash() }

// BrokerOps returns a generator of broker operations. The kinds are listed
// simplest first, and have the weights 1 : 2 : 2 (hwm, read, append).
func BrokerOps() *rapid.Generator[BrokerOp] {
	return Custom(func(r Rand) BrokerOp {
		switch chooseByWeight(r, 1, 2, 2) {
		case 0:
			return BrokerOp{Kind: "hwm"}
		case 1:
			// From is chosen around the edges: negative, inside, at the end, past it.
			return BrokerOp{Kind: "read", From: int64(r.Intn(14)) - 2, Max: r.Intn(6) - 1}
		}
		var batch [][]byte
		for n := r.Intn(4); n > 0; n-- { // a batch can be empty
			value := make([]byte, r.Intn(4))
			for i := range value {
				value[i] = byte('a' + r.Intn(26))
			}
			batch = append(batch, value)
		}
		return BrokerOp{Kind: "append", Batch: batch}
	})
}

// brokerSpec is the broker spec as the spec of the lockstep run.
type brokerSpec struct{ b *brokerspec.Broker }

func (s brokerSpec) Apply(operation BrokerOp) BrokerResult {
	switch operation.Kind {
	case "append":
		return BrokerResult{Kind: "append", Base: s.b.Append(cloneBatch(operation.Batch))}
	case "hwm":
		return BrokerResult{Kind: "hwm", HWM: s.b.HighWatermark()}
	}
	records, ok := s.b.Read(contract.Offset(operation.From), operation.Max)
	return BrokerResult{Kind: "read", OK: ok, Records: records}
}

func (s brokerSpec) StateHash() uint64 { return s.b.StateHash() }

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

func overwriteWithJunk(value []byte) {
	for i := range value {
		value[i] = '!'
	}
}

// BrokerRun is the subject of one run of the broker suite. It holds the log
// under test and a mirror of the spec (an invariant gets only the subject, but
// it checks the high watermark against the spec).
type BrokerRun struct {
	log    BrokerSubject
	mirror *brokerspec.Broker
}

// Apply runs the operation on the log under test. The suite writes over the
// batch after Append and over the records after Read, so a subject that shares
// memory with the caller fails later.
func (r *BrokerRun) Apply(operation BrokerOp) (BrokerResult, error) {
	switch operation.Kind {
	case "append":
		r.mirror.Append(cloneBatch(operation.Batch))
		batch := cloneBatch(operation.Batch)
		result := BrokerResult{Kind: "append", Base: r.log.Append(batch)}
		for _, v := range batch {
			overwriteWithJunk(v)
		}
		return result, nil
	case "hwm":
		return BrokerResult{Kind: "hwm", HWM: r.log.HighWatermark()}, nil
	}
	records, ok := r.log.Read(contract.Offset(operation.From), operation.Max)
	result := BrokerResult{Kind: "read", OK: ok}
	for _, rec := range records {
		result.Records = append(result.Records, brokerspec.StoredRecord{Offset: rec.Offset, Value: bytes.Clone(rec.Value)})
		overwriteWithJunk(rec.Value)
	}
	return result, nil
}

// StateHash is the hash of the log under test. A log with no spec hash cannot
// show one, so the run uses the hash of the mirror: there is nothing to
// compare.
func (r *BrokerRun) StateHash() uint64 {
	if h, ok := r.log.(hasStateHash); ok {
		return h.StateHash()
	}
	return r.mirror.StateHash()
}

func (r *BrokerRun) invariant() error {
	if s, a := r.mirror.HighWatermark(), r.log.HighWatermark(); s != a {
		return fmt.Errorf("the high watermark is %d, but the spec says %d", a, s)
	}
	return nil
}

// equalBroker compares the answer of the spec with the answer of the subject.
func equalBroker(want, got BrokerResult) error {
	switch want.Kind {
	case "append":
		if want.Base != got.Base {
			return fmt.Errorf("append returned the base offset %d, but the spec returned %d", got.Base, want.Base)
		}
	case "hwm":
		if want.HWM != got.HWM {
			return fmt.Errorf("the high watermark is %d, but the spec says %d", got.HWM, want.HWM)
		}
	default:
		return equalBrokerRead(want, got)
	}
	return nil
}

func equalBrokerRead(want, got BrokerResult) error {
	if got.OK != want.OK {
		return fmt.Errorf("read returned ok = %v, but the spec returned ok = %v", got.OK, want.OK)
	}
	if len(got.Records) != len(want.Records) {
		return fmt.Errorf("read returned %d records, but the spec returned %d", len(got.Records), len(want.Records))
	}
	for i, r := range got.Records {
		s := want.Records[i]
		if r.Offset != s.Offset || !bytes.Equal(r.Value, s.Value) {
			return fmt.Errorf("read: record %d has offset %d and value %q, but the spec has offset %d and value %q",
				i, r.Offset, r.Value, s.Offset, s.Value)
		}
	}
	return nil
}

// BrokerLockstep returns the lockstep check of the broker suite. Checks: the
// base offset of Append, whether Read is in range, every record offset and
// value, the high watermark, and the state hash when the subject has one.
// Each call of newSubject must return an empty subject.
func BrokerLockstep(newSubject func() BrokerSubject) spectest.Lockstep[BrokerOp, BrokerResult, *BrokerRun] {
	return spectest.Lockstep[BrokerOp, BrokerResult, *BrokerRun]{
		Name: "conformance.BrokerOp",
		NewSpec: func() spectest.Spec[BrokerOp, BrokerResult] {
			return brokerSpec{b: &brokerspec.Broker{}}
		},
		NewSubject: func(*rapid.T) *BrokerRun {
			return &BrokerRun{log: newSubject(), mirror: &brokerspec.Broker{}}
		},
		Ops:           BrokerOps(),
		Equal:         equalBroker,
		Invariant:     (*BrokerRun).invariant,
		Format:        func(operation BrokerOp) string { return fmt.Sprintf("%#v", operation) },
		Rerun:         spectest.Rerun{EnvPrefix: EnvPrefix},
		StepsPerCheck: suiteSteps,
	}
}
