package spec_test

import (
	"bytes"
	"fmt"
	"testing"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/conformance"
	"github.com/manifesterio/mds/broker/spec"
	"github.com/manifesterio/mds/controlplane/contract"
	"github.com/manifesterio/mds/dst/check/spectest"
)

// This file checks spec.Broker against a naive model: a plain slice of values.
// The model has no high watermark field. The high watermark is the length. The
// check is a spectest.Lockstep in which the naive model is the spec, and
// spec.Broker is the subject.

// naiveBroker is the naive model. It is the spec of the lockstep run.
type naiveBroker struct{ model [][]byte }

// Apply gives the answer that the rules require, from the plain slice.
func (n *naiveBroker) Apply(operation conformance.BrokerOp) conformance.BrokerResult {
	switch operation.Kind {
	case "append":
		result := conformance.BrokerResult{Kind: "append", Base: contract.Offset(len(n.model))}
		for _, v := range operation.Batch {
			n.model = append(n.model, bytes.Clone(v))
		}
		return result
	case "hwm":
		return conformance.BrokerResult{Kind: "hwm", HWM: contract.Offset(len(n.model))}
	}
	result := conformance.BrokerResult{Kind: "read", OK: operation.From >= 0 && operation.From <= int64(len(n.model))}
	if !result.OK {
		return result
	}
	for i := operation.From; i < int64(len(n.model)); i++ {
		if operation.Max > 0 && len(result.Records) == operation.Max {
			break
		}
		result.Records = append(result.Records, spec.StoredRecord{Offset: contract.Offset(i), Value: n.model[i]})
	}
	return result
}

// StateHash is not used: the model and the spec keep their state in different
// shapes, so the lockstep compares no hash.
func (n *naiveBroker) StateHash() uint64 { return 0 }

// brokerUnderTest runs spec.Broker as the subject. It also keeps a twin that
// gets the same appends, so the state hashes of the two must match.
type brokerUnderTest struct {
	spec     *spec.Broker
	twin     *spec.Broker
	appended int // records appended so far: the high watermark must be this
}

// Apply runs the operation on the spec. The caller changes its own bytes after
// Append and after Read. The spec must not see it.
func (b *brokerUnderTest) Apply(operation conformance.BrokerOp) (conformance.BrokerResult, error) {
	switch operation.Kind {
	case "append":
		input := make([][]byte, len(operation.Batch))
		for i, v := range operation.Batch {
			input[i] = bytes.Clone(v)
		}
		result := conformance.BrokerResult{Kind: "append", Base: b.spec.Append(input)}
		b.twin.Append(operation.Batch)
		b.appended += len(operation.Batch)
		for _, v := range input {
			for i := range v {
				v[i] = '!'
			}
		}
		return result, nil
	case "hwm":
		return conformance.BrokerResult{Kind: "hwm", HWM: b.spec.HighWatermark()}, nil
	}
	records, ok := b.spec.Read(contract.Offset(operation.From), operation.Max)
	result := conformance.BrokerResult{Kind: "read", OK: ok}
	if !ok && records != nil {
		return result, fmt.Errorf("Read(from %d, max %d): ok is false but %d records came back", operation.From, operation.Max, len(records))
	}
	for _, r := range records {
		result.Records = append(result.Records, spec.StoredRecord{Offset: r.Offset, Value: bytes.Clone(r.Value)})
		for j := range r.Value {
			r.Value[j] = '?'
		}
	}
	return result, nil
}

func (b *brokerUnderTest) invariant() error {
	if g, w := b.spec.HighWatermark(), contract.Offset(b.appended); g != w {
		return fmt.Errorf("HighWatermark is %d, the model expected %d", g, w)
	}
	if g, w := b.spec.StateHash(), b.twin.StateHash(); g != w {
		return fmt.Errorf("two partitions with the same appends have different state hashes: %d and %d", g, w)
	}
	return nil
}

func TestBrokerMatchesNaiveModel(t *testing.T) {
	l := spectest.Lockstep[conformance.BrokerOp, conformance.BrokerResult, *brokerUnderTest]{
		Name:          "conformance.BrokerOp",
		NewSpec:       func() spectest.Spec[conformance.BrokerOp, conformance.BrokerResult] { return &naiveBroker{} },
		NewSubject:    func(*rapid.T) *brokerUnderTest { return &brokerUnderTest{spec: &spec.Broker{}, twin: &spec.Broker{}} },
		Ops:           conformance.BrokerOps(),
		Equal:         equalNaive,
		Invariant:     (*brokerUnderTest).invariant,
		Format:        func(operation conformance.BrokerOp) string { return fmt.Sprintf("%#v", operation) },
		Rerun:         spectest.Rerun{EnvPrefix: conformance.EnvPrefix},
		Checks:        300,
		StepsPerCheck: 80,
	}
	l.Check(t)
}

// equalNaive compares the answer of the naive model with the answer of the
// spec.
func equalNaive(want, got conformance.BrokerResult) error {
	if want.Kind != got.Kind || want.Base != got.Base || want.HWM != got.HWM || want.OK != got.OK {
		return fmt.Errorf("the spec answered %+v, but the naive model expected %+v", got, want)
	}
	if len(got.Records) != len(want.Records) {
		return fmt.Errorf("the spec returned %d records, but the naive model expected %d", len(got.Records), len(want.Records))
	}
	for i, r := range got.Records {
		w := want.Records[i]
		if r.Offset != w.Offset || !bytes.Equal(r.Value, w.Value) {
			return fmt.Errorf("record %d is offset %d value %q, but the naive model expected offset %d value %q",
				i, r.Offset, r.Value, w.Offset, w.Value)
		}
	}
	return nil
}
