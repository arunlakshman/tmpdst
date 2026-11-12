package spec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/manifesterio/mds/broker/internal/verify/speccheck/checker"
	"github.com/manifesterio/mds/controlplane/contract"
)

// This file checks Broker against a naive model: a plain slice of values.
// The model has no high watermark field. The high watermark is the length.

type brokerPair struct {
	spec  *Broker
	twin  *Broker // receives the same appends, so the state hashes must match
	model [][]byte
}

func stepBrokerModel(s *brokerPair, operation SuiteOp) error {
	switch operation.Kind {
	case "append":
		wantBase := contract.Offset(len(s.model))
		input := make([][]byte, len(operation.Batch))
		for i, v := range operation.Batch {
			input[i] = bytes.Clone(v)
			s.model = append(s.model, bytes.Clone(v))
		}
		if got := s.spec.Append(input); got != wantBase {
			return fmt.Errorf("Append returned base offset %d, the model expected %d", got, wantBase)
		}
		s.twin.Append(operation.Batch)
		// The caller changes its own bytes after Append. The spec must not see it.
		for _, v := range input {
			for i := range v {
				v[i] = '!'
			}
		}
	case "hwm":
		if got, want := s.spec.HighWatermark(), contract.Offset(len(s.model)); got != want {
			return fmt.Errorf("HighWatermark is %d, the model expected %d", got, want)
		}
	default:
		return checkBrokerRead(s, operation)
	}
	return nil
}

func checkBrokerRead(s *brokerPair, operation SuiteOp) error {
	recs, ok := s.spec.Read(contract.Offset(operation.From), operation.Max)
	wantOK := operation.From >= 0 && operation.From <= int64(len(s.model))
	if ok != wantOK {
		return fmt.Errorf("Read(from %d, max %d): ok is %v, the model expected %v (high watermark %d)", operation.From, operation.Max, ok, wantOK, len(s.model))
	}
	if !ok {
		if recs != nil {
			return fmt.Errorf("Read(from %d, max %d): ok is false but %d records came back", operation.From, operation.Max, len(recs))
		}
		return nil
	}
	var want [][]byte
	for i := operation.From; i < int64(len(s.model)); i++ {
		if operation.Max > 0 && len(want) == operation.Max {
			break
		}
		want = append(want, s.model[i])
	}
	if len(recs) != len(want) {
		return fmt.Errorf("Read(from %d, max %d): got %d records, the model expected %d", operation.From, operation.Max, len(recs), len(want))
	}
	for i, r := range recs {
		if r.Offset != contract.Offset(operation.From)+contract.Offset(i) || !bytes.Equal(r.Value, want[i]) {
			return fmt.Errorf("Read(from %d, max %d): record %d is offset %d value %q, the model expected offset %d value %q",
				operation.From, operation.Max, i, r.Offset, r.Value, operation.From+int64(i), want[i])
		}
		// The caller changes the returned bytes. The log must not change.
		for j := range r.Value {
			r.Value[j] = '?'
		}
	}
	return nil
}

func checkBrokerState(s *brokerPair) error {
	if g, w := s.spec.HighWatermark(), contract.Offset(len(s.model)); g != w {
		return fmt.Errorf("HighWatermark is %d, the model expected %d", g, w)
	}
	if g, w := s.spec.StateHash(), s.twin.StateHash(); g != w {
		return fmt.Errorf("two partitions with the same appends have different state hashes: %d and %d", g, w)
	}
	return nil
}

func TestBrokerMatchesNaiveModel(t *testing.T) {
	c := checker.SpecChecker[*brokerPair, SuiteOp]{
		Name:      "SuiteOp",
		New:       func() *brokerPair { return &brokerPair{spec: &Broker{}, twin: &Broker{}} },
		Gen:       genSuiteOp,
		Step:      stepBrokerModel,
		Invariant: checkBrokerState,
		Format:    func(o SuiteOp) string { return fmt.Sprintf("%#v", o) },
		Ops:       80,
	}
	checker.Run(t, c, checker.Options{
		Seeds: 300,
		Seed:  -1,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./spec -run %s (seed %d)", t.Name(), seed)
		},
	})
}
