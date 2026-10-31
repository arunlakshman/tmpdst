package speccheck

import (
	"bytes"
	"fmt"
	"testing"

	"mds/internal/core"
	"mds/internal/verify/speccheck/checker"
)

// This file checks Partition against a naive model: a plain slice of values.
// The model has no high watermark field. The high watermark is the length.

type partitionModelOp struct {
	Kind  string // append, read, hwm
	Batch [][]byte
	From  int64
	Max   int
}

type partitionPair struct {
	spec  *Partition
	twin  *Partition // receives the same appends, so the state hashes must match
	model [][]byte
}

func genPartitionModelOp(r *checker.Rand, _ int) partitionModelOp {
	switch r.Intn(5) {
	case 0, 1:
		var batch [][]byte
		for n := r.Intn(4); n > 0; n-- { // a batch can be empty
			v := make([]byte, r.Intn(4))
			for i := range v {
				v[i] = byte('a' + r.Intn(26))
			}
			batch = append(batch, v)
		}
		return partitionModelOp{Kind: "append", Batch: batch}
	case 2, 3:
		// From is chosen around the edges: negative, inside, at the end and past it.
		return partitionModelOp{Kind: "read", From: int64(r.Intn(14)) - 2, Max: r.Intn(6) - 1}
	}
	return partitionModelOp{Kind: "hwm"}
}

func stepPartitionModel(s *partitionPair, op partitionModelOp) error {
	switch op.Kind {
	case "append":
		wantBase := core.Offset(len(s.model))
		input := make([][]byte, len(op.Batch))
		for i, v := range op.Batch {
			input[i] = bytes.Clone(v)
			s.model = append(s.model, bytes.Clone(v))
		}
		if got := s.spec.Append(input); got != wantBase {
			return fmt.Errorf("Append returned base offset %d, the model expected %d", got, wantBase)
		}
		s.twin.Append(op.Batch)
		// The caller changes its own bytes after Append. The spec must not see it.
		for _, v := range input {
			for i := range v {
				v[i] = '!'
			}
		}
	case "hwm":
		if got, want := s.spec.HighWatermark(), core.Offset(len(s.model)); got != want {
			return fmt.Errorf("HighWatermark is %d, the model expected %d", got, want)
		}
	default:
		return checkPartitionRead(s, op)
	}
	return nil
}

func checkPartitionRead(s *partitionPair, op partitionModelOp) error {
	recs, ok := s.spec.Read(core.Offset(op.From), op.Max)
	wantOK := op.From >= 0 && op.From <= int64(len(s.model))
	if ok != wantOK {
		return fmt.Errorf("Read(from %d, max %d): ok is %v, the model expected %v (high watermark %d)", op.From, op.Max, ok, wantOK, len(s.model))
	}
	if !ok {
		if recs != nil {
			return fmt.Errorf("Read(from %d, max %d): ok is false but %d records came back", op.From, op.Max, len(recs))
		}
		return nil
	}
	var want [][]byte
	for i := op.From; i < int64(len(s.model)); i++ {
		if op.Max > 0 && len(want) == op.Max {
			break
		}
		want = append(want, s.model[i])
	}
	if len(recs) != len(want) {
		return fmt.Errorf("Read(from %d, max %d): got %d records, the model expected %d", op.From, op.Max, len(recs), len(want))
	}
	for i, r := range recs {
		if r.Offset != core.Offset(op.From)+core.Offset(i) || !bytes.Equal(r.Value, want[i]) {
			return fmt.Errorf("Read(from %d, max %d): record %d is offset %d value %q, the model expected offset %d value %q",
				op.From, op.Max, i, r.Offset, r.Value, op.From+int64(i), want[i])
		}
		// The caller changes the returned bytes. The log must not change.
		for j := range r.Value {
			r.Value[j] = '?'
		}
	}
	return nil
}

func checkPartitionState(s *partitionPair) error {
	if g, w := s.spec.HighWatermark(), core.Offset(len(s.model)); g != w {
		return fmt.Errorf("HighWatermark is %d, the model expected %d", g, w)
	}
	if g, w := s.spec.StateHash(), s.twin.StateHash(); g != w {
		return fmt.Errorf("two partitions with the same appends have different state hashes: %d and %d", g, w)
	}
	return nil
}

func TestPartitionMatchesNaiveModel(t *testing.T) {
	c := checker.SpecChecker[*partitionPair, partitionModelOp]{
		Name:      "partitionModelOp",
		New:       func() *partitionPair { return &partitionPair{spec: &Partition{}, twin: &Partition{}} },
		Gen:       genPartitionModelOp,
		Step:      stepPartitionModel,
		Invariant: checkPartitionState,
		Format:    func(o partitionModelOp) string { return fmt.Sprintf("%#v", o) },
		Ops:       80,
	}
	checker.Run(t, c, checker.Options{
		Seeds: 300,
		Seed:  -1,
		Replay: func(seed uint64) string {
			return fmt.Sprintf("go test ./internal/verify/speccheck -run %s (seed %d)", t.Name(), seed)
		},
	})
}
