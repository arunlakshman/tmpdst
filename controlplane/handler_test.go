package controlplane_test

import (
	"testing"

	"manifester/controlplane/grpctest"
	"manifester/core"
)

func TestGRPCMemRoundTrip(t *testing.T) {
	n := grpctest.StartMem(t)
	ctx := t.Context()
	if err := n.Client.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}
	tp := core.TP("t", 1)
	cs, err := n.Client.Call(ctx, core.CommitSegment{
		SegmentKey: "s",
		Batches:    []core.PendingBatch{{TP: tp, Records: 3, Loc: core.SegmentRef{Key: "s", Len: 9}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if as := cs.(core.CommitSegmentResp).Assigned; len(as) != 1 || as[0].Count != 3 {
		t.Fatalf("%+v", as)
	}
	lk, err := n.Client.Call(ctx, core.Lookup{TP: tp, MaxBytes: 100})
	if err != nil {
		t.Fatal(err)
	}
	if lr := lk.(core.LookupResp); lr.End != 3 {
		t.Fatalf("%+v", lr)
	}
}
