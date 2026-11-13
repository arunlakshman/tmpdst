package specbridge

import (
	"testing"

	"github.com/manifesterio/mds/broker/internal/core"
	"github.com/manifesterio/mds/controlplane/contract"
)

// TestExecCarriesTheSpecAnswer checks that Exec adds only the request id to
// the answer of the spec.
func TestExecCarriesTheSpecAnswer(t *testing.T) {
	c := New()
	if code := c.CreateTopic("t", 2); code != core.OK {
		t.Fatalf("create topic: %v", code)
	}
	commit := contract.CommitSegment{SegmentKey: "s", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 1), Records: 3},
	}}
	got := c.Exec(core.ControlPlaneCall{Req: 7, Op: commit}).(core.ControlPlaneDone)
	if got.Req != 7 || got.Err != core.OK || got.Outcome != core.Applied {
		t.Fatalf("commit: %+v", got)
	}
	if assigned := got.Resp.(contract.CommitSegmentResp).Assigned; len(assigned) != 1 || assigned[0].Base != 0 || assigned[0].Count != 3 {
		t.Fatalf("assigned %+v", assigned)
	}
	if end := c.End(contract.NewTopicPartition("t", 1)); end != 3 {
		t.Fatalf("end %d, want 3", end)
	}
	bad := c.Exec(core.ControlPlaneCall{Req: 8, Op: contract.FindSegments{From: -1}}).(core.ControlPlaneDone)
	if bad.Req != 8 || bad.Err != core.ErrInvalid || bad.Outcome != core.NotApplied {
		t.Fatalf("bad lookup: %+v", bad)
	}
	if code := c.CreateTopic("", 1); code != core.ErrInvalid {
		t.Fatalf("empty topic name: %v", code)
	}
}

// notAControlPlaneCall is an IOCall that is not a ControlPlaneCall.
type notAControlPlaneCall struct{ core.IOCall }

func TestExecPanicsOnOtherCalls(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Exec did not panic on a non-control-plane call")
		}
	}()
	New().Exec(notAControlPlaneCall{})
}
