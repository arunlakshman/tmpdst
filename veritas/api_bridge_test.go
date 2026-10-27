package veritas

import (
	"testing"

	cpv1 "manifester/controlplane/api/v1"
	"manifester/core"
)

func TestExecRequestAPITypes(t *testing.T) {
	c := NewControlPlane()
	d := c.ExecRequest(1, &cpv1.CreateTopicRequest{Topic: "t", Partitions: 2})
	if d.Err != core.OK {
		t.Fatalf("create: %+v", d)
	}
	d = c.ExecRequest(2, &cpv1.CommitSegmentRequest{
		SegmentKey: "s",
		Batches: []*cpv1.PendingBatch{{
			Tp: &cpv1.TopicPartition{Topic: "t", Partition: 0}, Records: 3,
			Loc: &cpv1.SegmentRef{Key: "s", Off: 0, Len: 9},
		}},
	})
	if d.Err != core.OK {
		t.Fatalf("commit: %+v", d)
	}
	d = c.ExecRequest(3, &cpv1.LookupRequest{
		Tp: &cpv1.TopicPartition{Topic: "t", Partition: 0}, MaxBytes: 1 << 20,
	})
	if d.Err != core.OK {
		t.Fatalf("lookup: %+v", d)
	}
	d = c.ExecRequest(4, &cpv1.DescribeTopicsRequest{})
	if d.Err != core.OK {
		t.Fatalf("describe: %+v", d)
	}
	d = c.ExecRequest(5, "nope")
	if d.Err != core.ErrInvalid {
		t.Fatalf("bad msg: %+v", d)
	}
}
