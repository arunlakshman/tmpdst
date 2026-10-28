package logstore_test

import (
	"testing"

	"manifester/internal/controlplane/applier"
	"manifester/internal/core"
	"manifester/internal/verify/sim"
	"manifester/internal/verify/sim/engines/logstore"
)

func TestCPTruthCreateTopic(t *testing.T) {
	e := logstore.New(base())
	if e.CreateTopic("t0", 2) != core.OK {
		t.Fatal("CreateTopic")
	}
	if e.HighWatermark(core.NewTopicPartition("t0", 0)) != 0 {
		t.Fatal(e.HighWatermark(core.NewTopicPartition("t0", 0)))
	}
	if e.Gen() == 0 {
		t.Fatal("gen")
	}
	if e.Committed(core.NewTopicPartition("t0", 0)) != nil {
		t.Fatal("expected empty committed")
	}
}

func TestCPTruthSkipsForeignPayload(t *testing.T) {
	e, c, a := setup(base(), 1)
	e.Execute(c, a, core.LogAppend{Req: 1, Slot: 0, Data: []byte("foreign")})
	c.Run()
	if _, ok := e.Entry(0); !ok {
		t.Fatal("missing")
	}
}

func TestCPTruthReplaysNoopAndTrimStride(t *testing.T) {
	f := base()
	f.WindowSlots, f.TrimStride = 4, 2
	e, c, a := setup(f, 3)
	_ = e.CreateTopic("t", 1)
	for slot := range 6 {
		inner := applier.Encode(applier.Entry{ID: "n", Kind: applier.KindNoop})
		e.Execute(c, a, core.LogAppend{Req: core.RequestID(slot + 1), Slot: core.LogPosition(slot), Data: inner})
		c.Run()
		c.Delivered = nil
	}
	if e.Gen() < 6 {
		t.Fatal(e.Gen())
	}
	_ = sim.LogStoreFaults{}
}
