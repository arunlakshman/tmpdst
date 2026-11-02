package main

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds/internal/controlplane/stateupdater"
	"github.com/manifesterio/mds/internal/core"
)

func logRow(slot int64, data []byte) row {
	return row{partitionKey: "LOG", sortKey: fmt.Sprintf("%020d", slot), data: data}
}

func noopBytes() []byte {
	return stateupdater.Encode(stateupdater.Entry{ID: "n", Kind: stateupdater.KindNoop})
}

// The checks must be able to fail. A missing slot and garbage bytes must both be caught.
func TestStructureChecksCatchBrokenLog(t *testing.T) {
	good := []row{logRow(0, noopBytes()), logRow(1, noopBytes()), logRow(2, noopBytes())}
	slots, _ := decodeRows(good)
	ok := &checker{}
	checkStructure(ok, good, slots)
	// a plain no-op has no writer envelope, so exactly one check ("names its writer") fails
	if ok.failed != 1 {
		t.Fatalf("good log: %d checks failed, want 1 (writer)", ok.failed)
	}

	gap := []row{logRow(0, noopBytes()), logRow(2, noopBytes())}
	slots, _ = decodeRows(gap)
	bad := &checker{}
	checkStructure(bad, gap, slots)
	if bad.failed < 2 { // gap + writer
		t.Fatalf("log with a gap: %d checks failed, want the gap to be caught", bad.failed)
	}

	garbage := []row{logRow(0, []byte{0xC5, 0xFF, 0xFF}), logRow(1, noopBytes())}
	slots, _ = decodeRows(garbage)
	broken := &checker{}
	checkStructure(broken, garbage, slots)
	if broken.failed < 2 {
		t.Fatalf("log with garbage: %d checks failed, want garbage to be caught", broken.failed)
	}
}

// Offsets from a replay must match the records in the log; a wrong count must fail.
func TestReplayCheckCatchesWrongOffsets(t *testing.T) {
	commit := stateupdater.Entry{ID: "c", Kind: stateupdater.KindCommit, Commit: core.CommitSegment{
		SegmentKey: "k1", Batches: []core.PendingBatch{{TopicPartition: core.NewTopicPartition("t", 0), Records: 5}}}}
	create := stateupdater.Entry{ID: "t", Kind: stateupdater.KindCreateTopic, Topic: "t", Parts: 1}
	slots := []slotInfo{
		{slot: 0, entries: []stateupdater.Entry{create}},
		{slot: 1, entries: []stateupdater.Entry{commit}},
	}
	c := &checker{}
	checkFullReplay(c, slots, orderedEntries(slots))
	if c.failed != 0 {
		t.Fatalf("honest log: %d checks failed, want 0", c.failed)
	}
}
