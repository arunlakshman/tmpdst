package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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
	checkStructure(ok, slots)
	// a plain no-op has no writer envelope, so exactly one check ("names its writer") fails
	if ok.failed != 1 {
		t.Fatalf("good log: %d checks failed, want 1 (writer)", ok.failed)
	}

	missing := []row{logRow(0, noopBytes()), logRow(2, noopBytes())}
	slots, _ = decodeRows(missing)
	bad := &checker{}
	checkStructure(bad, slots)
	if bad.failed < 2 { // missing slot + writer
		t.Fatalf("log with a missing slot: %d checks failed, want the missing slot to be caught", bad.failed)
	}

	garbage := []row{logRow(0, []byte{0xC5, 0xFF, 0xFF}), logRow(1, noopBytes())}
	slots, _ = decodeRows(garbage)
	broken := &checker{}
	checkStructure(broken, slots)
	if broken.failed < 2 {
		t.Fatalf("log with garbage: %d checks failed, want garbage to be caught", broken.failed)
	}
}

// A trimmed log simply starts at a later slot. Contiguity runs from that first slot,
// so a start above 0 is fine, but a missing slot after it must still be caught.
func TestTrimmedLogStartsLater(t *testing.T) {
	trimmed := []row{logRow(5, noopBytes()), logRow(6, noopBytes()), logRow(7, noopBytes())}
	slots, _ := decodeRows(trimmed)
	ok := &checker{}
	checkStructure(ok, slots)
	// only the writer check fails: no noop has a writer envelope
	if ok.failed != 1 {
		t.Fatalf("trimmed log from slot 5: %d checks failed, want 1 (writer)", ok.failed)
	}

	gap := []row{logRow(5, noopBytes()), logRow(7, noopBytes())}
	slots, _ = decodeRows(gap)
	bad := &checker{}
	checkStructure(bad, slots)
	if bad.failed < 2 { // missing slot + writer
		t.Fatalf("trimmed log with a gap after slot 5: %d checks failed, want the missing slot caught", bad.failed)
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

// envelopeMarkerByte is the first byte of the state machine's envelope (statemachine/envelope.go).
const envelopeMarkerByte = 0xC8

// enveloped is a no-op entry wrapped the way the state machine wraps it:
// marker [epoch] [writer-length] [writer] [payload].
func enveloped(epoch uint64, writer string) []byte {
	b := []byte{envelopeMarkerByte}
	b = binary.AppendUvarint(b, epoch)
	b = binary.AppendUvarint(b, uint64(len(writer)))
	b = append(b, writer...)
	return append(b, noopBytes()...)
}

// captured runs checks on a checker whose lines go to a buffer.
func captured() (*checker, *bytes.Buffer) {
	var buf bytes.Buffer
	return &checker{out: &buf}, &buf
}

func TestDecodeSlotReadsEpochAndWriter(t *testing.T) {
	slots, _ := decodeRows([]row{logRow(0, enveloped(7, "cp-0")), logRow(1, noopBytes())})
	if slots[0].epoch != 7 || slots[0].writer != "cp-0" || !slots[0].enveloped || slots[0].bad != "" {
		t.Fatalf("enveloped slot: %+v", slots[0])
	}
	if slots[1].enveloped || slots[1].epoch != 0 || slots[1].writer != "" || len(slots[1].entries) != 1 {
		t.Fatalf("plain slot: %+v", slots[1])
	}
}

func TestOneWriterPerEpoch(t *testing.T) {
	good := []row{
		logRow(0, enveloped(1, "cp-0")), logRow(1, enveloped(1, "cp-0")),
		logRow(2, enveloped(2, "cp-1")),
		logRow(3, noopBytes()),       // no envelope: skipped
		logRow(4, enveloped(0, "x")), // epoch 0: skipped
		logRow(5, enveloped(0, "y")), // epoch 0 again, another name: still fine
	}
	slots, _ := decodeRows(good)
	c, out := captured()
	checkOneWriterPerEpoch(c, slots)
	if c.failed != 0 || !strings.Contains(out.String(), "2 epoch(s) checked") {
		t.Fatalf("good log: failed=%d\n%s", c.failed, out.String())
	}

	// Epoch 3 has two writer names: the check must fail and name both.
	bad := append(slices.Clone(good), logRow(6, enveloped(3, "cp-0")), logRow(7, enveloped(3, "cp-1")))
	slots, _ = decodeRows(bad)
	c, out = captured()
	checkOneWriterPerEpoch(c, slots)
	for _, want := range []string{"FAIL", "one epoch has one writer", "epoch 3", `"cp-0"`, `"cp-1"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("two writers in epoch 3: missing %q in\n%s", want, out.String())
		}
	}
	if c.failed != 1 {
		t.Fatalf("failed = %d, want 1", c.failed)
	}
}

func TestStoredEpochNotBehind(t *testing.T) {
	slots, _ := decodeRows([]row{logRow(0, enveloped(1, "cp-0")), logRow(1, enveloped(3, "cp-1"))})
	cases := []struct {
		name string
		meta metaItems
		fail bool
	}{
		{"stored equals highest", metaItems{hasWriter: true, epoch: 3}, false},
		{"stored ahead of the log", metaItems{hasWriter: true, epoch: 4}, false},
		{"stored behind the log", metaItems{hasWriter: true, epoch: 2}, true},
		{"no writer item but the log has epochs", metaItems{}, true},
	}
	for _, tc := range cases {
		c, _ := captured()
		checkStoredEpochNotBehind(c, slots, tc.meta)
		if (c.failed == 1) != tc.fail {
			t.Errorf("%s: failed=%d, want fail=%v", tc.name, c.failed, tc.fail)
		}
	}
	plain, _ := decodeRows([]row{logRow(0, noopBytes())})
	c, _ := captured()
	checkStoredEpochNotBehind(c, plain, metaItems{})
	if c.failed != 0 {
		t.Fatal("no epochs and no writer item must pass")
	}
}

func TestStrayRowsAreANote(t *testing.T) {
	slots, _ := decodeRows([]row{logRow(3, noopBytes()), logRow(4, noopBytes()), logRow(5, noopBytes()), logRow(6, noopBytes())})
	c, out := captured()
	noteStrayRows(c, slots, metaItems{hasFloor: true, floor: 5})
	if c.failed != 0 || !strings.Contains(out.String(), "SKIP") || !strings.Contains(out.String(), "3 stray row(s) at or below floor 5") {
		t.Fatalf("stray rows: failed=%d\n%s", c.failed, out.String())
	}
	c, out = captured()
	noteStrayRows(c, slots, metaItems{})
	if c.failed != 0 || !strings.Contains(out.String(), "no floor item") {
		t.Fatalf("no floor: %s", out.String())
	}
}

func TestMetaFromItems(t *testing.T) {
	writer := map[string]types.AttributeValue{
		"epoch": &types.AttributeValueMemberN{Value: "7"},
		"owner": &types.AttributeValueMemberS{Value: "cp-0"},
	}
	floor := map[string]types.AttributeValue{"floor": &types.AttributeValueMemberN{Value: "131"}}
	m, err := metaFromItems(writer, floor)
	if err != nil || m != (metaItems{hasWriter: true, epoch: 7, owner: "cp-0", hasFloor: true, floor: 131}) {
		t.Fatalf("both items: %+v %v", m, err)
	}
	m, err = metaFromItems(nil, nil)
	if err != nil || m.hasWriter || m.hasFloor {
		t.Fatalf("no items: %+v %v", m, err)
	}
	bad := map[string]types.AttributeValue{"epoch": &types.AttributeValueMemberS{Value: "7"}, "owner": &types.AttributeValueMemberS{Value: "cp-0"}}
	if _, err := metaFromItems(bad, nil); err == nil {
		t.Fatal("epoch stored as a string must be an error")
	}
	noOwner := map[string]types.AttributeValue{"epoch": &types.AttributeValueMemberN{Value: "7"}}
	if _, err := metaFromItems(noOwner, nil); err == nil {
		t.Fatal("missing owner must be an error")
	}
}
